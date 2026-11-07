import express, { Request, Response } from 'express';
import dotenv from 'dotenv';
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';

import { taskQueue } from './queue/taskQueue';
import { query } from './db/db';

dotenv.config();

export const app = express();

app.use(express.json());

// Bull-Board UI Setup
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

createBullBoard({
  queues: [new BullMQAdapter(taskQueue)],
  serverAdapter: serverAdapter,
});

app.use('/admin/queues', serverAdapter.getRouter());

// Health Check
app.get('/health', (_req: Request, res: Response) => {
  res.status(200).json({ status: 'ok', service: 'taskflow-api' });
});

// Producer: Dispatch Job
app.post('/api/jobs', async (req: Request, res: Response) => {
  try {
    const { name, task_type, payload } = req.body;

    if (!name || !task_type) {
      return res.status(400).json({ error: 'name and task_type are required fields' });
    }

    const dbResult = await query(
      `INSERT INTO jobs (name, task_type, payload, status)
       VALUES ($1, $2, $3, 'ACTIVE')
       RETURNING *`,
      [name, task_type, JSON.stringify(payload || {})]
    );

    const savedJob = dbResult.rows[0];

    const queueJob = await taskQueue.add(name, {
      jobDbId: savedJob.id,
      taskType: task_type,
      payload: payload || {},
    });

    return res.status(202).json({
      message: 'Job submitted and queued successfully',
      jobId: savedJob.id,
      queueJobId: queueJob.id,
      status: 'QUEUED',
    });
  } catch (error) {
    console.error('Error dispatching job:', error);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Producer: Recurring Cron Job
app.post('/api/jobs/schedule', async (req: Request, res: Response) => {
  try {
    const { name, task_type, cron, payload } = req.body;

    if (!name || !task_type || !cron) {
      return res.status(400).json({ 
        error: 'name, task_type, and cron expression are required fields' 
      });
    }

    const dbResult = await query(
      `INSERT INTO jobs (name, task_type, payload, cron_schedule, status)
       VALUES ($1, $2, $3, $4, 'ACTIVE')
       RETURNING *`,
      [name, task_type, JSON.stringify(payload || {}), cron]
    );

    const savedJob = dbResult.rows[0];

    const queueJob = await taskQueue.add(
      name,
      {
        jobDbId: savedJob.id,
        taskType: task_type,
        payload: payload || {},
      },
      {
        repeat: {
          pattern: cron,
        },
      } as any
    );

    return res.status(201).json({
      message: 'Recurring cron job scheduled successfully',
      jobId: savedJob.id,
      cronSchedule: cron,
      status: 'SCHEDULED',
    });
  } catch (error) {
    console.error('Error scheduling cron job:', error);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Observability: Job Status & Audit History
app.get('/api/jobs/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const jobResult = await query(`SELECT * FROM jobs WHERE id = $1`, [id]);
    if (jobResult.rows.length === 0) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const runsResult = await query(
      `SELECT * FROM job_runs WHERE job_id = $1 ORDER BY attempt ASC`,
      [id]
    );

    return res.status(200).json({
      job: jobResult.rows[0],
      runs: runsResult.rows,
    });
  } catch (error) {
    console.error('Error fetching job details:', error);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Observability: Metrics
app.get('/api/metrics', async (_req: Request, res: Response) => {
  try {
    const counts = await taskQueue.getJobCounts(
      'waiting',
      'active',
      'completed',
      'failed',
      'delayed'
    );

    return res.status(200).json({
      queue: 'taskflow-queue',
      timestamp: new Date().toISOString(),
      counts,
    });
  } catch (error) {
    console.error('Error fetching queue metrics:', error);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

// DLQ: Retry Failed Job
app.post('/api/jobs/:id/retry', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const jobResult = await query(`SELECT * FROM jobs WHERE id = $1`, [id]);
    if (jobResult.rows.length === 0) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const job = jobResult.rows[0];

    await query(
      `UPDATE jobs 
       SET status = 'ACTIVE', updated_at = NOW() 
       WHERE id = $1`,
      [id]
    );

    const newQueueJob = await taskQueue.add(
      job.name,
      {
        jobDbId: job.id,
        taskType: job.task_type,
        payload: job.payload,
      },
      {
        attempts: job.max_retries || 3,
        backoff: {
          type: 'exponential',
          delay: (job.backoff_factor || 2) * 1000,
        },
      }
    );

    return res.status(200).json({
      message: 'Failed job successfully recovered from DLQ and re-enqueued',
      jobId: job.id,
      newQueueJobId: newQueueJob.id,
      status: 'QUEUED',
    });
  } catch (error) {
    console.error('Error recovering job from DLQ:', error);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});