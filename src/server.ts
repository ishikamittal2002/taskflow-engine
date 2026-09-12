import express, { Request, Response } from 'express';
import dotenv from 'dotenv';
import { taskQueue } from './queue/taskQueue';
import { query } from './db/db';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());

// Health check endpoint
app.get('/health', (_req: Request, res: Response) => {
  res.status(200).json({ status: 'ok', service: 'taskflow-api' });
});

// Endpoint: Submit a new background task (Producer)
app.post('/api/jobs', async (req: Request, res: Response) => {
  try {
    const { name, task_type, payload } = req.body;

    if (!name || !task_type) {
      return res.status(400).json({ error: 'name and task_type are required fields' });
    }

    // 1. Persist the job record in PostgreSQL
    const dbResult = await query(
      `INSERT INTO jobs (name, task_type, payload, status)
       VALUES ($1, $2, $3, 'ACTIVE')
       RETURNING *`,
      [name, task_type, JSON.stringify(payload || {})]
    );

    const savedJob = dbResult.rows[0];

    // 2. Enqueue the task into BullMQ / Redis
    const queueJob = await taskQueue.add(name, {
      jobDbId: savedJob.id,
      taskType: task_type,
      payload: payload || {},
    });

    // 3. Respond immediately (Asynchronous Pattern: < 20ms)
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

app.listen(PORT, () => {
  console.log(`🚀 TaskFlow Producer API running on http://localhost:${PORT}`);
});