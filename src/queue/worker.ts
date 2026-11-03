import { Worker, Job } from 'bullmq';
import { redisConnection } from '../config/redis';
import { query } from '../db/db';
import { processTask } from './taskHandlers';

console.log('👷 Background Worker process initializing...');

export const taskWorker = new Worker(
  'taskflow-queue',
  async (job: Job) => {
    const { jobDbId, taskType, payload } = job.data;
    const startTime = Date.now();
    const attempt = job.attemptsMade + 1;

    console.log(`\n▶ [Worker] Processing Job ID: ${job.id} | Attempt #${attempt} | Type: ${taskType}`);

    const runRecord = await query(
      `INSERT INTO job_runs (job_id, attempt, status, started_at)
       VALUES ($1, $2, 'PROCESSING', NOW())
       RETURNING id`,
      [jobDbId, attempt]
    );
    const runId = runRecord.rows[0].id;

    try {
      const result = await processTask(taskType, payload);
      const durationMs = Date.now() - startTime;

      await query(
        `UPDATE job_runs 
         SET status = 'COMPLETED', output = $1, finished_at = NOW(), duration_ms = $2
         WHERE id = $3`,
        [JSON.stringify(result), durationMs, runId]
      );

      console.log(`✅ [Worker] Finished Job ID: ${job.id} in ${durationMs}ms`);
      return result;
    } catch (error: any) {
      const durationMs = Date.now() - startTime;

      await query(
        `UPDATE job_runs 
         SET status = 'FAILED', error_message = $1, finished_at = NOW(), duration_ms = $2
         WHERE id = $3`,
        [error.message || 'Unknown error', durationMs, runId]
      );

      console.error(`❌ [Worker] Job ID: ${job.id} Attempt #${attempt} failed: ${error.message}`);
      throw error;
    }
  },
  {
    connection: redisConnection,
    concurrency: 2, // Maximum 2 jobs processed concurrently per worker instance
    limiter: {
      max: 3,       // Maximum 3 jobs processed...
      duration: 10000, // ...per 10-second rolling window (10,000 ms)
    },
  }
);

taskWorker.on('failed', (job, err) => {
  if (job && job.attemptsMade >= (job.opts.attempts || 3)) {
    console.error(`🚨 [Worker] Job ID: ${job.id} permanently failed after max retries.`);
  }
});