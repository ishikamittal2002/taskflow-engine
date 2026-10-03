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

    // Insert an in-progress record into job_runs
    const runRecord = await query(
      `INSERT INTO job_runs (job_id, attempt, status, started_at)
       VALUES ($1, $2, 'PROCESSING', NOW())
       RETURNING id`,
      [jobDbId, attempt]
    );
    const runId = runRecord.rows[0].id;

    try {
      // Execute the task
      const result = await processTask(taskType, payload);
      const durationMs = Date.now() - startTime;

      // Update job_runs table with success
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

      // Update job_runs table with failure
      await query(
        `UPDATE job_runs 
         SET status = 'FAILED', error_message = $1, finished_at = NOW(), duration_ms = $2
         WHERE id = $3`,
        [error.message || 'Unknown error', durationMs, runId]
      );

      console.error(`❌ [Worker] Job ID: ${job.id} Attempt #${attempt} failed: ${error.message}`);
      throw error; // Re-throw to trigger BullMQ retry / exponential backoff
    }
  },
  {
    connection: redisConnection,
    concurrency: 5, // Process up to 5 jobs in parallel
  }
);

taskWorker.on('failed', (job, err) => {
  if (job && job.attemptsMade >= (job.opts.attempts || 3)) {
    console.error(`🚨 [Worker] Job ID: ${job.id} permanently failed after max retries.`);
  }
});