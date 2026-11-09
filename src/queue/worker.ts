import { Worker, Job } from 'bullmq';
import { redisConnection } from '../config/redis';
import { query, pool } from '../db/db';
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
    concurrency: 2,
    limiter: {
      max: 3,
      duration: 10000,
    },
  }
);

taskWorker.on('failed', (job, err) => {
  if (job && job.attemptsMade >= (job.opts.attempts || 3)) {
    console.error(`🚨 [Worker] Job ID: ${job.id} permanently failed after max retries.`);
  }
});

let isWorkerShuttingDown = false;

const shutdownWorker = async (signal: string) => {
  if (isWorkerShuttingDown) return;
  isWorkerShuttingDown = true;

  console.log(`\n🛑 Received ${signal}. Shutting down worker process gracefully...`);

  try {
    await taskWorker.close();
    console.log('🔒 BullMQ Worker stopped and in-flight tasks finalized.');

    await redisConnection.quit();
    console.log('🔒 Closed Redis connection.');

    await pool.end();
    console.log('🔒 Closed PostgreSQL pool.');

    console.log('✅ Worker shutdown completed cleanly.');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error shutting down worker:', error);
    process.exit(1);
  }
};

process.on('SIGINT', () => shutdownWorker('SIGINT'));
process.on('SIGTERM', () => shutdownWorker('SIGTERM'));