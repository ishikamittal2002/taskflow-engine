import { Queue } from 'bullmq';
import { redisConnection } from '../config/redis';

// Name the queue 'taskflow-queue'
export const taskQueue = new Queue('taskflow-queue', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3, // Retry up to 3 times on failure
    backoff: {
      type: 'exponential',
      delay: 2000, // 2s -> 4s -> 8s
    },
    removeOnComplete: false, // Keep completed jobs in Redis for logs
    removeOnFail: false,     // Keep failed jobs for inspection
  },
});