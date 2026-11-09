import { app } from './app';
import { taskQueue } from './queue/taskQueue';
import { pool } from './db/db';
import { redisConnection } from './config/redis';

const PORT = process.env.PORT || 4000;

const server = app.listen(PORT, () => {
  console.log(`🚀 TaskFlow Producer API running on http://localhost:${PORT}`);
  console.log(`📊 TaskFlow Queue Dashboard running on http://localhost:${PORT}/admin/queues`);
});

let isShuttingDown = false;

const shutdown = async (signal: string) => {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log(`\n🛑 Received ${signal}. Starting graceful shutdown of Producer API...`);

  // Stop accepting incoming connections & drop idle sockets
  server.close(() => {
    console.log('🔒 Closed active HTTP server listener.');
  });
  if (typeof server.closeIdleConnections === 'function') {
    server.closeIdleConnections();
  }

  try {
    // Close BullMQ handles
    await taskQueue.close();
    console.log('🔒 Closed BullMQ queue connection.');

    // Close Redis connection
    await redisConnection.quit();
    console.log('🔒 Closed Redis connection.');

    // Drain PostgreSQL pool
    await pool.end();
    console.log('🔒 Closed PostgreSQL pool.');

    console.log('✅ Graceful shutdown completed cleanly.');
    process.exit(0);
  } catch (cleanupError) {
    console.error('❌ Error during resource teardown:', cleanupError);
    process.exit(1);
  }
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));