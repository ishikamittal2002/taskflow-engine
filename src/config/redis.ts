import Redis from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

const redisUrl = process.env.REDIS_URL || '';

export const redisConnection = new Redis(redisUrl, {
  maxRetriesPerRequest: null, // Required by BullMQ
  enableReadyCheck: false,
  keepAlive: 10000,           // Ping Upstash every 10s so it doesn't drop the connection
  tls: {
    rejectUnauthorized: false,
  },
  retryStrategy(times) {
    // Reconnect with backoff capped at 2 seconds
    return Math.min(times * 200, 2000);
  },
});

redisConnection.on('connect', () => {
  console.log('✅ Connected to Upstash Redis');
});

redisConnection.on('error', (err: any) => {
  // Silence transient TCP reset logs from Upstash idle drops
  if (err.code === 'ECONNRESET') return;
  console.error('❌ Redis Connection Error:', err.message);
});