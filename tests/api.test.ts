import { describe, it, expect, afterAll } from 'vitest';
import request from 'supertest';
import { app } from '../src/app';
import { pool } from '../src/db/db';
import { taskQueue } from '../src/queue/taskQueue';
import { redisConnection } from '../src/config/redis';

describe('TaskFlow Engine Integration Suite', () => {
  let createdJobId: string;

  afterAll(async () => {
    // Gracefully terminate connections after test completion
    await taskQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it('GET /health - verifies service availability', async () => {
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      status: 'ok',
      service: 'taskflow-api',
    });
  });

  it('POST /api/jobs - validates input payload rejection', async () => {
    const response = await request(app)
      .post('/api/jobs')
      .send({ name: 'incomplete-job' });

    expect(response.status).toBe(400);
    expect(response.body.error).toBe('name and task_type are required fields');
  });

  it('POST /api/jobs - dispatches and queues task successfully', async () => {
    const response = await request(app)
      .post('/api/jobs')
      .send({
        name: 'test-email-job',
        task_type: 'EMAIL',
        payload: { email: 'integration-test@example.com' },
      });

    expect(response.status).toBe(202);
    expect(response.body.message).toBe('Job submitted and queued successfully');
    expect(response.body.status).toBe('QUEUED');
    expect(response.body.jobId).toBeDefined();

    createdJobId = response.body.jobId;
  });

  it('GET /api/jobs/:id - retrieves dispatched job metadata', async () => {
    const response = await request(app).get(`/api/jobs/${createdJobId}`);

    expect(response.status).toBe(200);
    expect(response.body.job).toBeDefined();
    expect(response.body.job.id).toBe(createdJobId);
    expect(response.body.job.task_type).toBe('EMAIL');
    expect(Array.isArray(response.body.runs)).toBe(true);
  });

  it('GET /api/metrics - verifies queue metrics reporting', async () => {
    const response = await request(app).get('/api/metrics');

    expect(response.status).toBe(200);
    expect(response.body.queue).toBe('taskflow-queue');
    expect(response.body.counts).toHaveProperty('waiting');
    expect(response.body.counts).toHaveProperty('active');
    expect(response.body.counts).toHaveProperty('completed');
    expect(response.body.counts).toHaveProperty('failed');
  });

  it('POST /api/jobs/:id/retry - re-enqueues job into DLQ recovery path', async () => {
    const response = await request(app).post(`/api/jobs/${createdJobId}/retry`);

    expect(response.status).toBe(200);
    expect(response.body.message).toBe(
      'Failed job successfully recovered from DLQ and re-enqueued'
    );
    expect(response.body.status).toBe('QUEUED');
    expect(response.body.jobId).toBe(createdJobId);
  });
});