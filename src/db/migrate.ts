import { query, pool } from './db';

async function migrate() {
  console.log('⏳ Running database migrations...');

  const createTablesQuery = `
    CREATE TABLE IF NOT EXISTS jobs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(255) NOT NULL,
      task_type VARCHAR(100) NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}',
      cron_schedule VARCHAR(100),
      max_retries INT DEFAULT 3,
      backoff_factor INT DEFAULT 2,
      status VARCHAR(50) DEFAULT 'ACTIVE',
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS job_runs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
      attempt INT DEFAULT 1,
      status VARCHAR(50) NOT NULL,
      output JSONB,
      error_message TEXT,
      started_at TIMESTAMP WITH TIME ZONE,
      finished_at TIMESTAMP WITH TIME ZONE,
      duration_ms INT,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
    CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status);
  `;

  try {
    await query(createTablesQuery);
    console.log('✅ Tables created successfully in PostgreSQL (Neon)!');
  } catch (error) {
    console.error('❌ Migration failed:', error);
  } finally {
    await pool.end();
  }
}

migrate();