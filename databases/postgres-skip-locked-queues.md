# PostgreSQL `SKIP LOCKED` for Concurrent Job Queues

When multiple worker instances poll the same SQL table for background jobs, a naive `SELECT` followed by `UPDATE` causes race conditions (two workers claiming the same job). Adding `FOR UPDATE` prevents duplicates, but forces competing workers to wait in line behind row locks.

`FOR UPDATE SKIP LOCKED` solves this by instructing PostgreSQL to immediately skip any row currently locked by another transaction and lock the next available row instead. This turns a standard PostgreSQL table into a reliable, contention-free FIFO/priority queue without extra infrastructure.

## 🚀 Why Use `SKIP LOCKED`?

- **Zero Lock Contention**: Workers never block waiting for another worker's row lock; each grabs a distinct pending row in parallel.
- **Transactional Integrity**: Jobs can be enqueued in the exact same ACID transaction as business state changes (ideal for the Transactional Outbox pattern).
- **Minimal Stack**: Avoids introducing Redis, RabbitMQ, or SQS for moderate throughput workloads (hundreds to a few thousand jobs/sec).

---

## 🛠️ Schema & Atomic Claim Pattern

A common mistake is holding a database transaction open while executing slow work (e.g., calling an external API). Instead, use a **short claim transaction** with a visibility lease (`locked_until`), commit immediately, process the job, and finalize it in a second transaction.

### 1. Table & Partial Index

A partial index keeps queue lookups fast regardless of how many completed jobs accumulate in the table:

```sql
CREATE TABLE job_queue (
    id           BIGSERIAL PRIMARY KEY,
    job_type     TEXT NOT NULL,
    payload      JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending', -- pending | processing | completed | failed
    attempts     INT NOT NULL DEFAULT 0,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only index rows eligible for pickup or lease recovery
CREATE INDEX idx_job_queue_claimable
    ON job_queue (run_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_job_queue_stale_locks
    ON job_queue (locked_until)
    WHERE status = 'processing';
```

### 2. Claiming a Batch Atomically (CTE)

```sql
WITH next_jobs AS (
    SELECT id
    FROM job_queue
    WHERE status = 'pending'
      AND run_at <= now()
    ORDER BY run_at ASC, id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE job_queue q
SET status       = 'processing',
    locked_until = now() + interval '5 minutes',
    attempts     = q.attempts + 1
FROM next_jobs
WHERE q.id = next_jobs.id
RETURNING q.id, q.job_type, q.payload, q.attempts;
```

### 3. Worker Execution & Lease Recovery (TypeScript)

```typescript
import { Pool } from "pg";

interface Job {
  id: string;
  job_type: string;
  payload: Record<string, unknown>;
  attempts: number;
}

const CLAIM_SQL = `
  WITH next_jobs AS (
    SELECT id
    FROM job_queue
    WHERE status = 'pending'
      AND run_at <= now()
    ORDER BY run_at ASC, id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
  )
  UPDATE job_queue q
  SET status = 'processing',
      locked_until = now() + interval '5 minutes',
      attempts = q.attempts + 1
  FROM next_jobs
  WHERE q.id = next_jobs.id
  RETURNING q.id, q.job_type, q.payload, q.attempts;
`;

export async function processBatch(pool: Pool, batchSize = 10): Promise<number> {
  // Single statement executes in an implicit transaction and releases locks immediately
  const { rows } = await pool.query<Job>(CLAIM_SQL, [batchSize]);
  if (rows.length === 0) return 0;

  await Promise.all(
    rows.map(async (job) => {
      try {
        await handleJob(job);
        await pool.query(
          `UPDATE job_queue SET status = 'completed', locked_until = NULL WHERE id = $1`,
          [job.id]
        );
      } catch (err) {
        const nextStatus = job.attempts >= 5 ? "failed" : "pending";
        // Exponential backoff before retry
        const backoffSeconds = Math.min(300, 2 ** job.attempts);
        await pool.query(
          `UPDATE job_queue
           SET status = $2,
               locked_until = NULL,
               run_at = now() + ($3 || ' seconds')::interval
           WHERE id = $1`,
          [job.id, nextStatus, backoffSeconds]
        );
      }
    })
  );

  return rows.length;
}

// Run periodically (e.g., every 30s) to recover jobs from crashed workers
export async function reclaimStaleJobs(pool: Pool): Promise<number> {
  const { rowCount } = await pool.query(`
    UPDATE job_queue
    SET status = 'pending',
        locked_until = NULL
    WHERE status = 'processing'
      AND locked_until < now();
  `);
  return rowCount ?? 0;
}

async function handleJob(job: Job): Promise<void> {
  // Perform idempotent side effects here
}
```

---

## ⚠️ Common Pitfalls to Avoid

1. **Holding Transactions Open During External Calls**: Never keep `BEGIN ... FOR UPDATE SKIP LOCKED` open while sending emails or calling webhooks. It exhausts the DB connection pool and prevents PostgreSQL `VACUUM` from reclaiming dead tuples. Always claim-and-commit first, then process.
2. **Omitting Partial Indexes**: Without `WHERE status = 'pending'`, PostgreSQL scans historical `completed` rows, degrading pickup latency from sub-millisecond to seconds as the table grows.
3. **Forgetting Lease Recovery (Zombie Jobs)**: If a worker is OOM-killed mid-job, that row stays `'processing'` forever unless a reaper resets rows where `locked_until < now()`.
4. **Assuming Strict Global Ordering**: `SKIP LOCKED` guarantees at-least-once delivery without duplicate concurrent execution, not strict FIFO completion order—a crashed job retried later will execute out of order, so handlers **must be idempotent**.
5. **Neglecting Table Bloat & Autovacuum**: High-churn queues generate dead tuples rapidly. Delete/archive completed jobs promptly and tune `autovacuum_vacuum_scale_factor` (e.g., `0.02`) on queue tables.
