# Idempotency Keys for Safe Distributed API Retries

In distributed systems, network calls fail ambiguously: a client sending a `POST /payments` request may time out after the server already committed the charge, but before the response arrived. Retrying blindly creates duplicate charges or corrupted state; not retrying leaves the client stuck.

An **Idempotency Key** (typically a client-generated UUID sent via an `Idempotency-Key` header) makes non-idempotent operations (`POST`, `PATCH`) safely retryable. The server atomically records the key alongside the request fingerprint and caches the final HTTP status and response body so retries replay the exact original outcome without re-executing side effects.

## 🚀 Core Guarantees

- **At-Most-Once Side Effect Execution**: Concurrent or sequential retries with the same key never trigger duplicate writes or external charges.
- **Deterministic Replay**: Retrying a completed request returns the stored response (status code + body) rather than an error.
- **Payload Mismatch Protection**: Reusing an existing key with different request parameters fails loudly (`422 Unprocessable Entity`) to prevent silent client bugs.

---

## 🛠️ State Machine & Atomic Persistence

A naive `SELECT` followed by `INSERT` has a TOCTOU race condition when two identical requests arrive milliseconds apart. Instead, use a unique constraint (`INSERT ... ON CONFLICT DO NOTHING`) to claim an `in_progress` lock atomically, store a SHA-256 hash of the request payload, and persist the serialized response upon completion.

### 1. Schema

```sql
CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    request_hash    TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('in_progress', 'completed')),
    response_status INT,
    response_body   JSONB,
    locked_until    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_idempotency_keys_created_at
    ON idempotency_keys (created_at);
```

### 2. Middleware / Handler Flow (TypeScript + PostgreSQL)

```typescript
import crypto from "node:crypto";
import { Pool } from "pg";

interface HttpRequest {
  headers: { "idempotency-key"?: string };
  method: string;
  path: string;
  body: unknown;
}

interface HttpResponse {
  status: number;
  body: unknown;
}

interface IdempotencyRecord {
  request_hash: string;
  status: "in_progress" | "completed";
  response_status: number | null;
  response_body: unknown;
  locked_until: Date;
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => a.localeCompare(b));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(",")}}`;
}

function hashRequest(req: HttpRequest): string {
  // Canonicalize key ordering so equivalent JSON payloads produce the same digest
  const canonical = stableStringify({
    method: req.method,
    path: req.path,
    body: req.body ?? null,
  });
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

export async function withIdempotency(
  pool: Pool,
  req: HttpRequest,
  handler: () => Promise<HttpResponse>
): Promise<HttpResponse> {
  const key = req.headers["idempotency-key"];
  if (!key) {
    return { status: 400, body: { error: "Missing Idempotency-Key header" } };
  }

  const requestHash = hashRequest(req);

  // 1. Attempt atomic claim; steal lock only if an earlier worker crashed past locked_until
  const claimResult = await pool.query<IdempotencyRecord>(
    `INSERT INTO idempotency_keys (idempotency_key, request_hash, status, locked_until)
     VALUES ($1, $2, 'in_progress', now() + interval '30 seconds')
     ON CONFLICT (idempotency_key) DO UPDATE
       SET locked_until = now() + interval '30 seconds'
       WHERE idempotency_keys.status = 'in_progress'
         AND idempotency_keys.locked_until < now()
         AND idempotency_keys.request_hash = $2
     RETURNING request_hash, status, response_status, response_body, locked_until`,
    [key, requestHash]
  );

  // 2. If row wasn't claimed, another record already exists and is either active or completed
  if (claimResult.rowCount === 0) {
    const existing = await pool.query<IdempotencyRecord>(
      `SELECT request_hash, status, response_status, response_body
       FROM idempotency_keys
       WHERE idempotency_key = $1`,
      [key]
    );

    const record = existing.rows[0];
    if (!record) {
      // Defensive fallback if expired row was concurrently purged
      return { status: 409, body: { error: "Concurrent idempotency conflict, retry" } };
    }

    if (record.request_hash !== requestHash) {
      return {
        status: 422,
        body: { error: "Idempotency-Key reused with a different request payload" },
      };
    }

    if (record.status === "in_progress") {
      // Another identical request is still executing; ask client to back off and retry
      return { status: 409, body: { error: "Request already in progress" } };
    }

    return {
      status: record.response_status ?? 200,
      body: record.response_body,
    };
  }

  // 3. Execute business logic and persist final outcome
  try {
    const res = await handler();

    // Only cache deterministic outcomes (2xx and 4xx client errors); release key on 5xx
    if (res.status >= 500) {
      await pool.query(`DELETE FROM idempotency_keys WHERE idempotency_key = $1`, [key]);
    } else {
      await pool.query(
        `UPDATE idempotency_keys
         SET status = 'completed',
             response_status = $2,
             response_body = $3::jsonb
         WHERE idempotency_key = $1`,
        [key, res.status, JSON.stringify(res.body)]
      );
    }

    return res;
  } catch (err) {
    // Unexpected crash/5xx before business commit: free the key so client retries can proceed
    await pool.query(`DELETE FROM idempotency_keys WHERE idempotency_key = $1`, [key]);
    throw err;
  }
}
```

> **Tip**: When your side effect is purely local to the same PostgreSQL database (e.g., inserting an order or ledger entry), execute the handler and the `UPDATE idempotency_keys SET status = 'completed'` inside the **same database transaction** so business state and idempotency record commit or roll back atomically.

---

## ⚠️ Common Pitfalls to Avoid

1. **Check-Then-Insert Without Unique Constraints**: Reading `SELECT * FROM idempotency_keys` before inserting allows parallel double-clicks or eager client retries to both pass the check and execute twice. Always rely on `PRIMARY KEY` / `UNIQUE` constraints (`ON CONFLICT`).
2. **Caching Transient `5xx` Errors**: If a downstream service temporarily returns `503 Service Unavailable`, caching that `503` under the idempotency key permanently breaks all future retries with the same key. Delete the key on `5xx` (or unexpected exceptions) before any durable side effect occurred.
3. **Ignoring Request Fingerprinting**: If a client bug accidentally sends the same UUID for two different amounts (`$10` vs `$100`), returning the `$10` cached success silently drops the `$100` request. Always compare `request_hash` and return `422`.
4. **Scoping Keys Globally Without Tenant/User Isolation**: Two different users generating keys (or predictable client keys like `order-123`) can collide. Always composite-key by `(tenant_id, user_id, idempotency_key)` in multi-tenant APIs.
5. **Unbounded Table Growth**: Idempotency keys don't need to live forever—most payment and API providers retain them for **24 to 72 hours**. Schedule periodic cleanup (`DELETE FROM idempotency_keys WHERE created_at < now() - interval '24 hours'`) or use time-partitioned tables.
