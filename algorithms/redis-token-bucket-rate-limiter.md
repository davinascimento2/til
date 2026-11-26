# Atomic Token Bucket Rate Limiting with Redis & Lua

Public APIs and internal gateways need rate limiting to protect downstream databases from traffic spikes, abusive scrapers, and noisy-neighbor tenants. Naive fixed-window counters (`INCR` + `EXPIRE` per minute) suffer from **boundary bursts**—a client can send 100 requests at `00:59` and another 100 at `01:00`, doubling the intended throughput—and non-atomic read-modify-write logic fails under concurrency.

The **Token Bucket** algorithm smooths sustained throughput while explicitly allowing bounded bursts. A bucket holds up to `capacity` tokens and refills continuously at a steady `refill_rate`. Each request consumes `N` tokens; if enough tokens exist, the request proceeds, otherwise it is rejected with an exact `retryAfterMs` backoff. Executing the state transition inside a single **Redis Lua script** makes the check, refill, and deduction strictly atomic across all distributed application nodes.

## 🚀 Core Guarantees

- **Atomicity Without Distributed Locks**: Redis executes Lua scripts single-threadedly, eliminating check-then-act race conditions without `WATCH`/`MULTI` retry loops.
- **No Boundary Burst Spikes**: Continuous time-based refilling prevents the 2x edge-of-window spike inherent to fixed-window counters.
- **Clock-Skew Immunity**: Computing elapsed time via `redis.call("TIME")` uses a single authoritative clock rather than drifting application-server clocks.
- **Self-Cleaning State**: Every write sets `PEXPIRE` to the full-refill duration so idle client keys expire automatically once their bucket would be full again.

---

## 🛠️ Production Implementation (Redis Lua + TypeScript)

Passing `capacity`, `refillTokens`, `intervalMs`, and `requested` as integers lets Lua compute the fractional refill rate internally, avoiding client-side floating-point serialization quirks.

```typescript
import type { Redis } from "ioredis";

export interface RateLimitConfig {
  /** Maximum burst size (bucket capacity). */
  capacity: number;
  /** Tokens added per intervalMs. */
  refillTokens: number;
  /** Refill window in milliseconds. */
  intervalMs: number;
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
  resetAfterMs: number;
}

const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_tokens = tonumber(ARGV[2])
local interval_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

if requested <= 0 or requested > capacity then
  return { 0, 0, -1, 0 }
end

local refill_rate = refill_tokens / interval_ms
local time = redis.call("TIME")
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local bucket = redis.call("HMGET", key, "tokens", "last_refill_ms")
local tokens = tonumber(bucket[1])
local last_refill_ms = tonumber(bucket[2])

if tokens == nil or last_refill_ms == nil then
  tokens = capacity
  last_refill_ms = now_ms
else
  local elapsed_ms = math.max(0, now_ms - last_refill_ms)
  tokens = math.min(capacity, tokens + elapsed_ms * refill_rate)
  last_refill_ms = now_ms
end

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
  allowed = 1
  tokens = tokens - requested
else
  local deficit = requested - tokens
  retry_after_ms = math.ceil(deficit / refill_rate)
end

local reset_after_ms = math.ceil((capacity - tokens) / refill_rate)
-- Key can safely expire once the bucket would be 100% refilled
local ttl_ms = math.max(1000, math.ceil(capacity / refill_rate))

redis.call("HSET", key, "tokens", tostring(tokens), "last_refill_ms", tostring(last_refill_ms))
redis.call("PEXPIRE", key, ttl_ms)

return { allowed, math.floor(tokens), retry_after_ms, reset_after_ms }
`;

export class TokenBucketLimiter {
  constructor(private readonly redis: Redis) {
    // Register once via defineCommand so ioredis uses EVALSHA automatically
    if (!("consumeTokenBucket" in this.redis)) {
      this.redis.defineCommand("consumeTokenBucket", {
        numberOfKeys: 1,
        lua: TOKEN_BUCKET_LUA,
      });
    }
  }

  async consume(
    identifier: string,
    config: RateLimitConfig,
    cost = 1
  ): Promise<RateLimitResult> {
    const key = `rl:tb:${identifier}`;
    const cmd = this.redis as Redis & {
      consumeTokenBucket: (
        key: string,
        capacity: number,
        refillTokens: number,
        intervalMs: number,
        requested: number
      ) => Promise<[number, number, number, number]>;
    };

    const [allowed, remaining, retryAfterMs, resetAfterMs] =
      await cmd.consumeTokenBucket(
        key,
        config.capacity,
        config.refillTokens,
        config.intervalMs,
        cost
      );

    return {
      allowed: allowed === 1,
      remaining,
      retryAfterMs,
      resetAfterMs,
    };
  }
}
```

### Applying Standard HTTP Headers

```typescript
const result = await limiter.consume(`tenant:${tenantId}:orders`, {
  capacity: 20,      // Allow bursts up to 20 requests
  refillTokens: 10,  // Sustain 10 requests per second
  intervalMs: 1000,
});

res.setHeader("X-RateLimit-Remaining", String(result.remaining));
res.setHeader("X-RateLimit-Reset", String(Math.ceil(result.resetAfterMs / 1000)));

if (!result.allowed) {
  res.setHeader("Retry-After", String(Math.ceil(result.retryAfterMs / 1000)));
  return res.status(429).json({ error: "Too Many Requests" });
}
```

> **Tip**: For weighted endpoints (e.g., expensive GraphQL queries or batch exports), pass `cost > 1` to `consume()` so heavy requests drain proportionally more tokens from the same tenant bucket.

---

## ⚠️ Common Pitfalls to Avoid

1. **Non-Atomic `INCR` Followed by `EXPIRE`**: If a client crashes or the connection drops between `INCR` and `EXPIRE`, the key persists forever without a TTL and permanently locks out the user. Always bundle state mutation and `PEXPIRE` inside a single Lua script or Redis transaction.
2. **Using Application Server Timestamps (`Date.now()`)**: NTP drift across Kubernetes nodes causes negative elapsed times or premature refills when different pods write `last_refill_ms`. Always read `redis.call("TIME")` inside the Lua script.
3. **Omitting `PEXPIRE` on Idle Buckets**: Storing millions of one-off IP buckets without an expiration leaks Redis memory indefinitely. Setting TTL to `capacity / refill_rate` safely evicts keys once they would have refilled to full capacity anyway.
4. **Hard-Failing Requests on Redis Outages**: Treating a transient Redis timeout as an unhandled 500 or rejecting all traffic turns your rate limiter into a single point of failure. Wrap Redis calls with a tight timeout (e.g., `50ms`) and **fail open** (or fall back to an in-memory local limiter) for non-critical routes.
5. **Running `EVAL` Instead of `EVALSHA` on Every Request**: Sending the full Lua script text on every API call wastes network bandwidth and CPU parsing overhead. Use `EVALSHA` (or `ioredis.defineCommand`) to cache the compiled script SHA on Redis.
