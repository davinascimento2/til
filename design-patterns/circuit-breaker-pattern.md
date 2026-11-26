# Circuit Breaker Pattern with Sliding Window & Half-Open Probing

When a downstream dependency slows down or starts failing, naive callers keep sending requests and retrying immediately. Worker threads and connection pools quickly saturate waiting for timeouts, turning a localized outage into a **cascading failure** across your entire system.

The **Circuit Breaker** pattern wraps unstable remote calls in a finite-state machine (`CLOSED` → `OPEN` → `HALF_OPEN`). When failure rates exceed a threshold over a recent window, the breaker trips `OPEN` and fails fast in microseconds—giving the downstream service breathing room to recover while protecting upstream resources.

## 🚀 Core Guarantees

- **Fail-Fast Load Shedding**: While `OPEN`, calls reject immediately without opening sockets or exhausting thread/connection pools.
- **Bounded Recovery Probing**: Transitions to `HALF_OPEN` after a cooldown and permits only a small number of probe requests, preventing a thundering herd from crushing a recovering dependency.
- **Statistically Sound Tripping**: Evaluating failure rate over a fixed-size sliding window with a `minimumCalls` floor prevents a single error during low traffic from tripping the circuit.
- **Fault Isolation via Error Classification**: Distinguishes transient infrastructure faults (`5xx`, timeouts, `ECONNRESET`) from expected domain/client errors (`400`, `404`), so bad user input never trips the breaker.

---

## 🛠️ State Machine & Sliding Window Implementation (TypeScript)

A production circuit breaker needs three non-trivial details that toy examples omit:
1. **Ring-buffer sliding window** so old failures decay naturally as new calls arrive.
2. **In-flight probe counter in `HALF_OPEN`** so concurrent callers cannot all sneak through at the exact millisecond the cooldown expires.
3. **Per-call timeout enforcement** (`AbortSignal.any` + `AbortSignal.timeout`) because a hung socket that takes 60s to fail is worse than an instant `503`.

```typescript
export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export class CircuitOpenError extends Error {
  constructor(public readonly retryAfterMs: number) {
    super(`Circuit breaker is OPEN; retry after ${retryAfterMs}ms`);
    this.name = "CircuitOpenError";
  }
}

export interface CircuitBreakerOptions {
  /** Number of recent calls tracked in the count-based sliding window. */
  windowSize: number;
  /** Minimum recorded calls before failure rate can trip the circuit. */
  minimumCalls: number;
  /** Fraction in (0, 1] of failed calls that trips CLOSED -> OPEN. */
  failureRateThreshold: number;
  /** How long the breaker stays OPEN before allowing HALF_OPEN probes. */
  openDurationMs: number;
  /** Maximum concurrent/sequential probes permitted in HALF_OPEN. */
  halfOpenMaxCalls: number;
  /** Per-attempt execution timeout in milliseconds. */
  callTimeoutMs: number;
  /** Return false for expected 4xx/business errors that should not trip the breaker. */
  isFailure?: (err: unknown) => boolean;
}

export class CircuitBreaker {
  private state: CircuitState = "CLOSED";
  private generation = 0;
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private halfOpenSuccesses = 0;

  // Fixed-capacity ring buffer: true = failure, false = success
  private readonly window: boolean[];
  private windowIndex = 0;
  private windowCount = 0;
  private windowFailures = 0;

  private readonly isFailure: (err: unknown) => boolean;

  constructor(private readonly opts: CircuitBreakerOptions) {
    if (opts.windowSize < 1 || opts.minimumCalls < 1 || opts.halfOpenMaxCalls < 1) {
      throw new RangeError("windowSize, minimumCalls, and halfOpenMaxCalls must be >= 1");
    }
    this.window = new Array<boolean>(opts.windowSize).fill(false);
    this.isFailure = opts.isFailure ?? (() => true);
  }

  get currentState(): CircuitState {
    if (this.state === "OPEN" && Date.now() - this.openedAt >= this.opts.openDurationMs) {
      return "HALF_OPEN";
    }
    return this.state;
  }

  async execute<T>(
    fn: (signal: AbortSignal) => Promise<T>,
    callerSignal?: AbortSignal
  ): Promise<T> {
    const callGeneration = this.beforeCall();

    const timeoutSignal = AbortSignal.timeout(this.opts.callTimeoutMs);
    const signal = callerSignal
      ? AbortSignal.any([callerSignal, timeoutSignal])
      : timeoutSignal;

    try {
      const result = await fn(signal);
      this.onSuccess(callGeneration);
      return result;
    } catch (err) {
      // Caller-initiated aborts shouldn't penalize downstream health
      if (callerSignal?.aborted && !timeoutSignal.aborted) {
        this.releaseHalfOpenSlot(callGeneration);
        throw err;
      }

      if (this.isFailure(err)) {
        this.onFailure(callGeneration);
      } else {
        this.onSuccess(callGeneration);
      }
      throw err;
    }
  }

  private beforeCall(): number {
    const now = Date.now();

    if (this.state === "OPEN") {
      const elapsed = now - this.openedAt;
      if (elapsed < this.opts.openDurationMs) {
        throw new CircuitOpenError(this.opts.openDurationMs - elapsed);
      }
      // Cooldown expired: transition to HALF_OPEN and bump generation to fence stale calls
      this.state = "HALF_OPEN";
      this.generation++;
      this.halfOpenInFlight = 0;
      this.halfOpenSuccesses = 0;
    }

    if (this.state === "HALF_OPEN") {
      if (this.halfOpenInFlight + this.halfOpenSuccesses >= this.opts.halfOpenMaxCalls) {
        throw new CircuitOpenError(this.opts.callTimeoutMs);
      }
      this.halfOpenInFlight++;
    }

    return this.generation;
  }

  private onSuccess(callGeneration: number): void {
    if (callGeneration !== this.generation) return;

    if (this.state === "HALF_OPEN") {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.opts.halfOpenMaxCalls) {
        this.transitionToClosed();
      }
      return;
    }

    if (this.state === "CLOSED") {
      this.recordOutcome(false);
    }
  }

  private onFailure(callGeneration: number): void {
    if (callGeneration !== this.generation) return;

    if (this.state === "HALF_OPEN") {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
      // Any probe failure immediately re-opens the circuit
      this.transitionToOpen();
      return;
    }

    if (this.state === "CLOSED") {
      this.recordOutcome(true);
      if (
        this.windowCount >= this.opts.minimumCalls &&
        this.windowFailures / this.windowCount >= this.opts.failureRateThreshold
      ) {
        this.transitionToOpen();
      }
    }
  }

  private releaseHalfOpenSlot(callGeneration: number): void {
    if (callGeneration === this.generation && this.state === "HALF_OPEN") {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
    }
  }

  private recordOutcome(failed: boolean): void {
    if (this.windowCount === this.opts.windowSize) {
      if (this.window[this.windowIndex]) {
        this.windowFailures--;
      }
    } else {
      this.windowCount++;
    }

    this.window[this.windowIndex] = failed;
    if (failed) {
      this.windowFailures++;
    }
    this.windowIndex = (this.windowIndex + 1) % this.opts.windowSize;
  }

  private transitionToOpen(): void {
    this.state = "OPEN";
    this.generation++;
    this.openedAt = Date.now();
    this.halfOpenInFlight = 0;
    this.halfOpenSuccesses = 0;
  }

  private transitionToClosed(): void {
    this.state = "CLOSED";
    this.generation++;
    this.halfOpenInFlight = 0;
    this.halfOpenSuccesses = 0;
    this.window.fill(false);
    this.windowIndex = 0;
    this.windowCount = 0;
    this.windowFailures = 0;
  }
}
```

### Usage Example: Protecting an External Payment Gateway

```typescript
class HttpError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
  }
}

const gatewayBreaker = new CircuitBreaker({
  windowSize: 20,
  minimumCalls: 10,
  failureRateThreshold: 0.5, // Trip if >= 50% of last 20 calls failed
  openDurationMs: 15_000,
  halfOpenMaxCalls: 3,
  callTimeoutMs: 2_500,
  // Only trip on network/timeout errors or 5xx/429 responses; ignore 4xx client mistakes
  isFailure: (err) => !(err instanceof HttpError && err.status >= 400 && err.status < 500 && err.status !== 429),
});

export async function authorizeCharge(orderId: string, signal?: AbortSignal) {
  return gatewayBreaker.execute(async (attemptSignal) => {
    const res = await fetch("https://payments.example.com/v1/authorize", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ orderId }),
      signal: attemptSignal,
    });

    if (!res.ok) {
      throw new HttpError(res.status, `Gateway returned ${res.status}`);
    }
    return res.json();
  }, signal);
}
```

> **Tip**: Always place **Retries (with Exponential Backoff + Full Jitter) *outside* or *with* circuit awareness**: never retry a `CircuitOpenError`. Retrying an open circuit defeats fail-fast load shedding and burns request latency budgets.

---

## ⚠️ Common Pitfalls to Avoid

1. **Allowing Unlimited Concurrent Calls in `HALF_OPEN`**: If 500 requests/sec hit an expired `OPEN` timer simultaneously without an in-flight probe cap (`halfOpenMaxCalls`), all 500 flood the recovering dependency at once and knock it back offline.
2. **Tripping on `4xx` Client Errors**: Counting `400 Bad Request`, `401 Unauthorized`, or `404 Not Found` as breaker failures allows one buggy client (or bad payload batch) to trip the circuit and block healthy traffic for everyone.
3. **Omitting Per-Call Timeouts**: A circuit breaker without a tight call timeout (`AbortSignal.timeout`) reacts too late—if the downstream stalls silently for 60 seconds per socket, your connection pool still saturates before enough failures are recorded to trip `OPEN`.
4. **Using Absolute Failure Counts Without `minimumCalls`**: Tripping after "3 consecutive failures" or "50% failure rate" without a minimum sample floor means 1 failed health check at 3:00 AM immediately opens the circuit.
5. **Sharing One Breaker Across Unrelated Tenants or Endpoints**: Wrapping an entire HTTP client in a single global breaker means an outage on `/reports/export` also kills `/auth/token`. Scope breakers per **dependency + criticality** (or per downstream host/endpoint group) and emit state-transition metrics (`closed → open`) for alerting.
