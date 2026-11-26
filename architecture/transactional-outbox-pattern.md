# Transactional Outbox Pattern for Reliable Event Publishing

Updating a database and publishing a domain event to a message broker (Kafka, RabbitMQ, SQS) in the same request creates a **dual-write problem**. Because a relational database and a message broker cannot share a local ACID transaction, a crash or network blip between the two operations causes silent data divergence: either the database commits and the event is lost forever, or the event is published and the database rolls back, leaving downstream consumers reacting to state that never existed.

The **Transactional Outbox Pattern** eliminates dual writes by persisting the domain event inside an `outbox` table **within the exact same database transaction** as the business state mutation. An asynchronous **Message Relay** (polling worker or Change Data Capture via Debezium) then reads committed outbox rows and publishes them to the broker with **at-least-once delivery**, pairing with **idempotent consumers** to achieve effectively-once processing.

## 🚀 Core Guarantees

- **Atomicity Across State & Events**: Business entities and their corresponding events commit or roll back together in a single local ACID transaction—no distributed two-phase commit (`2PC`) required.
- **Resilience to Broker Outages**: If Kafka or RabbitMQ is unreachable, writes continue succeeding; events buffer safely in PostgreSQL and drain automatically once the broker recovers.
- **At-Least-Once Delivery**: Because the relay marks events as published only *after* receiving a broker ACK, crashes can cause duplicate publishes—making downstream consumer idempotency mandatory.

---

## 🛠️ Schema, Atomic Write & Relay Implementation

Below is a complete production pattern using **PostgreSQL** and **Go** (`pgx`), covering the outbox schema, atomic business write, relay polling with `SKIP LOCKED`, and consumer-side deduplication.

### 1. Outbox & Inbox Schema

```sql
CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    total_cents BIGINT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,           -- e.g. 'Order'
    aggregate_id   UUID NOT NULL,           -- Partition key for broker ordering
    event_type     TEXT NOT NULL,           -- e.g. 'OrderCreated'
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- Keep relay scans fast regardless of historical row volume
CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at, id)
    WHERE published_at IS NULL;

-- Consumer-side deduplication table (Inbox pattern)
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 2. Atomic Write + Relay Worker (Go + `pgx`)

```go
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OrderCreatedPayload struct {
	OrderID    uuid.UUID `json:"order_id"`
	CustomerID uuid.UUID `json:"customer_id"`
	TotalCents int64     `json:"total_cents"`
}

type OutboxEvent struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	Payload       []byte
}

type Broker interface {
	// Publish sends synchronously (or awaits ACK) keyed by aggregateID to preserve per-entity order.
	Publish(ctx context.Context, topic, key string, eventID uuid.UUID, payload []byte) error
}

// CreateOrder persists both the order and its outbox event in one ACID transaction.
func CreateOrder(ctx context.Context, pool *pgxpool.Pool, customerID uuid.UUID, totalCents int64) (uuid.UUID, error) {
	orderID := uuid.New()
	eventID := uuid.New()

	payload, err := json.Marshal(OrderCreatedPayload{
		OrderID:    orderID,
		CustomerID: customerID,
		TotalCents: totalCents,
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal event payload: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO orders (id, customer_id, total_cents, status)
		 VALUES ($1, $2, $3, 'pending')`,
		orderID, customerID, totalCents,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert order: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload)
		 VALUES ($1, 'Order', $2, 'OrderCreated', $3::jsonb)`,
		eventID, orderID, payload,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit transaction: %w", err)
	}
	return orderID, nil
}

// RelayBatch claims unpublished rows, publishes them to the broker, and marks them delivered.
func RelayBatch(ctx context.Context, pool *pgxpool.Pool, broker Broker, batchSize int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Lock only unpublished rows for this relay worker; concurrent relays skip locked rows
	rows, err := tx.Query(ctx,
		`SELECT id, aggregate_type, aggregate_id, event_type, payload
		 FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY created_at ASC, id ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		batchSize,
	)
	if err != nil {
		return 0, err
	}

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload); err != nil {
			rows.Close()
			return 0, err
		}
		events = append(events, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}

	deliveredIDs := make([]uuid.UUID, 0, len(events))
	for _, ev := range events {
		topic := fmt.Sprintf("domain.%s.%s", ev.AggregateType, ev.EventType)
		pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := broker.Publish(pubCtx, topic, ev.AggregateID.String(), ev.ID, ev.Payload)
		cancel()
		if err != nil {
			// Stop batch on first failure to preserve ordering for remaining events
			break
		}
		deliveredIDs = append(deliveredIDs, ev.ID)
	}

	if len(deliveredIDs) > 0 {
		_, err = tx.Exec(ctx,
			`UPDATE outbox_events
			 SET published_at = now()
			 WHERE id = ANY($1)`,
			deliveredIDs,
		)
		if err != nil {
			return 0, fmt.Errorf("mark published: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(deliveredIDs), nil
}
```

### 3. Idempotent Consumer (Inbox Pattern)

Because the relay can crash *after* `broker.Publish` succeeds and *before* `tx.Commit` marks `published_at`, consumers must deduplicate by `event_id` inside their own local transaction:

```sql
-- Inside the consumer's ACID transaction:
INSERT INTO processed_events (event_id)
VALUES ($1)
ON CONFLICT (event_id) DO NOTHING;

-- Check rowCount / RETURNING: if 0 rows inserted, ACK the message and skip side effects.
```

> **Polling vs. Log-Based CDC**: Simple polling with `FOR UPDATE SKIP LOCKED` works great up to ~1,000–2,000 events/sec with minimal ops overhead. Beyond that—or when sub-50ms publish latency is required—replace the polling relay with **Change Data Capture (Debezium + Kafka Connect)** tailing PostgreSQL's Write-Ahead Log (`WAL`) directly, eliminating polling queries entirely.

---

## ⚠️ Common Pitfalls to Avoid

1. **Publishing Before the Database Commit**: Never fire the message broker call inside the business transaction before `COMMIT`. If the transaction rolls back (constraint violation, deadlock, serialization failure), downstream services have already consumed a phantom event; if the broker call hangs, you hold database locks open.
2. **Omitting Consumer Deduplication**: An outbox guarantees **at-least-once** delivery, never exactly-once delivery. Always propagate a stable `event_id` (UUID) in message headers/payload and deduplicate on the consumer side via a `processed_events` unique constraint or idempotent upsert.
3. **Dropping Partition Keys (`aggregate_id`)**: Publishing events without keying by `aggregate_id` lets Kafka/SQS distribute `OrderCreated` and `OrderCancelled` across different partitions or workers, causing out-of-order processing.
4. **Assuming `SERIAL`/`BIGSERIAL` IDs Commit in Order**: Under concurrent transactions, transaction #2 (`id = 102`) may commit *before* transaction #1 (`id = 101`). If a cursor-based poller tracks `last_seen_id = 102`, it permanently skips `101`. Use `published_at IS NULL` with `SKIP LOCKED` (or WAL-based CDC), which only observes committed rows.
5. **Letting the Outbox Table Grow Unbounded**: Retaining millions of published rows bloats indexes and slows autovacuum. Either delete rows immediately upon publish (when audit history isn't needed in the outbox), run a scheduled retention cleanup (`DELETE FROM outbox_events WHERE published_at < now() - interval '7 days'`), or use daily range partitions you can `DROP` instantaneously.
