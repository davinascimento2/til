# Zero-Downtime Graceful Shutdown & Connection Draining

During rolling deployments, autoscaling scale-downs, or node drains, orchestrators terminate containers by sending `SIGTERM` followed by `SIGKILL` after a grace period. If a service exits immediately upon receiving `SIGTERM`—or keeps accepting new requests while shutting down—clients receive `502 Bad Gateway` / `ECONNRESET` errors, and background jobs leave partial writes or orphaned locks.

**Graceful shutdown** coordinates two parallel timelines: the **load balancer / orchestrator** removing the instance from active endpoints, and the **application process** stopping new ingress, draining in-flight requests, and flushing downstream resources before exiting cleanly.

## 🚀 Core Guarantees

- **Zero Dropped In-Flight Requests**: Active HTTP/gRPC requests complete normally before listeners and connection pools close.
- **No `502`s During Endpoint Propagation**: Accounting for asynchronous endpoint removal (`kube-proxy`, ALB target deregistration) prevents traffic from hitting an already-closed socket.
- **Clean Resource Finalization**: Background workers, message consumers, telemetry buffers, and database pools flush and close in dependency order.
- **Bounded Exit Time**: Shutdown contexts enforce hard deadlines so a stuck downstream call does not trigger an ungraceful `SIGKILL` mid-cleanup.

---

## 🛠️ Shutdown Lifecycle Timeline

```text
t = 0s   Orchestrator sends SIGTERM & marks Pod Terminating (async endpoint removal)
t = 0-5s Readiness probe returns 503; preStop sleep absorbs kube-proxy/LB propagation lag
t = 5s   Server stops accepting new connections (Close/Shutdown), keeps active ones open
t = 5-25s In-flight HTTP requests & worker jobs finish within shutdown timeout
t = 25s  Flush OpenTelemetry/logs -> Close Kafka/Queue consumers -> Close DB pool -> Exit 0
t = 30s  terminationGracePeriodSeconds expires -> SIGKILL (only if process hung)
```

### 1. Application Shutdown Sequence (Go)

Using `signal.NotifyContext` decouples OS signal handling from server teardown, while an explicit readiness flag allows load balancers to detect draining immediately.

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	var shuttingDown atomic.Bool
	db := mustOpenDB() // *sql.DB connection pool

	// Base context for in-flight requests; canceled only if shutdown deadline expires
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz/ready", func(w http.ResponseWriter, _ *http.Request) {
		if shuttingDown.Load() {
			// Immediately fail readiness so load balancers stop routing new traffic
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if err := db.PingContext(rootCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		// Handler inherits rootCtx via BaseContext; stays alive during draining
		select {
		case <-time.After(150 * time.Millisecond):
			w.WriteHeader(http.StatusCreated)
		case <-r.Context().Done():
			http.Error(w, "request canceled", http.StatusServiceUnavailable)
		}
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return rootCtx },
	}

	// Listen for SIGINT / SIGTERM
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server crashed", "err", err)
			os.Exit(1)
		}
	}()

	<-sigCtx.Done()
	logger.Info("shutdown signal received, entering drain phase")
	shuttingDown.Store(true)

	// Optional buffer if orchestrator lacks preStop hook; keeps listener open during LB deregistration
	time.Sleep(2 * time.Second)

	// Drain active HTTP connections with a bounded timeout (< terminationGracePeriodSeconds)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelDrain()

	if err := srv.Shutdown(drainCtx); err != nil {
		logger.Error("forced http shutdown after timeout", "err", err)
		cancelRoot() // Abort any hung request contexts
		_ = srv.Close()
	}

	// Teardown downstream dependencies only AFTER all HTTP handlers have returned
	if err := db.Close(); err != nil {
		logger.Error("failed to close db pool", "err", err)
	}

	logger.Info("graceful shutdown complete")
}

func mustOpenDB() *sql.DB { return nil }
```

### 2. Kubernetes Pod Lifecycle Alignment

In Kubernetes, `SIGTERM` and EndpointSlice removal happen **concurrently**, not sequentially. A `preStop` sleep ensures `kube-proxy`, Ingress controllers, or cloud ALBs finish propagating the endpoint removal before the application closes its listening socket.

```yaml
spec:
  terminationGracePeriodSeconds: 35
  containers:
    - name: api
      image: ghcr.io/acme/orders-api:1.4.2
      readinessProbe:
        httpGet:
          path: /healthz/ready
          port: 8080
        periodSeconds: 2
        failureThreshold: 1
      lifecycle:
        preStop:
          exec:
            # Give kube-proxy / Ingress controllers time to remove pod IP from iptables/upstreams
            command: ["sh", "-c", "sleep 5"]
```

> **Tip**: Always keep `preStop sleep + app drain buffer + shutdown timeout < terminationGracePeriodSeconds` (e.g., `5s + 2s + 20s = 27s < 35s`). Leave a `5–10s` margin for DB pool closure, log flushing, and container runtime overhead before `SIGKILL` fires.

---

## ⚠️ Common Pitfalls to Avoid

1. **Closing the Listening Socket Immediately on `SIGTERM`**: Because Kubernetes EndpointSlice updates and cloud load balancer deregistration are asynchronous, traffic continues arriving for `2–5` seconds after `SIGTERM`. Rejecting connections immediately causes transient `502 Bad Gateway` errors.
2. **Closing the Database Pool Before Draining HTTP Handlers**: If `db.Close()` runs concurrently with `srv.Shutdown()`, in-flight requests still executing queries fail with `sql: database is closed`. Always sequence teardown: **stop ingress → drain handlers/workers → flush telemetry → close DB/cache pools**.
3. **Wrapping the Server `BaseContext` in `signal.NotifyContext`**: Passing the OS signal context directly as `BaseContext` cancels every active `r.Context()` the instant `SIGTERM` arrives, aborting valid in-flight DB queries instead of letting them finish. Use a separate `rootCtx` that is only canceled if the drain timeout expires.
4. **Running Entrypoint via Shell Form (`CMD node server.js`)**: Shell wrappers like `/bin/sh -c` often run as PID 1 without forwarding `SIGTERM` to child processes. Always use JSON exec form (`CMD ["/app/server"]`) so the application receives `SIGTERM` directly.
5. **Ignoring HTTP Keep-Alive Connections**: Upstream proxies (Nginx, Envoy, ALB) reuse persistent TCP connections. `http.Server.Shutdown` closes idle keep-alive sockets gracefully, whereas custom TCP servers or `srv.Close()` abruptly send TCP RST packets, breaking clients mid-reuse.
