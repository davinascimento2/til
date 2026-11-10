# Java 21 Virtual Threads & Structured Concurrency

Virtual Threads (Project Loom) decouple Java threads from OS threads (1:1 mapping replaced by M:N scheduling over Carrier Threads). This allows handling millions of concurrent blocking I/O operations without reactive programming overhead.

## 🚀 Key Advantages

- **High Throughput for I/O Bound Work**: Blocking calls (HTTP, JDBC, file system) unmount the virtual thread from the carrier thread instead of freezing an OS thread.
- **Simplified Stack Traces**: Traditional thread-per-request model with simple imperative debugging.
- **Zero Configuration**: Eliminates manual thread pool sizing for I/O workloads.

---

## 🛠️ Code Example: StructuredTaskScope

Structured concurrency ensures child subtasks terminate before the parent scope completes, preventing thread leaks and orphan executions.

```java
import java.util.concurrent.StructuredTaskScope;
import java.time.Duration;

public class OrderAggregationService {

    public record OrderSummary(String userDetails, String paymentStatus) {}

    public OrderSummary fetchOrderSummary(String orderId) throws InterruptedException {
        // ShutdownOnFailure stops remaining subtasks as soon as one fails
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            
            StructuredTaskScope.Subtask<String> userTask = 
                scope.fork(() -> fetchUserDetails(orderId));
            
            StructuredTaskScope.Subtask<String> paymentTask = 
                scope.fork(() -> verifyPayment(orderId));

            // Wait for both subtasks or first failure
            scope.join();
            scope.throwIfFailed();

            return new OrderSummary(userTask.get(), paymentTask.get());
        }
    }

    private String fetchUserDetails(String orderId) throws InterruptedException {
        Thread.sleep(50); // Simulating fast remote call
        return "User: Alice (Tier: Gold)";
    }

    private String verifyPayment(String orderId) throws InterruptedException {
        Thread.sleep(80); // Simulating payment gateway call
        return "Payment: Captured";
    }
}
```

---

## ⚠️ Anti-Patterns to Avoid

1. **Don't Pool Virtual Threads**: Virtual threads are cheap and disposable. Always create new ones instead of pooling (`Executors.newVirtualThreadPerTaskExecutor()`).
2. **Avoid Pinning inside `synchronized` blocks**: If a virtual thread blocks inside a `synchronized` block/method, it pins its carrier OS thread. Prefer `ReentrantLock`.
