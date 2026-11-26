# Bounded Concurrency & Backpressure with Python `asyncio.TaskGroup`

Running `asyncio.gather(*[fetch(u) for u in urls])` over thousands of items triggers **unbounded fan-out**: it opens thousands of concurrent sockets simultaneously, exhausting file descriptors, database connection pools, and memory—or getting immediately throttled with `429 Too Many Requests`. Conversely, firing background work with bare `asyncio.create_task()` creates **untracked orphan tasks** whose exceptions disappear silently or outlive the request scope.

Python 3.11 introduced `asyncio.TaskGroup` (structured concurrency) and `asyncio.timeout()`. Pairing `TaskGroup` with `asyncio.Semaphore` or a bounded `asyncio.Queue` gives you deterministic task lifetimes, fail-fast cancellation, and explicit backpressure.

## 🚀 Core Guarantees

- **Structured Lifetime**: Every child task created inside `async with asyncio.TaskGroup()` is guaranteed to finish—or be cancelled and awaited—before control leaves the block. No leaked background tasks.
- **Fail-Fast Cancellation & `ExceptionGroup`**: If any task raises an unhandled exception, `TaskGroup` automatically cancels all sibling tasks, waits for them to clean up, and surfaces all failures together as an `ExceptionGroup` (caught with `except*`).
- **Bounded Resource Pressure**: Combining `TaskGroup` with `asyncio.Semaphore` caps concurrent in-flight I/O, while a bounded `asyncio.Queue(maxsize=N)` applies backpressure to fast producers so memory stays flat.

---

## 🛠️ Production Patterns

### 1. Bounded Fan-Out with `TaskGroup` + `Semaphore` + `timeout`

Use this pattern when the input collection is moderate in size (tens to a few thousand items) and you want to cap concurrent downstream calls while enforcing a strict overall deadline.

```python
import asyncio
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class FetchResult:
    item_id: str
    payload: dict[str, object]


class TransientUpstreamError(Exception):
    pass


async def fetch_item(
    item_id: str,
    semaphore: asyncio.Semaphore,
) -> FetchResult:
    # Acquire inside the task so concurrency is capped without blocking task scheduling
    async with semaphore:
        async with asyncio.timeout(2.0):
            await asyncio.sleep(0.05)  # Simulated async HTTP/DB I/O
            if item_id == "bad-item":
                raise TransientUpstreamError(f"upstream 503 for {item_id}")
            return FetchResult(item_id=item_id, payload={"ok": True})


async def fetch_all_bounded(
    item_ids: Iterable[str],
    *,
    max_concurrency: int = 20,
    total_timeout_s: float = 10.0,
) -> list[FetchResult]:
    semaphore = asyncio.Semaphore(max_concurrency)
    tasks: list[asyncio.Task[FetchResult]] = []

    try:
        async with asyncio.timeout(total_timeout_s):
            async with asyncio.TaskGroup() as tg:
                for item_id in item_ids:
                    tasks.append(tg.create_task(fetch_item(item_id, semaphore)))
    except* TransientUpstreamError as eg:
        # Handle known domain errors grouped from one or more failed subtasks
        raise RuntimeError(
            f"Aborted batch after {len(eg.exceptions)} upstream failure(s)"
        ) from eg

    # Safe to read .result(): TaskGroup guarantees all tasks completed cleanly here
    return [t.result() for t in tasks]
```

### 2. Streaming Backpressure with a Fixed Worker Pool (`asyncio.Queue`)

For large or unbounded streams (e.g., reading millions of rows from a cursor or Kafka topic), creating one `Task` per item still allocates millions of coroutine objects in RAM. Instead, spawn a fixed pool of workers inside a `TaskGroup` pulling from a bounded `asyncio.Queue(maxsize=...)`.

```python
import asyncio
from collections.abc import AsyncIterator

_SENTINEL = object()


async def process_stream(
    items: AsyncIterator[str],
    *,
    worker_count: int = 8,
    queue_capacity: int = 64,
) -> None:
    queue: asyncio.Queue[str | object] = asyncio.Queue(maxsize=queue_capacity)

    async def worker(worker_id: int) -> None:
        while True:
            item = await queue.get()
            try:
                if item is _SENTINEL:
                    return
                # Process single item (replace with DB write / RPC call)
                await asyncio.sleep(0.02)
            finally:
                queue.task_done()

    async def producer() -> None:
        async for item in items:
            # Blocks automatically when queue hits queue_capacity -> backpressure
            await queue.put(item)

        # Normal completion: send one sentinel per worker so each exits cleanly.
        # If producer or any worker raises instead, TaskGroup cancels the rest automatically.
        for _ in range(worker_count):
            await queue.put(_SENTINEL)

    async with asyncio.TaskGroup() as tg:
        for i in range(worker_count):
            tg.create_task(worker(i), name=f"worker-{i}")
        tg.create_task(producer(), name="producer")
```

> **Tip**: In Python 3.13+, `queue.shutdown()` replaces the `_SENTINEL` boilerplate cleanly by raising `asyncio.QueueShutDown` in waiting workers as soon as the queue is drained or aborted.

---

## ⚠️ Common Pitfalls to Avoid

1. **Catching `BaseException` Without Re-Raising `CancelledError`**: `TaskGroup` relies on `asyncio.CancelledError` (a `BaseException` since Python 3.8) to abort sibling tasks when one task fails. Swallowing `except BaseException:` or bare `except:` breaks structured cancellation and can hang the entire group indefinitely.
2. **Spawning 100k Tasks Around a `Semaphore`**: Calling `tg.create_task()` in a loop over 100,000 items eagerly allocates 100,000 `Task` frames in memory even if `Semaphore(20)` limits execution. Use a bounded `asyncio.Queue` with a fixed worker pool whenever input size is unbounded or large.
3. **Awaiting `queue.join()` Inside the Wrong Scope**: Calling `await queue.join()` inside the `TaskGroup` block can deadlock if a worker crashes—because `TaskGroup` cancels siblings only when exiting the `async with` block or when the task calling `join()` gets cancelled. Let workers exit cleanly via sentinels (or `queue.shutdown()`) and let `TaskGroup` await them.
4. **Blocking the Event Loop Inside Async Tasks**: Running synchronous I/O (`requests.get`, `time.sleep`, synchronous DB drivers) or heavy CPU loops inside a `TaskGroup` stalls the single event-loop thread, starving every other concurrent task and triggering false `asyncio.timeout` failures. Offload blocking calls with `await asyncio.to_thread(...)`.
5. **Reusing `except` Instead of `except*` Around `TaskGroup`**: `TaskGroup` wraps task errors in `ExceptionGroup` (even when only a single task fails). Plain `except ValueError:` will **not** catch a `ValueError` raised inside a `TaskGroup`—always use `except* ValueError:` to unwrap and handle grouped exceptions properly.
