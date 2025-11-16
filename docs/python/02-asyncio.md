# Part II.2: Asyncio - Event Loop, Tasks, and Coroutines

**Navigation:** [← GIL Internals](./01-gil-internals.md) | [Home](../README.md) | [Next: Threading & Multiprocessing →](./03-threading-multiprocessing.md)

---

## Table of Contents

1. [Asyncio Fundamentals](#asyncio-fundamentals)
2. [Event Loop Internals](#event-loop-internals)
3. [Coroutines and Tasks](#coroutines-and-tasks)
4. [Concurrency Patterns](#concurrency-patterns)
5. [Performance Optimization](#performance-optimization)
6. [Common Pitfalls](#common-pitfalls)

---

## Asyncio Fundamentals

### What is Asyncio?

**Asyncio** is Python's built-in library for writing concurrent code using the **async/await** syntax. It provides:
- Single-threaded cooperative multitasking
- Event loop for scheduling coroutines
- Non-blocking I/O operations
- High-level APIs for networking, subprocesses, queues

**Key Principle**: Cooperative concurrency—coroutines voluntarily yield control.

### Asyncio vs Threading vs Multiprocessing

| Feature | Asyncio | Threading | Multiprocessing |
|---------|---------|-----------|-----------------|
| Concurrency Type | Cooperative | Preemptive | Parallel |
| GIL Impact | None (single thread) | Limited by GIL | No GIL (separate processes) |
| Overhead | Very low | Low | High |
| Best For | I/O-bound | I/O-bound, some CPU | CPU-bound |
| Memory Sharing | Easy | Easy | Complex (IPC) |
| Scalability | 10K+ tasks | 10s-100s threads | Limited by cores |
| Debugging | Moderate | Hard | Harder |

### When to Use Asyncio

**Use Asyncio When**:
- High number of I/O operations (network, files, databases)
- Many concurrent connections (web servers, chat apps)
- Latency-sensitive applications
- Need fine-grained control over scheduling

**Don't Use Asyncio When**:
- CPU-bound tasks (no parallelism in single thread)
- Blocking libraries (unless wrapped with `run_in_executor`)
- Simple scripts (overhead not worth it)

---

## Event Loop Internals

### Event Loop Architecture

The **event loop** is the core of asyncio—it schedules and executes asynchronous tasks.

**Simplified Event Loop**:

```python
class SimpleEventLoop:
    def __init__(self):
        self.ready_queue = deque()  # Ready-to-run tasks
        self.io_selector = selectors.DefaultSelector()  # I/O multiplexing
        self.scheduled = []  # Delayed tasks (heap)
    
    def run_forever(self):
        while True:
            # 1. Run all ready tasks
            while self.ready_queue:
                task = self.ready_queue.popleft()
                try:
                    task.step()  # Execute one step
                except StopIteration:
                    task.set_result(None)
            
            # 2. Process I/O events
            timeout = self._calculate_timeout()  # Next scheduled task time
            events = self.io_selector.select(timeout)
            for key, mask in events:
                callback = key.data
                self.ready_queue.append(callback)
            
            # 3. Process scheduled (delayed) tasks
            now = time.time()
            while self.scheduled and self.scheduled[0].when <= now:
                task = heappop(self.scheduled)
                self.ready_queue.append(task.callback)
```

**Real asyncio event loop** (CPython implementation):

1. **`asyncio.SelectorEventLoop`** (Unix/Windows default): Uses `select.select()`, `select.epoll()`, or `select.kqueue()`.
2. **`asyncio.ProactorEventLoop`** (Windows IOCP): Uses `IOCP` for better Windows I/O performance.
3. **`uvloop`** (optional): Fast event loop based on `libuv` (2-4x faster than default).

### I/O Multiplexing

**How asyncio waits for I/O without blocking**:

```python
import selectors
import socket

selector = selectors.DefaultSelector()

# Register socket for READ events
sock = socket.socket()
sock.setblocking(False)
sock.connect_ex(('example.com', 80))

def handle_read():
    data = sock.recv(4096)
    print(f"Received: {data}")

selector.register(sock, selectors.EVENT_READ, handle_read)

# Event loop polls for ready sockets
events = selector.select(timeout=1.0)
for key, mask in events:
    callback = key.data
    callback()  # Call handle_read when socket is ready
```

**Asyncio abstracts this** with `await`:

```python
async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()  # Yields control while waiting
```

### Event Loop vs GIL

**Key Insight**: Asyncio runs in a **single thread** → no GIL contention!

```python
import asyncio

async def cpu_bound():
    total = 0
    for i in range(10_000_000):
        total += i * i
    return total

# This is SLOW—no parallelism!
# Single thread, GIL is irrelevant (only one thread)
await asyncio.gather(cpu_bound(), cpu_bound())
```

**For CPU-bound + asyncio**: Use `run_in_executor`:

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor

async def main():
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, cpu_bound)
    print(result)

asyncio.run(main())
# Now we get parallelism (process pool) + asyncio (event loop)
```

---

## Coroutines and Tasks

### Coroutines

**Coroutine**: A function defined with `async def` that can pause and resume.

```python
async def my_coroutine():
    print("Start")
    await asyncio.sleep(1)  # Yield control for 1 second
    print("End")
    return 42

# Create coroutine object (not executed yet)
coro = my_coroutine()

# Run it
result = asyncio.run(coro)  # Prints "Start", waits 1s, prints "End"
print(result)  # 42
```

**Coroutine States**:
1. **Created**: `async def` called → coroutine object
2. **Running**: Executing bytecode
3. **Suspended**: Awaiting another coroutine
4. **Finished**: Returned or raised exception

### Tasks

**Task**: A wrapper around a coroutine that schedules it on the event loop.

```python
async def say_hello():
    await asyncio.sleep(1)
    print("Hello")

async def main():
    # Create task (starts running immediately)
    task = asyncio.create_task(say_hello())
    
    # Do other work
    await asyncio.sleep(0.5)
    print("Doing other work...")
    
    # Wait for task to complete
    await task

asyncio.run(main())
# Output:
# Doing other work... (after 0.5s)
# Hello (after 1s total)
```

**Task vs Coroutine**:
- **Coroutine**: Lazy—must be awaited to run
- **Task**: Eager—starts running when created

### `await`, `asyncio.gather()`, and `asyncio.create_task()`

**`await`**: Wait for a single coroutine.

```python
result = await some_coroutine()
```

**`asyncio.create_task()`**: Schedule coroutine concurrently.

```python
task1 = asyncio.create_task(coro1())
task2 = asyncio.create_task(coro2())
await task1
await task2
# coro1 and coro2 run concurrently
```

**`asyncio.gather()`**: Run multiple coroutines concurrently, collect results.

```python
results = await asyncio.gather(coro1(), coro2(), coro3())
# All run concurrently, returns [result1, result2, result3]
```

**Performance Comparison**:

```python
import asyncio
import time

async def task(n):
    await asyncio.sleep(1)
    return n * 2

# Sequential (SLOW: 3 seconds)
async def sequential():
    r1 = await task(1)
    r2 = await task(2)
    r3 = await task(3)
    return [r1, r2, r3]

# Concurrent with gather (FAST: 1 second)
async def concurrent_gather():
    return await asyncio.gather(task(1), task(2), task(3))

# Concurrent with create_task (FAST: 1 second)
async def concurrent_tasks():
    t1 = asyncio.create_task(task(1))
    t2 = asyncio.create_task(task(2))
    t3 = asyncio.create_task(task(3))
    return await asyncio.gather(t1, t2, t3)

start = time.time()
asyncio.run(sequential())
print(f"Sequential: {time.time() - start:.2f}s")  # 3.0s

start = time.time()
asyncio.run(concurrent_gather())
print(f"Concurrent: {time.time() - start:.2f}s")  # 1.0s
```

---

## Concurrency Patterns

### 1. Rate Limiting with Semaphore

**Problem**: Limit concurrent requests (e.g., to avoid overwhelming a server).

```python
import asyncio
import aiohttp

async def fetch(session, url, semaphore):
    async with semaphore:  # At most N concurrent
        async with session.get(url) as response:
            return await response.text()

async def main():
    semaphore = asyncio.Semaphore(10)  # Max 10 concurrent requests
    
    urls = [f"https://example.com/page{i}" for i in range(100)]
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
    
    print(f"Fetched {len(results)} pages")

asyncio.run(main())
```

### 2. Timeout Handling

**Pattern**: Fail fast if operation takes too long.

```python
import asyncio

async def slow_operation():
    await asyncio.sleep(10)
    return "Done"

async def main():
    try:
        result = await asyncio.wait_for(slow_operation(), timeout=2.0)
    except asyncio.TimeoutError:
        print("Operation timed out")

asyncio.run(main())
```

### 3. Fan-Out / Fan-In (Map-Reduce)

**Pattern**: Distribute work, then aggregate results.

```python
import asyncio

async def process_item(item):
    await asyncio.sleep(0.1)  # Simulate work
    return item * 2

async def fan_out_fan_in(items):
    # Fan-out: Create tasks for all items
    tasks = [asyncio.create_task(process_item(item)) for item in items]
    
    # Fan-in: Wait for all results
    results = await asyncio.gather(*tasks)
    
    return sum(results)  # Aggregate

asyncio.run(fan_out_fan_in([1, 2, 3, 4, 5]))  # Returns 30
```

### 4. Producer-Consumer with Queues

**Pattern**: Decouple producers from consumers.

```python
import asyncio

async def producer(queue, n):
    for i in range(n):
        await asyncio.sleep(0.1)
        await queue.put(i)
        print(f"Produced {i}")
    await queue.put(None)  # Sentinel

async def consumer(queue):
    while True:
        item = await queue.get()
        if item is None:
            break
        await asyncio.sleep(0.2)
        print(f"Consumed {item}")
        queue.task_done()

async def main():
    queue = asyncio.Queue(maxsize=5)
    
    prod = asyncio.create_task(producer(queue, 10))
    cons = asyncio.create_task(consumer(queue))
    
    await prod
    await queue.join()  # Wait for all items processed
    await cons

asyncio.run(main())
```

### 5. Retry with Exponential Backoff

**Pattern**: Retry failed operations with increasing delays.

```python
import asyncio
import random

async def unreliable_operation():
    if random.random() < 0.7:
        raise Exception("Random failure")
    return "Success"

async def retry_with_backoff(coro_func, max_retries=5):
    for attempt in range(max_retries):
        try:
            return await coro_func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = 2 ** attempt  # Exponential: 1, 2, 4, 8, 16
            print(f"Retry {attempt + 1} after {delay}s due to {e}")
            await asyncio.sleep(delay)

asyncio.run(retry_with_backoff(unreliable_operation))
```

### 6. Cancellation and Cleanup

**Pattern**: Cancel tasks and clean up resources.

```python
import asyncio

async def long_task():
    try:
        print("Starting long task")
        await asyncio.sleep(10)
        print("Long task completed")
    except asyncio.CancelledError:
        print("Task was cancelled, cleaning up...")
        # Cleanup code here
        raise  # Re-raise to propagate cancellation

async def main():
    task = asyncio.create_task(long_task())
    
    await asyncio.sleep(2)
    task.cancel()  # Request cancellation
    
    try:
        await task
    except asyncio.CancelledError:
        print("Confirmed task cancellation")

asyncio.run(main())
```

**Best Practice**: Always handle `CancelledError` for cleanup (use `try/finally`).

---

## Performance Optimization

### 1. Use `uvloop` for 2-4x Speedup

**uvloop**: Drop-in replacement for asyncio's event loop, based on `libuv`.

```bash
pip install uvloop
```

```python
import asyncio
import uvloop

# Set uvloop as default event loop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Now all asyncio.run() calls use uvloop
asyncio.run(main())
```

**Benchmark** (typical results):

| Operation | asyncio | uvloop | Speedup |
|-----------|---------|--------|---------|
| HTTP requests | 10K req/s | 30K req/s | 3x |
| TCP echo server | 50K conn/s | 120K conn/s | 2.4x |

### 2. Avoid Blocking Operations

**BAD** (blocks event loop):

```python
import time

async def bad():
    time.sleep(1)  # BLOCKS entire event loop!
    return "done"
```

**GOOD** (yields control):

```python
async def good():
    await asyncio.sleep(1)  # Cooperative, doesn't block
    return "done"
```

**For truly blocking calls** (CPU-bound or blocking I/O):

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

def blocking_operation():
    # Some blocking library (e.g., requests, PIL)
    import time
    time.sleep(1)
    return "done"

async def main():
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, blocking_operation)
    print(result)

asyncio.run(main())
```

### 3. Limit Concurrency (Avoid Overload)

**Problem**: Too many concurrent tasks → memory exhaustion or remote server overload.

**Solution**: Use `Semaphore` or `asyncio.Queue` with bounded size.

```python
async def controlled_concurrency(urls, max_concurrent=10):
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_limit(url):
        async with semaphore:
            return await fetch(url)
    
    tasks = [fetch_with_limit(url) for url in urls]
    return await asyncio.gather(*tasks)
```

### 4. Batch I/O Operations

**Pattern**: Combine multiple small I/O ops into fewer large ones.

```python
# BAD: Many small writes
async def slow_writes(file, items):
    for item in items:
        await file.write(item)  # Many context switches

# GOOD: Batched writes
async def fast_writes(file, items):
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= 100:
            await file.write(''.join(batch))
            batch.clear()
    if batch:
        await file.write(''.join(batch))
```

### 5. Use Connection Pooling

**Pattern**: Reuse connections instead of creating new ones.

```python
import aiohttp

# BAD: New session per request
async def bad():
    for _ in range(100):
        async with aiohttp.ClientSession() as session:
            async with session.get('https://example.com') as resp:
                await resp.text()

# GOOD: Reuse session (connection pool)
async def good():
    async with aiohttp.ClientSession() as session:
        for _ in range(100):
            async with session.get('https://example.com') as resp:
                await resp.text()
```

**aiohttp automatically pools connections** within a session.

---

## Common Pitfalls

### 1. Forgetting `await`

**Problem**: Coroutine not executed.

```python
async def my_coro():
    return 42

# WRONG: Creates coroutine object but doesn't run it
result = my_coro()  # <coroutine object>
# RuntimeWarning: coroutine 'my_coro' was never awaited

# CORRECT:
result = await my_coro()  # 42
```

### 2. Blocking the Event Loop

**Problem**: One blocking call stalls all tasks.

```python
import asyncio
import time

async def task1():
    print("Task 1 start")
    time.sleep(2)  # BLOCKS event loop!
    print("Task 1 end")

async def task2():
    print("Task 2 start")
    await asyncio.sleep(1)
    print("Task 2 end")

asyncio.run(asyncio.gather(task1(), task2()))
# Output:
# Task 1 start
# (2 second pause - both tasks blocked!)
# Task 1 end
# Task 2 start
# Task 2 end
```

**Fix**: Use `await asyncio.sleep()` or `run_in_executor()`.

### 3. Mixing Asyncio with Synchronous Code

**Problem**: Can't call async functions from sync code.

```python
async def async_func():
    return 42

def sync_func():
    result = async_func()  # WRONG: Returns coroutine, not result
    # result = await async_func()  # WRONG: await only in async functions
    
    # CORRECT: Use asyncio.run (creates new event loop)
    result = asyncio.run(async_func())  # OK but heavyweight
```

**Best Practice**: Keep async and sync boundaries clean; prefer fully async or fully sync.

### 4. CancelledError Not Handled

**Problem**: Resource leaks when tasks are cancelled.

```python
async def task():
    connection = await open_connection()
    await asyncio.sleep(100)  # Might be cancelled here
    await connection.close()  # Never reached!

# CORRECT:
async def task():
    connection = await open_connection()
    try:
        await asyncio.sleep(100)
    finally:
        await connection.close()  # Always runs, even if cancelled
```

### 5. Sharing Mutable State

**Problem**: Race conditions despite single-threaded.

```python
counter = 0

async def increment():
    global counter
    temp = counter  # Read
    await asyncio.sleep(0)  # Yield control!
    counter = temp + 1  # Write (might be stale)

await asyncio.gather(*(increment() for _ in range(10)))
print(counter)  # Not 10! Race condition
```

**Why**: `await` can yield, allowing other tasks to run.

**Fix**: Use locks or avoid shared mutable state.

```python
counter = 0
lock = asyncio.Lock()

async def increment():
    global counter
    async with lock:
        temp = counter
        await asyncio.sleep(0)
        counter = temp + 1

await asyncio.gather(*(increment() for _ in range(10)))
print(counter)  # 10, correctly synchronized
```

---

## Summary: Asyncio Takeaways

1. **Asyncio is cooperative concurrency** in a single thread—ideal for I/O-bound tasks.
2. **Event loop schedules coroutines** using I/O multiplexing (select/epoll/kqueue).
3. **Use `uvloop` for 2-4x performance boost** (drop-in replacement).
4. **Avoid blocking calls**—use `await asyncio.sleep()` or `run_in_executor()`.
5. **Limit concurrency** with `Semaphore` to avoid overload.
6. **Handle `CancelledError`** for proper cleanup.
7. **Patterns**: Rate limiting, retries, fan-out/fan-in, producer-consumer.
8. **Asyncio is NOT for CPU-bound tasks**—use multiprocessing or `run_in_executor` with `ProcessPoolExecutor`.

---

**Next:** [Threading, Multiprocessing, and Subinterpreters →](./03-threading-multiprocessing.md)

**Navigation:** [← GIL Internals](./01-gil-internals.md) | [Home](../README.md) | [Next →](./03-threading-multiprocessing.md)
