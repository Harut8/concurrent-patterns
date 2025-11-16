# Concurrency Code Examples

**Navigation:** [← Decision Matrix](../decision-matrix.md) | [Home](../README.md)

---

## Table of Contents

1. [Python Examples](#python-examples)
2. [Cross-Language Comparisons](#cross-language-comparisons)
3. [Design Patterns](#design-patterns)
4. [Production-Ready Templates](#production-ready-templates)

---

## Python Examples

### Example 1: Producer-Consumer with Queue (Threading)

```python
import threading
import queue
import time
import random

def producer(q, stop_event):
    """Produce items until stopped."""
    item_id = 0
    while not stop_event.is_set():
        item = f"item-{item_id}"
        q.put(item)
        print(f"Produced: {item}")
        item_id += 1
        time.sleep(random.uniform(0.1, 0.5))
    
    # Signal consumers to stop (poison pill)
    q.put(None)

def consumer(q, consumer_id):
    """Consume items until poison pill received."""
    while True:
        item = q.get()
        if item is None:
            q.put(None)  # Pass poison pill to other consumers
            break
        
        print(f"Consumer {consumer_id} processing: {item}")
        time.sleep(random.uniform(0.2, 0.8))  # Simulate work
        q.task_done()

if __name__ == "__main__":
    q = queue.Queue(maxsize=10)
    stop_event = threading.Event()
    
    # Start producer
    prod_thread = threading.Thread(target=producer, args=(q, stop_event))
    prod_thread.start()
    
    # Start multiple consumers
    consumers = []
    for i in range(3):
        cons_thread = threading.Thread(target=consumer, args=(q, i))
        cons_thread.start()
        consumers.append(cons_thread)
    
    # Run for 5 seconds
    time.sleep(5)
    stop_event.set()
    
    # Wait for completion
    prod_thread.join()
    for cons in consumers:
        cons.join()
    
    print("All done!")
```

**Key Concepts**:
- `queue.Queue` is thread-safe (uses locks internally)
- `task_done()` and `join()` for synchronization
- Poison pill pattern for graceful shutdown

---

### Example 2: Async Web Scraper with Rate Limiting

```python
import asyncio
import aiohttp
from aiohttp import ClientSession
import time

async def fetch_url(session: ClientSession, url: str, semaphore: asyncio.Semaphore):
    """Fetch URL with rate limiting."""
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                html = await response.text()
                return {
                    'url': url,
                    'status': response.status,
                    'length': len(html)
                }
        except asyncio.TimeoutError:
            return {'url': url, 'error': 'Timeout'}
        except Exception as e:
            return {'url': url, 'error': str(e)}

async def scrape_urls(urls: list[str], max_concurrent: int = 10):
    """Scrape multiple URLs concurrently with rate limiting."""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async with ClientSession() as session:
        tasks = [fetch_url(session, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
    
    return results

async def main():
    urls = [f"https://example.com/page{i}" for i in range(100)]
    
    start = time.time()
    results = await scrape_urls(urls, max_concurrent=20)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if 'error' not in r)
    print(f"Scraped {successful}/{len(urls)} URLs in {elapsed:.2f}s")
    print(f"Throughput: {len(urls) / elapsed:.2f} URLs/sec")

if __name__ == "__main__":
    asyncio.run(main())
```

**Key Concepts**:
- `asyncio.Semaphore` for rate limiting
- Connection pooling via `ClientSession`
- Error handling with `asyncio.gather`
- Timeout handling

---

### Example 3: CPU-Bound Parallelism with Multiprocessing

```python
from multiprocessing import Pool, cpu_count
import time
import numpy as np

def monte_carlo_pi(n_samples: int) -> float:
    """Estimate π using Monte Carlo method."""
    # Generate random points in unit square
    x = np.random.uniform(-1, 1, n_samples)
    y = np.random.uniform(-1, 1, n_samples)
    
    # Count points inside unit circle
    inside_circle = (x**2 + y**2) <= 1
    pi_estimate = 4 * np.sum(inside_circle) / n_samples
    
    return pi_estimate

def parallel_monte_carlo_pi(total_samples: int, n_processes: int = None):
    """Estimate π using parallel Monte Carlo."""
    if n_processes is None:
        n_processes = cpu_count()
    
    samples_per_process = total_samples // n_processes
    
    with Pool(processes=n_processes) as pool:
        # Each process estimates π
        estimates = pool.map(monte_carlo_pi, [samples_per_process] * n_processes)
    
    # Average estimates
    final_estimate = np.mean(estimates)
    return final_estimate

if __name__ == "__main__":
    total_samples = 100_000_000
    
    # Sequential
    start = time.time()
    pi_seq = monte_carlo_pi(total_samples)
    seq_time = time.time() - start
    print(f"Sequential: π ≈ {pi_seq:.6f}, time = {seq_time:.2f}s")
    
    # Parallel
    start = time.time()
    pi_par = parallel_monte_carlo_pi(total_samples)
    par_time = time.time() - start
    print(f"Parallel:   π ≈ {pi_par:.6f}, time = {par_time:.2f}s")
    print(f"Speedup: {seq_time / par_time:.2f}x")
```

**Key Concepts**:
- `Pool.map` for embarrassingly parallel tasks
- NumPy releases GIL (true parallelism even in workers)
- Speedup proportional to cores

---

### Example 4: Hybrid Asyncio + Multiprocessing

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor
import time

def cpu_intensive_task(data: list[int]) -> int:
    """CPU-intensive computation."""
    return sum(x * x for x in data)

async def io_task(url: str) -> str:
    """Simulate I/O-bound task."""
    await asyncio.sleep(0.5)  # Simulate network delay
    return f"Data from {url}"

async def hybrid_pipeline(urls: list[str], process_pool: ProcessPoolExecutor):
    """Pipeline combining I/O and CPU tasks."""
    loop = asyncio.get_running_loop()
    
    # Phase 1: I/O-bound (concurrent with asyncio)
    io_tasks = [io_task(url) for url in urls]
    io_results = await asyncio.gather(*io_tasks)
    
    # Convert results to numeric data for CPU task
    data_batches = [[ord(c) for c in result] for result in io_results]
    
    # Phase 2: CPU-bound (parallel with multiprocessing)
    cpu_tasks = [
        loop.run_in_executor(process_pool, cpu_intensive_task, batch)
        for batch in data_batches
    ]
    cpu_results = await asyncio.gather(*cpu_tasks)
    
    return cpu_results

async def main():
    urls = [f"https://api.example.com/data/{i}" for i in range(20)]
    
    with ProcessPoolExecutor(max_workers=4) as process_pool:
        start = time.time()
        results = await hybrid_pipeline(urls, process_pool)
        elapsed = time.time() - start
        
        print(f"Processed {len(urls)} items in {elapsed:.2f}s")
        print(f"Results: {results[:5]}...")  # Show first 5

if __name__ == "__main__":
    asyncio.run(main())
```

**Key Concepts**:
- `run_in_executor` bridges asyncio and multiprocessing
- I/O concurrency with asyncio, CPU parallelism with processes
- Best of both worlds

---

### Example 5: Lock-Free Counter with Atomics (ctypes)

```python
from multiprocessing import Process, Value
import ctypes
import time

def increment_with_lock(counter, lock, n):
    """Thread-safe increment using lock."""
    for _ in range(n):
        with lock:
            counter.value += 1

def increment_atomic(counter, n):
    """Lock-free increment using atomic operations (simulated)."""
    # Note: Python doesn't have true lock-free atomics in stdlib
    # This uses Value which internally uses locks
    # For true atomics, need C extension or libraries
    for _ in range(n):
        # In C: __sync_fetch_and_add(&counter, 1)
        # In Python: No built-in, but Value is thread-safe
        counter.value += 1

if __name__ == "__main__":
    n_increments = 1_000_000
    n_processes = 4
    
    # Using Value with lock (default)
    counter = Value(ctypes.c_longlong, 0)
    
    start = time.time()
    processes = [
        Process(target=increment_atomic, args=(counter, n_increments))
        for _ in range(n_processes)
    ]
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    elapsed = time.time() - start
    
    print(f"Final counter: {counter.value}")
    print(f"Expected: {n_increments * n_processes}")
    print(f"Time: {elapsed:.2f}s")
    print(f"Throughput: {n_increments * n_processes / elapsed:.0f} ops/sec")
```

**Note**: Python lacks true lock-free atomics in stdlib. For production lock-free code:
- Use C extensions with `_Py_atomic_*` API
- Or use libraries like `python-atomics` (bindings to C atomics)

---

## Cross-Language Comparisons

### Pattern: Fan-Out/Fan-In

#### Python (Asyncio)

```python
import asyncio

async def worker(task_id: int) -> int:
    await asyncio.sleep(1)  # Simulate work
    return task_id * 2

async def fan_out_fan_in(tasks: list[int]) -> list[int]:
    # Fan-out: Create all tasks
    futures = [asyncio.create_task(worker(t)) for t in tasks]
    
    # Fan-in: Wait for all results
    results = await asyncio.gather(*futures)
    return results

# Usage
asyncio.run(fan_out_fan_in([1, 2, 3, 4, 5]))
```

#### Go

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

func worker(taskID int) int {
    time.Sleep(1 * time.Second)
    return taskID * 2
}

func fanOutFanIn(tasks []int) []int {
    results := make(chan int, len(tasks))
    var wg sync.WaitGroup
    
    // Fan-out
    for _, task := range tasks {
        wg.Add(1)
        go func(t int) {
            defer wg.Done()
            results <- worker(t)
        }(task)
    }
    
    // Close results when done
    go func() {
        wg.Wait()
        close(results)
    }()
    
    // Fan-in
    var output []int
    for result := range results {
        output = append(output, result)
    }
    
    return output
}

func main() {
    tasks := []int{1, 2, 3, 4, 5}
    results := fanOutFanIn(tasks)
    fmt.Println(results)
}
```

#### Rust (Tokio)

```rust
use tokio::task;
use std::time::Duration;

async fn worker(task_id: i32) -> i32 {
    tokio::time::sleep(Duration::from_secs(1)).await;
    task_id * 2
}

async fn fan_out_fan_in(tasks: Vec<i32>) -> Vec<i32> {
    // Fan-out
    let handles: Vec<_> = tasks
        .into_iter()
        .map(|t| task::spawn(worker(t)))
        .collect();
    
    // Fan-in
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    
    results
}

#[tokio::main]
async fn main() {
    let tasks = vec![1, 2, 3, 4, 5];
    let results = fan_out_fan_in(tasks).await;
    println!("{:?}", results);
}
```

**Comparison**:
- **Python**: Explicit task creation, `gather` for fan-in
- **Go**: Channels for results, WaitGroup for synchronization
- **Rust**: `task::spawn` for concurrency, `join handles` for results

---

## Design Patterns

### Pattern 1: Circuit Breaker

```python
import asyncio
from enum import Enum
from datetime import datetime, timedelta

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout_seconds=60):
        self.failure_threshold = failure_threshold
        self.timeout = timedelta(seconds=timeout_seconds)
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            if datetime.now() - self.last_failure_time > self.timeout:
                self.state = CircuitState.HALF_OPEN
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

# Usage
async def unreliable_service():
    # Simulate failing service
    import random
    if random.random() < 0.5:
        raise Exception("Service failed")
    return "Success"

async def main():
    cb = CircuitBreaker(failure_threshold=3, timeout_seconds=5)
    
    for i in range(10):
        try:
            result = await cb.call(unreliable_service)
            print(f"Attempt {i}: {result}")
        except Exception as e:
            print(f"Attempt {i}: {e}")
        await asyncio.sleep(1)

asyncio.run(main())
```

---

### Pattern 2: Actor Model (Simple Implementation)

```python
import asyncio
from typing import Any, Callable

class Actor:
    def __init__(self):
        self.mailbox = asyncio.Queue()
        self.running = False
    
    async def start(self):
        """Start processing messages."""
        self.running = True
        while self.running:
            message = await self.mailbox.get()
            await self.handle_message(message)
    
    async def handle_message(self, message: Any):
        """Override this in subclasses."""
        raise NotImplementedError
    
    async def send(self, message: Any):
        """Send message to actor."""
        await self.mailbox.put(message)
    
    def stop(self):
        """Stop actor."""
        self.running = False

class CounterActor(Actor):
    def __init__(self):
        super().__init__()
        self.count = 0
    
    async def handle_message(self, message: dict):
        action = message.get('action')
        
        if action == 'increment':
            self.count += 1
        elif action == 'get':
            reply_to = message.get('reply_to')
            if reply_to:
                await reply_to.put(self.count)
        elif action == 'stop':
            self.stop()

async def main():
    counter = CounterActor()
    
    # Start actor
    actor_task = asyncio.create_task(counter.start())
    
    # Send messages
    await counter.send({'action': 'increment'})
    await counter.send({'action': 'increment'})
    await counter.send({'action': 'increment'})
    
    # Get count
    reply_queue = asyncio.Queue()
    await counter.send({'action': 'get', 'reply_to': reply_queue})
    count = await reply_queue.get()
    print(f"Count: {count}")  # 3
    
    # Stop actor
    await counter.send({'action': 'stop'})
    await actor_task

asyncio.run(main())
```

---

## Production-Ready Templates

### Template 1: Async Web API Client

```python
import asyncio
import aiohttp
from typing import Optional, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self, base_url: str, max_concurrent: int = 10, timeout: int = 30):
        self.base_url = base_url
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()
    
    async def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """GET request with rate limiting and error handling."""
        url = f"{self.base_url}/{endpoint}"
        
        async with self.semaphore:
            try:
                async with self.session.get(url, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching {url}")
                raise
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching {url}: {e}")
                raise
    
    async def batch_get(self, endpoints: list[str]) -> list[Dict[str, Any]]:
        """Fetch multiple endpoints concurrently."""
        tasks = [self.get(endpoint) for endpoint in endpoints]
        return await asyncio.gather(*tasks, return_exceptions=True)

# Usage
async def main():
    async with APIClient("https://api.example.com", max_concurrent=20) as client:
        # Single request
        user = await client.get("users/123")
        
        # Batch requests
        endpoints = [f"users/{i}" for i in range(100)]
        results = await client.batch_get(endpoints)
        
        successful = sum(1 for r in results if not isinstance(r, Exception))
        print(f"Fetched {successful}/{len(endpoints)} successfully")

asyncio.run(main())
```

---

## Summary: Key Patterns

1. **Producer-Consumer**: Use `queue.Queue` (threading) or `asyncio.Queue` (asyncio)
2. **Fan-Out/Fan-In**: `asyncio.gather` or `multiprocessing.Pool.map`
3. **Rate Limiting**: `asyncio.Semaphore` or token bucket
4. **Circuit Breaker**: Fail fast, auto-recovery
5. **Actor Model**: Isolated state, message-passing
6. **Hybrid**: Combine asyncio (I/O) + multiprocessing (CPU)

---

**Next:** [Performance Comparisons →](../performance-comparison.md)

**Navigation:** [← Decision Matrix](../decision-matrix.md) | [Home](../README.md)
