# Concurrency Decision Matrix and Playbook

**Navigation:** [← Taxonomy](./taxonomy.md) | [Home](./README.md) | [Next: Code Examples →](./examples/README.md)

---

## Table of Contents

1. [Decision Tree](#decision-tree)
2. [Decision Matrix by Workload](#decision-matrix-by-workload)
3. [Language-Specific Recommendations](#language-specific-recommendations)
4. [Pattern Selection Flowchart](#pattern-selection-flowchart)
5. [Performance vs Complexity Trade-offs](#performance-vs-complexity-trade-offs)

---

## Decision Tree

### Start Here: Choosing Concurrency Strategy

```
┌─────────────────────────────────────┐
│ What type of workload?              │
└─────────────────┬───────────────────┘
                  │
      ┌───────────┴───────────┐
      │                       │
  CPU-Bound              I/O-Bound
      │                       │
      │                       │
  ┌───▼────────┐         ┌───▼────────┐
  │ Multicore? │         │ # of tasks? │
  └───┬────────┘         └───┬────────┘
      │                      │
   ┌──┴──┐              ┌────┴────┐
  Yes   No             Few      Many
   │     │              │          │
   │     └─→ Single    │          │
   │         Thread    │          │
   │                   │          │
   ▼                   ▼          ▼
Parallel          Threading   Asyncio
Strategy          (GIL OK)    (Event Loop)
   │
   ├─→ Python: multiprocessing, subinterpreters
   ├─→ Go: goroutines
   ├─→ Rust: rayon, std::thread
   ├─→ Java: ForkJoinPool, virtual threads
   └─→ C++: std::thread, OpenMP
```

### Detailed Decision Path

**Step 1: Characterize Your Workload**

```
Question 1: CPU-Bound or I/O-Bound?
├─ CPU-Bound: Computation-heavy (cryptography, ML, simulations)
│  → Needs parallel execution across cores
│  → Python: multiprocessing, Cython, NumPy
│  → Other: true parallelism (threads or coroutines with work-stealing)
│
└─ I/O-Bound: Waiting for external resources (network, disk, database)
   → Concurrency (not necessarily parallelism) is sufficient
   → Python: asyncio, threading
   → Go: goroutines
   → Rust: Tokio
   → Java: virtual threads (Project Loom)

Question 2: How many concurrent tasks?
├─ Few (< 100): Threading is fine
├─ Many (100-10K): Asyncio or lightweight threads (goroutines, virtual threads)
└─ Very Many (10K+): Event loop (asyncio, Tokio) or actors

Question 3: Need shared state?
├─ Yes: Shared-memory concurrency
│  ├─ Low contention: Fine-grained locks or lock-free
│  └─ High contention: Message-passing or partitioning
│
└─ No: Message-passing (actors, channels)
   → Easier reasoning, no locks, better for distributed systems
```

---

## Decision Matrix by Workload

### Matrix 1: Python Concurrency Selection

| Workload | Best Choice | Alternative | Avoid | Notes |
|----------|-------------|-------------|-------|-------|
| **I/O-Bound, Few Tasks** | `threading` | `asyncio` | `multiprocessing` | Threading is simpler for < 100 tasks |
| **I/O-Bound, Many Tasks** | `asyncio` | `threading` + pool | `multiprocessing` | Asyncio scales to 10K+ tasks |
| **CPU-Bound, Single Core** | Optimize algorithm | Cython, C extension | `multiprocessing` | No parallelism available |
| **CPU-Bound, Multi-Core** | `multiprocessing` | Subinterpreters (3.12+), Cython | `threading` | GIL prevents threading speedup |
| **Mixed (I/O + CPU)** | `asyncio` + `ProcessPoolExecutor` | `threading` + `ProcessPoolExecutor` | Pure `asyncio` | Offload CPU to processes |
| **Real-Time** | Avoid Python | C/C++ with wait-free | All Python options | GIL and GC introduce latency |
| **High-Throughput Web** | `asyncio` (+ uvloop) | `threading` (limited) | `multiprocessing` (overhead) | Event loop ideal for web |
| **Data Processing Pipeline** | `multiprocessing.Pool` | Dask, Ray | `threading` | Parallel map-reduce |
| **Background Tasks** | `threading.Thread` (daemon) | `asyncio.create_task` | `multiprocessing.Process` | Lightweight for simple tasks |

**Legend**:
- **Best Choice**: Optimal for this workload
- **Alternative**: Works but not ideal
- **Avoid**: Poor fit or anti-pattern

### Matrix 2: Cross-Language Concurrency Selection

| Use Case | Python | Go | Rust | Java | C++ | Erlang |
|----------|--------|----|----|------|------|--------|
| **Web Server** | asyncio + FastAPI | goroutines + net/http | Tokio + Actix | Virtual threads + Spring | Asio coroutines | Cowboy |
| **API Gateway** | asyncio + aiohttp | goroutines + channels | Tokio + hyper | Virtual threads + Reactor | Envoy (C++) | Phoenix |
| **CPU-Intensive** | multiprocessing + NumPy | goroutines | rayon + std::thread | ForkJoinPool | OpenMP, TBB | Avoid |
| **Real-Time** | ❌ (GIL + GC) | ⚠️ (GC pauses) | ✅ (no GC, control) | ⚠️ (GC pauses) | ✅ (full control) | ⚠️ (soft RT only) |
| **Microservices** | FastAPI + asyncio | net/http + goroutines | Actix + Tokio | Spring Boot | gRPC + C++ | Elixir + OTP |
| **Data Pipeline** | Dask, Ray, multiprocessing | goroutines + channels | rayon | Spark, Flink | TBB | Flow |
| **Chat/Messaging** | asyncio + WebSocket | goroutines + channels | Tokio + WS | Vert.x, Akka | Asio | ✅✅ (BEAM ideal) |
| **Game Server** | ⚠️ (latency issues) | goroutines | Tokio + ECS | Netty | ✅ (low latency) | ⚠️ (soft RT) |
| **ML Training** | PyTorch (C++ backend) | ❌ | Burn, candle | DL4J | libtorch | ❌ |
| **IoT/Edge** | MicroPython | TinyGo | ✅✅ (embedded) | ❌ (too heavy) | ✅ (bare metal) | ❌ |

**Legend**:
- ✅✅ = Excellent fit
- ✅ = Good fit
- ⚠️ = Usable with caveats
- ❌ = Poor fit

---

## Language-Specific Recommendations

### Python Playbook

#### When to Use Threading

```python
# Use Case: I/O-bound with few concurrent tasks
import threading
import requests

def fetch_url(url):
    response = requests.get(url)  # Blocks, but releases GIL
    return response.text

urls = ["https://example.com"] * 10

threads = [threading.Thread(target=fetch_url, args=(url,)) for url in urls]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

**When**:
- I/O-bound tasks (network, file I/O)
- < 100 concurrent tasks
- Using libraries that release GIL (requests, NumPy)
- Simpler code > marginal performance gain

**Avoid**:
- CPU-bound pure Python code
- 1000s of concurrent tasks

#### When to Use Asyncio

```python
# Use Case: I/O-bound with many concurrent tasks
import asyncio
import aiohttp

async def fetch_url(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

asyncio.run(main())
```

**When**:
- I/O-bound with 100+ concurrent tasks
- Web servers, API clients
- Real-time applications (WebSockets, streaming)
- Need fine-grained control over scheduling

**Avoid**:
- CPU-bound tasks
- Heavy use of blocking libraries
- Simple scripts (overkill)

#### When to Use Multiprocessing

```python
# Use Case: CPU-bound computation
from multiprocessing import Pool

def cpu_intensive(n):
    return sum(i * i for i in range(n))

if __name__ == "__main__":
    with Pool(processes=4) as pool:
        results = pool.map(cpu_intensive, [10_000_000] * 4)
```

**When**:
- CPU-bound tasks (computation, data processing)
- Multi-core machines available
- Tasks are independent (embarrassingly parallel)

**Avoid**:
- High communication overhead between tasks
- Large shared data (pickling overhead)
- Short-lived tasks (process creation overhead)

#### Hybrid Strategy

```python
# Best of Both Worlds: Asyncio + ProcessPoolExecutor
import asyncio
from concurrent.futures import ProcessPoolExecutor

def cpu_bound_task(data):
    # Expensive computation
    return process(data)

async def main():
    loop = asyncio.get_running_loop()
    
    with ProcessPoolExecutor() as pool:
        # I/O concurrency with asyncio
        data_futures = [fetch_data(url) for url in urls]
        data_list = await asyncio.gather(*data_futures)
        
        # CPU parallelism with processes
        cpu_futures = [
            loop.run_in_executor(pool, cpu_bound_task, data)
            for data in data_list
        ]
        results = await asyncio.gather(*cpu_futures)
    
    return results

asyncio.run(main())
```

**When**:
- Mixed I/O and CPU workload
- Need both high concurrency and parallelism
- Web servers with background processing

---

### Go Playbook

#### Goroutines + Channels (Standard Pattern)

```go
func main() {
    urls := []string{"url1", "url2", "url3"}
    results := make(chan string, len(urls))
    
    // Fan-out
    for _, url := range urls {
        go func(u string) {
            result := fetch(u)
            results <- result  // Send to channel
        }(url)
    }
    
    // Fan-in
    for i := 0; i < len(urls); i++ {
        fmt.Println(<-results)
    }
}
```

**When**: Nearly always (Go's strength)

**Best Practices**:
- Use buffered channels for known capacity
- Close channels when done producing
- Use `select` for multiplexing
- Use `context.Context` for cancellation

---

### Rust Playbook

#### Async Rust with Tokio

```rust
#[tokio::main]
async fn main() {
    let urls = vec!["url1", "url2", "url3"];
    
    let tasks: Vec<_> = urls.into_iter()
        .map(|url| tokio::spawn(fetch(url)))
        .collect();
    
    for task in tasks {
        let result = task.await.unwrap();
        println!("{}", result);
    }
}
```

**When**:
- I/O-bound async tasks
- Need zero-cost abstractions
- Async libraries available (tokio, async-std)

**Avoid**:
- CPU-bound (use `rayon` instead)
- Synchronous libraries (blocking)

#### Data Parallelism with Rayon

```rust
use rayon::prelude::*;

fn main() {
    let data: Vec<_> = (0..1_000_000).collect();
    
    let sum: i32 = data.par_iter()
        .map(|&x| x * x)
        .sum();
    
    println!("{}", sum);
}
```

**When**:
- CPU-bound data processing
- Embarrassingly parallel workloads
- Need automatic work-stealing

---

### Java Playbook

#### Virtual Threads (Java 21+)

```java
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    List<Future<String>> futures = urls.stream()
        .map(url -> executor.submit(() -> fetch(url)))
        .toList();
    
    for (Future<String> future : futures) {
        System.out.println(future.get());
    }
}
```

**When**:
- I/O-bound with many concurrent tasks (10K+)
- Blocking APIs (JDBC, traditional I/O)
- Need simplicity of threads + scalability of async

**Replaces**: Complex async code, thread pools

---

## Pattern Selection Flowchart

### Shared-State vs Message-Passing

```
Do you need shared state?
│
├─ No → Message-Passing
│  ├─ Single machine: Channels (Go, Rust), Queues (Python)
│  ├─ Distributed: Actors (Akka, Erlang), Message Queues (Kafka, RabbitMQ)
│  └─ Benefits: No locks, easier reasoning, naturally distributed
│
└─ Yes → Shared-State
   │
   ├─ Low Contention → Fine-Grained Locks or Lock-Free
   │  ├─ Simple: Mutex per resource
   │  ├─ Read-heavy: Read-Write Lock
   │  └─ High performance: Lock-free structures (CAS-based)
   │
   └─ High Contention → Reconsider Architecture
      ├─ Partition data (shard by key)
      ├─ Use message-passing instead
      └─ Batch operations to reduce contention
```

### Lock-Free vs Locks

```
Need synchronization?
│
├─ Critical section < 100ns → Spinlock
│
├─ Critical section < 10μs → Mutex
│
├─ Read-heavy workload → RWLock or Lock-Free
│
├─ Real-time / No blocking → Lock-Free or Wait-Free
│
└─ Complex logic → Mutex (simplicity > perf)
```

---

## Performance vs Complexity Trade-offs

### Trade-off Matrix

| Approach | Performance | Complexity | Correctness Risk | Best For |
|----------|-------------|------------|------------------|----------|
| **Single-Threaded** | ⭐ | ⭐ | ⭐⭐⭐⭐⭐ | Simple, I/O-light |
| **Mutex-Based** | ⭐⭐ | ⭐⭐ | ⭐⭐⭐ | General concurrency |
| **Lock-Free** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | High contention, low latency |
| **Wait-Free** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | Real-time critical |
| **Message-Passing** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | Distributed, isolation |
| **Asyncio/Event Loop** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | I/O-bound, many tasks |
| **Actors** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | Distributed, stateful services |
| **Data Parallelism** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ | SIMD, GPU, map-reduce |

**Legend**: ⭐ = Poor, ⭐⭐⭐⭐⭐ = Excellent

### Complexity Budget

**When to Invest in Complex Concurrency**:

| Criteria | Threshold | Recommendation |
|----------|-----------|----------------|
| **Throughput Gain** | > 2x | Consider lock-free |
| **Latency Reduction** | > 50% | Consider lock-free |
| **Tail Latency (p99)** | > 10x improvement | Consider wait-free |
| **Code Maintenance Cost** | High | Stick to locks or message-passing |
| **Team Expertise** | Low | Use libraries, avoid custom lock-free |

**Rule of Thumb**: Use the simplest approach that meets requirements. Lock-free is rarely necessary.

---

## Real-World Decision Examples

### Example 1: Web API Server

**Requirements**:
- 10,000 requests/second
- Mostly I/O (database queries, external APIs)
- Response time < 100ms

**Decision Path**:
1. I/O-Bound → Asyncio or lightweight threads
2. High concurrency → Event loop or virtual threads
3. Python → **Asyncio + FastAPI + uvloop**
4. Java → **Virtual Threads (Project Loom)**
5. Go → **Goroutines + net/http**
6. Rust → **Tokio + Actix-web**

### Example 2: Data Processing Pipeline

**Requirements**:
- Process 1TB CSV file
- CPU-bound transformations
- Multi-core machine

**Decision Path**:
1. CPU-Bound → Parallelism required
2. Embarrassingly parallel → Data parallelism
3. Python → **multiprocessing.Pool** or **Dask**
4. Java → **ForkJoinPool** or **Spark**
5. Rust → **Rayon**
6. C++ → **OpenMP**

### Example 3: Chat Server

**Requirements**:
- 100,000 concurrent connections
- Low latency (< 10ms)
- Message broadcasting

**Decision Path**:
1. I/O-Bound + High concurrency → Event loop or actors
2. Stateful per-connection → Actors ideal
3. Erlang/Elixir → **BEAM + OTP** (best fit!)
4. Go → **Goroutines + channels**
5. Rust → **Tokio + actor framework**
6. Python → **Asyncio** (but latency may be issue)

### Example 4: ML Model Training

**Requirements**:
- GPU acceleration
- Multi-node distributed training
- Python ecosystem

**Decision Path**:
1. CPU/GPU-Bound → Specialized frameworks
2. Python → **PyTorch** (C++ backend) or **TensorFlow**
3. Data-parallel across GPUs → Framework handles it
4. Multi-node → **torch.distributed** or **Horovod**

---

## Summary: Decision-Making Principles

1. **Start simple**: Use locks or message-passing before lock-free.
2. **Profile first**: Measure before optimizing. GIL contention may not be your bottleneck.
3. **Match tool to workload**: I/O-bound → asyncio/goroutines; CPU-bound → multiprocessing/threads.
4. **Complexity budget**: Only invest in complex concurrency (lock-free, wait-free) if clearly justified.
5. **Language constraints matter**: Python GIL limits parallelism; Go/Rust/Java provide better CPU-bound concurrency.
6. **Distributed requires rethinking**: Message-passing (actors, queues) over shared-state.
7. **Use libraries**: Don't roll your own lock-free structures (use Java `ConcurrentHashMap`, Rust `crossbeam`, etc.).

---

**Next:** [Code Examples →](./examples/README.md)

**Navigation:** [← Taxonomy](./taxonomy.md) | [Home](./README.md) | [Next →](./examples/README.md)
