# Concurrency Patterns Taxonomy

**Navigation:** [← Home](./README.md) | [Next: Decision Matrix →](./decision-matrix.md)

---

## Complete Taxonomy of Concurrency Models

### Visual Taxonomy

```
Concurrency Models
├── Shared-Memory Concurrency
│   ├── Synchronization-Based
│   │   ├── Locks & Mutexes
│   │   │   ├── Mutex (Mutual Exclusion)
│   │   │   ├── Recursive Mutex
│   │   │   ├── Read-Write Lock
│   │   │   ├── Spinlock
│   │   │   └── Semaphore (Counting, Binary)
│   │   ├── Monitors
│   │   │   ├── Condition Variables
│   │   │   └── Java synchronized blocks
│   │   └── Barriers & Latches
│   │       ├── Barrier (cyclic)
│   │       ├── CountDownLatch
│   │       └── Phaser
│   │
│   ├── Lock-Free & Wait-Free
│   │   ├── Atomic Operations
│   │   │   ├── Compare-And-Swap (CAS)
│   │   │   ├── Load-Linked/Store-Conditional (LL/SC)
│   │   │   ├── Fetch-And-Add (FAA)
│   │   │   └── Test-And-Set (TAS)
│   │   ├── Lock-Free Data Structures
│   │   │   ├── Treiber Stack
│   │   │   ├── Michael-Scott Queue
│   │   │   ├── Lock-Free Hash Map
│   │   │   └── Lock-Free Skip List
│   │   └── Memory Reclamation
│   │       ├── Hazard Pointers
│   │       ├── Epoch-Based Reclamation (EBR)
│   │       └── Reference Counting
│   │
│   └── Transactional Memory
│       ├── Software Transactional Memory (STM)
│       ├── Hardware Transactional Memory (HTM)
│       └── Hybrid TM
│
├── Message-Passing Concurrency
│   ├── Channels
│   │   ├── Go Channels (CSP)
│   │   ├── Rust Channels (mpsc, crossbeam)
│   │   └── Buffered vs Unbuffered
│   │
│   ├── Actor Model
│   │   ├── Erlang/Elixir (BEAM)
│   │   ├── Akka (JVM)
│   │   ├── Orleans (.NET)
│   │   └── Ray (Python)
│   │
│   └── Message Queues
│       ├── In-Memory (ZeroMQ, nanomsg)
│       ├── Persistent (RabbitMQ, Kafka)
│       └── Cloud (SQS, Pub/Sub)
│
├── Event-Driven / Reactive
│   ├── Event Loops
│   │   ├── JavaScript (Node.js, Browser)
│   │   ├── Python asyncio
│   │   ├── Vert.x (JVM)
│   │   └── libuv (C)
│   │
│   ├── Reactive Streams
│   │   ├── RxJS (JavaScript)
│   │   ├── Project Reactor (Java)
│   │   ├── Akka Streams
│   │   └── ReactiveX
│   │
│   └── Callbacks & Promises/Futures
│       ├── Callbacks (continuation-passing)
│       ├── Promises/Futures
│       ├── Async/Await
│       └── Coroutines
│
├── Data-Parallel
│   ├── SIMD (Single Instruction Multiple Data)
│   ├── GPU Computing
│   │   ├── CUDA
│   │   ├── OpenCL
│   │   └── Vulkan Compute
│   ├── MapReduce / Spark
│   └── Vectorized Operations (NumPy, Polars)
│
└── Hybrid / Structured
    ├── Structured Concurrency
    │   ├── Python Trio
    │   ├── Kotlin Coroutines
    │   └── Swift async/await
    │
    ├── Fork-Join
    │   ├── Java ForkJoinPool
    │   ├── Cilk
    │   └── OpenMP
    │
    └── Work-Stealing
        ├── Go Scheduler
        ├── Rust Tokio
        └── Java ForkJoinPool
```

---

## Taxonomy by Synchronization Primitive

### 1. Locks & Mutexes

| Type | Blocking | Recursive | Priority | Use Case |
|------|----------|-----------|----------|----------|
| Mutex | Yes | No | None | General mutual exclusion |
| Recursive Mutex | Yes | Yes | None | Nested lock acquisition |
| Read-Write Lock | Yes | N/A | Readers preferred | Read-heavy workloads |
| Spinlock | No (busy-wait) | No | None | Very short critical sections |
| Semaphore (Binary) | Yes | N/A | None | Signaling between threads |
| Semaphore (Counting) | Yes | N/A | None | Resource counting |

### 2. Atomic Operations

| Operation | Consensus Number | ABA-Safe | Hardware Support |
|-----------|------------------|----------|------------------|
| Read/Write | 1 | N/A | Universal |
| Test-And-Set | 2 | No | x86, ARM |
| Fetch-And-Add | 2 | No | x86, ARM |
| Swap | 2 | No | x86, ARM |
| Compare-And-Swap | ∞ | No | x86, ARM, POWER |
| Load-Linked/Store-Conditional | ∞ | Yes | ARM, RISC-V, POWER |

### 3. Progress Guarantees

```
Hierarchy (Strongest → Weakest):

Wait-Free
  ├── Definition: Every thread makes progress in finite steps
  ├── Example: Wait-free counter (per-thread)
  └── Cost: High complexity, helping overhead

Lock-Free
  ├── Definition: System makes progress (some thread completes)
  ├── Example: Treiber stack, Michael-Scott queue
  └── Cost: CAS retries, memory reclamation

Obstruction-Free
  ├── Definition: Thread makes progress if alone
  ├── Example: Adaptive algorithms with fallback
  └── Cost: No progress guarantee under contention

Blocking
  ├── Definition: Thread may block indefinitely
  ├── Example: Mutex-based algorithms
  └── Cost: Deadlock risk, priority inversion
```

---

## Taxonomy by Language

### Python

```
Python Concurrency
├── Threading (Preemptive, GIL-limited)
│   ├── threading.Thread
│   ├── threading.Lock, RLock, Semaphore
│   ├── queue.Queue (thread-safe)
│   └── concurrent.futures.ThreadPoolExecutor
│
├── Multiprocessing (Parallel, no GIL)
│   ├── multiprocessing.Process
│   ├── multiprocessing.Pool
│   ├── multiprocessing.Queue, Pipe
│   └── concurrent.futures.ProcessPoolExecutor
│
├── Asyncio (Cooperative, single-threaded)
│   ├── async/await
│   ├── asyncio.Task, asyncio.Future
│   ├── asyncio.Queue
│   └── asyncio.Semaphore, Lock
│
├── Subinterpreters (Parallel, Python 3.12+)
│   ├── Per-interpreter GIL (PEP 684)
│   ├── Isolated interpreters
│   └── Channel-based communication (PEP 554)
│
└── Libraries
    ├── Trio (structured concurrency)
    ├── Curio (alternative event loop)
    ├── Twisted (legacy event-driven)
    └── Ray (distributed actors)
```

### Go

```
Go Concurrency
├── Goroutines (Lightweight threads, M:N model)
├── Channels (CSP-style message passing)
│   ├── Buffered channels
│   ├── Unbuffered channels
│   └── select statement
├── sync Package
│   ├── sync.Mutex, sync.RWMutex
│   ├── sync.WaitGroup
│   ├── sync.Cond
│   └── sync.Once
├── sync/atomic Package
│   ├── atomic.Int32, Int64, etc.
│   └── atomic.Value
└── Context (cancellation, deadlines)
```

### Rust

```
Rust Concurrency
├── Ownership System (compile-time data race prevention)
│   ├── Send trait (safe to transfer ownership)
│   └── Sync trait (safe to share reference)
│
├── Threading (std::thread)
│   ├── Thread spawning
│   ├── JoinHandle
│   └── thread::spawn
│
├── Synchronization (std::sync)
│   ├── Mutex<T>, RwLock<T>
│   ├── Arc<T> (atomic reference counting)
│   ├── Barrier, Condvar
│   └── Once
│
├── Channels (std::sync::mpsc)
│   ├── channel() (unbuffered)
│   └── sync_channel() (buffered)
│
├── Async Rust
│   ├── async/await
│   ├── Future trait
│   ├── Tokio runtime
│   ├── async-std
│   └── crossbeam (lock-free utilities)
│
└── Atomics (std::sync::atomic)
    ├── AtomicBool, AtomicI32, etc.
    └── Ordering (Relaxed, Acquire, Release, SeqCst)
```

### Java

```
Java Concurrency
├── Threads (java.lang.Thread)
│   ├── Thread class
│   ├── Runnable interface
│   └── Virtual Threads (Project Loom, Java 21+)
│
├── Executors (java.util.concurrent)
│   ├── ThreadPoolExecutor
│   ├── ForkJoinPool
│   ├── ScheduledExecutorService
│   └── CompletableFuture
│
├── Synchronization
│   ├── synchronized keyword
│   ├── ReentrantLock, ReadWriteLock
│   ├── Semaphore, CountDownLatch, CyclicBarrier
│   └── Phaser
│
├── Concurrent Collections
│   ├── ConcurrentHashMap
│   ├── CopyOnWriteArrayList
│   ├── BlockingQueue (ArrayBlockingQueue, LinkedBlockingQueue)
│   └── ConcurrentSkipListMap
│
├── Atomics (java.util.concurrent.atomic)
│   ├── AtomicInteger, AtomicLong, AtomicBoolean
│   ├── AtomicReference
│   └── LongAdder, DoubleAdder
│
└── Memory Model (JMM)
    ├── volatile keyword
    ├── happens-before relationship
    └── final field semantics
```

### C++

```
C++ Concurrency
├── Threading (std::thread, C++11+)
│   ├── std::thread
│   ├── std::jthread (C++20, RAII)
│   └── thread_local
│
├── Synchronization (std::mutex, std::condition_variable)
│   ├── std::mutex, std::timed_mutex, std::recursive_mutex
│   ├── std::shared_mutex (read-write lock, C++17)
│   ├── std::lock_guard, std::unique_lock, std::scoped_lock
│   └── std::condition_variable
│
├── Atomics (std::atomic, C++11+)
│   ├── std::atomic<T>
│   ├── Memory orders:
│   │   ├── memory_order_relaxed
│   │   ├── memory_order_acquire / release
│   │   ├── memory_order_acq_rel
│   │   └── memory_order_seq_cst
│   └── Fences: atomic_thread_fence
│
├── Futures & Promises
│   ├── std::future, std::promise
│   ├── std::async
│   └── std::packaged_task
│
└── Coroutines (C++20)
    ├── co_await, co_yield, co_return
    ├── std::coroutine_handle
    └── Asio coroutines
```

### Erlang/Elixir

```
Erlang/Elixir Concurrency (BEAM)
├── Processes (Lightweight, isolated)
│   ├── spawn/1, spawn_link/1
│   ├── Per-process heap
│   └── Preemptive scheduling
│
├── Message Passing
│   ├── send (!) operator
│   ├── receive expression
│   └── Mailboxes (per-process queue)
│
├── OTP Framework
│   ├── GenServer (generic server)
│   ├── Supervisor (fault tolerance)
│   ├── Application (lifecycle management)
│   └── ETS/DETS (concurrent tables)
│
└── Links & Monitors
    ├── Process links (bidirectional)
    └── Monitors (unidirectional)
```

---

## Taxonomy by Memory Model

### Sequential Consistency (SC)

**Definition**: Global order of all operations respecting program order.

**Languages**: High-level Java (with `volatile`), C++ (`memory_order_seq_cst`), Rust (`SeqCst`)

**Hardware**: (Rare) Simple CPUs, TSO-like models

### Total Store Order (TSO)

**Definition**: Store buffers allowed; reads can bypass earlier writes.

**Languages**: C++ (default for x86), Rust (on x86)

**Hardware**: x86, x86-64

### Relaxed Memory Models

**Definition**: Extensive reordering allowed.

**Languages**: C++ (explicit memory orders), Rust (explicit orderings), Java (non-volatile reads/writes)

**Hardware**: ARM, RISC-V, POWER

---

## Taxonomy by Use Case

### High-Throughput Web Servers

**Best Models**:
- **Asyncio** (Python): Single-threaded event loop for I/O-bound
- **Goroutines + Channels** (Go): Lightweight concurrency
- **Virtual Threads** (Java 21+): Millions of threads
- **Tokio** (Rust): Zero-cost async

### Real-Time Systems

**Best Models**:
- **Wait-Free Algorithms**: Guaranteed per-thread progress
- **Lock-Free Structures**: Bounded worst-case
- **Priority-Based Scheduling**: Real-time OS features

### CPU-Bound Computation

**Best Models**:
- **Multiprocessing** (Python): Bypass GIL
- **Work-Stealing Pools**: Java ForkJoinPool, Go scheduler
- **Data Parallelism**: SIMD, GPU

### Distributed Systems

**Best Models**:
- **Actor Model**: Erlang/Elixir, Akka
- **Message Queues**: Kafka, RabbitMQ
- **CRDTs**: Eventual consistency

---

## Summary: Key Taxonomy Insights

1. **Shared-Memory** requires synchronization (locks or lock-free); **Message-Passing** avoids shared state.
2. **Progress guarantees** range from **blocking** (weakest) to **wait-free** (strongest).
3. **Language choice** determines available primitives: Python (GIL-limited), Go (CSP), Rust (ownership), Java (virtual threads).
4. **Memory models** range from **SC** (intuitive, slow) to **relaxed** (fast, subtle).
5. **Use case** drives model selection: I/O-bound → asyncio; CPU-bound → multiprocessing; distributed → actors.

---

**Next:** [Decision Matrix and Playbook →](./decision-matrix.md)

**Navigation:** [← Home](./README.md) | [Next →](./decision-matrix.md)
