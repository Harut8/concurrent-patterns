# Part I: Core Concurrency Theory & Memory Models

**Navigation:** [← Home](../README.md) | [Next: Herlihy & Shavit Concepts →](./02-herlihy-shavit.md)

---

## Table of Contents

1. [Introduction](#introduction)
2. [Fundamental Concepts](#fundamental-concepts)
3. [Threads vs Processes](#threads-vs-processes)
4. [Scheduling Strategies](#scheduling-strategies)
5. [Memory Models Overview](#memory-models-overview)
6. [Synchronization Primitives](#synchronization-primitives)
7. [Progress Guarantees](#progress-guarantees)
8. [Hardware Fundamentals](#hardware-fundamentals)

---

## Introduction

Concurrency is fundamentally about **managing multiple computations that may execute simultaneously**. This encompasses everything from multi-threaded applications on a single core (time-sliced concurrency) to truly parallel execution across multiple cores, distributed systems spanning data centers, and heterogeneous compute involving CPUs, GPUs, and specialized accelerators.

### Why Concurrency Matters

Modern systems demand concurrency for:
- **Performance**: Utilizing multiple cores and distributed resources
- **Responsiveness**: Keeping UI threads responsive during I/O
- **Scalability**: Handling thousands to millions of concurrent requests
- **Resource Efficiency**: Maximizing hardware utilization
- **Latency Hiding**: Overlapping computation with I/O operations

### The Fundamental Challenge

The core difficulty in concurrent programming stems from **shared mutable state**:
- Multiple threads accessing the same memory location
- Non-atomic operations executing concurrently
- Memory reordering by compilers and hardware
- Cache coherence protocols introducing subtle bugs

---

## Fundamental Concepts

### Concurrency vs Parallelism

**Concurrency** is a program structure—the composition of independently executing computations. **Parallelism** is simultaneous execution of (possibly related) computations.

```
Concurrency: Structure dealing with multiple things at once
Parallelism: Execution doing multiple things simultaneously
```

**Key Insight**: You can have concurrency without parallelism (single-core multitasking), but not parallelism without concurrency (parallel execution requires concurrent structure).

### Critical Sections

A **critical section** is a code region that accesses shared resources and must not be concurrently executed by more than one thread.

```python
# Critical section example
shared_counter = 0

def increment():
    global shared_counter
    # CRITICAL SECTION START
    temp = shared_counter  # Read
    temp = temp + 1        # Modify
    shared_counter = temp  # Write
    # CRITICAL SECTION END
```

**Race Condition**: The outcome depends on the non-deterministic ordering of operations across threads.

### Mutual Exclusion

**Mutual exclusion** ensures that when one thread is executing a critical section, no other thread is executing code for the same resource.

**Properties** (from Herlihy & Shavit):
1. **Safety (Mutual Exclusion)**: At most one thread in critical section
2. **Liveness (Progress)**: If a thread tries to enter, eventually some thread succeeds
3. **Fairness (Starvation-Freedom)**: Every thread that tries to enter eventually succeeds

---

## Threads vs Processes

### Process Model

**Process**: An isolated execution environment with:
- Separate memory address space
- Own file descriptors and resources
- IPC required for communication (pipes, sockets, shared memory)
- High creation/context-switch overhead

**Advantages**:
- Strong isolation and fault tolerance
- No shared memory issues (unless explicitly using shared memory)
- Can utilize multiple cores naturally

**Disadvantages**:
- High overhead (memory, creation time)
- Complex IPC mechanisms
- Difficult to share data efficiently

### Thread Model

**Thread**: A lightweight execution context within a process:
- Shared memory address space
- Shared file descriptors
- Low creation/context-switch overhead
- Each has own stack and registers

**Advantages**:
- Low overhead compared to processes
- Easy data sharing via shared memory
- Fast context switching

**Disadvantages**:
- Race conditions on shared data
- No fault isolation
- Complexity of synchronization

### Comparative Cost Analysis

| Operation | Time (approx) | Notes |
|-----------|---------------|-------|
| Thread creation | 1-10 μs | OS dependent |
| Process creation | 100-1000 μs | Fork overhead |
| Thread context switch | 1-5 μs | Cache effects matter |
| Process context switch | 10-100 μs | TLB flush, memory map change |
| Mutex lock/unlock | 25-100 ns | Uncontended |
| Mutex lock/unlock | 1-10 μs | Contended |

---

## Scheduling Strategies

### Preemptive Scheduling

**Preemptive**: OS can interrupt running thread at any time.
- Used by: POSIX threads, Java threads, C++ threads
- Pros: Fair CPU distribution, responsive
- Cons: Race conditions, requires synchronization

### Cooperative Scheduling

**Cooperative**: Thread voluntarily yields control.
- Used by: Python asyncio, JavaScript event loop, Go (with preemption)
- Pros: No locks needed for single-threaded event loops, predictable
- Cons: One blocking call stalls everything, requires discipline

### Work-Stealing

**Work-Stealing**: Idle threads steal work from busy threads' queues.
- Used by: Go scheduler, Java ForkJoinPool, Tokio (Rust)
- Pros: Automatic load balancing, high throughput
- Cons: Overhead of queue management, potential cache thrashing

### M:N (Hybrid) Scheduling

**M:N Model**: M user-level threads mapped to N OS threads.
- Used by: Go (goroutines on OS threads), Erlang BEAM
- Pros: Lightweight user threads, good scaling
- Cons: Complex implementation, debugging challenges

---

## Memory Models Overview

A **memory model** defines what values a read operation can return based on write operations in a concurrent system.

### Sequential Consistency (SC)

**Definition** (Lamport, 1979): "The result of any execution is the same as if the operations of all processors were executed in some sequential order, and the operations of each individual processor appear in this sequence in the order specified by its program."

**In Simple Terms**: All threads see all memory operations in the same order, and each thread's operations appear in program order.

```
Thread 1: x = 1; r1 = y;
Thread 2: y = 1; r2 = x;

Under SC: Cannot have r1 == 0 && r2 == 0
(One thread must see the other's write)
```

**Properties**:
- Intuitive and easy to reason about
- Expensive to implement in hardware
- Requires fences/barriers to prevent reordering

### Total Store Order (TSO)

**TSO**: Nearly sequential consistency, but allows stores to be buffered.

**Key Difference from SC**: Reads can bypass earlier writes in the store buffer.

```
Thread 1: x = 1; r1 = y;
Thread 2: y = 1; r2 = x;

Under TSO: CAN have r1 == 0 && r2 == 0
(Each thread reads before the other's buffered write is visible)
```

**Used By**: x86/x64 architecture
**Implementation**: Store buffers allow CPU to continue while waiting for cache coherence

### Relaxed Memory Models

**Relaxed Models** allow extensive reordering for performance.

**Types of Reordering**:
1. **LoadLoad**: Earlier load can execute after later load
2. **LoadStore**: Earlier load can execute after later store
3. **StoreStore**: Earlier store can execute after later store
4. **StoreLoad**: Earlier store can execute after later load (most expensive to prevent)

**Used By**: ARM, RISC-V, POWER
**Requires**: Explicit memory fences/barriers for ordering

### Language-Level Memory Models

**C++11/Rust/Swift Memory Model**:
- Provides `memory_order` specifications for atomics
- `memory_order_seq_cst`: Sequential consistency
- `memory_order_acquire`/`release`: Synchronizes-with relationship
- `memory_order_relaxed`: No ordering guarantees
- `memory_order_consume`: Data-dependency ordering (deprecated in C++)

**Java Memory Model**:
- `volatile` provides visibility and happens-before guarantees
- Synchronized blocks establish happens-before
- Final fields have special initialization guarantees

**Go Memory Model**:
- Based on happens-before relationships
- Channel operations synchronize
- Mutex lock/unlock synchronizes

**Python Memory Model**:
- Less formally specified
- GIL provides implicit synchronization for most operations
- But beware: GIL doesn't prevent all races (more in Python section)

---

## Synchronization Primitives

### Locks and Mutexes

**Mutex** (Mutual Exclusion): Ensures exclusive access to a resource.

```python
import threading

lock = threading.Lock()
shared_data = []

def thread_safe_append(item):
    with lock:  # Acquire lock
        shared_data.append(item)
    # Lock released automatically
```

**Types**:
1. **Simple Mutex**: Binary lock (locked/unlocked)
2. **Recursive Mutex**: Same thread can acquire multiple times
3. **Read-Write Lock**: Multiple readers OR single writer
4. **Spinlock**: Busy-wait instead of sleeping

**Spinlock Use Cases**:
- Very short critical sections (< 100 ns)
- Real-time systems where sleeping is unacceptable
- Kernel space where sleeping is prohibited

### Semaphores

**Semaphore**: Counter-based synchronization primitive.

```python
import threading

# Allow max 3 concurrent accesses
semaphore = threading.Semaphore(3)

def limited_resource_access():
    with semaphore:
        # At most 3 threads here simultaneously
        access_limited_resource()
```

**Types**:
1. **Binary Semaphore**: Count of 0 or 1 (like mutex)
2. **Counting Semaphore**: Arbitrary count
3. **Bounded Semaphore**: Prevents count from exceeding initial value

### Condition Variables

**Condition Variable**: Wait for a condition while releasing lock.

```python
import threading

lock = threading.Lock()
condition = threading.Condition(lock)
queue = []

def producer():
    with condition:
        queue.append(item)
        condition.notify()  # Wake one waiter

def consumer():
    with condition:
        while not queue:  # Always use while, not if!
            condition.wait()  # Releases lock, waits, reacquires
        item = queue.pop(0)
    return item
```

**Critical Pattern**: Always use `while` loop for wait condition (spurious wakeups).

### Barriers

**Barrier**: Synchronization point where all threads must arrive before any proceed.

```python
import threading

barrier = threading.Barrier(3)  # Wait for 3 threads

def worker():
    do_phase_1()
    barrier.wait()  # All threads wait here
    do_phase_2()    # Only proceed when all 3 arrive
```

### Latches/CountDownLatch

**Latch**: One-time synchronization point (countdown from N to 0).

```python
# Not in Python stdlib, but common pattern
class CountDownLatch:
    def __init__(self, count):
        self.count = count
        self.lock = threading.Lock()
        self.event = threading.Event()
    
    def count_down(self):
        with self.lock:
            self.count -= 1
            if self.count == 0:
                self.event.set()
    
    def wait(self):
        self.event.wait()
```

---

## Progress Guarantees

From Herlihy & Shavit, we classify algorithms by their **progress guarantees**:

### Blocking

**Definition**: Some thread might be prevented from making progress indefinitely.

**Example**: Mutex-based algorithms
- If thread holding lock is descheduled/crashes, others block forever
- Subject to deadlock, priority inversion

### Non-Blocking (Lock-Free)

**Definition**: System as a whole makes progress (at least one thread completes in finite steps).

**Guarantee**: Some thread always makes progress, but individual threads might starve.

**Example**: Lock-free queue using CAS
```python
def lock_free_push(item):
    while True:
        old_head = head.get()
        item.next = old_head
        if head.compare_and_set(old_head, item):
            return  # Success
        # CAS failed, retry (some other thread succeeded)
```

### Wait-Free

**Definition**: Every thread makes progress in a finite number of its own steps.

**Guarantee**: Strongest progress guarantee—no thread can starve.

**Example**: Wait-free counter
```python
# Conceptual: actual implementation requires array of per-thread counters
def wait_free_increment():
    my_index = thread_id()
    counter[my_index] += 1  # Each thread has own counter
    # Reading total requires summing all counters
```

**Trade-off**: Wait-free algorithms are often complex and have higher overhead.

### Obstruction-Free

**Definition**: A thread makes progress if it runs in isolation (no contention).

**Guarantee**: Weaker than lock-free; progress only when alone.

**Use Case**: Adaptive algorithms that fall back to locks under high contention.

### Hierarchy

```
Wait-Free (strongest)
    ↑
Lock-Free
    ↑
Obstruction-Free
    ↑
Blocking (weakest)
```

---

## Hardware Fundamentals

### Cache Hierarchy

Modern CPUs have multiple cache levels:

```
Core 1:                    Core 2:
  L1 I-Cache (32 KB)         L1 I-Cache (32 KB)
  L1 D-Cache (32 KB)         L1 D-Cache (32 KB)
  L2 Cache (256 KB)          L2 Cache (256 KB)
         ↓                          ↓
    L3 Cache (Shared, 8-32 MB)
                ↓
         Main Memory (DRAM)
```

**Access Latencies** (approximate):
- L1 Cache: ~1 ns (4 cycles)
- L2 Cache: ~3 ns (12 cycles)
- L3 Cache: ~12 ns (42 cycles)
- Main Memory: ~60-100 ns (200+ cycles)

**Implications for Concurrency**:
- **False Sharing**: Two threads modifying different variables in same cache line
- **True Sharing**: Multiple threads accessing same data
- **Cache Coherence**: Overhead of keeping caches synchronized

### Cache Coherence Protocols

**MESI Protocol** (Modified, Exclusive, Shared, Invalid):

- **Modified (M)**: Cache line is dirty, exclusive to this cache
- **Exclusive (E)**: Cache line is clean, exclusive to this cache
- **Shared (S)**: Cache line is clean, may be in other caches
- **Invalid (I)**: Cache line is invalid

**Impact on Performance**:
- Write to shared cache line → invalidate all other caches (expensive!)
- Read from modified line in another cache → wait for writeback

### Cache Line Size

Typical cache line size: **64 bytes**

**False Sharing Example**:
```python
# BAD: Two threads, two variables in same cache line
class BadCounters:
    counter1 = 0  # Modified by thread 1
    counter2 = 0  # Modified by thread 2, likely in same cache line!

# GOOD: Pad to separate cache lines
class GoodCounters:
    counter1 = 0
    _pad = [0] * 8  # 64 bytes padding
    counter2 = 0
```

### NUMA (Non-Uniform Memory Access)

**NUMA Architecture**: Multiple CPU sockets, each with local memory.

```
Socket 0        Socket 1
  CPU 0-7         CPU 8-15
  Local RAM       Local RAM
      ↕               ↕
    Interconnect (e.g., Intel QPI)
```

**Latency**:
- Local memory access: ~60 ns
- Remote memory access: ~120 ns (2x penalty!)

**NUMA-Aware Programming**:
- Allocate memory on same socket as accessing thread
- Use `numactl` or `libnuma` to control placement
- Python: mostly transparent, but matters for multiprocessing

### Memory Fences and Barriers

**Memory Fence**: Instruction that enforces ordering constraints.

**Types**:
1. **LoadLoad Fence**: Earlier loads complete before later loads
2. **StoreStore Fence**: Earlier stores complete before later stores
3. **LoadStore Fence**: Earlier loads complete before later stores
4. **Full Fence**: All operations before fence complete before any after

**x86 Examples**:
- `MFENCE`: Full memory fence
- `LFENCE`: Load fence
- `SFENCE`: Store fence
- `LOCK` prefix: Acts as full fence

**Cost**: Fences are expensive (10-100+ cycles depending on type and system).

### Atomic Operations

**Compare-And-Swap (CAS)**:
```c
bool CAS(int* location, int expected, int new_value) {
    atomic {
        if (*location == expected) {
            *location = new_value;
            return true;
        }
        return false;
    }
}
```

**Fetch-And-Add (FAA)**:
```c
int FAA(int* location, int increment) {
    atomic {
        int old = *location;
        *location = old + increment;
        return old;
    }
}
```

**Test-And-Set (TAS)**:
```c
bool TAS(bool* location) {
    atomic {
        bool old = *location;
        *location = true;
        return old;
    }
}
```

**Load-Linked/Store-Conditional (LL/SC)** (ARM, RISC-V):
```c
int LL(int* location) {
    // Load and mark location as linked
}

bool SC(int* location, int value) {
    // Store only if no intervening writes to location
    // Returns success/failure
}
```

**Advantage of LL/SC**: Immune to ABA problem (more in lock-free section).

---

## Summary: Key Takeaways

1. **Concurrency is about structure; parallelism is about execution.**
2. **Memory models define what values reads can see**—sequential consistency is intuitive but expensive; relaxed models are fast but subtle.
3. **Progress guarantees range from blocking (weakest) to wait-free (strongest).**
4. **Hardware matters**: cache coherence, false sharing, NUMA all impact concurrent performance.
5. **Choose synchronization primitives based on use case**:
   - Mutex: General-purpose mutual exclusion
   - Semaphore: Resource counting
   - Condition Variable: Wait for complex conditions
   - Barrier: Synchronize phases
6. **Atomic operations enable lock-free programming** but require deep understanding of memory models.

---

**Next:** [The Art of Multiprocessor Programming - Key Concepts →](./02-herlihy-shavit.md)

**Navigation:** [← Home](../README.md) | [Next →](./02-herlihy-shavit.md)
