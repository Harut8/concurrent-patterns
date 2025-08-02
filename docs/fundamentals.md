# Fundamentals of Concurrency

## Table of Contents
1. [Introduction to Concurrency](#introduction-to-concurrency)
2. [Hardware Foundations](#hardware-foundations)
3. [Memory Models](#memory-models)
4. [Synchronization Theory](#synchronization-theory)
5. [Race Conditions and Critical Sections](#race-conditions-and-critical-sections)
6. [Deadlock, Livelock, and Starvation](#deadlock-livelock-and-starvation)
7. [Performance Considerations](#performance-considerations)

## Introduction to Concurrency

Concurrency is the composition of independently executing processes, while parallelism is the simultaneous execution of multiple tasks. Understanding this distinction is crucial for staff engineers designing scalable systems.

### Historical Context

The foundations of concurrent programming were established by pioneers like:
- **Edsger Dijkstra** (1968): Introduced semaphores in THE operating system
- **Tony Hoare** (1978): Developed Communicating Sequential Processes (CSP)
- **Carl Hewitt** (1973): Created the Actor Model
- **Leslie Lamport** (1978): Formalized distributed system ordering

### Why Concurrency Matters

1. **Moore's Law Plateau**: Clock speeds stopped increasing around 2005
2. **Multicore Revolution**: Modern processors have multiple cores
3. **Distributed Systems**: Cloud computing requires concurrent design
4. **User Experience**: Responsive applications need asynchronous processing
5. **Resource Utilization**: Maximize hardware efficiency

## Hardware Foundations

### Processor Architecture

Modern processors provide several features essential for concurrency:

#### Cache Hierarchy
```
CPU Core
├── L1 Cache (32KB, 1-2 cycles)
├── L2 Cache (256KB-1MB, 10-20 cycles)
├── L3 Cache (8-32MB, 40-75 cycles)
└── Main Memory (GB, 200-300 cycles)
```

#### Cache Coherence Protocols

**MESI Protocol** (Modified, Exclusive, Shared, Invalid):
- **Modified**: Cache line is dirty and exclusive to this core
- **Exclusive**: Cache line is clean and exclusive to this core
- **Shared**: Cache line is clean and may be in other caches
- **Invalid**: Cache line is invalid

**MOESI Protocol**: Adds "Owned" state for better performance

#### Atomic Instructions

Hardware provides atomic operations essential for lock-free programming:

1. **Compare-and-Swap (CAS)**:
   ```assembly
   cmpxchg [memory], new_value
   ```

2. **Load-Linked/Store-Conditional (LL/SC)**:
   ```assembly
   ll    $t0, 0($a0)     # Load linked
   # ... modify $t0 ...
   sc    $t0, 0($a0)     # Store conditional
   ```

3. **Fetch-and-Add**:
   ```assembly
   lock xadd [memory], increment
   ```

### NUMA (Non-Uniform Memory Access)

In NUMA systems, memory access costs vary by location:

```
Node 0          Node 1
┌─────────┐    ┌─────────┐
│ CPU 0-7 │    │ CPU 8-15│
│ Memory  │    │ Memory  │
└─────────┘    └─────────┘
     │              │
     └──────────────┘
      Interconnect
```

**Implications**:
- Local memory access: ~100ns
- Remote memory access: ~300ns
- Thread affinity becomes critical
- Data locality affects performance significantly

## Memory Models

Memory models define the ordering guarantees for memory operations in concurrent programs.

### Sequential Consistency

**Definition**: "A multiprocessor is sequentially consistent if the result of any execution is the same as if the operations of all processors were executed in some sequential order, and the operations of each individual processor appear in this sequence in the order specified by its program." - Leslie Lamport

**Properties**:
- All processors see the same order of operations
- Operations appear atomic
- Program order is preserved

### Relaxed Memory Models

Real hardware often provides weaker guarantees for performance:

#### x86-64 Memory Model (TSO - Total Store Order)
- Loads can be reordered with loads
- Stores can be reordered with stores
- Loads cannot be reordered with stores
- Stores cannot be reordered with loads

#### ARM/PowerPC Memory Models
- Much more relaxed
- Requires explicit memory barriers
- Better performance potential
- More complex programming model

### Memory Barriers/Fences

Synchronization primitives that enforce ordering:

1. **Full Barrier**: Prevents all reordering
2. **Acquire Barrier**: Prevents later operations from moving before
3. **Release Barrier**: Prevents earlier operations from moving after
4. **Store Barrier**: Orders store operations
5. **Load Barrier**: Orders load operations

## Synchronization Theory

### Consensus Numbers

**Definition**: The consensus number of an object is the maximum number of processes for which the object can solve the consensus problem in a wait-free manner.

**Hierarchy**:
1. **Consensus Number 1**: Read/Write registers, FIFO queues, stacks
2. **Consensus Number 2**: Test-and-set, swap, fetch-and-add
3. **Consensus Number ∞**: Compare-and-swap, load-linked/store-conditional

**Implications**:
- Higher consensus numbers enable more powerful synchronization
- CAS can implement any concurrent object wait-free
- Understanding this hierarchy guides primitive selection

### Progress Guarantees

#### Wait-Freedom
- **Definition**: Every thread completes its operation in a finite number of steps
- **Strongest guarantee**: No thread can be delayed indefinitely
- **Example**: Wait-free queue using CAS

#### Lock-Freedom
- **Definition**: At least one thread makes progress in a finite number of steps
- **Weaker than wait-free**: Individual threads may starve
- **Example**: Lock-free stack using CAS

#### Obstruction-Freedom
- **Definition**: A thread makes progress if it runs in isolation
- **Weakest guarantee**: Progress only when no contention
- **Example**: Software transactional memory

#### Blocking
- **Definition**: Thread progress depends on scheduler decisions
- **Traditional approach**: Using locks and mutexes
- **Risk**: Deadlock, priority inversion, convoying

### Linearizability

**Definition**: A concurrent object is linearizable if each operation appears to take effect instantaneously at some point between its invocation and response.

**Properties**:
- Operations appear atomic
- Real-time ordering is preserved
- Compositional (linearizable objects compose)

**Example**: Linearizable vs Non-Linearizable Queue
```
Thread 1: enqueue(A) ────────────────── ok
Thread 2:           dequeue() ── A
Thread 3:                    dequeue() ── empty

Linearizable: dequeue operations can be ordered consistently
Non-linearizable: Would violate FIFO ordering
```

## Race Conditions and Critical Sections

### Race Conditions

**Definition**: A race condition occurs when the correctness of a program depends on the relative timing of events.

#### Data Races vs Race Conditions

**Data Race**: Concurrent access to shared memory where at least one access is a write, without proper synchronization.

**Race Condition**: Broader concept including logical errors in concurrent programs.

```python
# Data race example
shared_counter = 0

def increment():
    global shared_counter
    temp = shared_counter  # Read
    temp = temp + 1        # Modify
    shared_counter = temp  # Write
    # Another thread can interfere between these operations
```

### Critical Sections

**Definition**: A code section that accesses shared resources and must be executed atomically.

**Properties**:
1. **Mutual Exclusion**: At most one thread in critical section
2. **Progress**: If no thread is in critical section, one waiting thread must enter
3. **Bounded Waiting**: Waiting time must be finite
4. **No Assumptions**: About relative thread speeds

#### Peterson's Algorithm (Historical)

```python
class PetersonLock:
    def __init__(self):
        self.flag = [False, False]
        self.turn = 0
    
    def acquire(self, thread_id):
        other = 1 - thread_id
        self.flag[thread_id] = True
        self.turn = other
        while self.flag[other] and self.turn == other:
            pass  # Busy wait
    
    def release(self, thread_id):
        self.flag[thread_id] = False
```

**Note**: Peterson's algorithm doesn't work on modern processors due to memory reordering.

## Deadlock, Livelock, and Starvation

### Deadlock

**Definition**: A situation where two or more threads are blocked forever, waiting for each other.

#### Coffman Conditions (All must be true for deadlock):
1. **Mutual Exclusion**: Resources cannot be shared
2. **Hold and Wait**: Threads hold resources while waiting for others
3. **No Preemption**: Resources cannot be forcibly taken
4. **Circular Wait**: Circular chain of waiting threads

#### Deadlock Prevention

**Break Mutual Exclusion**: Make resources shareable (not always possible)

**Break Hold and Wait**: Acquire all resources atomically
```python
def transfer_money(from_account, to_account, amount):
    # Always acquire locks in same order to prevent circular wait
    first, second = (from_account, to_account) if id(from_account) < id(to_account) else (to_account, from_account)
    with first.lock:
        with second.lock:
            from_account.withdraw(amount)
            to_account.deposit(amount)
```

**Break No Preemption**: Use timeouts
```python
def acquire_with_timeout(lock, timeout):
    if lock.acquire(timeout=timeout):
        return True
    return False  # Could not acquire, avoid deadlock
```

**Break Circular Wait**: Order resources globally
```python
# Global lock ordering prevents circular wait
LOCK_ORDER = {account.id: index for index, account in enumerate(all_accounts)}

def transfer_with_ordering(from_account, to_account, amount):
    locks = sorted([from_account.lock, to_account.lock], 
                   key=lambda lock: LOCK_ORDER[lock.account_id])
    for lock in locks:
        lock.acquire()
    try:
        from_account.withdraw(amount)
        to_account.deposit(amount)
    finally:
        for lock in reversed(locks):
            lock.release()
```

#### Deadlock Detection

**Wait-for Graph**: Detect cycles in resource dependency graph
```python
class DeadlockDetector:
    def __init__(self):
        self.wait_for = {}  # thread -> resource it's waiting for
        self.held_by = {}   # resource -> thread holding it
    
    def detect_cycle(self):
        visited = set()
        rec_stack = set()
        
        def dfs(thread):
            if thread in rec_stack:
                return True  # Cycle detected
            if thread in visited:
                return False
            
            visited.add(thread)
            rec_stack.add(thread)
            
            # Check what this thread is waiting for
            if thread in self.wait_for:
                resource = self.wait_for[thread]
                if resource in self.held_by:
                    holder = self.held_by[resource]
                    if dfs(holder):
                        return True
            
            rec_stack.remove(thread)
            return False
        
        for thread in self.wait_for:
            if dfs(thread):
                return True
        return False
```

### Livelock

**Definition**: Threads are not blocked but continuously change state in response to other threads, making no progress.

**Example**: Two people trying to pass each other in a hallway, both stepping in the same direction repeatedly.

```python
# Livelock example with exponential backoff
import random
import time

class LivelockPrevention:
    def __init__(self):
        self.attempt_count = 0
    
    def acquire_with_backoff(self, lock):
        while True:
            if lock.try_acquire():
                return True
            
            # Exponential backoff with jitter
            backoff_time = (2 ** self.attempt_count) * 0.001  # Base delay
            jitter = random.uniform(0, backoff_time)
            time.sleep(backoff_time + jitter)
            
            self.attempt_count += 1
            if self.attempt_count > 10:
                # Fallback to blocking acquire
                lock.acquire()
                return True
```

### Starvation

**Definition**: A thread is perpetually denied access to resources it needs.

**Causes**:
- Unfair scheduling policies
- Priority inversion
- Resource allocation policies

**Solutions**:
- Fair locks (FIFO ordering)
- Priority inheritance
- Aging (gradually increase priority)

```python
class FairLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.queue = []
        self.current_holder = None
    
    def acquire(self):
        thread_id = threading.current_thread().ident
        
        with self.condition:
            if self.current_holder is None and not self.queue:
                self.current_holder = thread_id
                return
            
            # Add to queue and wait
            self.queue.append(thread_id)
            while self.current_holder != thread_id:
                self.condition.wait()
    
    def release(self):
        with self.condition:
            if self.queue:
                self.current_holder = self.queue.pop(0)
                self.condition.notify_all()
            else:
                self.current_holder = None
```

## Performance Considerations

### Amdahl's Law

**Formula**: Speedup = 1 / ((1 - P) + P/N)

Where:
- P = Proportion of program that can be parallelized
- N = Number of processors

**Implications**:
- Serial portions limit speedup
- 90% parallel code: max 10x speedup regardless of cores
- Focus on reducing serial bottlenecks

### Gustafson's Law

**Alternative perspective**: Fixed time, scale problem size

**Formula**: Speedup = N - α(N - 1)

Where α is the serial fraction

**Implications**:
- Larger problems can achieve better speedup
- Weak scaling vs strong scaling

### Cache Effects

#### False Sharing

**Problem**: Multiple threads accessing different variables in the same cache line

```python
# Bad: False sharing
class BadCounter:
    def __init__(self):
        self.counters = [0] * 8  # Likely in same cache line

# Good: Padding to separate cache lines
class GoodCounter:
    def __init__(self):
        self.counters = []
        for i in range(8):
            # Pad to cache line size (64 bytes on x86)
            padded_counter = [0] + [0] * 15  # 64 bytes
            self.counters.append(padded_counter)
```

#### Cache Line Bouncing

**Problem**: Cache lines moving between cores due to writes

**Solution**: 
- Minimize shared writes
- Use thread-local storage
- Batch operations

### Lock Contention

#### Lock Granularity Trade-offs

**Coarse-grained locks**:
- Pros: Simple, less overhead
- Cons: Reduced parallelism

**Fine-grained locks**:
- Pros: Better parallelism
- Cons: Complex, more overhead

```python
# Coarse-grained: One lock for entire hash table
class CoarseHashTable:
    def __init__(self):
        self.lock = threading.RLock()
        self.buckets = [[] for _ in range(16)]
    
    def put(self, key, value):
        with self.lock:
            bucket = self.buckets[hash(key) % len(self.buckets)]
            bucket.append((key, value))

# Fine-grained: One lock per bucket
class FineHashTable:
    def __init__(self):
        self.bucket_locks = [threading.RLock() for _ in range(16)]
        self.buckets = [[] for _ in range(16)]
    
    def put(self, key, value):
        bucket_index = hash(key) % len(self.buckets)
        with self.bucket_locks[bucket_index]:
            self.buckets[bucket_index].append((key, value))
```

#### Lock-Free Alternatives

When possible, use lock-free data structures:
- Atomic operations
- Compare-and-swap loops
- Memory ordering considerations

### NUMA Considerations

```python
import os
import threading

class NUMAOptimizedWorker:
    def __init__(self, numa_node):
        self.numa_node = numa_node
        self.local_data = None
    
    def run(self):
        # Bind thread to NUMA node
        os.sched_setaffinity(0, {self.numa_node})
        
        # Allocate data on local NUMA node
        self.local_data = self.allocate_local_memory()
        
        # Process data with good locality
        self.process_data()
    
    def allocate_local_memory(self):
        # Platform-specific NUMA memory allocation
        # This is pseudocode - actual implementation varies
        return allocate_on_node(self.numa_node, size=1024*1024)
```

### Profiling and Measurement

#### Key Metrics

1. **Throughput**: Operations per second
2. **Latency**: Time per operation
3. **Scalability**: Performance vs thread count
4. **Cache Misses**: L1, L2, L3 miss rates
5. **Context Switches**: Scheduler overhead
6. **Lock Contention**: Time spent waiting for locks

#### Tools

- **perf**: Linux performance profiling
- **Intel VTune**: Detailed CPU analysis
- **ThreadSanitizer**: Race condition detection
- **Helgrind**: Valgrind's thread error detector

```bash
# Example perf commands
perf stat -e cache-misses,cache-references ./program
perf record -e cpu-cycles ./program
perf report
```

---

*This foundational knowledge is essential for understanding the advanced patterns and implementations covered in subsequent sections. Staff engineers must master these concepts to design and optimize concurrent systems effectively.*
