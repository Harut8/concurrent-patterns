# Part I.3: Lock-Free and Wait-Free Algorithms

**Navigation:** [← Herlihy & Shavit](./02-herlihy-shavit.md) | [Home](../README.md) | [Next: Memory Models →](./04-memory-models.md)

---

## Table of Contents

1. [Introduction](#introduction)
2. [The ABA Problem](#the-aba-problem)
3. [Lock-Free Data Structures](#lock-free-data-structures)
4. [Wait-Free Algorithms](#wait-free-algorithms)
5. [Practical Lock-Free Patterns](#practical-lock-free-patterns)
6. [Performance Considerations](#performance-considerations)
7. [Debugging and Testing](#debugging-and-testing)

---

## Introduction

**Lock-free** and **wait-free** algorithms provide progress guarantees without using traditional locks. They are essential for high-performance concurrent systems, real-time systems, and situations where lock overhead or deadlock risks are unacceptable.

### When to Use Lock-Free Algorithms

**Use Lock-Free When**:
- High contention on shared data
- Deadlock/priority inversion unacceptable (real-time)
- Very short critical sections (lock overhead > work)
- Building low-level infrastructure (OS, runtime)

**Use Locks When**:
- Complex critical sections
- Lower contention
- Simpler code > marginal performance gain
- Application-level (not infrastructure-level)

### The Spectrum of Synchronization

```
No Sync → Locks → Lock-Free → Wait-Free
(broken)  (simple) (fast, complex) (guaranteed progress, very complex)
```

---

## The ABA Problem

### Problem Definition

**ABA Problem**: A memory location is read, changes from A → B → A, and a CAS succeeds even though the state changed.

**Example**: Lock-Free Stack Pop

```python
# Thread 1                    # Thread 2
head = load(stack.head)       
  # head points to A
  # A.next points to B
                              pop() removes A
                              pop() removes B
                              push(A)  # A is back on top!
next = head.next  # Still B
CAS(stack.head, head, next)  # SUCCEEDS! But A was reused
```

**Result**: Thread 1's CAS succeeds, but the stack state changed (B was removed and A reused). Depending on memory allocator, B might be freed and A.next is now dangling!

### ABA Problem Manifestations

1. **Memory Reclamation**: Pointers to freed and reallocated memory
2. **Version Counting**: Same pointer, different logical version
3. **Reference Counting**: Incorrectly incremented/decremented

### Solutions to ABA

#### 1. Tagged Pointers (Version Counting)

**Idea**: Pack a version counter with the pointer.

```python
class TaggedPointer:
    def __init__(self, ptr, tag=0):
        self.ptr = ptr
        self.tag = tag
    
    def pack(self):
        # Assumes 64-bit, pointers use 48 bits
        return (self.tag << 48) | self.ptr
    
    @staticmethod
    def unpack(value):
        ptr = value & 0xFFFFFFFFFFFF
        tag = value >> 48
        return TaggedPointer(ptr, tag)

# In CAS:
def pop():
    while True:
        old_tagged = load_atomic(stack.head)
        old_ptr, old_tag = TaggedPointer.unpack(old_tagged)
        if old_ptr is None:
            return None
        next_ptr = old_ptr.next
        new_tagged = TaggedPointer(next_ptr, old_tag + 1).pack()
        if CAS(stack.head, old_tagged, new_tagged):
            return old_ptr.value
```

**Limitation**: Version counter can wrap (typically 16 bits). If it wraps back, ABA still possible (rare but not impossible).

#### 2. Hazard Pointers (Memory Reclamation)

**Idea**: Mark pointers as "in use" before dereferencing.

```python
MAX_THREADS = 64
hazard_pointers = [None] * MAX_THREADS

def pop(thread_id):
    while True:
        old_head = load(stack.head)
        # MARK as hazardous
        hazard_pointers[thread_id] = old_head
        # Double-check it's still valid
        if old_head != load(stack.head):
            continue
        if old_head is None:
            hazard_pointers[thread_id] = None
            return None
        next_node = old_head.next
        if CAS(stack.head, old_head, next_node):
            hazard_pointers[thread_id] = None
            return old_head.value

def retire_node(node):
    # Don't free if any thread has it in hazard_pointers
    while any(hp == node for hp in hazard_pointers):
        pass  # Wait or defer
    free(node)
```

**Advantages**: Solves ABA completely for pointer-based structures.

**Disadvantages**: Overhead of checking hazard pointers; bounded by number of threads.

#### 3. Epoch-Based Reclamation (EBR)

**Idea**: Group operations into epochs. Only reclaim memory from old epochs.

```python
global_epoch = AtomicInt(0)
thread_epochs = [None] * MAX_THREADS
retired = [[] for _ in range(3)]  # Per epoch

def enter_epoch(thread_id):
    thread_epochs[thread_id] = global_epoch.load()

def exit_epoch(thread_id):
    thread_epochs[thread_id] = None

def try_advance_epoch():
    # Advance if all threads are in current or newer epoch
    current = global_epoch.load()
    if all(te is None or te >= current for te in thread_epochs):
        global_epoch.CAS(current, current + 1)
        # Free nodes from 2 epochs ago
        for node in retired[current % 3]:
            free(node)
        retired[current % 3] = []

def retire_node(node):
    epoch = global_epoch.load()
    retired[epoch % 3].append(node)
```

**Advantages**: Low overhead; doesn't track individual pointers.

**Disadvantages**: Memory reclamation is delayed; requires epoch advancement.

#### 4. LL/SC (Load-Linked/Store-Conditional)

**Idea**: Hardware primitive immune to ABA.

```c
// ARM architecture
int LL(int* addr) {
    // Load and mark address as "linked"
}

bool SC(int* addr, int value) {
    // Store only if no intervening writes to addr
    // Returns success/failure
}

// Pop using LL/SC
Node* pop() {
    while (true) {
        Node* old_head = LL(&stack->head);
        if (old_head == NULL) return NULL;
        Node* next = old_head->next;
        if (SC(&stack->head, next))
            return old_head;
        // SC failed, retry
    }
}
```

**Advantages**: No ABA problem; hardware guarantee.

**Disadvantages**: Not available on x86 (only CAS); can fail spuriously (must retry).

---

## Lock-Free Data Structures

### Lock-Free Stack (Treiber Stack)

**Properties**: Lock-free, simple, but susceptible to ABA.

```python
class LockFreeStack:
    def __init__(self):
        self.head = AtomicReference(None)
    
    def push(self, value):
        new_node = Node(value)
        while True:
            old_head = self.head.load()
            new_node.next = old_head
            if self.head.compare_exchange(old_head, new_node):
                return
            # CAS failed, retry
    
    def pop(self):
        while True:
            old_head = self.head.load()
            if old_head is None:
                return None  # Empty
            next_node = old_head.next
            # ABA problem here! Use hazard pointers or versioning
            if self.head.compare_exchange(old_head, next_node):
                return old_head.value
```

**Linearization Point**: The successful CAS in push/pop.

### Lock-Free Queue (Michael-Scott Queue)

**Properties**: Lock-free, FIFO, uses dummy node to avoid empty-queue issues.

```python
class LockFreeQueue:
    def __init__(self):
        dummy = Node(None)
        self.head = AtomicReference(dummy)
        self.tail = AtomicReference(dummy)
    
    def enqueue(self, value):
        new_node = Node(value)
        while True:
            tail = self.tail.load()
            next_node = tail.next.load()
            
            # Check consistency
            if tail == self.tail.load():
                if next_node is None:
                    # Tail is pointing to last node
                    if tail.next.compare_exchange(None, new_node):
                        # Enqueue done; try to swing tail
                        self.tail.compare_exchange(tail, new_node)
                        return
                else:
                    # Tail is lagging; help move it forward
                    self.tail.compare_exchange(tail, next_node)
    
    def dequeue(self):
        while True:
            head = self.head.load()
            tail = self.tail.load()
            next_node = head.next.load()
            
            # Check consistency
            if head == self.head.load():
                if head == tail:
                    # Queue appears empty or tail lagging
                    if next_node is None:
                        return None  # Actually empty
                    # Tail is lagging; help move it
                    self.tail.compare_exchange(tail, next_node)
                else:
                    # Queue not empty
                    value = next_node.value
                    if self.head.compare_exchange(head, next_node):
                        # Reclaim old dummy (with hazard pointers/EBR)
                        return value
```

**Key Insight**: "Helping" pattern—if a thread sees tail lagging, it helps move it forward.

**Linearization Point**:
- Enqueue: tail.next CAS
- Dequeue: head CAS

### Lock-Free Hash Map (Simplified)

**Idea**: Array of atomic references to linked lists.

```python
class LockFreeHashMap:
    def __init__(self, size=16):
        self.buckets = [AtomicReference(None) for _ in range(size)]
        self.size = size
    
    def _hash(self, key):
        return hash(key) % self.size
    
    def put(self, key, value):
        bucket_idx = self._hash(key)
        while True:
            head = self.buckets[bucket_idx].load()
            # Search for existing key
            node = head
            while node:
                if node.key == key:
                    # Update existing (or use versioning)
                    node.value = value  # Unsafe! Need CAS on value
                    return
                node = node.next
            
            # Key not found; insert at head
            new_node = Node(key, value, head)
            if self.buckets[bucket_idx].compare_exchange(head, new_node):
                return
    
    def get(self, key):
        bucket_idx = self._hash(key)
        node = self.buckets[bucket_idx].load()
        while node:
            if node.key == key:
                return node.value
            node = node.next
        return None
```

**Limitation**: Resizing is complex (requires copying or incremental rehashing).

**Better Approach**: Split-ordered lists (Java ConcurrentHashMap), or lock-free hash tries.

### Lock-Free Skip List

**Properties**: Lock-free, ordered, supports search/insert/delete.

**Idea**: Skip list with CAS on pointers at each level.

```python
class LockFreeSkipList:
    MAX_LEVEL = 16
    
    class Node:
        def __init__(self, key, value, level):
            self.key = key
            self.value = value
            self.next = [AtomicReference(None) for _ in range(level + 1)]
            self.marked = [AtomicBool(False) for _ in range(level + 1)]
    
    def __init__(self):
        self.head = self.Node(float('-inf'), None, self.MAX_LEVEL)
        self.tail = self.Node(float('inf'), None, self.MAX_LEVEL)
        for i in range(self.MAX_LEVEL + 1):
            self.head.next[i].store(self.tail)
    
    def find(self, key):
        preds = [None] * (self.MAX_LEVEL + 1)
        succs = [None] * (self.MAX_LEVEL + 1)
        
        retry = True
        while retry:
            retry = False
            pred = self.head
            for level in range(self.MAX_LEVEL, -1, -1):
                curr = pred.next[level].load()
                # Traverse level
                while curr.key < key:
                    pred = curr
                    curr = curr.next[level].load()
                    # Check if marked for deletion
                    if curr.marked[level].load():
                        # Help remove
                        retry = True
                        break
                preds[level] = pred
                succs[level] = curr
        
        return preds, succs
    
    def insert(self, key, value):
        top_level = random_level()  # Random height
        while True:
            preds, succs = self.find(key)
            if succs[0].key == key:
                # Key exists; update or fail
                return False
            
            new_node = self.Node(key, value, top_level)
            for level in range(top_level + 1):
                new_node.next[level].store(succs[level])
            
            # CAS at level 0
            if not preds[0].next[0].compare_exchange(succs[0], new_node):
                continue  # Retry
            
            # Insert at higher levels
            for level in range(1, top_level + 1):
                while True:
                    pred = preds[level]
                    succ = succs[level]
                    if pred.next[level].compare_exchange(succ, new_node):
                        break
                    # Retry find for this level
                    preds, succs = self.find(key)
            
            return True
```

**Complexity**: O(log n) expected for search/insert/delete.

**Note**: Full implementation requires logical deletion (marking) before physical removal.

---

## Wait-Free Algorithms

Wait-free algorithms guarantee that **every thread** makes progress in a finite number of steps.

### Wait-Free vs Lock-Free

| Property | Lock-Free | Wait-Free |
|----------|-----------|-----------|
| System progress | ✓ | ✓ |
| Per-thread progress | ✗ (starvation possible) | ✓ (guaranteed) |
| Complexity | Medium | High |
| Performance | Good | Variable (helping overhead) |
| Use case | Most concurrent systems | Real-time, critical |

### Wait-Free Counter

**Simple Approach**: Per-thread counters.

```python
class WaitFreeCounter:
    def __init__(self, max_threads):
        self.counters = [AtomicInt(0) for _ in range(max_threads)]
    
    def increment(self, thread_id):
        self.counters[thread_id].fetch_add(1)
        # No retry! Wait-free
    
    def read(self):
        total = 0
        for counter in self.counters:
            total += counter.load()
        return total
```

**Trade-off**: Read is O(n) where n = number of threads. Write is O(1).

**Alternative**: Use Fetch-And-Add on shared counter (wait-free if FAA is wait-free, which it is on most hardware).

### Wait-Free Queue (Kogan & Petrank)

**Idea**: Use helping—if a thread is slow, other threads help it complete.

**Simplified Structure**:

```python
class WaitFreeQueue:
    def __init__(self):
        self.head = AtomicReference(Node(None))
        self.tail = AtomicReference(self.head.load())
        self.operations = [None] * MAX_THREADS  # Pending operations
    
    def enqueue(self, value, thread_id):
        op = EnqueueOp(value)
        self.operations[thread_id] = op
        
        # Help all pending operations (including mine)
        self.help_all()
        
        # My operation is now complete
        return
    
    def help_all(self):
        for i in range(MAX_THREADS):
            op = self.operations[i]
            if op and not op.completed:
                self.help(op)
    
    def help(self, op):
        if isinstance(op, EnqueueOp):
            # Actually insert op's node
            new_node = Node(op.value)
            tail = self.tail.load()
            # CAS tail.next to new_node
            if tail.next.compare_exchange(None, new_node):
                self.tail.compare_exchange(tail, new_node)
            op.completed = True
```

**Key Idea**: Every thread helps complete all pending operations. Thus, even a slow thread makes progress because others help it.

**Overhead**: Potentially expensive—every operation may help all others.

### Fast-Path Slow-Path Pattern

**Idea**: Fast path is lock-free; slow path is wait-free (used rarely).

```python
class FastSlowQueue:
    def __init__(self):
        self.lock_free_queue = LockFreeQueue()
        self.slow_path_count = AtomicInt(0)
    
    def enqueue(self, value, thread_id):
        # Try lock-free fast path
        for _ in range(MAX_RETRIES):
            if self.lock_free_queue.try_enqueue(value):
                return
        
        # Fast path failed; use wait-free slow path
        self.slow_path_count.fetch_add(1)
        self.wait_free_enqueue(value, thread_id)
```

**Benefit**: Common case is fast; worst case is guaranteed progress.

---

## Practical Lock-Free Patterns

### 1. Single-Writer Principle

**Idea**: If only one thread writes, many patterns simplify.

```python
# Single-writer, multiple-reader queue (ring buffer)
class SPSCQueue:
    def __init__(self, capacity):
        self.buffer = [None] * capacity
        self.capacity = capacity
        self.write_index = AtomicInt(0)  # Written by producer only
        self.read_index = AtomicInt(0)   # Written by consumer only
    
    def enqueue(self, value):
        # Producer only
        write_pos = self.write_index.load()
        next_pos = (write_pos + 1) % self.capacity
        if next_pos == self.read_index.load():
            return False  # Full
        self.buffer[write_pos] = value
        self.write_index.store(next_pos)  # Release
        return True
    
    def dequeue(self):
        # Consumer only
        read_pos = self.read_index.load()
        if read_pos == self.write_index.load():
            return None  # Empty
        value = self.buffer[read_pos]
        self.read_index.store((read_pos + 1) % self.capacity)  # release
        return value
```

**Advantage**: No CAS needed! Just atomic loads/stores with proper ordering.

**Use Case**: Event loops, actor mailboxes.

### 2. Read-Copy-Update (RCU)

**Idea**: Readers read without synchronization; writers create new versions.

```python
class RCUMap:
    def __init__(self):
        self.data = AtomicReference({})  # Immutable dict
    
    def get(self, key):
        # Read without locking
        snapshot = self.data.load()
        return snapshot.get(key)
    
    def put(self, key, value):
        # Writers update
        while True:
            old_data = self.data.load()
            new_data = {**old_data, key: value}  # Copy
            if self.data.compare_exchange(old_data, new_data):
                # Old data will be GC'd when no readers reference it
                return
```

**Advantage**: Readers are extremely fast (no synchronization).

**Disadvantage**: Writers are slow (copy entire structure).

**Use Case**: Read-heavy workloads (e.g., configuration, routing tables).

### 3. Elimination Backoff

**Idea**: If CAS contention is high, pair up threads to "eliminate" each other.

```python
class EliminationBackoffStack:
    def __init__(self):
        self.stack = TreiberStack()
        self.elimination_array = [AtomicReference(None) for _ in range(16)]
    
    def push(self, value):
        # Try fast path
        if self.stack.try_push(value, max_retries=3):
            return
        
        # High contention; try elimination
        slot = random.randint(0, 15)
        if self.elimination_array[slot].compare_exchange(None, ('PUSH', value)):
            # Wait for pop to eliminate me
            for _ in range(ELIMINATION_TIMEOUT):
                if self.elimination_array[slot].load() is None:
                    return  # Eliminated!
            # Timeout; revert
            self.elimination_array[slot].store(None)
        
        # Fallback to stack
        self.stack.push(value)
    
    def pop(self):
        # Try fast path
        result = self.stack.try_pop(max_retries=3)
        if result is not None:
            return result
        
        # Try elimination
        slot = random.randint(0, 15)
        op = self.elimination_array[slot].load()
        if op and op[0] == 'PUSH':
            if self.elimination_array[slot].compare_exchange(op, None):
                return op[1]  # Eliminated!
        
        # Fallback
        return self.stack.pop()
```

**Benefit**: Reduces contention on central data structure.

---

## Performance Considerations

### When Lock-Free is Faster

1. **High Contention**: Locks cause waiting; lock-free makes progress
2. **Short Critical Sections**: Lock overhead > work
3. **Unpredictable Delays**: One slow thread doesn't block others
4. **Real-Time**: Deadlines require guaranteed progress

### When Locks are Faster

1. **Low Contention**: Lock fast-path is very cheap
2. **Complex Critical Sections**: Lock-free complexity outweighs benefit
3. **Memory Reclamation Complexity**: Hazard pointers add overhead
4. **Lack of Hardware Support**: No CAS or weak memory model

### Benchmarking Lock-Free Structures

```python
import time
import threading

def benchmark_structure(structure, operations, num_threads):
    results = [0] * num_threads
    
    def worker(tid):
        start = time.perf_counter()
        for op in operations:
            op(structure)
        results[tid] = time.perf_counter() - start
    
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    overall_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    overall_end = time.perf_counter()
    
    total_ops = len(operations) * num_threads
    throughput = total_ops / (overall_end - overall_start)
    
    print(f"Throughput: {throughput:.2f} ops/sec")
    print(f"Avg latency: {1000 * sum(results) / (num_threads * len(operations)):.3f} ms")
```

**Metrics**:
- **Throughput**: Operations per second
- **Latency**: Time per operation
- **Scalability**: Throughput vs. number of threads
- **Contention**: Performance under high contention

---

## Debugging and Testing

### Challenges

1. **Non-Determinism**: Bugs occur rarely and are hard to reproduce
2. **Timing Sensitivity**: Adding print statements changes behavior
3. **Memory Ordering**: Bugs depend on subtle reorderings
4. **ABA Bugs**: Rare but catastrophic

### Testing Strategies

#### 1. Stress Testing

```python
def stress_test_stack():
    stack = LockFreeStack()
    errors = []
    
    def push_worker():
        for i in range(10000):
            stack.push(i)
    
    def pop_worker():
        for _ in range(10000):
            val = stack.pop()
            if val is not None and val < 0:
                errors.append(f"Invalid value: {val}")
    
    threads = [threading.Thread(target=push_worker) for _ in range(10)]
    threads += [threading.Thread(target=pop_worker) for _ in range(10)]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    assert not errors, f"Errors found: {errors}"
```

#### 2. Model Checking (TLA+, Spin)

**TLA+ Spec for Lock-Free Stack**:

```tla
EXTENDS Integers, Sequences

VARIABLES stack, head

Push(value) ==
    /\ head' \in Node  (* Non-deterministic new node *)
    /\ head'.next = head
    /\ head'.value = value
    /\ head' = head  (* CAS succeeded *)

Pop ==
    /\ head # NULL
    /\ head' = head.next

Invariant ==
    /\ (* No cycles *)
    /\ (* All reachable nodes are in stack *)
```

**Model Check**: Exhaustively explore interleavings to find violations.

#### 3. Linearizability Checking (Lincheck for JVM)

```java
@StressTest
public class LockFreeStackTest {
    private LockFreeStack<Integer> stack = new LockFreeStack<>();
    
    @Operation
    public void push(int value) {
        stack.push(value);
    }
    
    @Operation
    public Integer pop() {
        return stack.pop();
    }
    
    @Test
    public void runTest() {
        LinChecker.check(LockFreeStackTest.class);
        // Automatically checks linearizability
    }
}
```

#### 4. Thread Sanitizer (C/C++)

```bash
clang++ -fsanitize=thread -g lock_free_stack.cpp -o stack
./stack
# Detects data races at runtime
```

**Python**: ThreadSanitizer not available, but can use race condition detectors like `pytest-race`.

---

## Summary: Lock-Free Takeaways

1. **ABA Problem**: Real and dangerous; use versioning, hazard pointers, EBR, or LL/SC.
2. **Lock-free guarantees system progress**; wait-free guarantees per-thread progress.
3. **Memory reclamation is the hard part**: Hazard pointers and EBR are standard solutions.
4. **Common structures**: Stack (Treiber), Queue (Michael-Scott), Hash Map, Skip List.
5. **Performance trade-offs**: Lock-free wins under contention but has overhead (CAS retries, memory reclamation).
6. **Testing is critical**: Use stress tests, model checking, and linearizability checkers.
7. **Use libraries**: Don't roll your own unless necessary (e.g., Java ConcurrentHashMap, Rust crossbeam).

---

**Next:** [Memory Models and Consistency →](./04-memory-models.md)

**Navigation:** [← Herlihy & Shavit](./02-herlihy-shavit.md) | [Home](../README.md) | [Next →](./04-memory-models.md)
