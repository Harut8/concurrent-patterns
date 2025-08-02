# Lock-Free Programming

## Table of Contents
1. [Introduction](#introduction)
2. [Theoretical Foundations](#theoretical-foundations)
3. [Memory Models and Ordering](#memory-models-and-ordering)
4. [Atomic Operations](#atomic-operations)
5. [The ABA Problem](#the-aba-problem)
6. [Lock-Free Data Structures](#lock-free-data-structures)
7. [Memory Reclamation](#memory-reclamation)
8. [Performance Considerations](#performance-considerations)

## Introduction

Lock-free programming is a paradigm for designing concurrent algorithms that do not use traditional locking mechanisms. Instead, it relies on atomic operations and careful memory ordering to ensure correctness while providing better scalability and avoiding issues like deadlock and priority inversion.

### Why Lock-Free Programming?

**Advantages**:
- **No Deadlock**: Impossible by definition
- **No Priority Inversion**: High-priority threads never wait for low-priority ones
- **Better Scalability**: Reduced contention and context switching
- **Fault Tolerance**: One thread's failure doesn't block others
- **Real-Time Guarantees**: Bounded execution time

**Disadvantages**:
- **Complexity**: Much harder to design and verify
- **ABA Problem**: Subtle correctness issues
- **Memory Reclamation**: Complex lifetime management
- **Platform Dependency**: Hardware-specific optimizations
- **Debugging Difficulty**: Non-deterministic behavior

### Progress Guarantees

1. **Wait-Free**: Every thread completes operations in bounded steps
2. **Lock-Free**: At least one thread makes progress in bounded steps
3. **Obstruction-Free**: Threads make progress when running alone

## Theoretical Foundations

### Linearizability

**Definition**: A concurrent object is linearizable if each operation appears to take effect instantaneously at some point between its invocation and response.

**Properties**:
- Operations appear atomic
- Real-time ordering preserved
- Compositional property
- Strongest consistency model for concurrent objects

### Consensus Numbers

**Hierarchy of Synchronization Primitives**:

1. **Level 1**: Read/Write registers, FIFO queues, stacks
   - Can solve consensus for 1 thread only
   - Limited computational power

2. **Level 2**: Test-and-set, swap, fetch-and-add
   - Can solve consensus for 2 threads
   - More powerful but still limited

3. **Level ∞**: Compare-and-swap, load-linked/store-conditional
   - Can solve consensus for any number of threads
   - Universal synchronization primitives

### Universal Construction

Any sequential object can be made concurrent using compare-and-swap:

```python
class UniversalConstruction:
    """
    Universal construction using compare-and-swap.
    Can implement any concurrent object wait-free.
    """
    def __init__(self, sequential_object):
        self.head = AtomicReference(Node(sequential_object.copy(), 0))
    
    def apply_operation(self, operation):
        """Apply operation wait-free using CAS"""
        while True:
            current = self.head.get()
            new_object = current.object.copy()
            result = new_object.apply(operation)
            new_node = Node(new_object, current.seq + 1)
            
            if self.head.compare_and_swap(current, new_node):
                return result
            # Retry if CAS failed
```

## Memory Models and Ordering

### Sequential Consistency

**Definition**: "A multiprocessor is sequentially consistent if the result of any execution is the same as if the operations of all processors were executed in some sequential order, and the operations of each individual processor appear in this sequence in the order specified by its program." - Leslie Lamport

### Relaxed Memory Models

Real hardware provides weaker guarantees for performance:

#### x86-64 (Total Store Order)
- Loads can be reordered with loads
- Stores can be reordered with stores  
- Loads cannot pass stores
- Strong model, easier programming

#### ARM/PowerPC (Weak Ordering)
- All reorderings possible
- Requires explicit barriers
- Better performance potential
- More complex programming

### Memory Ordering Semantics

#### Acquire-Release Semantics
```cpp
// C++ memory ordering example
std::atomic<int> data{0};
std::atomic<bool> ready{false};

// Producer
void producer() {
    data.store(42, std::memory_order_relaxed);
    ready.store(true, std::memory_order_release);  // Release
}

// Consumer  
void consumer() {
    if (ready.load(std::memory_order_acquire)) {   // Acquire
        int value = data.load(std::memory_order_relaxed);
        // Guaranteed to see data == 42
    }
}
```

#### Memory Barriers/Fences

1. **Full Barrier**: Prevents all reordering
2. **Acquire Barrier**: Prevents later operations from moving before
3. **Release Barrier**: Prevents earlier operations from moving after
4. **Store Barrier**: Orders store operations
5. **Load Barrier**: Orders load operations

## Atomic Operations

### Compare-and-Swap (CAS)

**Signature**: `bool CAS(location, expected, new_value)`

**Semantics**:
```python
def compare_and_swap(location, expected, new_value):
    """Atomic compare-and-swap operation"""
    current = location.value
    if current == expected:
        location.value = new_value
        return True
    return False
```

**Usage Pattern**:
```python
def atomic_increment(atomic_int):
    """Lock-free increment using CAS loop"""
    while True:
        current = atomic_int.get()
        new_value = current + 1
        if atomic_int.compare_and_swap(current, new_value):
            return new_value
        # Retry if CAS failed due to interference
```

### Load-Linked/Store-Conditional (LL/SC)

**Alternative to CAS on some architectures**:

```assembly
retry:
    ll    $t0, 0($a0)     # Load linked
    addi  $t0, $t0, 1     # Increment
    sc    $t0, 0($a0)     # Store conditional
    beq   $t0, $zero, retry  # Retry if failed
```

**Advantages over CAS**:
- No ABA problem
- More flexible
- Better composability

### Fetch-and-Add

**Atomic increment/decrement**:
```python
def fetch_and_add(location, delta):
    """Atomic fetch-and-add operation"""
    old_value = location.value
    location.value += delta
    return old_value
```

**Use Cases**:
- Counters and statistics
- Array indexing
- Reference counting

## The ABA Problem

### Problem Description

The ABA problem occurs when:
1. Thread 1 reads value A from location
2. Thread 2 changes location from A to B and back to A
3. Thread 1's CAS succeeds but assumes no change occurred
4. The assumption may be incorrect

### Example Scenario

```python
# Lock-free stack with ABA problem
class Node:
    def __init__(self, data, next_node=None):
        self.data = data
        self.next = next_node

class LockFreeStack:
    def __init__(self):
        self.head = AtomicReference(None)
    
    def push(self, data):
        new_node = Node(data)
        while True:
            current_head = self.head.get()
            new_node.next = current_head
            if self.head.compare_and_swap(current_head, new_node):
                return
    
    def pop(self):
        while True:
            current_head = self.head.get()
            if current_head is None:
                return None
            
            next_node = current_head.next
            # ABA problem can occur here!
            if self.head.compare_and_swap(current_head, next_node):
                return current_head.data
```

**ABA Scenario**:
1. Thread 1 reads head (A), prepares to pop
2. Thread 2 pops A, pops B, pushes A back
3. Thread 1's CAS succeeds but B is now dangling
4. Accessing B causes undefined behavior

### Solutions to ABA Problem

#### 1. Hazard Pointers

```python
class HazardPointer:
    """Safe memory reclamation using hazard pointers"""
    def __init__(self):
        self.protected_pointers = set()
        self.retired_pointers = []
        self.lock = threading.Lock()
    
    def protect(self, pointer):
        """Mark pointer as protected from reclamation"""
        with self.lock:
            self.protected_pointers.add(pointer)
    
    def unprotect(self, pointer):
        """Remove protection from pointer"""
        with self.lock:
            self.protected_pointers.discard(pointer)
    
    def retire(self, pointer):
        """Retire pointer for later reclamation"""
        with self.lock:
            self.retired_pointers.append(pointer)
            if len(self.retired_pointers) > 100:
                self.reclaim()
    
    def reclaim(self):
        """Reclaim retired pointers not in hazard set"""
        safe_to_delete = []
        for ptr in self.retired_pointers:
            if ptr not in self.protected_pointers:
                safe_to_delete.append(ptr)
        
        for ptr in safe_to_delete:
            self.retired_pointers.remove(ptr)
            del ptr
```

#### 2. Epoch-Based Reclamation

```python
class EpochManager:
    """Epoch-based memory reclamation"""
    def __init__(self):
        self.global_epoch = AtomicInteger(0)
        self.thread_epochs = {}
        self.retired_objects = {0: [], 1: [], 2: []}
        self.lock = threading.Lock()
    
    def enter_critical_section(self):
        """Enter critical section and update epoch"""
        thread_id = threading.current_thread().ident
        current_epoch = self.global_epoch.get()
        self.thread_epochs[thread_id] = current_epoch
        return current_epoch
    
    def exit_critical_section(self):
        """Exit critical section"""
        thread_id = threading.current_thread().ident
        if thread_id in self.thread_epochs:
            del self.thread_epochs[thread_id]
        self.try_advance_epoch()
    
    def retire_object(self, obj):
        """Retire object in current epoch"""
        current_epoch = self.global_epoch.get()
        epoch_index = current_epoch % 3
        with self.lock:
            self.retired_objects[epoch_index].append(obj)
    
    def try_advance_epoch(self):
        """Try to advance global epoch"""
        current_epoch = self.global_epoch.get()
        
        # Check if all threads are in current epoch
        min_thread_epoch = min(self.thread_epochs.values(), 
                              default=current_epoch)
        
        if min_thread_epoch == current_epoch:
            # Safe to advance epoch
            new_epoch = current_epoch + 1
            if self.global_epoch.compare_and_swap(current_epoch, new_epoch):
                # Reclaim objects from old epoch
                old_epoch_index = (new_epoch - 2) % 3
                with self.lock:
                    for obj in self.retired_objects[old_epoch_index]:
                        del obj
                    self.retired_objects[old_epoch_index].clear()
```

#### 3. Double-Width CAS

```python
class DoubleWidthCAS:
    """Use version counter to prevent ABA"""
    def __init__(self, initial_value=None):
        self.value = initial_value
        self.version = 0
        self.lock = threading.Lock()  # Simulating atomic double-width CAS
    
    def compare_and_swap(self, expected_value, expected_version, 
                        new_value, new_version):
        """Double-width compare-and-swap"""
        with self.lock:
            if (self.value == expected_value and 
                self.version == expected_version):
                self.value = new_value
                self.version = new_version
                return True
            return False
    
    def get(self):
        """Get current value and version"""
        with self.lock:
            return self.value, self.version

class ABAFreeStack:
    """Stack using versioned pointers to prevent ABA"""
    def __init__(self):
        self.head = DoubleWidthCAS(None)
    
    def push(self, data):
        new_node = Node(data)
        while True:
            current_head, version = self.head.get()
            new_node.next = current_head
            
            if self.head.compare_and_swap(current_head, version,
                                         new_node, version + 1):
                return
    
    def pop(self):
        while True:
            current_head, version = self.head.get()
            if current_head is None:
                return None
            
            next_node = current_head.next
            if self.head.compare_and_swap(current_head, version,
                                         next_node, version + 1):
                return current_head.data
```

## Lock-Free Data Structures

### Lock-Free Stack

```python
class LockFreeStack:
    """Lock-free stack using compare-and-swap"""
    def __init__(self):
        self.head = AtomicReference(None)
        self.size = AtomicInteger(0)
    
    def push(self, item):
        """Push item onto stack"""
        new_node = Node(item)
        while True:
            current_head = self.head.get()
            new_node.next = current_head
            
            if self.head.compare_and_swap(current_head, new_node):
                self.size.increment_and_get()
                return True
    
    def pop(self):
        """Pop item from stack"""
        while True:
            current_head = self.head.get()
            if current_head is None:
                return None
            
            next_node = current_head.next
            if self.head.compare_and_swap(current_head, next_node):
                self.size.decrement_and_get()
                return current_head.data
```

### Lock-Free Queue (Michael & Scott Algorithm)

```python
class LockFreeQueue:
    """Lock-free queue using Michael & Scott algorithm"""
    def __init__(self):
        dummy = Node(None)
        self.head = AtomicReference(dummy)
        self.tail = AtomicReference(dummy)
    
    def enqueue(self, item):
        """Enqueue item to queue"""
        new_node = Node(item)
        
        while True:
            tail = self.tail.get()
            next_node = tail.next
            
            # Check if tail is still the last node
            if tail == self.tail.get():
                if next_node is None:
                    # Try to link new node at end
                    if self.cas_next(tail, None, new_node):
                        break
                else:
                    # Tail was lagging, try to advance it
                    self.tail.compare_and_swap(tail, next_node)
        
        # Try to advance tail to new node
        self.tail.compare_and_swap(tail, new_node)
    
    def dequeue(self):
        """Dequeue item from queue"""
        while True:
            head = self.head.get()
            tail = self.tail.get()
            next_node = head.next
            
            # Check consistency
            if head == self.head.get():
                if head == tail:
                    if next_node is None:
                        return None  # Queue is empty
                    # Tail is lagging, advance it
                    self.tail.compare_and_swap(tail, next_node)
                else:
                    if next_node is None:
                        continue  # Inconsistent state
                    
                    # Read data before CAS
                    data = next_node.data
                    
                    # Try to advance head
                    if self.head.compare_and_swap(head, next_node):
                        return data
    
    def cas_next(self, node, expected, new_value):
        """Atomic CAS on node's next pointer"""
        # In real implementation, this would be atomic
        if node.next == expected:
            node.next = new_value
            return True
        return False
```

### Lock-Free Hash Table

```python
class LockFreeHashTable:
    """Lock-free hash table with open addressing"""
    def __init__(self, initial_capacity=16):
        self.capacity = initial_capacity
        self.buckets = [AtomicReference(None) for _ in range(initial_capacity)]
        self.size = AtomicInteger(0)
        self.threshold = initial_capacity * 0.75
    
    def put(self, key, value):
        """Put key-value pair"""
        hash_value = hash(key)
        
        while True:
            bucket_index = hash_value % self.capacity
            bucket = self.buckets[bucket_index]
            
            # Try to find existing key or empty slot
            current = bucket.get()
            
            if current is None:
                # Empty bucket, try to insert
                new_node = HashNode(key, value, hash_value)
                if bucket.compare_and_swap(None, new_node):
                    self.size.increment_and_get()
                    self.check_resize()
                    return None
            else:
                # Traverse chain
                prev = None
                while current is not None:
                    if current.hash == hash_value and current.key == key:
                        # Key exists, update value
                        old_value = current.value
                        current.value = value
                        return old_value
                    prev = current
                    current = current.next
                
                # Key not found, add to end of chain
                new_node = HashNode(key, value, hash_value)
                if prev and self.cas_next(prev, None, new_node):
                    self.size.increment_and_get()
                    self.check_resize()
                    return None
    
    def get(self, key):
        """Get value by key"""
        hash_value = hash(key)
        bucket_index = hash_value % self.capacity
        current = self.buckets[bucket_index].get()
        
        while current is not None:
            if current.hash == hash_value and current.key == key:
                return current.value
            current = current.next
        
        return None
    
    def check_resize(self):
        """Check if resize is needed"""
        if self.size.get() > self.threshold:
            self.resize()
    
    def resize(self):
        """Resize hash table (simplified)"""
        # In practice, this would be more complex
        # involving gradual migration of buckets
        pass
```

## Memory Reclamation

### The Problem

In lock-free data structures, determining when it's safe to reclaim memory is challenging:
- Threads may hold references to nodes
- No global synchronization point
- Race conditions in reclamation

### Safe Memory Reclamation (SMR) Schemes

#### 1. Reference Counting

```python
class RefCountedNode:
    """Node with atomic reference counting"""
    def __init__(self, data):
        self.data = data
        self.next = None
        self.ref_count = AtomicInteger(1)
    
    def acquire(self):
        """Increment reference count"""
        self.ref_count.increment_and_get()
    
    def release(self):
        """Decrement reference count and delete if zero"""
        if self.ref_count.decrement_and_get() == 0:
            # Safe to delete
            if self.next:
                self.next.release()
            del self
```

#### 2. Quiescent State-Based Reclamation

```python
class QuiescentStateManager:
    """Quiescent state-based memory reclamation"""
    def __init__(self):
        self.active_threads = set()
        self.retired_objects = []
        self.lock = threading.Lock()
    
    def enter_critical_section(self):
        """Thread enters critical section"""
        thread_id = threading.current_thread().ident
        with self.lock:
            self.active_threads.add(thread_id)
    
    def exit_critical_section(self):
        """Thread exits critical section"""
        thread_id = threading.current_thread().ident
        with self.lock:
            self.active_threads.discard(thread_id)
            if not self.active_threads:
                # All threads quiescent, safe to reclaim
                self.reclaim_all()
    
    def retire_object(self, obj):
        """Retire object for later reclamation"""
        with self.lock:
            self.retired_objects.append(obj)
    
    def reclaim_all(self):
        """Reclaim all retired objects"""
        for obj in self.retired_objects:
            del obj
        self.retired_objects.clear()
```

## Performance Considerations

### Cache Effects

#### False Sharing
```python
# Bad: False sharing between counters
class BadCounters:
    def __init__(self):
        self.counters = [AtomicInteger(0) for _ in range(8)]

# Good: Cache line padding
class GoodCounters:
    def __init__(self):
        self.counters = []
        for i in range(8):
            # Pad to cache line size (64 bytes)
            counter = AtomicInteger(0)
            padding = [0] * 15  # 64 bytes total
            self.counters.append((counter, padding))
```

#### Cache Line Bouncing
- Minimize shared writes
- Use thread-local storage
- Batch operations

### NUMA Considerations

```python
class NUMAAwareLockFree:
    """NUMA-aware lock-free data structure"""
    def __init__(self):
        self.numa_nodes = self.detect_numa_topology()
        self.per_node_structures = {}
        
        for node in self.numa_nodes:
            # Allocate structure on specific NUMA node
            self.per_node_structures[node] = self.create_local_structure(node)
    
    def get_local_structure(self):
        """Get structure for current NUMA node"""
        current_node = self.get_current_numa_node()
        return self.per_node_structures.get(current_node, 
                                           self.per_node_structures[0])
```

### Backoff Strategies

```python
class ExponentialBackoff:
    """Exponential backoff for CAS loops"""
    def __init__(self, min_delay=1, max_delay=1000):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.current_delay = min_delay
    
    def backoff(self):
        """Perform backoff delay"""
        import random
        import time
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0, self.current_delay)
        time.sleep((self.current_delay + jitter) / 1000000)  # microseconds
        
        # Exponential increase
        self.current_delay = min(self.current_delay * 2, self.max_delay)
    
    def reset(self):
        """Reset backoff delay"""
        self.current_delay = self.min_delay
```

### Measuring Lock-Free Performance

```python
class LockFreeMetrics:
    """Metrics for lock-free data structures"""
    def __init__(self):
        self.operations = AtomicInteger(0)
        self.cas_successes = AtomicInteger(0)
        self.cas_failures = AtomicInteger(0)
        self.retries = AtomicInteger(0)
    
    def record_operation(self):
        """Record successful operation"""
        self.operations.increment_and_get()
    
    def record_cas_success(self):
        """Record successful CAS"""
        self.cas_successes.increment_and_get()
    
    def record_cas_failure(self):
        """Record failed CAS"""
        self.cas_failures.increment_and_get()
        self.retries.increment_and_get()
    
    def get_stats(self):
        """Get performance statistics"""
        ops = self.operations.get()
        successes = self.cas_successes.get()
        failures = self.cas_failures.get()
        
        return {
            'operations': ops,
            'cas_success_rate': successes / (successes + failures) if (successes + failures) > 0 else 0,
            'average_retries': failures / ops if ops > 0 else 0
        }
```

---

*Lock-free programming represents the cutting edge of concurrent algorithm design. While complex, it provides unmatched scalability and robustness for high-performance systems. Staff engineers must understand these concepts to architect the next generation of concurrent systems.*
