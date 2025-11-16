# Part I.2: The Art of Multiprocessor Programming - Key Concepts

**Navigation:** [← Foundations](./01-foundations.md) | [Home](../README.md) | [Next: Lock-Free Algorithms →](./03-lock-free.md)

---

## Table of Contents

1. [Introduction to Herlihy & Shavit's Framework](#introduction)
2. [Consensus Numbers](#consensus-numbers)
3. [Linearizability](#linearizability)
4. [Sequential Consistency vs Linearizability](#sequential-consistency-vs-linearizability)
5. [Universal Constructions](#universal-constructions)
6. [The Impossibility Results](#impossibility-results)
7. [Practical Implications](#practical-implications)

---

## Introduction

**"The Art of Multiprocessor Programming"** by Maurice Herlihy and Nir Shavit (with Luchangco and Spear in 2nd edition) is the foundational textbook for understanding concurrent algorithm theory. Published originally in 2008 with a revised edition in 2012 and 2nd edition in 2020, it provides the theoretical framework for analyzing concurrent data structures and algorithms.

### Core Contributions

The book introduces several fundamental concepts:

1. **Linearizability**: The gold standard for concurrent object correctness
2. **Consensus Numbers**: A hierarchy of synchronization primitives
3. **Wait-Free/Lock-Free Progress Guarantees**: Formal definitions
4. **Universal Constructions**: Building wait-free objects from consensus
5. **Impossibility Results**: What cannot be done without certain primitives

These concepts form the theoretical foundation that guides practical concurrent programming across all languages and systems.

---

## Consensus Numbers

### The Consensus Problem

**Definition**: N threads each start with a private input value. They must agree on one of these values such that:
1. **Consistent**: All threads decide on the same value
2. **Valid**: The decided value was some thread's input
3. **Wait-Free**: Every thread decides in finite steps

### Consensus Number

**Definition**: The **consensus number** of an object is the maximum number of threads for which the object can solve consensus.

Objects are ranked by their power to solve consensus:

| Object Type | Consensus Number | Example Primitives |
|-------------|------------------|-------------------|
| Read/Write | 1 | Atomic read/write registers |
| Test-And-Set, Swap | 2 | `getAndSet()`, Swap |
| Fetch-And-Add, Queue, Stack | 2 | `getAndAdd()`, FIFO, LIFO |
| Compare-And-Swap, LL/SC | ∞ | `compareAndSwap()`, Load-Linked/Store-Conditional |

### Key Theorem (Herlihy, 1991)

**Theorem**: It is impossible to construct a wait-free implementation of an object with consensus number n from objects with consensus number < n.

**Implication**: CAS and LL/SC are **universal**—they can implement any wait-free object.

### Proof Sketch: Read/Write has Consensus Number 1

**Claim**: Atomic read/write registers cannot solve consensus for 2 threads.

**Intuition**: Any protocol using only reads and writes must have a "critical state" where the next operation decides the outcome. But if two threads execute simultaneously from that state, they may read before the other writes, leading to disagreement.

**Formal Proof** (Simplified):
1. Consider a wait-free consensus protocol using only read/write
2. There must be a state where the outcome is not yet determined
3. From that state, consider two threads executing their next operations
4. If both are reads: they see the same state, must decide identically, but both inputs should be possible
5. If both are writes: each writes its own value, but then subsequent reads can't distinguish
6. Contradiction → impossible

### Proof Sketch: CAS has Consensus Number ∞

**Construction**: Use CAS to implement consensus for N threads.

```python
# Pseudocode for N-thread consensus using CAS
shared_decision = None  # Atomic reference

def decide(my_value):
    if shared_decision.compare_and_swap(None, my_value):
        # I won! My value is the decision
        return my_value
    else:
        # Someone else won
        return shared_decision.get()
```

**Why it works**: CAS atomically checks and updates, ensuring exactly one thread "wins" and sets the decision. All other threads see that decision.

---

## Linearizability

### Definition

**Linearizability** (Herlihy & Wing, 1990): A concurrent object is linearizable if:

1. Each operation appears to take effect **instantaneously** at some point between its invocation and response (the **linearization point**)
2. Operations can be ordered such that:
   - The order respects the actual real-time ordering (if op1 completes before op2 starts, op1 comes first)
   - The sequential behavior matches the object's specification

### Visual Example

```
Thread 1: |--push(A)--|        |--pop()=A--|
Thread 2:        |--push(B)--|        |--pop()=B--|
Time:     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━→

Linearization Points (✓):
         ✓       ✓      ✓      ✓
    push(A)  push(B)  pop()=A  pop()=B

Sequential Order: push(A), push(B), pop()→A, pop()→B ✓ Valid
```

### Non-Linearizable Example

```
Thread 1: |--enq(1)--|  |--deq()=2--|
Thread 2:        |--enq(2)--|

This is NOT linearizable if deq() returns 2:
- enq(1) must linearize before deq()=2 (deq returns 2, not 1)
- But enq(2) must also linearize before deq()=2
- Yet enq(2) starts after enq(1) completes
- So sequential order would be: enq(1), enq(2), deq()=2
- But deq() should return 1 (FIFO), not 2!
- Contradiction → Not linearizable
```

### Linearizability vs Sequential Consistency

**Sequential Consistency** (SC): All threads see the same order of operations, respecting per-thread program order.

**Linearizability**: Stronger than SC—additionally requires respecting real-time ordering.

**Key Difference**:

```
Thread 1: write(x, 1)  read(y)=0
Thread 2: write(y, 1)  read(x)=0

Sequential Consistency: ALLOWED (both reads before both writes in some order)
Linearizability: FORBIDDEN (real-time order is violated)
```

**Composability**:
- **Linearizability is compositional**: If each object is linearizable, the system is linearizable
- **SC is NOT compositional**: Composing SC objects doesn't guarantee system-wide SC

**Why Linearizability Matters**: Compositional reasoning is crucial for modular system design.

---

## Sequential Consistency vs Linearizability

### Sequential Consistency (Lamport, 1979)

**Definition**: Execution is equivalent to some interleaving of threads' operations that:
1. Respects each thread's program order
2. Is the same interleaving for all threads

**Does NOT require**: Respecting real-time ordering across threads.

### Linearizability (Herlihy & Wing, 1990)

**Definition**: Each operation has a linearization point, and:
1. Operations ordered by linearization points respect real-time order
2. Sequential execution respects object's specification

**DOES require**: Respecting real-time ordering.

### Comparison Table

| Property | Sequential Consistency | Linearizability |
|----------|------------------------|-----------------|
| Respects program order | ✓ | ✓ |
| Same order for all threads | ✓ | ✓ |
| Respects real-time order | ✗ | ✓ |
| Compositional | ✗ | ✓ |
| Allows buffering/caching | ✓ | Limited |
| Typical use | Hardware memory models | Concurrent data structures |

### Example: Compositional Reasoning

**Scenario**: Two linearizable queues Q1 and Q2.

```python
Thread 1: Q1.enq(1); x = Q2.deq()
Thread 2: Q2.enq(2); y = Q1.deq()
```

**Linearizability Guarantee**: The system is linearizable because each queue is.

**Possible Sequential Histories**:
- Q1.enq(1) → Q2.enq(2) → Q2.deq()=2 → Q1.deq()=1 (x=2, y=1)
- Q2.enq(2) → Q1.enq(1) → Q1.deq()=1 → Q2.deq()=2 (x=2, y=1)
- etc.

**With SC (non-compositional)**: Cannot easily reason about Q1 and Q2 independently.

---

## Universal Constructions

### Universal Object

**Definition**: An object is **universal** if it can implement any other object in a wait-free manner (for any number of threads).

**Theorem (Herlihy)**: CAS and LL/SC are universal for any number of threads.

### Universal Construction Using CAS

**Idea**: Maintain a shared log of operations. Use CAS to append to the log.

```python
class UniversalConstruction:
    def __init__(self, initial_state):
        self.head = Node(initial_state)  # Shared pointer to log head
    
    def apply_operation(self, operation):
        while True:
            # Read current head
            old_head = self.head.get()
            
            # Compute new state
            new_state = operation(old_head.state)
            new_node = Node(new_state, next=old_head)
            
            # Try to CAS new head
            if self.head.compare_and_swap(old_head, new_node):
                return new_state  # Success
            # CAS failed, retry
```

**Properties**:
- **Wait-Free**: Every thread makes progress in O(1) CAS attempts
- **Linearization Point**: The successful CAS
- **Works for any object**: Just need to define `operation(state)`

**Limitations**:
- **Memory**: Unbounded log (can use garbage collection)
- **Performance**: Not always optimal for specific data structures

### Practical Universal Constructions

Real-world systems use optimized structures rather than generic universal constructions:
- **Lock-Free Stacks**: CAS on head pointer
- **Lock-Free Queues**: CAS on head and tail (Michael-Scott Queue)
- **Lock-Free Hash Tables**: CAS on array elements
- **Lock-Free Skip Lists**: CAS on node pointers

These are **hand-optimized** for performance but follow the principles of universal constructions.

---

## Impossibility Results

### FLP Impossibility (Fischer, Lynch, Paterson, 1985)

**Theorem**: In an asynchronous distributed system, it is impossible to guarantee consensus in the presence of even one crash failure.

**Context**: Asynchronous = no bounds on message delays or processing speeds.

**Implication**: Practical consensus algorithms (Paxos, Raft) require either:
- Timing assumptions (partial synchrony)
- Randomization
- Failure detectors

**Does NOT apply to**: Shared-memory multiprocessors with CAS (different model).

### Impossibility of Wait-Free Implementations

**Theorem (Herlihy, 1991)**: Cannot implement an object with consensus number n using only objects with consensus number < n in a wait-free manner.

**Example**: Cannot build a wait-free CAS using only read/write registers.

**Implication**: Hardware must provide strong primitives (CAS, LL/SC) for universal wait-free constructions.

### Impossibility of Deterministic Wait-Free Consensus with Read/Write

**Theorem**: No deterministic wait-free consensus protocol exists using only atomic read/write registers.

**Proof Sketch**:
1. Consider bivalent states (outcome not yet determined)
2. From any bivalent state, both outcomes are still possible
3. Show that any protocol must have a bivalent state from which both threads can execute
4. Leads to infinite execution without deciding

**Practical Workarounds**:
- Use stronger primitives (CAS, LL/SC)
- Allow blocking (use locks)
- Randomized algorithms (not deterministic)

---

## Practical Implications

### 1. Hardware Primitives Matter

**Insight**: Your hardware's atomic operations determine what concurrent algorithms are possible.

**x86/x64** (Intel, AMD):
- Strong: CAS, Fetch-And-Add, locked instructions
- Memory Model: TSO (relatively strong)
- Implication: Can build universal lock-free structures

**ARM, RISC-V**:
- Strong: LL/SC (Load-Linked/Store-Conditional)
- Memory Model: Relaxed (weaker)
- Implication: Requires more fences, but LL/SC avoids ABA problem

**Older SPARC, MIPS**:
- Limited atomics
- Implication: May require locks for complex data structures

### 2. Linearizability as Design Goal

**When to use**:
- Concurrent data structures (queues, stacks, maps)
- Systems requiring compositional reasoning
- Correctness-critical applications

**Verification**:
- Model checking (e.g., Spin, TLA+)
- Linearizability checkers (e.g., Lincheck for JVM)
- Property-based testing with happens-before assertions

### 3. Consensus Numbers Guide Design

**Guideline**: Choose primitives with sufficient consensus number.

**Example: Concurrent Counter**
- **Read/Write only**: Cannot be wait-free
- **Fetch-And-Add**: Consensus number ∞ on same object—perfect fit!

**Example: Concurrent Queue**
- **Read/Write only**: Cannot be wait-free (consensus number 1)
- **CAS**: Can build Michael-Scott Queue (lock-free)

### 4. Wait-Free vs Lock-Free Tradeoffs

**Wait-Free**:
- Pro: Guaranteed per-thread progress
- Con: Often complex, higher overhead
- Use: Real-time systems, critical paths

**Lock-Free**:
- Pro: Guaranteed system progress, simpler than wait-free
- Con: Individual threads might starve
- Use: High-throughput servers, non-real-time systems

**Blocking (Locks)**:
- Pro: Simple, well-understood
- Con: Deadlock risk, priority inversion
- Use: Most application-level code

### 5. Language and Library Support

**C++**:
- `std::atomic` with `memory_order` for lock-free programming
- `std::mutex`, `std::shared_mutex` for locks

**Java**:
- `java.util.concurrent.atomic.*` package
- `java.util.concurrent.*` data structures (ConcurrentHashMap, etc.)

**Rust**:
- `std::sync::atomic` for atomics
- `crossbeam` library for lock-free structures

**Go**:
- `sync/atomic` package
- Channels (higher-level abstraction)

**Python**:
- Limited lock-free support (GIL complicates)
- `threading.Lock`, `multiprocessing` for parallelism

---

## Herlihy & Shavit's Methodological Contributions

### 1. Formal Proof Techniques

The book teaches rigorous proof methods:
- **Invariant-based reasoning**
- **Simulation arguments**
- **Adversarial scheduling analysis**

### 2. Correctness Conditions Hierarchy

```
Linearizability (strongest)
    ↓
Sequential Consistency
    ↓
Quiescent Consistency
    ↓
Observational Refinement
```

### 3. Progress Conditions Hierarchy

```
Wait-Free (strongest)
    ↓
Lock-Free
    ↓
Obstruction-Free
    ↓
Deadlock-Free
    ↓
Starvation-Free
    ↓
Blocking (weakest)
```

### 4. Complexity Measures

- **Step Complexity**: Number of steps in an operation
- **Contention**: Number of concurrent operations
- **Amortized Complexity**: Average over operation sequences

---

## Key Algorithms from Herlihy & Shavit

### 1. Bakery Lock (Lamport)

**Properties**: Starvation-free, uses only read/write

```python
# Simplified Bakery Lock
class BakeryLock:
    def __init__(self, n):
        self.flag = [False] * n
        self.label = [0] * n
    
    def lock(self, i):
        self.flag[i] = True
        self.label[i] = 1 + max(self.label)
        # Wait until no earlier thread
        for j in range(len(self.flag)):
            while self.flag[j] and (self.label[j], j) < (self.label[i], i):
                pass  # Spin
    
    def unlock(self, i):
        self.flag[i] = False
```

### 2. Peterson Lock (2-thread)

**Properties**: Deadlock-free, uses read/write

```python
class PetersonLock:
    def __init__(self):
        self.flag = [False, False]
        self.victim = 0
    
    def lock(self, i):
        j = 1 - i
        self.flag[i] = True
        self.victim = i
        while self.flag[j] and self.victim == i:
            pass  # Spin
    
    def unlock(self, i):
        self.flag[i] = False
```

### 3. CLH Queue Lock

**Properties**: FIFO, space-efficient, local spinning

```python
class CLHLock:
    def __init__(self):
        self.tail = AtomicReference(QNode())
    
    def lock(self):
        my_node = QNode()
        my_node.locked = True
        pred = self.tail.get_and_set(my_node)
        while pred.locked:
            pass  # Spin on predecessor
        return pred  # Keep for unlock
    
    def unlock(self, pred_node):
        my_node.locked = False
        my_node = pred_node  # Reuse predecessor's node
```

### 4. Treiber Stack (Lock-Free)

**Properties**: Lock-free, uses CAS

```python
class TreiberStack:
    def __init__(self):
        self.head = AtomicReference(None)
    
    def push(self, value):
        new_node = Node(value)
        while True:
            old_head = self.head.get()
            new_node.next = old_head
            if self.head.compare_and_swap(old_head, new_node):
                return
    
    def pop(self):
        while True:
            old_head = self.head.get()
            if old_head is None:
                return None
            new_head = old_head.next
            if self.head.compare_and_swap(old_head, new_head):
                return old_head.value
```

### 5. Michael-Scott Queue (Lock-Free)

**Properties**: Lock-free, FIFO, uses CAS on both head and tail

```python
class MSQueue:
    def __init__(self):
        dummy = Node(None)
        self.head = AtomicReference(dummy)
        self.tail = AtomicReference(dummy)
    
    def enqueue(self, value):
        new_node = Node(value)
        while True:
            last = self.tail.get()
            next_node = last.next.get()
            if last == self.tail.get():  # Still tail?
                if next_node is None:  # Tail is last?
                    if last.next.compare_and_swap(None, new_node):
                        self.tail.compare_and_swap(last, new_node)
                        return
                else:
                    # Help move tail forward
                    self.tail.compare_and_swap(last, next_node)
    
    def dequeue(self):
        while True:
            first = self.head.get()
            last = self.tail.get()
            next_node = first.next.get()
            if first == self.head.get():
                if first == last:
                    if next_node is None:
                        return None  # Empty
                    # Help move tail
                    self.tail.compare_and_swap(last, next_node)
                else:
                    value = next_node.value
                    if self.head.compare_and_swap(first, next_node):
                        return value
```

---

## Summary: Herlihy & Shavit Takeaways

1. **Consensus numbers classify primitive power**: CAS/LL/SC are universal; read/write is not.
2. **Linearizability is the gold standard** for concurrent object correctness.
3. **Linearizability is compositional**; sequential consistency is not.
4. **Universal constructions show what's possible**, but hand-optimized structures are faster.
5. **Impossibility results define limits**: Some problems cannot be solved without strong primitives.
6. **Progress guarantees matter**: Wait-free > Lock-free > Obstruction-free > Blocking.
7. **Theory guides practice**: Understanding consensus numbers and linearizability helps choose the right primitives and verify correctness.

---

**Next:** [Lock-Free and Wait-Free Algorithms →](./03-lock-free.md)

**Navigation:** [← Foundations](./01-foundations.md) | [Home](../README.md) | [Next →](./03-lock-free.md)
