# Synchronization Primitives

## Table of Contents
1. [Overview](#overview)
2. [Semaphores](#semaphores)
3. [Condition Variables](#condition-variables)
4. [Barriers and Latches](#barriers-and-latches)
5. [Reader-Writer Locks](#reader-writer-locks)
6. [Advanced Synchronization](#advanced-synchronization)

## Overview

Synchronization primitives are the fundamental building blocks for coordinating concurrent execution. Understanding their properties, trade-offs, and correct usage is essential for staff engineers designing concurrent systems.

### Classification by Consensus Number

According to Herlihy's hierarchy:

1. **Consensus Number 1**: Read/Write registers, FIFO queues, stacks
2. **Consensus Number 2**: Test-and-set, swap, fetch-and-add, semaphores
3. **Consensus Number ∞**: Compare-and-swap, load-linked/store-conditional

### Design Principles

1. **Correctness**: Mutual exclusion, progress, bounded waiting
2. **Performance**: Minimize contention and context switches
3. **Fairness**: Prevent starvation and ensure equitable access
4. **Composability**: Primitives should work together without deadlock
5. **Scalability**: Performance should degrade gracefully with contention

## Semaphores

### Historical Context

Introduced by Edsger Dijkstra in 1965 for the THE operating system. Semaphores are one of the oldest and most fundamental synchronization primitives.

### Mathematical Definition

A semaphore S is an integer variable that can only be accessed through two atomic operations:
- **P(S)** (Proberen - Dutch for "to test"): `while S ≤ 0 do skip; S := S - 1`
- **V(S)** (Verhogen - Dutch for "to increment"): `S := S + 1`

### Types of Semaphores

#### Binary Semaphore (Mutex)
- Values: 0 or 1
- Equivalent to mutex lock
- Provides mutual exclusion

#### Counting Semaphore
- Values: 0 to N
- Manages resource pools
- Controls access to N identical resources

#### Fair Semaphore
- FIFO ordering of waiters
- Prevents starvation
- Higher overhead but guaranteed fairness

#### Priority Semaphore
- Priority-based ordering
- Higher priority threads acquire first
- Useful in real-time systems

### Semaphore Use Cases

1. **Resource Pool Management**: Database connections, thread pools
2. **Producer-Consumer**: Bounded buffer synchronization
3. **Rate Limiting**: Throttling concurrent operations
4. **Barrier Synchronization**: Coordinating multiple threads

### Implementation Considerations

- **Spurious Wakeups**: Always check condition in loop
- **Timeout Handling**: Graceful degradation on timeouts
- **Exception Safety**: Ensure proper cleanup
- **Performance**: Minimize lock contention

## Condition Variables

### Theoretical Foundation

Condition variables allow threads to wait for certain conditions to become true. They are always used in conjunction with a mutex to avoid race conditions.

### Monitor Pattern

The monitor pattern encapsulates shared data with synchronization:
- Mutual exclusion for data access
- Condition variables for coordination
- Structured programming approach

### Key Concepts

#### Wait Semantics
```python
# Standard pattern
with lock:
    while not condition():
        condition_var.wait()
    # condition is true here
```

#### Signal vs Broadcast
- **Signal**: Wake one waiting thread
- **Broadcast**: Wake all waiting threads
- Choose based on condition semantics

#### Mesa vs Hoare Semantics
- **Mesa**: Signaling thread continues, signaled thread competes for lock
- **Hoare**: Signaling thread blocks, signaled thread runs immediately
- Most systems use Mesa semantics

### Advanced Patterns

#### Bounded Buffer
Classic producer-consumer synchronization using condition variables.

#### Reader-Writer Coordination
Multiple readers or single writer using condition variables.

#### State Machines
Complex synchronization patterns using multiple condition variables.

## Barriers and Latches

### Cyclic Barrier

Allows a set of threads to wait for each other to reach a common synchronization point.

**Properties**:
- Reusable across multiple phases
- Optional barrier action
- Automatic reset after all threads arrive
- Exception handling for broken barriers

**Use Cases**:
- Parallel algorithms with phases
- Bulk synchronous parallel (BSP) programs
- Iterative computations

### CountDown Latch

One-time synchronization primitive that allows threads to wait until a count reaches zero.

**Properties**:
- One-time use only
- Cannot be reset
- Multiple threads can wait
- Efficient for completion signaling

**Use Cases**:
- Service startup coordination
- Task completion waiting
- Resource initialization

### Phaser

Flexible barrier supporting dynamic registration and multiple phases.

**Advanced Features**:
- Dynamic party registration/deregistration
- Hierarchical phasers
- Termination support
- Arrival without waiting

**Use Cases**:
- Complex parallel algorithms
- Fork-join frameworks
- Pipeline synchronization

## Reader-Writer Locks

### Problem Statement

Multiple threads accessing shared data:
- **Readers**: Only read data (can be concurrent)
- **Writers**: Modify data (must be exclusive)

### Variants

#### Basic Reader-Writer Lock
- Multiple readers OR one writer
- May suffer from writer starvation
- Simple implementation

#### Fair Reader-Writer Lock
- Prevents writer starvation
- Uses waiting writer count
- Balanced performance

#### Write-Preferring Lock
- Gives preference to writers
- Prevents reader starvation of writers
- Better for write-heavy workloads

#### Upgradable Lock
- Allows upgrading read lock to write lock
- Prevents upgrade deadlocks
- Complex but powerful

### Performance Considerations

1. **Read-Heavy Workloads**: Basic RW lock performs well
2. **Write-Heavy Workloads**: Consider write-preferring variants
3. **Mixed Workloads**: Fair locks provide balanced performance
4. **Upgrade Patterns**: Use upgradable locks carefully

## Advanced Synchronization

### Priority Inheritance

Solves priority inversion in real-time systems:
- Low priority thread holds lock
- High priority thread waits
- Medium priority thread preempts low priority
- Solution: Inherit high priority temporarily

### Adaptive Locks

Locks that adapt behavior based on contention:
- Spin under low contention
- Block under high contention
- Monitor contention patterns
- Optimize for workload characteristics

### Hierarchical Locks

Prevent deadlock through lock ordering:
- Assign levels to locks
- Acquire in ascending order only
- Compile-time or runtime checking
- Systematic deadlock prevention

### Lock-Free Alternatives

When possible, consider lock-free approaches:
- Atomic operations
- Compare-and-swap loops
- Memory ordering considerations
- Higher complexity but better scalability

### Performance Optimization

#### Cache Considerations
- False sharing avoidance
- Cache line alignment
- NUMA awareness
- Memory access patterns

#### Contention Reduction
- Lock splitting
- Lock striping
- Reader-writer separation
- Optimistic concurrency

#### Scalability Patterns
- Hierarchical locking
- Delegation patterns
- Work stealing
- Lock-free data structures

---

*Understanding these synchronization primitives deeply is essential for staff engineers. Each primitive has specific use cases, performance characteristics, and correctness requirements that must be carefully considered in system design.*
