# Actor Model Patterns

## Table of Contents
1. [Introduction](#introduction)
2. [Theoretical Foundations](#theoretical-foundations)
3. [Actor Lifecycle Management](#actor-lifecycle-management)
4. [Supervision Strategies](#supervision-strategies)
5. [Mailbox Patterns](#mailbox-patterns)
6. [Communication Patterns](#communication-patterns)
7. [Fault Tolerance](#fault-tolerance)
8. [Performance Optimization](#performance-optimization)

## Introduction

The Actor Model, conceived by Carl Hewitt in 1973, is a mathematical model of concurrent computation that treats actors as the fundamental units of computation. This paradigm has gained significant traction in building distributed, fault-tolerant systems.

### Core Principles

1. **Everything is an Actor**: All computation occurs within actors
2. **Isolation**: Actors have private state, no shared memory
3. **Asynchronous Communication**: Actors communicate via message passing
4. **Location Transparency**: Actor references work regardless of location
5. **Supervision**: Hierarchical fault tolerance through supervision trees

### Actor Capabilities

Upon receiving a message, an actor can:
1. **Create** new actors
2. **Send** messages to other actors
3. **Designate** behavior for the next message
4. **Change** its internal state

## Theoretical Foundations

### Mathematical Model

An actor system can be formally described as:
- **A**: Set of actor addresses
- **M**: Set of possible messages
- **B**: Set of behaviors (functions from messages to actions)
- **→**: Message delivery relation

**Actor Semantics**:
```
Actor(address, behavior, mailbox) where:
- address ∈ A (unique identifier)
- behavior ∈ B (current message handler)
- mailbox ⊆ M* (ordered message queue)
```

### Concurrency Model

Unlike traditional threading models, the Actor Model provides:
- **No Race Conditions**: Private state eliminates data races
- **No Deadlocks**: No shared locks or resources
- **Natural Parallelism**: Each actor can run on different cores/machines
- **Fault Isolation**: Actor failures don't propagate automatically

### Comparison with Other Models

| Aspect | Actor Model | Shared Memory | CSP |
|--------|-------------|---------------|-----|
| State | Private | Shared | Private |
| Communication | Async Messages | Shared Variables | Sync Channels |
| Synchronization | Message Order | Locks/Semaphores | Channel Ops |
| Fault Tolerance | Supervision | Manual | Manual |
| Scalability | Excellent | Limited | Good |

## Actor Lifecycle Management

### Actor States

Actors progress through well-defined states:
- **CREATED**: Actor instantiated but not started
- **STARTING**: Actor initialization in progress
- **RUNNING**: Actor processing messages normally
- **STOPPING**: Actor shutting down gracefully
- **STOPPED**: Actor terminated normally
- **FAILED**: Actor terminated due to error
- **RESTARTING**: Actor being restarted by supervisor

### Lifecycle Hooks

Actors provide hooks for lifecycle events:
- **pre_start()**: Called before actor starts processing messages
- **post_stop()**: Called after actor stops processing messages
- **pre_restart()**: Called before actor restart
- **post_restart()**: Called after actor restart

## Supervision Strategies

### Supervision Hierarchy

Supervisors implement fault tolerance through hierarchical supervision:

#### Supervision Strategies
1. **ONE_FOR_ONE**: Restart only the failed child
2. **ONE_FOR_ALL**: Restart all children when one fails
3. **REST_FOR_ONE**: Restart failed child and those started after it
4. **ESCALATE**: Escalate failure to parent supervisor

#### Restart Policies
1. **PERMANENT**: Always restart the actor
2. **TEMPORARY**: Never restart the actor
3. **TRANSIENT**: Restart only on abnormal termination

### Circuit Breaker Pattern

Circuit breakers prevent cascading failures:
- **CLOSED**: Normal operation, requests pass through
- **OPEN**: Failing state, requests are rejected
- **HALF_OPEN**: Testing state, limited requests allowed

## Mailbox Patterns

### Mailbox Types

#### Unbounded Mailbox
- Unlimited message capacity
- Risk of memory exhaustion
- Simple FIFO ordering

#### Bounded Mailbox
- Limited message capacity
- Configurable overflow policies:
  - **DROP_NEWEST**: Discard new messages when full
  - **DROP_OLDEST**: Remove oldest messages when full
  - **BLOCK**: Block sender until space available

#### Priority Mailbox
- Messages processed by priority
- Uses heap queue for efficiency
- Prevents starvation with sequence numbers

#### Stash Mailbox
- Selective message processing
- Stash unwanted messages for later
- Useful for state machine actors

## Communication Patterns

### Request-Response Pattern

Synchronous-style communication over asynchronous message passing:
- Generate unique request IDs
- Track pending requests with timeouts
- Handle responses and timeouts appropriately

### Publish-Subscribe Pattern

Event-driven communication:
- Central event bus actor
- Topic-based message routing
- Dynamic subscription management
- Dead letter handling for failed deliveries

### Worker Pool Pattern

Load balancing across multiple workers:
- Pool manager coordinates work distribution
- Round-robin or other assignment strategies
- Worker lifecycle management
- Backpressure handling

## Fault Tolerance

### Let It Crash Philosophy

The Actor Model embraces failure as normal:
- Don't try to handle every possible error
- Let actors fail and restart them clean
- Use supervision to contain failures
- Design for recovery, not prevention

### Error Kernel Pattern

Isolate critical functionality:
- Keep error-prone code in separate actors
- Minimize critical code paths
- Use delegation for risky operations
- Maintain system stability through isolation

### Bulkhead Pattern

Resource isolation prevents cascading failures:
- Separate resource pools by service type
- Independent failure domains
- Circuit breakers per service
- Graceful degradation capabilities

## Performance Optimization

### Message Batching

Reduce overhead by batching messages:
- Collect multiple messages before processing
- Trade latency for throughput
- Configurable batch sizes and timeouts

### Actor Pooling

Reuse actor instances:
- Pool actors for expensive initialization
- Round-robin assignment
- Lifecycle management within pools

### Location Transparency

Optimize for distributed deployment:
- Local vs remote actor references
- Message serialization considerations
- Network partition handling
- Load balancing across nodes

### Memory Management

Efficient resource usage:
- Message object pooling
- Garbage collection considerations
- Memory leak prevention
- Resource cleanup in lifecycle hooks

---

*The Actor Model provides a powerful abstraction for building concurrent, distributed, and fault-tolerant systems. Understanding these patterns is essential for staff engineers architecting modern scalable applications.*
