# Complete Guide to Concurrency Patterns

This comprehensive guide covers all major concurrency patterns and when to use them.

## Table of Contents

1. [Concurrency Fundamentals](#concurrency-fundamentals)
2. [Threading Patterns](#threading-patterns)
3. [Asyncio Patterns](#asyncio-patterns)
4. [Multiprocessing Patterns](#multiprocessing-patterns)
5. [Actor Model](#actor-model)
6. [CSP (Communicating Sequential Processes)](#csp-patterns)
7. [Synchronization Primitives](#synchronization-primitives)
8. [Reactive Patterns](#reactive-patterns)
9. [Performance Guidelines](#performance-guidelines)
10. [Common Pitfalls](#common-pitfalls)

## Concurrency Fundamentals

### What is Concurrency?

Concurrency is about dealing with multiple tasks at the same time. It's not necessarily about doing multiple things simultaneously (that's parallelism), but about managing multiple tasks that may be in progress at the same time.

### Concurrency vs Parallelism

- **Concurrency**: Multiple tasks making progress (may or may not be simultaneous)
- **Parallelism**: Multiple tasks executing simultaneously on multiple cores

### Python's Concurrency Models

1. **Threading**: Concurrent execution with shared memory (limited by GIL for CPU tasks)
2. **Asyncio**: Cooperative multitasking with event loops
3. **Multiprocessing**: True parallelism with separate processes
4. **Actor Model**: Message-passing concurrency
5. **CSP**: Channel-based communication

## Threading Patterns

### When to Use Threading

- I/O-bound tasks (file operations, network requests)
- Tasks that spend time waiting
- Mixed workloads with some blocking operations

### Producer-Consumer Pattern

```python
from src.threading_patterns.producer_consumer import ProducerConsumer

# Create producer-consumer system
pc = ProducerConsumer(buffer_capacity=10)

# Add producers and consumers
pc.add_producer(100, lambda: f"data-{time.time()}")
pc.add_consumer(lambda item: process_data(item))

# Run the simulation
pc.start()
pc.wait_for_completion()
```

### Thread Pools

```python
from src.threading_patterns.thread_pool import BackpressureThreadPool

# Create pool with backpressure handling
pool = BackpressureThreadPool(
    max_workers=4,
    max_queue_size=100,
    rejection_policy=RejectionPolicy.CALLER_RUNS
)

# Submit tasks
future = pool.submit(cpu_intensive_task, data)
result = future.result()
```

### Reader-Writer Locks

```python
from src.synchronization.reader_writer import BasicReadWriteLock

lock = BasicReadWriteLock()

# Multiple readers can access simultaneously
with lock.read_lock():
    data = shared_resource.read()

# Only one writer at a time
with lock.write_lock():
    shared_resource.write(new_data)
```

## Asyncio Patterns

### When to Use Asyncio

- I/O-bound tasks with high concurrency needs
- Network programming
- Event-driven applications
- Tasks that can be broken into cooperative chunks

### Async Producer-Consumer

```python
from src.asyncio_patterns.async_producer_consumer import AsyncProducerConsumer

async def main():
    pc = AsyncProducerConsumer(buffer_capacity=100)
    
    # Add async producers and consumers
    pc.add_producer(lambda: generate_data(), production_rate=10.0)
    pc.add_consumer(lambda item: process_data_async(item))
    
    # Run simulation
    await pc.run_simulation(duration=60.0)
```

### Rate Limiting

```python
from src.asyncio_patterns.rate_limiting import TokenBucketLimiter

limiter = TokenBucketLimiter(rate=10.0, capacity=20)

async def make_request():
    await limiter.acquire()  # Wait for rate limit
    # Make your request here
    return await api_call()
```

### Async Channels

```python
from src.csp_patterns.channels import BufferedChannel

ch = BufferedChannel(capacity=10)

async def producer():
    for i in range(100):
        await ch.send(f"item-{i}")

async def consumer():
    async for item in ch:
        await process_item(item)
```

## Multiprocessing Patterns

### When to Use Multiprocessing

- CPU-bound tasks
- Tasks that can benefit from true parallelism
- Computationally intensive work
- When you need to bypass the GIL

### Process Pools

```python
from src.multiprocessing_patterns.process_pool import ProcessPool

pool = ProcessPool(min_workers=2, max_workers=8)
pool.start()

# Submit CPU-intensive tasks
task_id = pool.submit(cpu_intensive_function, large_dataset)
result = pool.get_result()

pool.shutdown()
```

### Map-Reduce Pattern

```python
# Parallel processing of large datasets
def map_function(chunk):
    return [process_item(item) for item in chunk]

def reduce_function(results):
    return sum(results, [])

# Use process pool for parallel map-reduce
results = pool.map(map_function, data_chunks)
final_result = reduce_function(results)
```

## Actor Model

### When to Use Actors

- Distributed systems
- Fault-tolerant applications
- Systems with complex state management
- Message-driven architectures

### Basic Actor Usage

```python
from src.actor_model.actor_system import ActorSystem, Actor

class CalculatorActor(Actor):
    async def receive(self, message):
        if message.content["op"] == "add":
            a, b = message.content["args"]
            return a + b

# Create actor system
system = ActorSystem("calculator-system")
await system.start()

# Create and use actors
calc_ref = system.actor_of(CalculatorActor, "calculator")
result = await calc_ref.ask({"op": "add", "args": [5, 3]})
print(f"Result: {result}")  # Output: Result: 8
```

### Actor Supervision

```python
# Actors can supervise other actors for fault tolerance
class SupervisorActor(Actor):
    async def pre_start(self):
        # Create child actors
        self.worker = self.actor_system.actor_of(WorkerActor, "worker")
    
    async def receive(self, message):
        # Forward work to child actor
        return await self.worker.ask(message.content)
```

## CSP Patterns

### When to Use CSP

- Pipeline processing
- Producer-consumer scenarios
- Event streaming
- Coordinated concurrent processes

### Channel Communication

```python
from src.csp_patterns.channels import make_channel

# Synchronous channel (unbuffered)
ch = make_channel()  # Synchronous

async def sender():
    await ch.send("Hello, CSP!")

async def receiver():
    message = await ch.receive()
    print(f"Received: {message}")

# Buffered channel
buffered_ch = make_channel(capacity=10)
```

### Pipeline Pattern

```python
# Create processing pipeline
input_ch = make_channel(capacity=10)
output_ch = make_channel(capacity=10)

async def stage1():
    async for item in input_ch:
        processed = transform_data(item)
        await output_ch.send(processed)

async def stage2():
    async for item in output_ch:
        final_result = finalize_data(item)
        print(f"Final: {final_result}")
```

### Fan-Out/Fan-In Pattern

```python
# Fan-out: Distribute work to multiple workers
work_channels = [make_channel(capacity=5) for _ in range(3)]

async def distributor():
    for i, item in enumerate(work_items):
        channel = work_channels[i % len(work_channels)]
        await channel.send(item)

# Fan-in: Collect results from multiple workers
result_ch = make_channel(capacity=20)

async def worker(work_ch):
    async for item in work_ch:
        result = process_item(item)
        await result_ch.send(result)
```

## Synchronization Primitives

### Reader-Writer Locks

Use when you have shared data that's read frequently but written rarely:

```python
from src.synchronization.reader_writer import FairReadWriteLock

lock = FairReadWriteLock()

# Multiple readers can access simultaneously
def reader():
    with lock.read_lock():
        return shared_data.copy()

# Exclusive writer access
def writer():
    with lock.write_lock():
        shared_data.update(new_data)
```

### Barriers

Synchronize multiple threads at a checkpoint:

```python
from src.synchronization.barriers import CyclicBarrier

barrier = CyclicBarrier(3)  # Wait for 3 threads

def worker():
    # Do some work
    process_data()
    
    # Wait for all workers to reach this point
    barrier.wait()
    
    # Continue with synchronized work
    synchronized_operation()
```

### Semaphores

Control access to limited resources:

```python
from src.synchronization.semaphores import BoundedSemaphore

# Allow only 3 concurrent database connections
db_semaphore = BoundedSemaphore(3)

def database_operation():
    with db_semaphore:
        # Only 3 threads can execute this simultaneously
        return database.query(sql)
```

## Reactive Patterns

### When to Use Reactive Programming

- Event-driven systems
- Real-time data processing
- UI applications
- Stream processing

### Observable Streams

```python
from src.reactive_patterns.observables import Observable

# Create observable stream
stream = Observable.from_iterable([1, 2, 3, 4, 5])

# Transform and filter data
result = (stream
    .map(lambda x: x * 2)
    .filter(lambda x: x > 4)
    .reduce(lambda acc, x: acc + x, 0))

print(result)  # Output: 18 (6 + 8 + 10)
```

### Event Bus

```python
from src.reactive_patterns.event_bus import EventBus

bus = EventBus()

# Subscribe to events
@bus.subscribe("user_login")
def handle_login(event):
    print(f"User {event.data['user_id']} logged in")

# Publish events
bus.publish("user_login", {"user_id": 123, "timestamp": time.time()})
```

## Performance Guidelines

### Choosing the Right Pattern

| Task Type | Recommended Pattern | Reason |
|-----------|-------------------|---------|
| CPU-intensive | Multiprocessing | True parallelism, bypasses GIL |
| I/O-intensive | Asyncio | High concurrency, low overhead |
| Mixed workload | Threading | Balance of concurrency and simplicity |
| Distributed systems | Actor Model | Fault tolerance, location transparency |
| Pipeline processing | CSP | Clear data flow, composable |
| Shared state | Synchronization | Safe concurrent access |

### Performance Tips

1. **Measure First**: Always profile before optimizing
2. **Right Tool**: Choose the appropriate concurrency model
3. **Avoid Oversubscription**: Don't create more threads/processes than cores for CPU work
4. **Minimize Contention**: Reduce shared state and locking
5. **Batch Operations**: Group small operations together
6. **Use Connection Pools**: Reuse expensive resources

### Benchmarking

```python
from benchmarks.performance_comparison import BenchmarkRunner

runner = BenchmarkRunner()
runner.run_all()  # Compare all concurrency approaches
```

## Common Pitfalls

### 1. Race Conditions

**Problem**: Multiple threads accessing shared data without synchronization

```python
# BAD: Race condition
counter = 0

def increment():
    global counter
    counter += 1  # Not atomic!

# GOOD: Use locks
lock = threading.Lock()

def increment_safe():
    global counter
    with lock:
        counter += 1
```

### 2. Deadlocks

**Problem**: Circular dependency in lock acquisition

```python
# BAD: Potential deadlock
def transfer(from_account, to_account, amount):
    with from_account.lock:
        with to_account.lock:  # Order matters!
            from_account.withdraw(amount)
            to_account.deposit(amount)

# GOOD: Consistent lock ordering
def transfer_safe(from_account, to_account, amount):
    first_lock = min(from_account.lock, to_account.lock, key=id)
    second_lock = max(from_account.lock, to_account.lock, key=id)
    
    with first_lock:
        with second_lock:
            from_account.withdraw(amount)
            to_account.deposit(amount)
```

### 3. GIL Limitations

**Problem**: Using threading for CPU-intensive tasks

```python
# BAD: Threading for CPU work (limited by GIL)
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(cpu_intensive_task, data) 
               for data in datasets]

# GOOD: Multiprocessing for CPU work
with ProcessPoolExecutor() as executor:
    futures = [executor.submit(cpu_intensive_task, data) 
               for data in datasets]
```

### 4. Resource Leaks

**Problem**: Not properly cleaning up resources

```python
# BAD: Resource leak
def bad_example():
    pool = ThreadPoolExecutor()
    pool.submit(some_task)
    # Pool never shutdown!

# GOOD: Proper cleanup
def good_example():
    with ThreadPoolExecutor() as pool:
        pool.submit(some_task)
    # Pool automatically shutdown
```

### 5. Blocking the Event Loop

**Problem**: Blocking operations in async code

```python
# BAD: Blocking the event loop
async def bad_async():
    time.sleep(1)  # Blocks entire event loop!
    return "done"

# GOOD: Use async alternatives
async def good_async():
    await asyncio.sleep(1)  # Non-blocking
    return "done"
```

## Best Practices Summary

1. **Start Simple**: Begin with the simplest solution that works
2. **Measure Performance**: Profile before and after optimizations
3. **Handle Errors**: Plan for failures and implement proper error handling
4. **Test Thoroughly**: Concurrent code is hard to debug, test extensively
5. **Document Assumptions**: Make threading assumptions explicit
6. **Use High-Level APIs**: Prefer concurrent.futures over raw threading
7. **Avoid Shared State**: Minimize mutable shared state when possible
8. **Plan for Scalability**: Design with future growth in mind

## Further Reading

- [Python's concurrent.futures documentation](https://docs.python.org/3/library/concurrent.futures.html)
- [Asyncio documentation](https://docs.python.org/3/library/asyncio.html)
- "Effective Python" by Brett Slatkin (Concurrency chapters)
- "Python Tricks" by Dan Bader (Concurrency section)
- "Architecture Patterns with Python" by Harry Percival and Bob Gregory

## Examples and Demos

Run the examples to see patterns in action:

```bash
# Basic patterns
python src/threading_patterns/producer_consumer.py
python src/asyncio_patterns/async_producer_consumer.py
python src/actor_model/actor_system.py

# Real-world examples
python examples/real_world_examples.py

# Performance comparisons
python benchmarks/performance_comparison.py

# Run tests
pytest tests/test_all_patterns.py -v
```
