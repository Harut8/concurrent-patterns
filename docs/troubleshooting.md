# Concurrency Troubleshooting Guide

**Navigation:** [← Performance](./performance-comparison.md) | [Home](./README.md)

---

## Table of Contents

1. [Deadlocks](#deadlocks)
2. [Race Conditions](#race-conditions)
3. [Performance Issues](#performance-issues)
4. [Memory Leaks](#memory-leaks)
5. [Python-Specific Issues](#python-specific-issues)
6. [Debugging Tools](#debugging-tools)

---

## Deadlocks

### Symptoms

- Program hangs indefinitely
- No CPU usage, no progress
- Threads/processes waiting forever

### Common Causes

#### 1. Lock Ordering Violation

**Problem**: Two threads acquire locks in different order.

```python
import threading

lock_a = threading.Lock()
lock_b = threading.Lock()

def thread1():
    with lock_a:
        time.sleep(0.1)  # Simulate work
        with lock_b:  # DEADLOCK!
            print("Thread 1")

def thread2():
    with lock_b:
        time.sleep(0.1)
        with lock_a:  # DEADLOCK!
            print("Thread 2")
```

**Fix**: Always acquire locks in the same order.

```python
# Fixed: Acquire in consistent order (e.g., by id)
def thread1():
    locks = sorted([lock_a, lock_b], key=id)
    with locks[0]:
        with locks[1]:
            print("Thread 1")

def thread2():
    locks = sorted([lock_a, lock_b], key=id)
    with locks[0]:
        with locks[1]:
            print("Thread 2")
```

#### 2. Recursive Deadlock

**Problem**: Single thread tries to acquire same lock twice.

```python
lock = threading.Lock()

def outer():
    with lock:
        inner()  # DEADLOCK!

def inner():
    with lock:  # Same lock!
        print("Inner")
```

**Fix**: Use `threading.RLock` (recursive lock).

```python
lock = threading.RLock()  # Allows re-acquisition by same thread

def outer():
    with lock:
        inner()  # OK

def inner():
    with lock:
        print("Inner")
```

#### 3. Waiting for Each Other (Circular Wait)

**Problem**: Thread A waits for Thread B, Thread B waits for Thread A.

```python
# Thread A
a_ready.wait()  # Wait for B
b_ready.set()   # Signal B

# Thread B  
b_ready.wait()  # Wait for A  
a_ready.set()   # Signal A
# DEADLOCK: Both wait first!
```

**Fix**: Ensure at least one thread signals first.

### Detecting Deadlocks

#### Python: `faulthandler` or `py-spy`

```bash
# Send SIGABRT to dump all thread stacks
python -Xfaulthandler script.py
# Press Ctrl+\ (SIGQUIT) to dump stacks

# Or use py-spy
py-spy dump --pid <pid>
```

**Output**:
```
Thread 0x123: waiting at lock_a.acquire()
Thread 0x456: waiting at lock_b.acquire()
```

#### Go: `SIGQUIT` or `pprof`

```bash
# Send SIGQUIT to dump goroutine stacks
kill -QUIT <pid>

# Or use pprof
curl http://localhost:6060/debug/pprof/goroutine?debug=2
```

#### Java: Thread Dump

```bash
jstack <pid>
# Look for "waiting to lock" and "locked by"
```

---

## Race Conditions

### Symptoms

- Intermittent bugs (non-deterministic)
- Corrupted data
- Assertion failures
- Works in debug mode, fails in release

### Common Causes

#### 1. Unprotected Shared State

**Problem**: Multiple threads access shared variable without synchronization.

```python
# WRONG: Race condition!
counter = 0

def increment():
    global counter
    temp = counter  # Read
    temp = temp + 1 # Modify
    counter = temp  # Write
    # Another thread can interleave between read and write!

threads = [threading.Thread(target=increment) for _ in range(100)]
for t in threads: t.start()
for t in threads: t.join()
print(counter)  # Expected: 100, Actual: varies (e.g., 87)
```

**Fix**: Use locks or atomics.

```python
# Correct: Lock-based
lock = threading.Lock()

def increment():
    global counter
    with lock:
        counter += 1

# Or use thread-safe alternatives
from threading import Lock
import threading

class SafeCounter:
    def __init__(self):
        self.value = 0
        self.lock = Lock()
    
    def increment(self):
        with self.lock:
            self.value += 1
```

#### 2. Time-of-Check to Time-of-Use (TOCTOU)

**Problem**: Condition checked, then acted upon, but state changed in between.

```python
# WRONG: TOCTOU race
if not queue.empty():  # Check
    item = queue.get()  # Use (queue might be empty now!)
```

**Fix**: Make check and action atomic.

```python
# Correct: try/except
try:
    item = queue.get_nowait()  # Atomic check-and-get
except queue.Empty:
    pass
```

#### 3. Asyncio Race Conditions

**Problem**: `await` yields control, state changes.

```python
# WRONG: Race despite single-threaded asyncio!
count = 0

async def increment():
    global count
    temp = count
    await asyncio.sleep(0)  # Yield control!
    count = temp + 1

await asyncio.gather(*(increment() for _ in range(10)))
print(count)  # Not 10!
```

**Fix**: Use `asyncio.Lock`.

```python
# Correct
lock = asyncio.Lock()

async def increment():
    global count
    async with lock:
        temp = count
        await asyncio.sleep(0)
        count = temp + 1
```

### Detecting Race Conditions

#### Python: `pytest-race` or Thread Sanitizer (C extensions)

```bash
pip install pytest-race
pytest --race
```

#### Go: Race Detector

```bash
go build -race myprogram.go
./myprogram
# Will detect races and print warnings
```

#### Rust: Compiler Prevents Most Races

Rust's ownership system prevents data races at compile time!

```rust
// Won't compile: can't share mutable state without Mutex
let mut counter = 0;
std::thread::spawn(|| {
    counter += 1;  // ERROR: can't borrow mutable
});
```

#### C++: Thread Sanitizer

```bash
clang++ -fsanitize=thread -g program.cpp -o program
./program
# Detects data races at runtime
```

---

## Performance Issues

### Symptom 1: Low CPU Usage Despite Concurrency

**Diagnosis**: Likely lock contention or waiting.

**Investigate**:

```python
# Profile with py-spy (GIL-aware)
py-spy top --pid <pid>
# Look for threads with low CPU usage

py-spy record --gil -o profile.svg --pid <pid>
# Visualize GIL contention
```

**Common Causes**:
1. **GIL Contention** (Python): CPU-bound threads fighting for GIL
   - **Fix**: Use multiprocessing
2. **Lock Contention**: All threads waiting for same lock
   - **Fix**: Fine-grained locks or lock-free
3. **I/O Blocking**: Threads waiting for I/O
   - **Fix**: Use asyncio or more threads

### Symptom 2: Slower with More Threads

**Diagnosis**: Overhead exceeding benefit.

**Python Example**:

```python
# CPU-bound with threading = SLOWER
import threading
import time

def cpu_work():
    total = 0
    for i in range(10_000_000):
        total += i * i

# Single-threaded: 2.5s
start = time.time()
cpu_work()
print(f"Single: {time.time() - start:.2f}s")

# Multi-threaded: 3.8s (SLOWER!)
start = time.time()
threads = [threading.Thread(target=cpu_work) for _ in range(4)]
for t in threads: t.start()
for t in threads: t.join()
print(f"Multi: {time.time() - start:.2f}s")
```

**Fix**: Use multiprocessing for CPU-bound tasks.

### Symptom 3: False Sharing (Cache Thrashing)

**Problem**: Multiple threads modifying different variables in same cache line.

```c
// BAD: counter1 and counter2 likely in same cache line
struct {
    long counter1;  // Thread 1 modifies
    long counter2;  // Thread 2 modifies
} data;
// Cache line ping-pongs between cores!
```

**Fix**: Pad to separate cache lines.

```c
// GOOD: Force different cache lines (64 bytes)
struct {
    long counter1;
    char pad[56];  // Padding to 64 bytes
    long counter2;
} data;
```

**Python**: Use separate objects (less control over memory layout).

---

## Memory Leaks

### Symptom: Growing Memory Usage

**Diagnosis**: Memory not being freed.

### Common Causes

#### 1. Thread Not Joined

```python
# LEAK: Threads not joined, resources not freed
threads = []
for i in range(1000):
    t = threading.Thread(target=work)
    t.start()
    threads.append(t)
# Forgot to join! Threads accumulate

# FIX:
for t in threads:
    t.join()
```

#### 2. Circular References in Threads

```python
# LEAK: Thread references object, object references thread
class Worker:
    def __init__(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
    
    def run(self):
        self.data = [0] * 1_000_000  # Large data
        while True:
            time.sleep(1)

# workers = [Worker() for _ in range(100)]
# Circular ref: Worker -> Thread -> Worker.run -> Worker
# Prevents GC
```

**Fix**: Use weak references or daemon threads.

```python
import weakref

class Worker:
    def __init__(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True  # Dies with main thread
        self.thread.start()
```

#### 3. Lock-Free Memory Not Reclaimed

```python
# LEAK: Lock-free stack, nodes never freed
class Node:
    def __init__(self, value, next=None):
        self.value = value
        self.next = next

class LockFreeStack:
    def __init__(self):
        self.head = None
    
    def push(self, value):
        new_node = Node(value)
        while True:
            old_head = self.head
            new_node.next = old_head
            if CAS(self.head, old_head, new_node):
                break
    
    def pop(self):
        while True:
            old_head = self.head
            if old_head is None:
                return None
            new_head = old_head.next
            if CAS(self.head, old_head, new_head):
                return old_head.value
                # old_head not freed! Memory leak

# FIX: Use hazard pointers or epoch-based reclamation
```

---

## Python-Specific Issues

### Issue 1: GIL Prevents Parallelism

**Symptom**: Multi-threaded CPU-bound code is slow.

**Diagnosis**: GIL limits to one thread at a time.

**Fix**: Use multiprocessing.

### Issue 2: Asyncio Mixed with Blocking Calls

**Problem**: Blocking call stalls entire event loop.

```python
import asyncio
import time

async def bad():
    time.sleep(1)  # BLOCKS event loop!
    return "done"

async def good():
    await asyncio.sleep(1)  # Cooperative, doesn't block
    return "done"

# Run both
asyncio.run(asyncio.gather(bad(), good()))
# Both take 2s (sequential) because bad() blocks!
```

**Fix**: Use `run_in_executor` for blocking calls.

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

def blocking_call():
    time.sleep(1)
    return "done"

async def good():
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, blocking_call)
    return result
```

### Issue 3: Forgotten `await`

**Problem**: Coroutine created but not run.

```python
async def fetch_data():
    return "data"

async def main():
    result = fetch_data()  # WRONG: coroutine object, not result!
    print(result)  # <coroutine object>
    
    # RuntimeWarning: coroutine 'fetch_data' was never awaited

# FIX:
async def main():
    result = await fetch_data()  # Correct
    print(result)  # "data"
```

---

## Debugging Tools

### Python

#### 1. `threading.enumerate()`

```python
import threading

# List all active threads
for thread in threading.enumerate():
    print(f"{thread.name}: {thread.is_alive()}")
```

#### 2. `faulthandler` (Stack Traces)

```python
import faulthandler
faulthandler.enable()

# On crash or signal, dumps all thread stacks
```

#### 3. `py-spy` (Production Profiler)

```bash
pip install py-spy

# Top-like view
py-spy top --pid <pid>

# Record flamegraph
py-spy record --gil -o profile.svg --pid <pid>

# Dump stacks
py-spy dump --pid <pid>
```

#### 4. `asyncio` Debug Mode

```python
import asyncio
import warnings

# Enable asyncio debug mode
asyncio.run(main(), debug=True)

# Or with environment variable
# PYTHONASYNCIODEBUG=1 python script.py
```

### Go

#### 1. `GODEBUG` (Scheduler Trace)

```bash
GODEBUG=schedtrace=1000 ./myprogram
# Prints scheduler stats every 1000ms
```

#### 2. `pprof` (CPU/Memory Profiling)

```go
import _ "net/http/pprof"

go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()

// Then: go tool pprof http://localhost:6060/debug/pprof/profile
```

#### 3. Race Detector

```bash
go run -race main.go
```

### Rust

#### 1. `tokio-console` (Async Task Inspector)

```rust
// In Cargo.toml
[dependencies]
console-subscriber = "0.1"

// In code
console_subscriber::init();

// Run tokio-console in terminal
```

#### 2. `cargo flamegraph` (Profiling)

```bash
cargo install flamegraph
cargo flamegraph
# Generates flamegraph.svg
```

---

## Summary: Troubleshooting Checklist

### Deadlock Checklist

- [ ] Acquire locks in consistent order
- [ ] Use timeout on lock acquisition
- [ ] Use `RLock` for recursive locking
- [ ] Avoid holding locks while waiting

### Race Condition Checklist

- [ ] Protect shared state with locks or atomics
- [ ] Make check-and-act atomic
- [ ] Use lock for asyncio shared state
- [ ] Enable race detector (Go, C++, Rust)

### Performance Checklist

- [ ] Profile before optimizing
- [ ] Python: Use asyncio for I/O, multiprocessing for CPU
- [ ] Avoid lock contention (fine-grained locks or lock-free)
- [ ] Avoid false sharing (pad cache lines)
- [ ] Use appropriate concurrency primitive

### Memory Leak Checklist

- [ ] Join all threads
- [ ] Avoid circular references
- [ ] Use daemon threads where appropriate
- [ ] Implement memory reclamation (lock-free)

---

**Navigation:** [← Performance](./performance-comparison.md) | [Home](./README.md)
