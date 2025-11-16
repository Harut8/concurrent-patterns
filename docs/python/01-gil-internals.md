# Part II.1: Python GIL Internals and Architecture

**Navigation:** [← Home](../README.md) | [Next: Asyncio →](./02-asyncio.md)

---

## Table of Contents

1. [Introduction to the GIL](#introduction-to-the-gil)
2. [GIL Implementation Details](#gil-implementation-details)
3. [Per-Interpreter GIL (PEP 684)](#per-interpreter-gil-pep-684)
4. [GIL Performance Implications](#gil-performance-implications)
5. [Working Around the GIL](#working-around-the-gil)
6. [The Future: No-GIL Python (PEP 703)](#the-future-no-gil-python-pep-703)

---

## Introduction to the GIL

### What is the GIL?

The **Global Interpreter Lock (GIL)** is a mutex that protects access to Python objects, preventing multiple threads from executing Python bytecode simultaneously in the same interpreter.

**Key Fact**: The GIL exists in **CPython** (reference implementation), but not in Jython, IronPython, or PyPy-STM.

### Why Does the GIL Exist?

**Historical Reason**: CPython's memory management uses reference counting. Without the GIL:
- Every INCREF/DECREF would need atomic operations or fine-grained locks
- Severe performance degradation for single-threaded code
- Complex implementation

**Design Trade-off**:
- **Pro**: Simple, fast single-threaded performance, easy C extension integration
- **Con**: No true parallelism for CPU-bound Python code

### What the GIL Does and Doesn't Protect

**GIL Protects**:
- Python object reference counts
- Internal interpreter state
- Bytecode execution atomicity (single bytecode instruction)

**GIL Does NOT Protect**:
- Application-level data races
- Non-atomic operations spanning multiple bytecodes
- C extension internals (C extensions must release GIL manually)

**Example of Race Condition Despite GIL**:

```python
# UNSAFE! Even with GIL
shared_list = []

def append_items():
    for i in range(1000):
        # This looks atomic but involves multiple bytecodes:
        # 1. LOAD_GLOBAL shared_list
        # 2. LOAD_METHOD append
        # 3. LOAD_FAST i
        # 4. CALL_METHOD
        # GIL can be released between bytecodes!
        shared_list.append(i)

# Multiple threads can still corrupt shared_list
# (though less likely than without GIL)
```

**Bytecode Level**:

```
# dis.dis(append_items) simplified
LOAD_GLOBAL    shared_list
LOAD_METHOD    append
LOAD_FAST      i
CALL_METHOD    1
# GIL can switch between any of these!
```

### GIL Switching Mechanism

#### Python 2 (Old GIL)

**Mechanism**: Instruction count (every 100 bytecodes by default).

```c
// Simplified Python 2 GIL
while (true) {
    execute_bytecode();
    if (--gil_counter <= 0) {
        gil_counter = 100;
        release_gil();
        // Other threads can acquire
        acquire_gil();
    }
}
```

**Problem**: **Priority Inversion** and **Convoy Effect**
- I/O thread wakes up, wants GIL
- CPU thread releases GIL briefly, immediately re-acquires
- I/O thread starves!

#### Python 3.2+ (New GIL)

**Mechanism**: Time-based switching (every 5ms by default, configurable via `sys.setswitchinterval()`).

```c
// Simplified Python 3 GIL
while (true) {
    acquire_gil();
    timeout = current_time + switch_interval;
    
    while (current_time < timeout) {
        execute_bytecode();
        if (gil_drop_request) {
            break;
        }
    }
    
    release_gil();
    // Wait for signal or timeout
}
```

**Improvements**:
1. **Predictable switching**: Every 5ms, not per instruction
2. **I/O priority**: I/O threads signal to request GIL
3. **Less overhead**: No per-bytecode counter check

**How it Works**:
1. Thread holds GIL for up to 5ms
2. Another thread requests GIL → sets `gil_drop_request` flag
3. Current thread sees flag, drops GIL
4. Waiting thread acquires GIL

### GIL Internals: Data Structures

**CPython 3.9+ GIL Structure** (simplified):

```c
struct _gil_runtime_state {
    unsigned long interval;        // Switch interval (microseconds)
    _Py_atomic_int locked;         // Is GIL locked?
    unsigned long switch_number;   // Count switches (debugging)
    
    PyCOND_T cond;                 // Condition variable
    PyMUTEX_T mutex;               // Mutex protecting GIL state
    
    _Py_atomic_int last_holder;    // Thread holding GIL
    _Py_atomic_int gil_drop_request; // Request to drop GIL
};
```

**Acquire GIL** (pseudocode):

```c
void take_gil(PyThreadState *tstate) {
    pthread_mutex_lock(&gil.mutex);
    
    if (!gil.locked) {
        // GIL is free
        gil.locked = 1;
        gil.last_holder = tstate->thread_id;
        pthread_mutex_unlock(&gil.mutex);
        return;
    }
    
    // GIL is held by another thread
    gil.gil_drop_request = 1;  // Request current holder to drop
    
    while (gil.locked) {
        // Wait on condition variable
        pthread_cond_wait(&gil.cond, &gil.mutex);
    }
    
    gil.locked = 1;
    gil.last_holder = tstate->thread_id;
    gil.gil_drop_request = 0;
    pthread_mutex_unlock(&gil.mutex);
}
```

**Release GIL** (pseudocode):

```c
void drop_gil(PyThreadState *tstate) {
    pthread_mutex_lock(&gil.mutex);
    gil.locked = 0;
    pthread_cond_signal(&gil.cond);  // Wake one waiting thread
    pthread_mutex_unlock(&gil.mutex);
}
```

---

## GIL Implementation Details

### When is the GIL Released?

The GIL is **automatically released** during:

1. **I/O Operations**:
   - `read()`, `write()`, `recv()`, `send()`
   - File I/O (if using unbuffered or large buffers)
   - Network I/O

2. **Long-Running C Extensions**:
   - NumPy operations (matrix multiplication, etc.)
   - Image processing (Pillow)
   - Compression (zlib, bz2)
   - Regex (re module, under certain conditions)

3. **Explicit Release in C Extensions**:
   ```c
   // C extension code
   Py_BEGIN_ALLOW_THREADS
   // Expensive C computation here (no Python API calls!)
   Py_END_ALLOW_THREADS
   ```

4. **Sleep**:
   - `time.sleep()` releases GIL
   - `threading.Lock.acquire(timeout=...)` releases GIL

### When is the GIL NOT Released?

1. **Pure Python CPU-Bound Code**:
   ```python
   # GIL held entire time
   def fibonacci(n):
       if n < 2:
           return n
       return fibonacci(n-1) + fibonacci(n-2)
   ```

2. **Short I/O Operations**:
   - Single-byte reads might not release GIL (too short to benefit)

3. **Python API Calls from C Extensions**:
   - If C extension uses Python API (Py_* functions), GIL must be held

### Measuring GIL Contention

**Using sys module**:

```python
import sys
import threading
import time

def monitor_gil():
    old_switch_count = sys.getswitchinterval()
    print(f"Switch interval: {old_switch_count}s")
    
    # Count GIL switches (Python 3.11+)
    if hasattr(sys, '_current_frames'):
        before = sys._current_frames()
        time.sleep(1)
        after = sys._current_frames()
        # Approximate GIL contention from frame differences

monitor_gil()
```

**Using `py-spy` (external tool)**:

```bash
py-spy top --pid <pid>
# Shows per-thread CPU usage
# If threads have low CPU despite high wall time → GIL contention

py-spy record --gil -o profile.svg --pid <pid>
# Records GIL wait times
```

**Using perf (Linux)**:

```bash
perf record -e sched:sched_switch -a -g -- python script.py
perf report
# Look for pthread_cond_wait (GIL waiting)
```

---

## Per-Interpreter GIL (PEP 684)

### Introduction (Python 3.12+)

**PEP 684** (accepted 2023, implemented in Python 3.12) allows each **subinterpreter** to have its own GIL.

**Before PEP 684**: All subinterpreters shared one GIL → no parallelism.

**After PEP 684**: Each subinterpreter can have its own GIL → true parallelism across subinterpreters!

### How It Works

**Creating Per-Interpreter GIL**:

```c
// C API (Python 3.12+)
PyInterpreterConfig config = {
    .use_main_obmalloc = 0,
    .allow_fork = 0,
    .allow_exec = 0,
    .allow_threads = 1,
    .allow_daemon_threads = 0,
    .check_multi_interp_extensions = 1,
    .gil = PyInterpreterConfig_OWN_GIL,  // Key!
};

PyThreadState *tstate = NULL;
PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);
```

**Python 3.13+**: Higher-level API via `interpreters` module (PEP 554).

```python
import interpreters

# Create isolated interpreter with own GIL
interp = interpreters.create(isolated=True)

# Run code in that interpreter
interp.run("import threading; print(threading.current_thread())")

# True parallelism across interpreters!
```

### Limitations and Challenges

**Challenges**:

1. **No Shared Objects**: Interpreters are isolated—cannot share Python objects directly.
   - Must use **channels** (message passing) or pickle

2. **Extension Module Compatibility**: Not all C extensions support per-interpreter GIL.
   - Extensions must explicitly opt-in via `Py_mod_multiple_interpreters`

3. **Overhead**: Creating interpreters is expensive (compared to threads).

**Use Cases**:

- **Multi-tenant systems**: Isolate user code
- **Plugin systems**: Isolate plugins from main app
- **Parallel CPU-bound tasks**: Alternative to multiprocessing (lower overhead)

### Example: Parallel Computation with Subinterpreters

```python
import interpreters
import time

def cpu_bound_task(n):
    """Compute sum of squares (CPU-bound)."""
    total = 0
    for i in range(n):
        total += i * i
    return total

# Create isolated interpreters
interp1 = interpreters.create(isolated=True)
interp2 = interpreters.create(isolated=True)

# Run in parallel (each has own GIL!)
start = time.time()
# Note: Actual API still evolving in Python 3.13
interp1.run(f"result1 = {cpu_bound_task.__code__}(10_000_000)")
interp2.run(f"result2 = {cpu_bound_task.__code__}(10_000_000)")
end = time.time()

print(f"Time with subinterpreters: {end - start:.2f}s")
# Should be ~2x faster than single-threaded (on multi-core)
```

**Current Status (2024-2025)**:

- Python 3.12: Low-level C API available
- Python 3.13: High-level `interpreters` module (PEP 554) in progress
- Python 3.14+: Expected to be stable and widely usable

---

## GIL Performance Implications

### CPU-Bound vs I/O-Bound

**I/O-Bound**: GIL is released during I/O → threads work well.

```python
import threading
import requests

urls = ["https://example.com"] * 10

def fetch(url):
    response = requests.get(url)  # GIL released during network I/O
    return len(response.content)

threads = [threading.Thread(target=fetch, args=(url,)) for url in urls]
for t in threads:
    t.start()
for t in threads:
    t.join()
# Threads provide speedup!
```

**CPU-Bound**: GIL held during Python execution → threads hurt performance.

```python
import threading

def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

threads = [threading.Thread(target=fibonacci, args=(30,)) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
# SLOWER than single-threaded due to GIL contention!
```

### GIL Contention Overhead

**Scenario**: 2 CPU-bound threads on 2-core machine.

**Without GIL (ideal)**:
- Thread 1: 100% of core 1
- Thread 2: 100% of core 2
- Total: 200% CPU usage

**With GIL (actual)**:
- Thread 1: Runs for 5ms, releases GIL, waits
- Thread 2: Runs for 5ms, releases GIL, waits
- Overhead: Context switches, cache thrashing, wakeups
- Total: ~110-130% CPU usage (overhead loses 70-90%)

**Benchmark** (CPU-bound task):

```python
import threading
import time

def burn_cpu(seconds):
    end = time.time() + seconds
    while time.time() < end:
        pass  # Busy loop

# Single-threaded
start = time.time()
burn_cpu(1)
elapsed_single = time.time() - start

# Multi-threaded (2 threads)
start = time.time()
t1 = threading.Thread(target=burn_cpu, args=(1,))
t2 = threading.Thread(target=burn_cpu, args=(1,))
t1.start()
t2.start()
t1.join()
t2.join()
elapsed_multi = time.time() - start

print(f"Single-threaded: {elapsed_single:.2f}s")
print(f"Multi-threaded:  {elapsed_multi:.2f}s")
print(f"Slowdown: {elapsed_multi / elapsed_single:.2f}x")
# Typical result: 1.3-1.5x SLOWER with threads!
```

---

## Working Around the GIL

### 1. Use Multiprocessing

**Multiprocessing**: Separate processes → separate interpreters → separate GILs.

```python
from multiprocessing import Pool

def cpu_bound_task(n):
    total = 0
    for i in range(n):
        total += i * i
    return total

if __name__ == "__main__":
    with Pool(processes=4) as pool:
        results = pool.map(cpu_bound_task, [10_000_000] * 4)
    print(results)
# True parallelism! 4x speedup on 4 cores
```

**Trade-offs**:
- **Pro**: True parallelism, isolation
- **Con**: High overhead (process creation, IPC), no shared memory

### 2. Use C Extensions That Release GIL

**NumPy** (releases GIL for most operations):

```python
import numpy as np
import threading

def matrix_multiply():
    a = np.random.rand(1000, 1000)
    b = np.random.rand(1000, 1000)
    c = np.dot(a, b)  # GIL released!

threads = [threading.Thread(target=matrix_multiply) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
# True parallelism if NumPy is multi-threaded (MKL, OpenBLAS)
```

**Custom C Extension**:

```c
// example.c
#include <Python.h>

static PyObject* expensive_computation(PyObject* self, PyObject* args) {
    long n;
    if (!PyArg_ParseTuple(args, "l", &n))
        return NULL;
    
    long result = 0;
    
    // Release GIL
    Py_BEGIN_ALLOW_THREADS
    
    for (long i = 0; i < n; i++) {
        result += i * i;
    }
    
    // Re-acquire GIL
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLong(result);
}
```

### 3. Use Asyncio (for I/O-Bound)

**Asyncio**: Cooperative concurrency within single thread—no GIL contention.

```python
import asyncio
import aiohttp

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    urls = ["https://example.com"] * 100
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
    print(f"Fetched {len(results)} URLs")

asyncio.run(main())
# No GIL contention; much faster than threads for I/O
```

### 4. Use Alternative Python Implementations

**PyPy**: JIT-compiled Python, has GIL but often faster than CPython.

**Jython**: Python on JVM, no GIL (uses JVM threading).

**IronPython**: Python on .NET, no GIL (uses .NET threading).

**GraalPy**: Python on GraalVM, no GIL.

**Trade-off**: Limited C extension support.

### 5. Use Cython with `nogil`

**Cython**: Python-to-C compiler, can release GIL for typed code.

```cython
# example.pyx
from cython.parallel import prange

def parallel_sum(long n) nogil:
    cdef long i, total = 0
    for i in prange(n, nogil=True):  # Parallel, no GIL
        total += i * i
    return total
```

Compile with OpenMP support → true parallelism.

---

## The Future: No-GIL Python (PEP 703)

### PEP 703: Making the GIL Optional

**Status**: Accepted in October 2023, implementation ongoing.

**Goal**: Make CPython's GIL optional via build-time flag.

**Timeline**:
- Python 3.13 (2024): Experimental no-GIL build available
- Python 3.14-3.15 (2025-2026): Stabilization
- Python 3.16+ (2027+): Potentially default

### How It Works

**Approach**: Replace reference counting with **deferred reference counting** + **biased reference counting**.

1. **Biased Reference Counting**: Each object "owned" by one thread initially; no atomic ops needed unless shared.
2. **Deferred Reference Counting**: Some INCREF/DECREF operations deferred and batched.
3. **Immortal Objects**: Common objects (None, True, small ints) made immortal → never freed → no refcount ops.

**Trade-off**: ~5-10% single-threaded slowdown, but unbounded multi-threaded speedup.

### Impact on Ecosystem

**C Extensions**: Must be updated to support no-GIL.
- Use `Py_INCREF` → `Py_XINCREF` (atomic)
- Avoid assuming GIL provides synchronization

**Libraries**: NumPy, Pandas, etc., will need updates.

**User Code**: Mostly transparent, but race conditions will become more visible.

### Building No-GIL Python (Python 3.13+)

```bash
# Clone CPython
git clone https://github.com/python/cpython
cd cpython

# Configure with --disable-gil
./configure --disable-gil
make -j$(nproc)
./python

# In Python:
import sys
print(sys._is_gil_enabled())  # False
```

### Example: No-GIL Speedup

```python
# nogil_test.py
import threading
import time

def cpu_bound():
    total = 0
    for i in range(50_000_000):
        total += i * i
    return total

# Single-threaded
start = time.time()
cpu_bound()
single_time = time.time() - start

# Multi-threaded
start = time.time()
threads = [threading.Thread(target=cpu_bound) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
multi_time = time.time() - start

print(f"Single: {single_time:.2f}s, Multi: {multi_time:.2f}s")
print(f"Speedup: {single_time / multi_time:.2f}x")

# With GIL: Speedup ~0.7x (slowdown!)
# Without GIL: Speedup ~3.5x (on 4 cores)
```

---

## Summary: GIL Takeaways

1. **GIL prevents parallel execution of Python bytecode** in the same interpreter.
2. **GIL is released during I/O** → threads work well for I/O-bound tasks.
3. **GIL is held during CPU-bound Python code** → use multiprocessing, C extensions, or await no-GIL Python.
4. **Per-interpreter GIL (PEP 684, Python 3.12+)** enables parallelism via subinterpreters.
5. **No-GIL Python (PEP 703, Python 3.13+)** is coming—~5-10% single-threaded cost, unbounded multi-threaded gain.
6. **Current best practices**:
   - I/O-bound: Use asyncio or threading
   - CPU-bound: Use multiprocessing, Cython, or NumPy
   - Mixed: Combine strategies (e.g., asyncio + process pool)

---

**Next:** [Asyncio: Event Loop, Tasks, and Coroutines →](./02-asyncio.md)

**Navigation:** [← Home](../README.md) | [Next →](./02-asyncio.md)
