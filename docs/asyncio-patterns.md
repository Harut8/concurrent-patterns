# Asyncio Patterns

## Table of Contents
1. [Introduction](#introduction)
2. [Event Loop Architecture](#event-loop-architecture)
3. [Coroutine Patterns](#coroutine-patterns)
4. [Task Management](#task-management)
5. [Synchronization Primitives](#synchronization-primitives)
6. [Connection Pooling](#connection-pooling)
7. [Streaming and Iterators](#streaming-and-iterators)
8. [Error Handling](#error-handling)

## Introduction

Asyncio is Python's built-in library for writing concurrent code using the async/await syntax. It provides a foundation for asynchronous programming using coroutines, event loops, and futures. Asyncio is particularly well-suited for I/O-bound and high-level structured network code.

### Core Concepts

1. **Event Loop**: The heart of asyncio applications
2. **Coroutines**: Functions defined with `async def`
3. **Tasks**: Wrapped coroutines that can be scheduled
4. **Futures**: Low-level awaitable objects
5. **Synchronization**: Async-safe coordination primitives

### Benefits

- **Single-threaded concurrency**: No thread safety concerns
- **Scalability**: Handle thousands of concurrent connections
- **Composability**: Easy to combine async operations
- **Ecosystem**: Rich ecosystem of async libraries
- **Performance**: Efficient for I/O-bound workloads

## Event Loop Architecture

### Event Loop Lifecycle

```python
import asyncio
import signal
import logging

class ManagedEventLoop:
    """Managed event loop with proper lifecycle handling"""
    
    def __init__(self):
        self.loop = None
        self.shutdown_event = asyncio.Event()
        self.tasks = set()
        self.cleanup_callbacks = []
    
    async def start(self):
        """Start the event loop with signal handling"""
        self.loop = asyncio.get_running_loop()
        
        # Set up signal handlers for graceful shutdown
        for sig in (signal.SIGTERM, signal.SIGINT):
            self.loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self.shutdown())
            )
        
        logging.info("Event loop started")
    
    async def shutdown(self):
        """Graceful shutdown of event loop"""
        logging.info("Initiating graceful shutdown...")
        
        # Signal shutdown to all components
        self.shutdown_event.set()
        
        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete or timeout
        if self.tasks:
            await asyncio.wait(self.tasks, timeout=30.0)
        
        # Run cleanup callbacks
        for callback in self.cleanup_callbacks:
            try:
                await callback()
            except Exception as e:
                logging.error(f"Cleanup error: {e}")
        
        logging.info("Shutdown complete")
    
    def add_task(self, coro):
        """Add task with automatic tracking"""
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task
    
    def add_cleanup_callback(self, callback):
        """Add cleanup callback for shutdown"""
        self.cleanup_callbacks.append(callback)
```

## Coroutine Patterns

### Coroutine Decorators

```python
import functools
import time
from typing import Callable

def retry_async(max_attempts: int = 3, delay: float = 1.0, 
                backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """Retry decorator for async functions"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        break
                    
                    logging.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}"
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        return wrapper
    return decorator

def timeout_async(seconds: float):
    """Timeout decorator for async functions"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs), 
                    timeout=seconds
                )
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"{func.__name__} timed out after {seconds} seconds"
                )
        return wrapper
    return decorator

def rate_limit_async(calls_per_second: float):
    """Rate limiting decorator for async functions"""
    min_interval = 1.0 / calls_per_second
    last_called = {}
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            key = id(func)
            
            if key in last_called:
                elapsed = now - last_called[key]
                if elapsed < min_interval:
                    await asyncio.sleep(min_interval - elapsed)
            
            last_called[key] = time.time()
            return await func(*args, **kwargs)
        return wrapper
    return decorator
```

### Async Context Managers

```python
class AsyncResourceManager:
    """Generic async resource manager"""
    
    def __init__(self, resource_factory, cleanup_func=None):
        self.resource_factory = resource_factory
        self.cleanup_func = cleanup_func
        self.resource = None
    
    async def __aenter__(self):
        self.resource = await self.resource_factory()
        return self.resource
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.resource and self.cleanup_func:
            await self.cleanup_func(self.resource)
        self.resource = None

class AsyncLockManager:
    """Async lock manager with timeout"""
    
    def __init__(self, lock: asyncio.Lock, timeout: float = None):
        self.lock = lock
        self.timeout = timeout
        self.acquired = False
    
    async def __aenter__(self):
        try:
            if self.timeout:
                await asyncio.wait_for(
                    self.lock.acquire(), 
                    timeout=self.timeout
                )
            else:
                await self.lock.acquire()
            self.acquired = True
            return self.lock
        except asyncio.TimeoutError:
            raise LockTimeoutError(f"Failed to acquire lock within {self.timeout}s")
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            self.lock.release()
            self.acquired = False
```

## Task Management

### Task Pools

```python
class AsyncTaskPool:
    """Pool for managing concurrent async tasks"""
    
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_tasks = set()
        self.completed_tasks = []
        self.failed_tasks = []
    
    async def submit(self, coro):
        """Submit coroutine to task pool"""
        async def wrapped_task():
            async with self.semaphore:
                try:
                    result = await coro
                    self.completed_tasks.append(result)
                    return result
                except Exception as e:
                    self.failed_tasks.append(e)
                    raise
                finally:
                    self.active_tasks.discard(asyncio.current_task())
        
        task = asyncio.create_task(wrapped_task())
        self.active_tasks.add(task)
        return task
    
    async def wait_all(self, timeout=None):
        """Wait for all tasks to complete"""
        if not self.active_tasks:
            return
        
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.active_tasks, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            # Cancel remaining tasks
            for task in self.active_tasks:
                if not task.done():
                    task.cancel()
            raise
    
    def cancel_all(self):
        """Cancel all active tasks"""
        for task in self.active_tasks:
            if not task.done():
                task.cancel()
    
    @property
    def stats(self):
        """Get pool statistics"""
        return {
            'active': len(self.active_tasks),
            'completed': len(self.completed_tasks),
            'failed': len(self.failed_tasks),
            'max_concurrent': self.max_concurrent
        }
```

## Synchronization Primitives

### Advanced Lock Patterns

```python
class AsyncRWLock:
    """Async reader-writer lock"""
    
    def __init__(self):
        self._readers = 0
        self._writer = False
        self._read_ready = asyncio.Condition()
        self._write_ready = asyncio.Condition()
    
    async def acquire_read(self):
        """Acquire read lock"""
        async with self._read_ready:
            while self._writer:
                await self._read_ready.wait()
            self._readers += 1
    
    async def release_read(self):
        """Release read lock"""
        async with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()
    
    async def acquire_write(self):
        """Acquire write lock"""
        async with self._write_ready:
            while self._writer or self._readers > 0:
                await self._write_ready.wait()
            self._writer = True
    
    async def release_write(self):
        """Release write lock"""
        async with self._write_ready:
            self._writer = False
            self._write_ready.notify_all()
        
        async with self._read_ready:
            self._read_ready.notify_all()
    
    def read_lock(self):
        """Context manager for read lock"""
        return AsyncLockContext(self.acquire_read, self.release_read)
    
    def write_lock(self):
        """Context manager for write lock"""
        return AsyncLockContext(self.acquire_write, self.release_write)

class AsyncLockContext:
    """Async lock context manager"""
    
    def __init__(self, acquire_func, release_func):
        self.acquire_func = acquire_func
        self.release_func = release_func
    
    async def __aenter__(self):
        await self.acquire_func()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release_func()
```

## Connection Pooling

### HTTP Connection Pool

```python
class AsyncHTTPPool:
    """Async HTTP connection pool with health checking"""
    
    def __init__(self, base_url: str, max_connections: int = 10, 
                 timeout: float = 30.0):
        self.base_url = base_url
        self.max_connections = max_connections
        self.timeout = timeout
        
        self.pool = asyncio.Queue(maxsize=max_connections)
        self.active_connections = set()
        self.total_connections = 0
        self.closed = False
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def start(self):
        """Start the connection pool"""
        # Pre-populate pool
        for _ in range(min(3, self.max_connections)):
            connection = await self._create_connection()
            await self.pool.put(connection)
    
    async def close(self):
        """Close all connections"""
        self.closed = True
        
        # Close all connections
        while not self.pool.empty():
            try:
                connection = self.pool.get_nowait()
                await connection.close()
            except asyncio.QueueEmpty:
                break
        
        for connection in self.active_connections:
            await connection.close()
    
    async def get_connection(self):
        """Get connection from pool"""
        if self.closed:
            raise RuntimeError("Pool is closed")
        
        try:
            connection = self.pool.get_nowait()
        except asyncio.QueueEmpty:
            if self.total_connections < self.max_connections:
                connection = await self._create_connection()
            else:
                connection = await asyncio.wait_for(
                    self.pool.get(), 
                    timeout=self.timeout
                )
        
        self.active_connections.add(connection)
        return connection
    
    async def return_connection(self, connection):
        """Return connection to pool"""
        self.active_connections.discard(connection)
        
        if self.closed:
            await connection.close()
            return
        
        try:
            self.pool.put_nowait(connection)
        except asyncio.QueueFull:
            await connection.close()
            self.total_connections -= 1
    
    async def _create_connection(self):
        """Create new HTTP connection"""
        import aiohttp
        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        )
        self.total_connections += 1
        return session
    
    async def request(self, method: str, path: str, **kwargs):
        """Make HTTP request using pooled connection"""
        connection = await self.get_connection()
        try:
            url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
            async with connection.request(method, url, **kwargs) as response:
                return await response.json()
        finally:
            await self.return_connection(connection)
```

## Streaming and Iterators

### Async Iterators

```python
class AsyncBatchIterator:
    """Async iterator that yields items in batches"""
    
    def __init__(self, source_iterator, batch_size: int, timeout: float = None):
        self.source_iterator = source_iterator
        self.batch_size = batch_size
        self.timeout = timeout
        self.buffer = []
    
    def __aiter__(self):
        return self
    
    async def __anext__(self):
        # Fill buffer up to batch size
        while len(self.buffer) < self.batch_size:
            try:
                if self.timeout:
                    item = await asyncio.wait_for(
                        self.source_iterator.__anext__(),
                        timeout=self.timeout
                    )
                else:
                    item = await self.source_iterator.__anext__()
                self.buffer.append(item)
            except StopAsyncIteration:
                if self.buffer:
                    batch = self.buffer[:]
                    self.buffer.clear()
                    return batch
                else:
                    raise StopAsyncIteration
            except asyncio.TimeoutError:
                if self.buffer:
                    batch = self.buffer[:]
                    self.buffer.clear()
                    return batch
                else:
                    raise StopAsyncIteration
        
        # Return full batch
        batch = self.buffer[:self.batch_size]
        self.buffer = self.buffer[self.batch_size:]
        return batch

class AsyncRateLimitedIterator:
    """Async iterator with rate limiting"""
    
    def __init__(self, source_iterator, rate_limit: float):
        self.source_iterator = source_iterator
        self.min_interval = 1.0 / rate_limit
        self.last_yield_time = 0
    
    def __aiter__(self):
        return self
    
    async def __anext__(self):
        # Enforce rate limit
        now = time.time()
        elapsed = now - self.last_yield_time
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        
        # Get next item
        item = await self.source_iterator.__anext__()
        self.last_yield_time = time.time()
        return item
```

## Error Handling

### Circuit Breaker

```python
class AsyncCircuitBreaker:
    """Async circuit breaker for fault tolerance"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half_open
        self.lock = asyncio.Lock()
    
    async def call(self, func, *args, **kwargs):
        """Call function through circuit breaker"""
        async with self.lock:
            if self.state == 'open':
                if self._should_attempt_reset():
                    self.state = 'half_open'
                else:
                    raise Exception("Circuit breaker is open")
        
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise
    
    async def _on_success(self):
        """Handle successful call"""
        async with self.lock:
            self.failure_count = 0
            self.state = 'closed'
    
    async def _on_failure(self):
        """Handle failed call"""
        async with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'open'
    
    def _should_attempt_reset(self) -> bool:
        """Check if should attempt to reset circuit breaker"""
        return (self.last_failure_time and 
                time.time() - self.last_failure_time >= self.recovery_timeout)

# Usage example
async def unreliable_service():
    """Simulate unreliable service"""
    import random
    if random.random() < 0.3:
        raise Exception("Service failed")
    return "Success"

async def main():
    breaker = AsyncCircuitBreaker(failure_threshold=3, recovery_timeout=10.0)
    
    for i in range(10):
        try:
            result = await breaker.call(unreliable_service)
            print(f"Call {i}: {result}")
        except Exception as e:
            print(f"Call {i} failed: {e}")
        
        await asyncio.sleep(1)
```

---

*Asyncio provides powerful abstractions for asynchronous programming in Python. Understanding these patterns is essential for building scalable, concurrent applications that can handle high loads efficiently.*
