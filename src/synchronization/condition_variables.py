"""
Condition Variable Patterns

Advanced condition variable implementations for thread coordination.
"""

import threading
import time
import asyncio
from typing import Optional, List, Dict, Any, Callable, TypeVar
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import logging
import queue
import weakref


T = TypeVar('T')


class WaitResult(Enum):
    """Result of a wait operation."""
    SIGNALED = "signaled"
    TIMEOUT = "timeout"
    SPURIOUS = "spurious"


@dataclass
class ConditionStats:
    """Condition variable statistics."""
    total_waits: int = 0
    total_signals: int = 0
    total_broadcasts: int = 0
    current_waiters: int = 0
    average_wait_time: float = 0.0
    spurious_wakeups: int = 0


class EnhancedCondition:
    """
    Enhanced condition variable with statistics and timeout handling.
    """
    
    def __init__(self, lock: Optional[threading.RLock] = None):
        self._lock = lock or threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._stats = ConditionStats()
        self._waiters = weakref.WeakSet()
    
    def wait(self, predicate: Optional[Callable[[], bool]] = None, 
             timeout: Optional[float] = None) -> WaitResult:
        """
        Wait for condition with optional predicate and timeout.
        """
        start_time = time.time()
        
        with self._condition:
            self._stats.current_waiters += 1
            self._stats.total_waits += 1
            self._waiters.add(threading.current_thread())
            
            try:
                if predicate is None:
                    # Simple wait
                    result = self._condition.wait(timeout=timeout)
                    return WaitResult.SIGNALED if result else WaitResult.TIMEOUT
                else:
                    # Wait with predicate
                    while not predicate():
                        result = self._condition.wait(timeout=timeout)
                        if not result:  # Timeout
                            return WaitResult.TIMEOUT
                        
                        # Check for spurious wakeup
                        if not predicate():
                            self._stats.spurious_wakeups += 1
                    
                    return WaitResult.SIGNALED
            
            finally:
                self._stats.current_waiters -= 1
                self._waiters.discard(threading.current_thread())
                
                # Update average wait time
                wait_time = time.time() - start_time
                self._stats.average_wait_time = (
                    (self._stats.average_wait_time * (self._stats.total_waits - 1) + wait_time) /
                    self._stats.total_waits
                )
    
    def notify(self, n: int = 1):
        """Notify n waiting threads."""
        with self._condition:
            self._condition.notify(n)
            self._stats.total_signals += min(n, self._stats.current_waiters)
    
    def notify_all(self):
        """Notify all waiting threads."""
        with self._condition:
            notified = self._stats.current_waiters
            self._condition.notify_all()
            self._stats.total_broadcasts += 1
            self._stats.total_signals += notified
    
    def get_stats(self) -> ConditionStats:
        """Get condition variable statistics."""
        with self._lock:
            return ConditionStats(
                total_waits=self._stats.total_waits,
                total_signals=self._stats.total_signals,
                total_broadcasts=self._stats.total_broadcasts,
                current_waiters=self._stats.current_waiters,
                average_wait_time=self._stats.average_wait_time,
                spurious_wakeups=self._stats.spurious_wakeups
            )
    
    def get_waiters(self) -> List[threading.Thread]:
        """Get list of currently waiting threads."""
        return list(self._waiters)
    
    def __enter__(self):
        """Context manager entry."""
        self._lock.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self._lock.release()


class BoundedBuffer:
    """
    Bounded buffer implementation using condition variables.
    """
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = []
        self.lock = threading.RLock()
        self.not_empty = EnhancedCondition(self.lock)
        self.not_full = EnhancedCondition(self.lock)
    
    def put(self, item: Any, timeout: Optional[float] = None) -> bool:
        """Put an item into the buffer."""
        with self.lock:
            # Wait for space
            result = self.not_full.wait(
                predicate=lambda: len(self.buffer) < self.capacity,
                timeout=timeout
            )
            
            if result == WaitResult.TIMEOUT:
                return False
            
            # Add item
            self.buffer.append(item)
            
            # Notify consumers
            self.not_empty.notify()
            return True
    
    def get(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Get an item from the buffer."""
        with self.lock:
            # Wait for item
            result = self.not_empty.wait(
                predicate=lambda: len(self.buffer) > 0,
                timeout=timeout
            )
            
            if result == WaitResult.TIMEOUT:
                return None
            
            # Remove item
            item = self.buffer.pop(0)
            
            # Notify producers
            self.not_full.notify()
            return item
    
    def size(self) -> int:
        """Get current buffer size."""
        with self.lock:
            return len(self.buffer)
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        with self.lock:
            return len(self.buffer) == 0
    
    def is_full(self) -> bool:
        """Check if buffer is full."""
        with self.lock:
            return len(self.buffer) >= self.capacity
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics."""
        with self.lock:
            return {
                'size': len(self.buffer),
                'capacity': self.capacity,
                'empty_stats': self.not_empty.get_stats(),
                'full_stats': self.not_full.get_stats()
            }


class ReadWriteLock:
    """
    Read-write lock using condition variables.
    """
    
    def __init__(self):
        self._lock = threading.RLock()
        self._condition = EnhancedCondition(self._lock)
        self._readers = 0
        self._writers = 0
        self._write_ready = False
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire read lock."""
        with self._lock:
            # Wait until no writers
            result = self._condition.wait(
                predicate=lambda: self._writers == 0 and not self._write_ready,
                timeout=timeout
            )
            
            if result == WaitResult.TIMEOUT:
                return False
            
            self._readers += 1
            return True
    
    def release_read(self):
        """Release read lock."""
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                # Notify waiting writers
                self._condition.notify_all()
    
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire write lock."""
        with self._lock:
            self._write_ready = True
            
            # Wait until no readers or writers
            result = self._condition.wait(
                predicate=lambda: self._readers == 0 and self._writers == 0,
                timeout=timeout
            )
            
            if result == WaitResult.TIMEOUT:
                self._write_ready = False
                return False
            
            self._writers += 1
            self._write_ready = False
            return True
    
    def release_write(self):
        """Release write lock."""
        with self._lock:
            self._writers -= 1
            # Notify all waiting threads
            self._condition.notify_all()
    
    def get_status(self) -> Dict[str, int]:
        """Get lock status."""
        with self._lock:
            return {
                'readers': self._readers,
                'writers': self._writers,
                'write_ready': self._write_ready
            }


class Monitor:
    """
    Monitor pattern implementation using condition variables.
    """
    
    def __init__(self):
        self._lock = threading.RLock()
        self._conditions: Dict[str, EnhancedCondition] = {}
    
    def get_condition(self, name: str) -> EnhancedCondition:
        """Get or create a named condition variable."""
        with self._lock:
            if name not in self._conditions:
                self._conditions[name] = EnhancedCondition(self._lock)
            return self._conditions[name]
    
    def wait(self, condition_name: str, predicate: Optional[Callable[[], bool]] = None,
             timeout: Optional[float] = None) -> WaitResult:
        """Wait on a named condition."""
        condition = self.get_condition(condition_name)
        return condition.wait(predicate=predicate, timeout=timeout)
    
    def signal(self, condition_name: str, n: int = 1):
        """Signal a named condition."""
        if condition_name in self._conditions:
            self._conditions[condition_name].notify(n)
    
    def broadcast(self, condition_name: str):
        """Broadcast to a named condition."""
        if condition_name in self._conditions:
            self._conditions[condition_name].notify_all()
    
    def get_monitor_stats(self) -> Dict[str, ConditionStats]:
        """Get statistics for all conditions."""
        with self._lock:
            return {name: cond.get_stats() for name, cond in self._conditions.items()}
    
    def __enter__(self):
        """Context manager entry."""
        self._lock.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self._lock.release()


class AsyncCondition:
    """
    Async condition variable for asyncio applications.
    """
    
    def __init__(self, lock: Optional[asyncio.Lock] = None):
        self._lock = lock or asyncio.Lock()
        self._waiters = []
        self._stats = ConditionStats()
    
    async def wait(self, predicate: Optional[Callable[[], bool]] = None,
                   timeout: Optional[float] = None) -> WaitResult:
        """Wait for condition asynchronously."""
        start_time = time.time()
        
        async with self._lock:
            self._stats.current_waiters += 1
            self._stats.total_waits += 1
            
            try:
                if predicate is None:
                    # Simple wait
                    future = asyncio.Future()
                    self._waiters.append(future)
                    
                    try:
                        if timeout is None:
                            await future
                        else:
                            await asyncio.wait_for(future, timeout=timeout)
                        return WaitResult.SIGNALED
                    except asyncio.TimeoutError:
                        return WaitResult.TIMEOUT
                    except asyncio.CancelledError:
                        return WaitResult.TIMEOUT
                else:
                    # Wait with predicate
                    while not predicate():
                        future = asyncio.Future()
                        self._waiters.append(future)
                        
                        try:
                            if timeout is None:
                                await future
                            else:
                                await asyncio.wait_for(future, timeout=timeout)
                        except asyncio.TimeoutError:
                            return WaitResult.TIMEOUT
                        except asyncio.CancelledError:
                            return WaitResult.TIMEOUT
                        
                        # Check for spurious wakeup
                        if not predicate():
                            self._stats.spurious_wakeups += 1
                    
                    return WaitResult.SIGNALED
            
            finally:
                self._stats.current_waiters -= 1
                
                # Update average wait time
                wait_time = time.time() - start_time
                self._stats.average_wait_time = (
                    (self._stats.average_wait_time * (self._stats.total_waits - 1) + wait_time) /
                    self._stats.total_waits
                )
    
    def notify(self, n: int = 1):
        """Notify n waiting coroutines."""
        notified = 0
        for _ in range(min(n, len(self._waiters))):
            if self._waiters:
                future = self._waiters.pop(0)
                if not future.done():
                    future.set_result(None)
                    notified += 1
        
        self._stats.total_signals += notified
    
    def notify_all(self):
        """Notify all waiting coroutines."""
        notified = 0
        while self._waiters:
            future = self._waiters.pop(0)
            if not future.done():
                future.set_result(None)
                notified += 1
        
        self._stats.total_broadcasts += 1
        self._stats.total_signals += notified
    
    def get_stats(self) -> ConditionStats:
        """Get condition variable statistics."""
        return ConditionStats(
            total_waits=self._stats.total_waits,
            total_signals=self._stats.total_signals,
            total_broadcasts=self._stats.total_broadcasts,
            current_waiters=self._stats.current_waiters,
            average_wait_time=self._stats.average_wait_time,
            spurious_wakeups=self._stats.spurious_wakeups
        )


class AsyncBoundedBuffer:
    """
    Async bounded buffer using async condition variables.
    """
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = []
        self.lock = asyncio.Lock()
        self.not_empty = AsyncCondition(self.lock)
        self.not_full = AsyncCondition(self.lock)
    
    async def put(self, item: Any, timeout: Optional[float] = None) -> bool:
        """Put an item into the buffer asynchronously."""
        async with self.lock:
            # Wait for space
            result = await self.not_full.wait(
                predicate=lambda: len(self.buffer) < self.capacity,
                timeout=timeout
            )
            
            if result == WaitResult.TIMEOUT:
                return False
            
            # Add item
            self.buffer.append(item)
            
            # Notify consumers
            self.not_empty.notify()
            return True
    
    async def get(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Get an item from the buffer asynchronously."""
        async with self.lock:
            # Wait for item
            result = await self.not_empty.wait(
                predicate=lambda: len(self.buffer) > 0,
                timeout=timeout
            )
            
            if result == WaitResult.TIMEOUT:
                return None
            
            # Remove item
            item = self.buffer.pop(0)
            
            # Notify producers
            self.not_full.notify()
            return item
    
    def size(self) -> int:
        """Get current buffer size."""
        return len(self.buffer)


# Example usage and demonstrations
def demo_enhanced_condition():
    """Demonstrate enhanced condition variable."""
    print("=== Enhanced Condition Variable Demo ===")
    
    condition = EnhancedCondition()
    shared_data = {'value': 0, 'ready': False}
    results = []
    
    def producer():
        with condition:
            time.sleep(1)  # Simulate work
            shared_data['value'] = 42
            shared_data['ready'] = True
            condition.notify_all()
            print("Producer: Data ready")
    
    def consumer(consumer_id: int):
        with condition:
            result = condition.wait(
                predicate=lambda: shared_data['ready'],
                timeout=3.0
            )
            
            if result == WaitResult.SIGNALED:
                results.append(f"Consumer {consumer_id}: Got {shared_data['value']}")
                print(f"Consumer {consumer_id}: Got {shared_data['value']}")
            else:
                results.append(f"Consumer {consumer_id}: Timeout")
                print(f"Consumer {consumer_id}: Timeout")
    
    # Start consumers first
    consumer_threads = []
    for i in range(3):
        thread = threading.Thread(target=consumer, args=(i,))
        consumer_threads.append(thread)
        thread.start()
    
    # Start producer
    producer_thread = threading.Thread(target=producer)
    producer_thread.start()
    
    # Wait for all threads
    producer_thread.join()
    for thread in consumer_threads:
        thread.join()
    
    print(f"Results: {results}")
    print(f"Condition stats: {condition.get_stats()}")


def demo_bounded_buffer():
    """Demonstrate bounded buffer with condition variables."""
    print("\n=== Bounded Buffer Demo ===")
    
    buffer = BoundedBuffer(capacity=3)
    results = []
    
    def producer(producer_id: int):
        for i in range(3):
            item = f"item-{producer_id}-{i}"
            if buffer.put(item, timeout=2.0):
                print(f"Producer {producer_id}: Put {item}")
                results.append(f"Produced: {item}")
            else:
                print(f"Producer {producer_id}: Timeout putting {item}")
            time.sleep(0.1)
    
    def consumer(consumer_id: int):
        for _ in range(2):
            item = buffer.get(timeout=2.0)
            if item:
                print(f"Consumer {consumer_id}: Got {item}")
                results.append(f"Consumed: {item}")
            else:
                print(f"Consumer {consumer_id}: Timeout")
            time.sleep(0.5)
    
    # Start producers and consumers
    threads = []
    
    # Start consumers
    for i in range(2):
        thread = threading.Thread(target=consumer, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Start producers
    for i in range(2):
        thread = threading.Thread(target=producer, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    print(f"Buffer stats: {buffer.get_stats()}")


def demo_read_write_lock():
    """Demonstrate read-write lock."""
    print("\n=== Read-Write Lock Demo ===")
    
    rw_lock = ReadWriteLock()
    shared_data = {'value': 0}
    results = []
    
    def reader(reader_id: int):
        if rw_lock.acquire_read(timeout=2.0):
            try:
                value = shared_data['value']
                print(f"Reader {reader_id}: Read {value}")
                results.append(f"Read-{reader_id}: {value}")
                time.sleep(0.5)  # Simulate reading
            finally:
                rw_lock.release_read()
        else:
            print(f"Reader {reader_id}: Timeout")
    
    def writer(writer_id: int):
        if rw_lock.acquire_write(timeout=2.0):
            try:
                shared_data['value'] += 1
                print(f"Writer {writer_id}: Wrote {shared_data['value']}")
                results.append(f"Write-{writer_id}: {shared_data['value']}")
                time.sleep(0.3)  # Simulate writing
            finally:
                rw_lock.release_write()
        else:
            print(f"Writer {writer_id}: Timeout")
    
    # Start readers and writers
    threads = []
    
    # Start some readers
    for i in range(3):
        thread = threading.Thread(target=reader, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Start a writer
    thread = threading.Thread(target=writer, args=(0,))
    threads.append(thread)
    thread.start()
    
    # Start more readers
    for i in range(3, 5):
        thread = threading.Thread(target=reader, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    print(f"Final value: {shared_data['value']}")
    print(f"Lock status: {rw_lock.get_status()}")


async def demo_async_condition():
    """Demonstrate async condition variable."""
    print("\n=== Async Condition Variable Demo ===")
    
    condition = AsyncCondition()
    shared_data = {'items': [], 'finished': False}
    results = []
    
    async def producer():
        await asyncio.sleep(1)  # Simulate async work
        
        for i in range(5):
            async with condition._lock:
                shared_data['items'].append(f"async-item-{i}")
                condition.notify()
            await asyncio.sleep(0.2)
        
        async with condition._lock:
            shared_data['finished'] = True
            condition.notify_all()
        
        print("Async producer: Finished")
    
    async def consumer(consumer_id: int):
        while True:
            async with condition._lock:
                result = await condition.wait(
                    predicate=lambda: shared_data['items'] or shared_data['finished'],
                    timeout=3.0
                )
                
                if result == WaitResult.TIMEOUT:
                    results.append(f"Async consumer {consumer_id}: Timeout")
                    break
                
                if shared_data['items']:
                    item = shared_data['items'].pop(0)
                    results.append(f"Async consumer {consumer_id}: Got {item}")
                    print(f"Async consumer {consumer_id}: Got {item}")
                elif shared_data['finished']:
                    results.append(f"Async consumer {consumer_id}: Finished")
                    break
    
    # Run producer and consumers concurrently
    tasks = [producer()]
    tasks.extend([consumer(i) for i in range(3)])
    
    await asyncio.gather(*tasks)
    
    print(f"Async results: {results}")
    print(f"Async condition stats: {condition.get_stats()}")


async def main():
    """Run all condition variable demonstrations."""
    demo_enhanced_condition()
    demo_bounded_buffer()
    demo_read_write_lock()
    await demo_async_condition()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
