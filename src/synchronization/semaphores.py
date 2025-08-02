"""
Semaphore Patterns

Various semaphore implementations for resource management and synchronization.
"""

import threading
import time
import asyncio
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import logging
import queue


class SemaphoreType(Enum):
    """Types of semaphores."""
    COUNTING = "counting"
    BINARY = "binary"
    BOUNDED = "bounded"
    FAIR = "fair"
    PRIORITY = "priority"


@dataclass
class SemaphoreStats:
    """Semaphore statistics."""
    current_permits: int
    max_permits: int
    waiting_threads: int
    total_acquires: int = 0
    total_releases: int = 0
    average_wait_time: float = 0.0


class CountingSemaphore:
    """
    Counting semaphore with configurable permits.
    """
    
    def __init__(self, permits: int = 1):
        if permits < 0:
            raise ValueError("Permits must be non-negative")
        
        self._permits = permits
        self._max_permits = permits
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._stats = SemaphoreStats(permits, permits, 0)
    
    def acquire(self, permits: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire permits from the semaphore."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        start_time = time.time()
        
        with self._condition:
            # Wait for enough permits
            while self._permits < permits:
                self._stats.waiting_threads += 1
                try:
                    if not self._condition.wait(timeout=timeout):
                        return False  # Timeout
                finally:
                    self._stats.waiting_threads -= 1
            
            # Acquire permits
            self._permits -= permits
            self._stats.current_permits = self._permits
            self._stats.total_acquires += permits
            
            # Update wait time statistics
            wait_time = time.time() - start_time
            self._stats.average_wait_time = (
                (self._stats.average_wait_time * (self._stats.total_acquires - permits) + wait_time) /
                self._stats.total_acquires
            )
            
            return True
    
    def release(self, permits: int = 1):
        """Release permits to the semaphore."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        with self._condition:
            self._permits = min(self._permits + permits, self._max_permits)
            self._stats.current_permits = self._permits
            self._stats.total_releases += permits
            
            # Notify waiting threads
            self._condition.notify_all()
    
    def try_acquire(self, permits: int = 1) -> bool:
        """Try to acquire permits without blocking."""
        return self.acquire(permits, timeout=0)
    
    def available_permits(self) -> int:
        """Get number of available permits."""
        with self._lock:
            return self._permits
    
    def get_stats(self) -> SemaphoreStats:
        """Get semaphore statistics."""
        with self._lock:
            return SemaphoreStats(
                current_permits=self._permits,
                max_permits=self._max_permits,
                waiting_threads=self._stats.waiting_threads,
                total_acquires=self._stats.total_acquires,
                total_releases=self._stats.total_releases,
                average_wait_time=self._stats.average_wait_time
            )
    
    def __enter__(self):
        """Context manager entry."""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()


class BinarySemaphore(CountingSemaphore):
    """
    Binary semaphore (mutex) with only 0 or 1 permits.
    """
    
    def __init__(self, initial_permit: bool = True):
        super().__init__(permits=1 if initial_permit else 0)
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire the binary semaphore."""
        return super().acquire(permits=1, timeout=timeout)
    
    def release(self):
        """Release the binary semaphore."""
        super().release(permits=1)
    
    def is_locked(self) -> bool:
        """Check if the semaphore is locked."""
        return self.available_permits() == 0


class BoundedSemaphore(CountingSemaphore):
    """
    Bounded semaphore that prevents releasing more permits than the maximum.
    """
    
    def release(self, permits: int = 1):
        """Release permits with bounds checking."""
        with self._condition:
            if self._permits + permits > self._max_permits:
                raise ValueError(f"Cannot release {permits} permits. Would exceed maximum of {self._max_permits}")
            
            super().release(permits)


class FairSemaphore:
    """
    Fair semaphore that grants permits in FIFO order.
    """
    
    def __init__(self, permits: int = 1):
        self._permits = permits
        self._max_permits = permits
        self._lock = threading.RLock()
        self._waiting_queue = queue.Queue()
        self._stats = SemaphoreStats(permits, permits, 0)
    
    def acquire(self, permits: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire permits fairly (FIFO order)."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        start_time = time.time()
        
        # Create event for this thread
        event = threading.Event()
        request = (permits, event, threading.current_thread().ident)
        
        with self._lock:
            self._waiting_queue.put(request)
            self._stats.waiting_threads += 1
        
        try:
            # Wait for our turn
            if not event.wait(timeout=timeout):
                # Timeout - remove from queue if still there
                with self._lock:
                    # Create a new queue without our request
                    new_queue = queue.Queue()
                    while not self._waiting_queue.empty():
                        try:
                            req = self._waiting_queue.get_nowait()
                            if req[2] != threading.current_thread().ident:
                                new_queue.put(req)
                        except queue.Empty:
                            break
                    self._waiting_queue = new_queue
                    self._stats.waiting_threads -= 1
                return False
            
            # Update statistics
            wait_time = time.time() - start_time
            with self._lock:
                self._stats.total_acquires += permits
                self._stats.average_wait_time = (
                    (self._stats.average_wait_time * (self._stats.total_acquires - permits) + wait_time) /
                    self._stats.total_acquires
                )
            
            return True
        
        except Exception:
            with self._lock:
                self._stats.waiting_threads -= 1
            raise
    
    def release(self, permits: int = 1):
        """Release permits and notify waiting threads."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        with self._lock:
            self._permits = min(self._permits + permits, self._max_permits)
            self._stats.current_permits = self._permits
            self._stats.total_releases += permits
            
            # Process waiting queue
            self._process_waiting_queue()
    
    def _process_waiting_queue(self):
        """Process the waiting queue and grant permits fairly."""
        while not self._waiting_queue.empty() and self._permits > 0:
            try:
                required_permits, event, thread_id = self._waiting_queue.get_nowait()
                
                if self._permits >= required_permits:
                    # Grant permits
                    self._permits -= required_permits
                    self._stats.current_permits = self._permits
                    self._stats.waiting_threads -= 1
                    event.set()
                else:
                    # Put back in queue
                    self._waiting_queue.put((required_permits, event, thread_id))
                    break
            except queue.Empty:
                break
    
    def available_permits(self) -> int:
        """Get number of available permits."""
        with self._lock:
            return self._permits
    
    def get_stats(self) -> SemaphoreStats:
        """Get semaphore statistics."""
        with self._lock:
            return SemaphoreStats(
                current_permits=self._permits,
                max_permits=self._max_permits,
                waiting_threads=self._stats.waiting_threads,
                total_acquires=self._stats.total_acquires,
                total_releases=self._stats.total_releases,
                average_wait_time=self._stats.average_wait_time
            )


class PrioritySemaphore:
    """
    Priority semaphore that grants permits based on priority.
    """
    
    def __init__(self, permits: int = 1):
        self._permits = permits
        self._max_permits = permits
        self._lock = threading.RLock()
        self._waiting_heap = []  # Min-heap for priorities
        self._stats = SemaphoreStats(permits, permits, 0)
        self._next_sequence = 0
    
    def acquire(self, permits: int = 1, priority: int = 0, timeout: Optional[float] = None) -> bool:
        """Acquire permits with priority (lower number = higher priority)."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        start_time = time.time()
        
        # Create event for this thread
        event = threading.Event()
        
        with self._lock:
            # Add to priority queue (priority, sequence, permits, event)
            import heapq
            heapq.heappush(self._waiting_heap, (priority, self._next_sequence, permits, event))
            self._next_sequence += 1
            self._stats.waiting_threads += 1
            
            # Try to process immediately
            self._process_waiting_heap()
        
        try:
            # Wait for our turn
            if not event.wait(timeout=timeout):
                # Timeout - remove from heap if still there
                with self._lock:
                    # Remove our entry from heap
                    self._waiting_heap = [item for item in self._waiting_heap if item[3] != event]
                    import heapq
                    heapq.heapify(self._waiting_heap)
                    self._stats.waiting_threads -= 1
                return False
            
            # Update statistics
            wait_time = time.time() - start_time
            with self._lock:
                self._stats.total_acquires += permits
                self._stats.average_wait_time = (
                    (self._stats.average_wait_time * (self._stats.total_acquires - permits) + wait_time) /
                    self._stats.total_acquires
                )
            
            return True
        
        except Exception:
            with self._lock:
                self._stats.waiting_threads -= 1
            raise
    
    def release(self, permits: int = 1):
        """Release permits and notify waiting threads by priority."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        with self._lock:
            self._permits = min(self._permits + permits, self._max_permits)
            self._stats.current_permits = self._permits
            self._stats.total_releases += permits
            
            # Process waiting heap
            self._process_waiting_heap()
    
    def _process_waiting_heap(self):
        """Process the waiting heap and grant permits by priority."""
        import heapq
        
        while self._waiting_heap and self._permits > 0:
            priority, sequence, required_permits, event = self._waiting_heap[0]
            
            if self._permits >= required_permits:
                # Grant permits
                heapq.heappop(self._waiting_heap)
                self._permits -= required_permits
                self._stats.current_permits = self._permits
                self._stats.waiting_threads -= 1
                event.set()
            else:
                # Can't satisfy highest priority request
                break
    
    def available_permits(self) -> int:
        """Get number of available permits."""
        with self._lock:
            return self._permits
    
    def get_stats(self) -> SemaphoreStats:
        """Get semaphore statistics."""
        with self._lock:
            return SemaphoreStats(
                current_permits=self._permits,
                max_permits=self._max_permits,
                waiting_threads=self._stats.waiting_threads,
                total_acquires=self._stats.total_acquires,
                total_releases=self._stats.total_releases,
                average_wait_time=self._stats.average_wait_time
            )


class AsyncSemaphore:
    """
    Async semaphore for asyncio applications.
    """
    
    def __init__(self, permits: int = 1):
        self._permits = permits
        self._max_permits = permits
        self._waiters = []
        self._stats = SemaphoreStats(permits, permits, 0)
    
    async def acquire(self, permits: int = 1) -> bool:
        """Acquire permits asynchronously."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        start_time = time.time()
        
        while self._permits < permits:
            # Create future for waiting
            future = asyncio.Future()
            self._waiters.append((permits, future))
            self._stats.waiting_threads += 1
            
            try:
                await future
            except asyncio.CancelledError:
                self._stats.waiting_threads -= 1
                raise
        
        # Acquire permits
        self._permits -= permits
        self._stats.current_permits = self._permits
        self._stats.total_acquires += permits
        
        # Update wait time statistics
        wait_time = time.time() - start_time
        self._stats.average_wait_time = (
            (self._stats.average_wait_time * (self._stats.total_acquires - permits) + wait_time) /
            self._stats.total_acquires
        )
        
        return True
    
    def release(self, permits: int = 1):
        """Release permits and notify waiting coroutines."""
        if permits <= 0:
            raise ValueError("Permits must be positive")
        
        self._permits = min(self._permits + permits, self._max_permits)
        self._stats.current_permits = self._permits
        self._stats.total_releases += permits
        
        # Notify waiting coroutines
        self._notify_waiters()
    
    def _notify_waiters(self):
        """Notify waiting coroutines that can now proceed."""
        satisfied_waiters = []
        
        for i, (required_permits, future) in enumerate(self._waiters):
            if self._permits >= required_permits and not future.done():
                self._permits -= required_permits
                self._stats.current_permits = self._permits
                self._stats.waiting_threads -= 1
                future.set_result(True)
                satisfied_waiters.append(i)
            elif self._permits < required_permits:
                break  # Can't satisfy any more waiters
        
        # Remove satisfied waiters
        for i in reversed(satisfied_waiters):
            del self._waiters[i]
    
    def available_permits(self) -> int:
        """Get number of available permits."""
        return self._permits
    
    def get_stats(self) -> SemaphoreStats:
        """Get semaphore statistics."""
        return SemaphoreStats(
            current_permits=self._permits,
            max_permits=self._max_permits,
            waiting_threads=self._stats.waiting_threads,
            total_acquires=self._stats.total_acquires,
            total_releases=self._stats.total_releases,
            average_wait_time=self._stats.average_wait_time
        )
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.release()


class ResourcePool:
    """
    Resource pool using semaphores for resource management.
    """
    
    def __init__(self, resources: List[Any]):
        self.resources = resources
        self.available_resources = queue.Queue()
        self.semaphore = CountingSemaphore(len(resources))
        
        # Initialize available resources
        for resource in resources:
            self.available_resources.put(resource)
    
    def acquire_resource(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Acquire a resource from the pool."""
        if self.semaphore.acquire(timeout=timeout):
            try:
                return self.available_resources.get_nowait()
            except queue.Empty:
                # This shouldn't happen, but release semaphore if it does
                self.semaphore.release()
                return None
        return None
    
    def release_resource(self, resource: Any):
        """Release a resource back to the pool."""
        self.available_resources.put(resource)
        self.semaphore.release()
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get resource pool statistics."""
        stats = self.semaphore.get_stats()
        return {
            'total_resources': len(self.resources),
            'available_resources': stats.current_permits,
            'in_use_resources': len(self.resources) - stats.current_permits,
            'waiting_threads': stats.waiting_threads,
            'total_acquires': stats.total_acquires,
            'total_releases': stats.total_releases
        }


# Example usage and demonstrations
def demo_counting_semaphore():
    """Demonstrate counting semaphore."""
    print("=== Counting Semaphore Demo ===")
    
    semaphore = CountingSemaphore(permits=3)
    results = []
    
    def worker(worker_id: int):
        print(f"Worker {worker_id} trying to acquire semaphore")
        
        if semaphore.acquire(timeout=2.0):
            print(f"Worker {worker_id} acquired semaphore")
            time.sleep(1)  # Simulate work
            print(f"Worker {worker_id} releasing semaphore")
            semaphore.release()
            results.append(f"Worker {worker_id} completed")
        else:
            print(f"Worker {worker_id} timed out")
            results.append(f"Worker {worker_id} timed out")
    
    # Start multiple workers
    threads = []
    for i in range(6):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    print(f"Results: {results}")
    print(f"Semaphore stats: {semaphore.get_stats()}")


def demo_fair_semaphore():
    """Demonstrate fair semaphore."""
    print("\n=== Fair Semaphore Demo ===")
    
    semaphore = FairSemaphore(permits=1)
    results = []
    
    def worker(worker_id: int, delay: float):
        time.sleep(delay)  # Stagger start times
        print(f"Worker {worker_id} requesting semaphore")
        
        if semaphore.acquire(timeout=5.0):
            print(f"Worker {worker_id} acquired semaphore")
            time.sleep(0.5)
            semaphore.release()
            results.append(worker_id)
        else:
            print(f"Worker {worker_id} timed out")
    
    # Start workers with different delays
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i, i * 0.1))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    print(f"Execution order: {results}")
    print(f"Fair semaphore stats: {semaphore.get_stats()}")


def demo_priority_semaphore():
    """Demonstrate priority semaphore."""
    print("\n=== Priority Semaphore Demo ===")
    
    semaphore = PrioritySemaphore(permits=1)
    results = []
    
    def worker(worker_id: int, priority: int):
        time.sleep(0.1)  # Let all workers queue up
        print(f"Worker {worker_id} requesting semaphore with priority {priority}")
        
        if semaphore.acquire(priority=priority, timeout=5.0):
            print(f"Worker {worker_id} (priority {priority}) acquired semaphore")
            time.sleep(0.2)
            semaphore.release()
            results.append((worker_id, priority))
        else:
            print(f"Worker {worker_id} timed out")
    
    # Start workers with different priorities
    threads = []
    priorities = [5, 1, 3, 2, 4]  # Lower number = higher priority
    for i, priority in enumerate(priorities):
        thread = threading.Thread(target=worker, args=(i, priority))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    print(f"Execution order (worker_id, priority): {results}")
    print(f"Priority semaphore stats: {semaphore.get_stats()}")


async def demo_async_semaphore():
    """Demonstrate async semaphore."""
    print("\n=== Async Semaphore Demo ===")
    
    semaphore = AsyncSemaphore(permits=2)
    results = []
    
    async def worker(worker_id: int):
        print(f"Async worker {worker_id} requesting semaphore")
        
        async with semaphore:
            print(f"Async worker {worker_id} acquired semaphore")
            await asyncio.sleep(1)  # Simulate async work
            print(f"Async worker {worker_id} releasing semaphore")
            results.append(worker_id)
    
    # Start multiple async workers
    tasks = [worker(i) for i in range(5)]
    await asyncio.gather(*tasks)
    
    print(f"Async results: {results}")
    print(f"Async semaphore stats: {semaphore.get_stats()}")


def demo_resource_pool():
    """Demonstrate resource pool."""
    print("\n=== Resource Pool Demo ===")
    
    # Create pool with database connections (simulated)
    resources = [f"DB_Connection_{i}" for i in range(3)]
    pool = ResourcePool(resources)
    
    def worker(worker_id: int):
        resource = pool.acquire_resource(timeout=2.0)
        if resource:
            print(f"Worker {worker_id} acquired {resource}")
            time.sleep(1)  # Simulate database work
            pool.release_resource(resource)
            print(f"Worker {worker_id} released {resource}")
        else:
            print(f"Worker {worker_id} could not acquire resource")
    
    # Start multiple workers
    threads = []
    for i in range(6):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    print(f"Resource pool stats: {pool.get_pool_stats()}")


async def main():
    """Run all semaphore demonstrations."""
    demo_counting_semaphore()
    demo_fair_semaphore()
    demo_priority_semaphore()
    await demo_async_semaphore()
    demo_resource_pool()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
