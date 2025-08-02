"""
Advanced Thread Pool Implementations

Various thread pool patterns including:
- Backpressure handling
- Priority-based execution
- Auto-scaling pools
- Custom rejection policies
"""

import threading
import queue
import time
import weakref
from typing import Any, Callable, Optional, Union, Dict, List
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from abc import ABC, abstractmethod
from enum import Enum
import logging


class RejectionPolicy(Enum):
    """Policies for handling rejected tasks when pool is full."""
    ABORT = "abort"
    CALLER_RUNS = "caller_runs"
    DISCARD = "discard"
    DISCARD_OLDEST = "discard_oldest"


class PoolState(Enum):
    """Thread pool states."""
    RUNNING = "running"
    SHUTDOWN = "shutdown"
    TERMINATED = "terminated"


class BackpressureThreadPool:
    """
    Thread pool with backpressure handling and flow control.
    Prevents memory exhaustion by limiting queue size.
    """
    
    def __init__(self, max_workers: int = 4, max_queue_size: int = 100,
                 rejection_policy: RejectionPolicy = RejectionPolicy.ABORT,
                 thread_name_prefix: str = "BackpressurePool"):
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self.rejection_policy = rejection_policy
        self.thread_name_prefix = thread_name_prefix
        
        self._work_queue = queue.Queue(maxsize=max_queue_size)
        self._threads: List[threading.Thread] = []
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._task_count = 0
        self._completed_count = 0
        self._rejected_count = 0
        
        # Start worker threads
        self._start_workers()
    
    def _start_workers(self):
        """Start the worker threads."""
        for i in range(self.max_workers):
            thread = threading.Thread(
                target=self._worker,
                name=f"{self.thread_name_prefix}-{i}",
                daemon=True
            )
            thread.start()
            self._threads.append(thread)
    
    def _worker(self):
        """Worker thread main loop."""
        while True:
            try:
                work_item = self._work_queue.get(timeout=1.0)
                if work_item is None:  # Shutdown signal
                    break
                
                fn, args, kwargs, future = work_item
                
                if not future.cancelled():
                    try:
                        result = fn(*args, **kwargs)
                        future.set_result(result)
                    except Exception as e:
                        future.set_exception(e)
                
                self._completed_count += 1
                self._work_queue.task_done()
                
            except queue.Empty:
                with self._shutdown_lock:
                    if self._shutdown:
                        break
            except Exception as e:
                logging.error(f"Worker thread error: {e}")
    
    def submit(self, fn: Callable, *args, **kwargs) -> Optional[Future]:
        """Submit a task for execution."""
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("Cannot schedule new tasks after shutdown")
        
        future = Future()
        work_item = (fn, args, kwargs, future)
        
        try:
            self._work_queue.put_nowait(work_item)
            self._task_count += 1
            return future
        except queue.Full:
            return self._handle_rejection(work_item)
    
    def _handle_rejection(self, work_item) -> Optional[Future]:
        """Handle task rejection based on policy."""
        fn, args, kwargs, future = work_item
        self._rejected_count += 1
        
        if self.rejection_policy == RejectionPolicy.ABORT:
            future.set_exception(RuntimeError("Task rejected: queue full"))
            return future
        
        elif self.rejection_policy == RejectionPolicy.CALLER_RUNS:
            # Execute in caller's thread
            try:
                result = fn(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            return future
        
        elif self.rejection_policy == RejectionPolicy.DISCARD:
            future.cancel()
            return None
        
        elif self.rejection_policy == RejectionPolicy.DISCARD_OLDEST:
            try:
                # Try to remove oldest task
                old_item = self._work_queue.get_nowait()
                old_future = old_item[3]
                old_future.cancel()
                
                # Add new task
                self._work_queue.put_nowait(work_item)
                return future
            except queue.Empty:
                future.cancel()
                return None
    
    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool."""
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True
        
        # Send shutdown signals to workers
        for _ in self._threads:
            self._work_queue.put(None)
        
        if wait:
            for thread in self._threads:
                thread.join()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            "max_workers": self.max_workers,
            "active_threads": sum(1 for t in self._threads if t.is_alive()),
            "queue_size": self._work_queue.qsize(),
            "max_queue_size": self.max_queue_size,
            "tasks_submitted": self._task_count,
            "tasks_completed": self._completed_count,
            "tasks_rejected": self._rejected_count,
            "rejection_policy": self.rejection_policy.value,
        }


class PriorityTask:
    """Task with priority for priority thread pool."""
    
    def __init__(self, priority: int, fn: Callable, args: tuple, kwargs: dict, future: Future):
        self.priority = priority
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.future = future
        self.timestamp = time.time()
    
    def __lt__(self, other):
        # Higher priority number = higher priority
        if self.priority != other.priority:
            return self.priority > other.priority
        # If same priority, FIFO order
        return self.timestamp < other.timestamp
    
    def execute(self):
        """Execute the task."""
        if not self.future.cancelled():
            try:
                result = self.fn(*self.args, **self.kwargs)
                self.future.set_result(result)
            except Exception as e:
                self.future.set_exception(e)


class PriorityThreadPool:
    """
    Thread pool that executes tasks based on priority.
    Higher priority tasks are executed first.
    """
    
    def __init__(self, max_workers: int = 4, max_queue_size: int = 100):
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        
        self._priority_queue = queue.PriorityQueue(maxsize=max_queue_size)
        self._threads: List[threading.Thread] = []
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        
        self._start_workers()
    
    def _start_workers(self):
        """Start worker threads."""
        for i in range(self.max_workers):
            thread = threading.Thread(
                target=self._worker,
                name=f"PriorityPool-{i}",
                daemon=True
            )
            thread.start()
            self._threads.append(thread)
    
    def _worker(self):
        """Worker thread main loop."""
        while True:
            try:
                task = self._priority_queue.get(timeout=1.0)
                if task is None:  # Shutdown signal
                    break
                
                task.execute()
                self._priority_queue.task_done()
                
            except queue.Empty:
                with self._shutdown_lock:
                    if self._shutdown:
                        break
            except Exception as e:
                logging.error(f"Priority worker error: {e}")
    
    def submit(self, fn: Callable, *args, priority: int = 0, **kwargs) -> Future:
        """Submit a task with priority."""
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("Cannot schedule new tasks after shutdown")
        
        future = Future()
        task = PriorityTask(priority, fn, args, kwargs, future)
        
        try:
            self._priority_queue.put_nowait(task)
            return future
        except queue.Full:
            future.set_exception(RuntimeError("Priority queue full"))
            return future
    
    def shutdown(self, wait: bool = True):
        """Shutdown the priority pool."""
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True
        
        for _ in self._threads:
            self._priority_queue.put(None)
        
        if wait:
            for thread in self._threads:
                thread.join()


class ScalingThreadPool:
    """
    Auto-scaling thread pool that adjusts worker count based on load.
    Scales up when queue is full, scales down when idle.
    """
    
    def __init__(self, core_workers: int = 2, max_workers: int = 10,
                 scale_up_threshold: float = 0.8, scale_down_threshold: float = 0.2,
                 idle_timeout: float = 60.0):
        self.core_workers = core_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.idle_timeout = idle_timeout
        
        self._work_queue = queue.Queue()
        self._workers: Dict[int, threading.Thread] = {}
        self._worker_id_counter = 0
        self._shutdown = False
        self._lock = threading.Lock()
        
        # Scaling monitor
        self._monitor_thread = threading.Thread(
            target=self._monitor_scaling,
            daemon=True
        )
        self._monitor_thread.start()
        
        # Start core workers
        self._scale_to(self.core_workers)
    
    def _create_worker(self) -> threading.Thread:
        """Create a new worker thread."""
        worker_id = self._worker_id_counter
        self._worker_id_counter += 1
        
        def worker():
            last_task_time = time.time()
            while True:
                try:
                    work_item = self._work_queue.get(timeout=1.0)
                    if work_item is None:  # Shutdown signal
                        break
                    
                    fn, args, kwargs, future = work_item
                    last_task_time = time.time()
                    
                    if not future.cancelled():
                        try:
                            result = fn(*args, **kwargs)
                            future.set_result(result)
                        except Exception as e:
                            future.set_exception(e)
                    
                    self._work_queue.task_done()
                    
                except queue.Empty:
                    # Check if we should terminate due to inactivity
                    if (len(self._workers) > self.core_workers and
                        time.time() - last_task_time > self.idle_timeout):
                        break
                    
                    with self._lock:
                        if self._shutdown:
                            break
            
            # Remove self from workers dict
            with self._lock:
                self._workers.pop(worker_id, None)
        
        thread = threading.Thread(target=worker, name=f"ScalingPool-{worker_id}")
        return thread
    
    def _scale_to(self, target_workers: int):
        """Scale to target number of workers."""
        with self._lock:
            current_workers = len(self._workers)
            
            if target_workers > current_workers:
                # Scale up
                for _ in range(target_workers - current_workers):
                    if len(self._workers) >= self.max_workers:
                        break
                    worker = self._create_worker()
                    worker.start()
                    self._workers[self._worker_id_counter - 1] = worker
            
            elif target_workers < current_workers:
                # Scale down by sending shutdown signals
                workers_to_remove = current_workers - target_workers
                for _ in range(workers_to_remove):
                    self._work_queue.put(None)
    
    def _monitor_scaling(self):
        """Monitor queue load and adjust worker count."""
        while True:
            try:
                time.sleep(5.0)  # Check every 5 seconds
                
                with self._lock:
                    if self._shutdown:
                        break
                
                queue_size = self._work_queue.qsize()
                current_workers = len(self._workers)
                
                # Calculate load ratio (approximation)
                load_ratio = min(queue_size / max(current_workers, 1), 1.0)
                
                if load_ratio > self.scale_up_threshold and current_workers < self.max_workers:
                    # Scale up
                    new_workers = min(current_workers + 1, self.max_workers)
                    self._scale_to(new_workers)
                    print(f"Scaled up to {new_workers} workers (load: {load_ratio:.2f})")
                
                elif load_ratio < self.scale_down_threshold and current_workers > self.core_workers:
                    # Scale down
                    new_workers = max(current_workers - 1, self.core_workers)
                    self._scale_to(new_workers)
                    print(f"Scaled down to {new_workers} workers (load: {load_ratio:.2f})")
                
            except Exception as e:
                logging.error(f"Scaling monitor error: {e}")
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a task for execution."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot schedule new tasks after shutdown")
        
        future = Future()
        work_item = (fn, args, kwargs, future)
        self._work_queue.put(work_item)
        return future
    
    def shutdown(self, wait: bool = True):
        """Shutdown the scaling pool."""
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
        
        # Send shutdown signals to all workers
        for _ in list(self._workers.keys()):
            self._work_queue.put(None)
        
        if wait:
            for worker in list(self._workers.values()):
                worker.join()
        
        self._monitor_thread.join()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scaling pool statistics."""
        return {
            "core_workers": self.core_workers,
            "max_workers": self.max_workers,
            "current_workers": len(self._workers),
            "queue_size": self._work_queue.qsize(),
            "scale_up_threshold": self.scale_up_threshold,
            "scale_down_threshold": self.scale_down_threshold,
        }


# Example usage and demonstrations
def cpu_intensive_task(n: int) -> int:
    """Simulate CPU-intensive work."""
    result = 0
    for i in range(n * 1000):
        result += i ** 2
    return result


def demo_backpressure_pool():
    """Demonstrate backpressure thread pool."""
    print("=== Backpressure Thread Pool Demo ===")
    
    pool = BackpressureThreadPool(
        max_workers=2,
        max_queue_size=5,
        rejection_policy=RejectionPolicy.CALLER_RUNS
    )
    
    futures = []
    for i in range(10):
        future = pool.submit(cpu_intensive_task, 100 + i)
        if future:
            futures.append(future)
        print(f"Submitted task {i}")
    
    # Wait for completion
    for i, future in enumerate(futures):
        try:
            result = future.result(timeout=5.0)
            print(f"Task {i} result: {result}")
        except Exception as e:
            print(f"Task {i} failed: {e}")
    
    print("Pool stats:", pool.get_stats())
    pool.shutdown()


def demo_priority_pool():
    """Demonstrate priority thread pool."""
    print("\n=== Priority Thread Pool Demo ===")
    
    pool = PriorityThreadPool(max_workers=2)
    
    # Submit tasks with different priorities
    tasks = [
        (1, "Low priority task"),
        (5, "High priority task"),
        (3, "Medium priority task"),
        (5, "Another high priority task"),
        (1, "Another low priority task"),
    ]
    
    futures = []
    for priority, description in tasks:
        future = pool.submit(
            lambda desc=description: f"Executed: {desc}",
            priority=priority
        )
        futures.append((priority, description, future))
        print(f"Submitted: {description} (priority: {priority})")
    
    # Collect results
    for priority, description, future in futures:
        result = future.result()
        print(f"Completed: {result}")
    
    pool.shutdown()


def demo_scaling_pool():
    """Demonstrate auto-scaling thread pool."""
    print("\n=== Auto-Scaling Thread Pool Demo ===")
    
    pool = ScalingThreadPool(
        core_workers=1,
        max_workers=4,
        scale_up_threshold=0.5,
        scale_down_threshold=0.1
    )
    
    # Submit burst of tasks
    futures = []
    for i in range(8):
        future = pool.submit(cpu_intensive_task, 50)
        futures.append(future)
        print(f"Submitted task {i}")
        time.sleep(0.1)  # Small delay to see scaling
    
    # Wait for completion
    for i, future in enumerate(futures):
        result = future.result()
        print(f"Task {i} completed")
    
    print("Final stats:", pool.get_stats())
    pool.shutdown()


if __name__ == "__main__":
    demo_backpressure_pool()
    demo_priority_pool()
    demo_scaling_pool()
