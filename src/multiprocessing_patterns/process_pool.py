"""
Advanced Process Pool Implementations

High-performance process pools for CPU-intensive tasks with:
- Dynamic scaling
- Load balancing
- Fault tolerance
- Memory management
"""

import multiprocessing as mp
import time
import queue
import os
import psutil
from typing import Any, Callable, List, Optional, Dict, Union
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
import logging
import pickle


class ProcessState(Enum):
    """Process worker states."""
    IDLE = "idle"
    BUSY = "busy"
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class Task:
    """Task to be executed by worker process."""
    task_id: str
    func: Callable
    args: tuple
    kwargs: dict
    priority: int = 0
    timeout: Optional[float] = None
    retries: int = 0
    max_retries: int = 3


@dataclass
class TaskResult:
    """Result of task execution."""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0
    worker_id: int = 0


class WorkerProcess(mp.Process):
    """Enhanced worker process with monitoring and fault tolerance."""
    
    def __init__(self, worker_id: int, task_queue: mp.Queue, 
                 result_queue: mp.Queue, shutdown_event: mp.Event):
        super().__init__(name=f"Worker-{worker_id}")
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.shutdown_event = shutdown_event
        self.state = ProcessState.IDLE
        self.tasks_completed = 0
        self.start_time = time.time()
        self.last_task_time = time.time()
    
    def run(self):
        """Main worker loop."""
        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get task with timeout
                    task = self.task_queue.get(timeout=1.0)
                    if task is None:  # Shutdown signal
                        break
                    
                    self.state = ProcessState.BUSY
                    self.last_task_time = time.time()
                    
                    # Execute task
                    result = self._execute_task(task)
                    
                    # Send result
                    self.result_queue.put(result)
                    self.tasks_completed += 1
                    self.state = ProcessState.IDLE
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f"Worker {self.worker_id} error: {e}")
                    self.state = ProcessState.FAILED
                    break
        
        except KeyboardInterrupt:
            pass
        finally:
            self.state = ProcessState.TERMINATED
            logging.info(f"Worker {self.worker_id} terminated. Completed {self.tasks_completed} tasks")
    
    def _execute_task(self, task: Task) -> TaskResult:
        """Execute a single task."""
        start_time = time.time()
        
        try:
            # Execute with timeout if specified
            if task.timeout:
                # Note: In a full implementation, you'd use signal.alarm or similar
                # for timeout handling in processes
                pass
            
            result = task.func(*task.args, **task.kwargs)
            execution_time = time.time() - start_time
            
            return TaskResult(
                task_id=task.task_id,
                success=True,
                result=result,
                execution_time=execution_time,
                worker_id=self.worker_id
            )
        
        except Exception as e:
            execution_time = time.time() - start_time
            return TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(e),
                execution_time=execution_time,
                worker_id=self.worker_id
            )


class ProcessPool:
    """
    Advanced process pool with dynamic scaling and monitoring.
    """
    
    def __init__(self, min_workers: int = 2, max_workers: Optional[int] = None,
                 task_queue_size: int = 100, enable_monitoring: bool = True):
        self.min_workers = min_workers
        self.max_workers = max_workers or mp.cpu_count()
        self.task_queue_size = task_queue_size
        self.enable_monitoring = enable_monitoring
        
        # Multiprocessing primitives
        self.task_queue = mp.Queue(maxsize=task_queue_size)
        self.result_queue = mp.Queue()
        self.shutdown_event = mp.Event()
        
        # Worker management
        self.workers: Dict[int, WorkerProcess] = {}
        self.worker_id_counter = 0
        self.running = False
        
        # Statistics
        self.tasks_submitted = 0
        self.tasks_completed = 0
        self.total_execution_time = 0.0
        
        # Monitoring
        if self.enable_monitoring:
            self.monitor_process = mp.Process(
                target=self._monitor_workers,
                name="PoolMonitor"
            )
    
    def start(self):
        """Start the process pool."""
        if self.running:
            return
        
        self.running = True
        
        # Start minimum workers
        for _ in range(self.min_workers):
            self._add_worker()
        
        # Start monitoring
        if self.enable_monitoring:
            self.monitor_process.start()
        
        logging.info(f"Process pool started with {len(self.workers)} workers")
    
    def _add_worker(self) -> WorkerProcess:
        """Add a new worker process."""
        worker_id = self.worker_id_counter
        self.worker_id_counter += 1
        
        worker = WorkerProcess(
            worker_id, self.task_queue, 
            self.result_queue, self.shutdown_event
        )
        worker.start()
        self.workers[worker_id] = worker
        
        logging.info(f"Added worker {worker_id}")
        return worker
    
    def _remove_worker(self, worker_id: int):
        """Remove a worker process."""
        worker = self.workers.get(worker_id)
        if worker:
            worker.terminate()
            worker.join(timeout=5.0)
            del self.workers[worker_id]
            logging.info(f"Removed worker {worker_id}")
    
    def submit(self, func: Callable, *args, priority: int = 0, 
               timeout: Optional[float] = None, **kwargs) -> str:
        """Submit a task for execution."""
        if not self.running:
            raise RuntimeError("Process pool is not running")
        
        task_id = f"task-{self.tasks_submitted}"
        task = Task(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            timeout=timeout
        )
        
        try:
            self.task_queue.put(task, timeout=1.0)
            self.tasks_submitted += 1
            return task_id
        except queue.Full:
            raise RuntimeError("Task queue is full")
    
    def get_result(self, timeout: Optional[float] = None) -> Optional[TaskResult]:
        """Get a completed task result."""
        try:
            result = self.result_queue.get(timeout=timeout)
            if result.success:
                self.tasks_completed += 1
                self.total_execution_time += result.execution_time
            return result
        except queue.Empty:
            return None
    
    def map(self, func: Callable, iterable, chunk_size: int = 1) -> List[Any]:
        """Map function over iterable using process pool."""
        # Submit all tasks
        task_ids = []
        for item in iterable:
            task_id = self.submit(func, item)
            task_ids.append(task_id)
        
        # Collect results
        results = {}
        completed = 0
        
        while completed < len(task_ids):
            result = self.get_result(timeout=10.0)
            if result:
                results[result.task_id] = result
                completed += 1
        
        # Return results in order
        ordered_results = []
        for task_id in task_ids:
            result = results[task_id]
            if result.success:
                ordered_results.append(result.result)
            else:
                raise Exception(f"Task {task_id} failed: {result.error}")
        
        return ordered_results
    
    def _monitor_workers(self):
        """Monitor worker processes and handle scaling."""
        while not self.shutdown_event.is_set():
            try:
                # Check worker health
                dead_workers = []
                for worker_id, worker in self.workers.items():
                    if not worker.is_alive():
                        dead_workers.append(worker_id)
                
                # Replace dead workers
                for worker_id in dead_workers:
                    logging.warning(f"Worker {worker_id} died, replacing")
                    self._remove_worker(worker_id)
                    if len(self.workers) < self.min_workers:
                        self._add_worker()
                
                # Auto-scaling based on queue size
                queue_size = self.task_queue.qsize()
                worker_count = len(self.workers)
                
                if queue_size > worker_count * 2 and worker_count < self.max_workers:
                    # Scale up
                    self._add_worker()
                    logging.info(f"Scaled up to {len(self.workers)} workers")
                
                elif queue_size == 0 and worker_count > self.min_workers:
                    # Scale down (remove one worker)
                    oldest_worker_id = min(self.workers.keys())
                    self._remove_worker(oldest_worker_id)
                    logging.info(f"Scaled down to {len(self.workers)} workers")
                
                time.sleep(5.0)  # Monitor every 5 seconds
                
            except Exception as e:
                logging.error(f"Monitor error: {e}")
    
    def shutdown(self, timeout: float = 10.0):
        """Shutdown the process pool gracefully."""
        if not self.running:
            return
        
        self.running = False
        self.shutdown_event.set()
        
        # Send shutdown signals to workers
        for _ in self.workers:
            try:
                self.task_queue.put(None, timeout=1.0)
            except queue.Full:
                pass
        
        # Wait for workers to finish
        for worker in self.workers.values():
            worker.join(timeout=timeout / len(self.workers))
            if worker.is_alive():
                worker.terminate()
        
        # Stop monitoring
        if self.enable_monitoring and self.monitor_process.is_alive():
            self.monitor_process.terminate()
            self.monitor_process.join(timeout=2.0)
        
        self.workers.clear()
        logging.info("Process pool shutdown complete")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        alive_workers = sum(1 for w in self.workers.values() if w.is_alive())
        
        return {
            "total_workers": len(self.workers),
            "alive_workers": alive_workers,
            "min_workers": self.min_workers,
            "max_workers": self.max_workers,
            "tasks_submitted": self.tasks_submitted,
            "tasks_completed": self.tasks_completed,
            "queue_size": self.task_queue.qsize(),
            "avg_execution_time": (
                self.total_execution_time / max(self.tasks_completed, 1)
            ),
            "is_running": self.running,
        }


class DynamicProcessPool:
    """
    Process pool that dynamically adjusts based on system load.
    """
    
    def __init__(self, target_cpu_usage: float = 0.8):
        self.target_cpu_usage = target_cpu_usage
        self.pool = ProcessPool(min_workers=1, max_workers=mp.cpu_count() * 2)
        self.cpu_monitor_interval = 2.0
        self.last_adjustment = time.time()
        self.adjustment_cooldown = 10.0  # seconds
    
    def start(self):
        """Start the dynamic pool."""
        self.pool.start()
        
        # Start CPU monitoring
        self.cpu_monitor = mp.Process(
            target=self._monitor_cpu_usage,
            name="CPUMonitor"
        )
        self.cpu_monitor.start()
    
    def _monitor_cpu_usage(self):
        """Monitor CPU usage and adjust worker count."""
        while self.pool.running:
            try:
                cpu_percent = psutil.cpu_percent(interval=self.cpu_monitor_interval)
                current_time = time.time()
                
                # Only adjust if cooldown period has passed
                if current_time - self.last_adjustment < self.adjustment_cooldown:
                    continue
                
                worker_count = len(self.pool.workers)
                
                if cpu_percent < self.target_cpu_usage * 50 and worker_count > 1:
                    # CPU usage is low, reduce workers
                    oldest_worker_id = min(self.pool.workers.keys())
                    self.pool._remove_worker(oldest_worker_id)
                    self.last_adjustment = current_time
                    logging.info(f"Reduced workers due to low CPU: {cpu_percent}%")
                
                elif cpu_percent > self.target_cpu_usage * 100 and worker_count < self.pool.max_workers:
                    # CPU usage is high, add workers
                    self.pool._add_worker()
                    self.last_adjustment = current_time
                    logging.info(f"Added worker due to high CPU: {cpu_percent}%")
                
            except Exception as e:
                logging.error(f"CPU monitor error: {e}")
    
    def submit(self, func: Callable, *args, **kwargs) -> str:
        """Submit task to dynamic pool."""
        return self.pool.submit(func, *args, **kwargs)
    
    def get_result(self, timeout: Optional[float] = None) -> Optional[TaskResult]:
        """Get result from dynamic pool."""
        return self.pool.get_result(timeout)
    
    def map(self, func: Callable, iterable) -> List[Any]:
        """Map function over iterable."""
        return self.pool.map(func, iterable)
    
    def shutdown(self, timeout: float = 10.0):
        """Shutdown the dynamic pool."""
        if hasattr(self, 'cpu_monitor') and self.cpu_monitor.is_alive():
            self.cpu_monitor.terminate()
            self.cpu_monitor.join(timeout=2.0)
        
        self.pool.shutdown(timeout)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get dynamic pool statistics."""
        stats = self.pool.get_stats()
        stats.update({
            "target_cpu_usage": self.target_cpu_usage,
            "current_cpu_usage": psutil.cpu_percent(),
            "memory_usage": psutil.virtual_memory().percent,
        })
        return stats


# Example CPU-intensive functions for testing
def cpu_intensive_task(n: int) -> int:
    """Simulate CPU-intensive work."""
    result = 0
    for i in range(n * 100000):
        result += i ** 2
    return result


def fibonacci(n: int) -> int:
    """Calculate Fibonacci number (inefficient recursive version)."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def prime_check(n: int) -> bool:
    """Check if a number is prime."""
    if n < 2:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True


# Example usage and demonstrations
def demo_process_pool():
    """Demonstrate basic process pool functionality."""
    print("=== Process Pool Demo ===")
    
    pool = ProcessPool(min_workers=2, max_workers=4)
    pool.start()
    
    try:
        # Submit CPU-intensive tasks
        task_ids = []
        for i in range(8):
            task_id = pool.submit(cpu_intensive_task, 1000 + i * 100)
            task_ids.append(task_id)
            print(f"Submitted task {task_id}")
        
        # Collect results
        results = {}
        for _ in range(len(task_ids)):
            result = pool.get_result(timeout=10.0)
            if result:
                results[result.task_id] = result
                print(f"Task {result.task_id} completed in {result.execution_time:.2f}s")
        
        # Print statistics
        stats = pool.get_stats()
        print(f"Pool stats: {stats}")
        
    finally:
        pool.shutdown()


def demo_dynamic_pool():
    """Demonstrate dynamic process pool."""
    print("\n=== Dynamic Process Pool Demo ===")
    
    pool = DynamicProcessPool(target_cpu_usage=0.7)
    pool.start()
    
    try:
        # Test map functionality
        numbers = list(range(20, 30))
        print(f"Calculating Fibonacci for: {numbers}")
        
        start_time = time.time()
        results = pool.map(fibonacci, numbers)
        end_time = time.time()
        
        print(f"Results: {results}")
        print(f"Total time: {end_time - start_time:.2f}s")
        
        # Print final statistics
        stats = pool.get_stats()
        print(f"Dynamic pool stats: {stats}")
        
    finally:
        pool.shutdown()


def demo_parallel_prime_check():
    """Demonstrate parallel prime checking."""
    print("\n=== Parallel Prime Check Demo ===")
    
    pool = ProcessPool(min_workers=mp.cpu_count())
    pool.start()
    
    try:
        # Check primes in range
        numbers = list(range(10000, 10100))
        
        # Submit all tasks
        task_ids = []
        for num in numbers:
            task_id = pool.submit(prime_check, num)
            task_ids.append((task_id, num))
        
        # Collect results
        primes = []
        for task_id, num in task_ids:
            result = pool.get_result(timeout=5.0)
            if result and result.success and result.result:
                primes.append(num)
        
        print(f"Found {len(primes)} primes: {primes[:10]}...")
        
    finally:
        pool.shutdown()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    demo_process_pool()
    demo_dynamic_pool()
    demo_parallel_prime_check()
