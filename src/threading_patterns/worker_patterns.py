"""
Worker Thread Patterns

Various worker thread implementations and patterns.
"""

import threading
import queue
import time
from typing import Callable, Any, Optional, List
from abc import ABC, abstractmethod


class WorkerThread(threading.Thread):
    """Generic worker thread that processes tasks from a queue."""
    
    def __init__(self, task_queue: queue.Queue, name: str = None):
        super().__init__(name=name, daemon=True)
        self.task_queue = task_queue
        self.running = False
        self.tasks_processed = 0
    
    def run(self):
        self.running = True
        while self.running:
            try:
                task = self.task_queue.get(timeout=1.0)
                if task is None:  # Shutdown signal
                    break
                
                self.process_task(task)
                self.tasks_processed += 1
                self.task_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Worker {self.name} error: {e}")
    
    def process_task(self, task):
        """Override this method to define task processing."""
        if callable(task):
            task()
        else:
            print(f"Processing task: {task}")
    
    def stop(self):
        self.running = False


class MasterWorkerPattern:
    """Master-worker pattern implementation."""
    
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.task_queue = queue.Queue()
        self.workers: List[WorkerThread] = []
        self.running = False
    
    def start(self):
        """Start all worker threads."""
        if self.running:
            return
        
        self.running = True
        for i in range(self.num_workers):
            worker = WorkerThread(self.task_queue, name=f"Worker-{i}")
            worker.start()
            self.workers.append(worker)
    
    def submit_task(self, task):
        """Submit a task for processing."""
        if not self.running:
            raise RuntimeError("Worker pool not started")
        self.task_queue.put(task)
    
    def stop(self, timeout: float = 5.0):
        """Stop all workers."""
        if not self.running:
            return
        
        self.running = False
        
        # Send stop signals
        for _ in self.workers:
            self.task_queue.put(None)
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=timeout)
        
        self.workers.clear()
    
    def get_stats(self):
        """Get worker statistics."""
        return {
            "num_workers": len(self.workers),
            "queue_size": self.task_queue.qsize(),
            "total_tasks_processed": sum(w.tasks_processed for w in self.workers),
            "running": self.running
        }


class PipelineWorker(WorkerThread):
    """Worker that processes tasks in a pipeline fashion."""
    
    def __init__(self, input_queue: queue.Queue, output_queue: queue.Queue,
                 processor: Callable[[Any], Any], name: str = None):
        super().__init__(input_queue, name)
        self.output_queue = output_queue
        self.processor = processor
    
    def process_task(self, task):
        """Process task and forward result to output queue."""
        try:
            result = self.processor(task)
            if result is not None:
                self.output_queue.put(result)
        except Exception as e:
            print(f"Pipeline worker {self.name} error: {e}")
