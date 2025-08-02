"""
Threading Patterns Module

Comprehensive collection of thread-based concurrency patterns including:
- Producer-Consumer patterns
- Thread pools and executors
- Reader-Writer locks
- Barrier synchronization
- Thread-safe data structures
"""

from .producer_consumer import *
from .thread_pool import *
from .reader_writer import *
from .barriers import *
from .thread_safe_collections import *
from .worker_patterns import *

__all__ = [
    # Producer-Consumer
    "ProducerConsumer",
    "BoundedBuffer", 
    "PriorityProducerConsumer",
    
    # Thread Pools
    "BackpressureThreadPool",
    "PriorityThreadPool",
    "ScalingThreadPool",
    
    # Reader-Writer
    "ReadWriteLock",
    "FairReadWriteLock",
    "UpgradableReadWriteLock",
    
    # Barriers
    "CyclicBarrier",
    "CountDownLatch",
    "Phaser",
    
    # Collections
    "ThreadSafeQueue",
    "ThreadSafeDict",
    "ThreadSafeSet",
    "LockFreeQueue",
    
    # Workers
    "WorkerThread",
    "MasterWorkerPattern",
    "PipelineWorker",
]
