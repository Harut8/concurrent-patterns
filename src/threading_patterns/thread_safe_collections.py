"""
Thread-Safe Collections

Implementation of thread-safe data structures for concurrent access.
"""

import threading
import queue
from typing import Any, Dict, Set, List, Optional, Iterator
from collections import defaultdict


class ThreadSafeDict:
    """Thread-safe dictionary implementation."""
    
    def __init__(self, initial_data: Optional[Dict] = None):
        self._data = initial_data or {}
        self._lock = threading.RLock()
    
    def __getitem__(self, key):
        with self._lock:
            return self._data[key]
    
    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value
    
    def __delitem__(self, key):
        with self._lock:
            del self._data[key]
    
    def __contains__(self, key):
        with self._lock:
            return key in self._data
    
    def __len__(self):
        with self._lock:
            return len(self._data)
    
    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)
    
    def pop(self, key, default=None):
        with self._lock:
            return self._data.pop(key, default)
    
    def keys(self):
        with self._lock:
            return list(self._data.keys())
    
    def values(self):
        with self._lock:
            return list(self._data.values())
    
    def items(self):
        with self._lock:
            return list(self._data.items())
    
    def update(self, other):
        with self._lock:
            self._data.update(other)
    
    def clear(self):
        with self._lock:
            self._data.clear()


class ThreadSafeSet:
    """Thread-safe set implementation."""
    
    def __init__(self, initial_data: Optional[Set] = None):
        self._data = initial_data or set()
        self._lock = threading.RLock()
    
    def add(self, item):
        with self._lock:
            self._data.add(item)
    
    def remove(self, item):
        with self._lock:
            self._data.remove(item)
    
    def discard(self, item):
        with self._lock:
            self._data.discard(item)
    
    def __contains__(self, item):
        with self._lock:
            return item in self._data
    
    def __len__(self):
        with self._lock:
            return len(self._data)
    
    def copy(self):
        with self._lock:
            return self._data.copy()
    
    def union(self, other):
        with self._lock:
            return self._data.union(other)
    
    def intersection(self, other):
        with self._lock:
            return self._data.intersection(other)


class ThreadSafeQueue:
    """Enhanced thread-safe queue with additional features."""
    
    def __init__(self, maxsize: int = 0):
        self._queue = queue.Queue(maxsize=maxsize)
        self._lock = threading.RLock()
        self._stats = {
            "items_put": 0,
            "items_got": 0,
            "put_waits": 0,
            "get_waits": 0
        }
    
    def put(self, item, block=True, timeout=None):
        try:
            self._queue.put(item, block=block, timeout=timeout)
            with self._lock:
                self._stats["items_put"] += 1
        except queue.Full:
            with self._lock:
                self._stats["put_waits"] += 1
            raise
    
    def get(self, block=True, timeout=None):
        try:
            item = self._queue.get(block=block, timeout=timeout)
            with self._lock:
                self._stats["items_got"] += 1
            return item
        except queue.Empty:
            with self._lock:
                self._stats["get_waits"] += 1
            raise
    
    def qsize(self):
        return self._queue.qsize()
    
    def empty(self):
        return self._queue.empty()
    
    def full(self):
        return self._queue.full()
    
    def get_stats(self):
        with self._lock:
            return self._stats.copy()


class LockFreeQueue:
    """Simple lock-free queue using Python's queue.Queue as base."""
    
    def __init__(self):
        # Note: This is a simplified version
        # True lock-free implementation would require atomic operations
        self._queue = queue.Queue()
    
    def put(self, item):
        self._queue.put_nowait(item)
    
    def get(self):
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None
    
    def size(self):
        return self._queue.qsize()
    
    def empty(self):
        return self._queue.empty()
