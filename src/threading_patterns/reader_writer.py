"""
Reader-Writer Lock Implementations for Threading Patterns

Basic reader-writer lock implementations using threading primitives.
"""

import threading
import time
from typing import Optional
from contextlib import contextmanager


class ReadWriteLock:
    """
    Basic reader-writer lock implementation.
    Allows multiple readers or one writer.
    """
    
    def __init__(self):
        self._read_count = 0
        self._write_count = 0
        self._read_ready = threading.Condition(threading.RLock())
        self._write_ready = threading.Condition(threading.RLock())
        self._readers = set()
        self._writer = None
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            if not self._read_ready.wait_for(
                lambda: self._write_count == 0,
                timeout=timeout
            ):
                return False
            
            self._read_count += 1
            self._readers.add(current_thread)
            return True
    
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire a write lock."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            if not self._write_ready.wait_for(
                lambda: self._read_count == 0 and self._write_count == 0,
                timeout=timeout
            ):
                return False
            
            self._write_count = 1
            self._writer = current_thread
            return True
    
    def release_read(self):
        """Release a read lock."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            if current_thread not in self._readers:
                raise RuntimeError("Thread does not hold a read lock")
            
            self._read_count -= 1
            self._readers.remove(current_thread)
            
            if self._read_count == 0:
                with self._write_ready:
                    self._write_ready.notify_all()
    
    def release_write(self):
        """Release a write lock."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            if self._writer != current_thread:
                raise RuntimeError("Thread does not hold the write lock")
            
            self._write_count = 0
            self._writer = None
            
            self._write_ready.notify_all()
            with self._read_ready:
                self._read_ready.notify_all()
    
    @contextmanager
    def read_lock(self, timeout: Optional[float] = None):
        """Context manager for read lock."""
        if self.acquire_read(timeout):
            try:
                yield
            finally:
                self.release_read()
        else:
            raise TimeoutError("Failed to acquire read lock")
    
    @contextmanager
    def write_lock(self, timeout: Optional[float] = None):
        """Context manager for write lock."""
        if self.acquire_write(timeout):
            try:
                yield
            finally:
                self.release_write()
        else:
            raise TimeoutError("Failed to acquire write lock")


# Alias for compatibility
FairReadWriteLock = ReadWriteLock
UpgradableReadWriteLock = ReadWriteLock
