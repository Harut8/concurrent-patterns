"""
Reader-Writer Lock Implementations

Various reader-writer lock patterns for shared resource access:
- Basic reader-writer locks
- Fair reader-writer locks
- Upgradable locks
- Write-preferring locks
"""

import threading
import time
from typing import Optional, ContextManager
from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
import logging


class LockType(Enum):
    """Types of locks that can be acquired."""
    READ = "read"
    WRITE = "write"
    UPGRADE = "upgrade"


class ReadWriteLock(ABC):
    """Abstract base class for reader-writer locks."""
    
    @abstractmethod
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock."""
        pass
    
    @abstractmethod
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire a write lock."""
        pass
    
    @abstractmethod
    def release_read(self):
        """Release a read lock."""
        pass
    
    @abstractmethod
    def release_write(self):
        """Release a write lock."""
        pass
    
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


class BasicReadWriteLock(ReadWriteLock):
    """
    Basic reader-writer lock implementation.
    Allows multiple readers or one writer.
    Writers may starve if readers keep coming.
    """
    
    def __init__(self):
        self._read_count = 0
        self._write_count = 0
        self._read_ready = threading.Condition(threading.RLock())
        self._write_ready = threading.Condition(threading.RLock())
        self._readers = set()  # Track reader threads
        self._writer = None    # Track writer thread
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            # Wait until no writers
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
            # Wait until no readers or writers
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
            
            # Notify waiting writers if no more readers
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
            
            # Notify all waiting threads
            self._write_ready.notify_all()
            with self._read_ready:
                self._read_ready.notify_all()
    
    def get_stats(self) -> dict:
        """Get lock statistics."""
        return {
            "read_count": self._read_count,
            "write_count": self._write_count,
            "active_readers": len(self._readers),
            "active_writer": self._writer.name if self._writer else None,
        }


class FairReadWriteLock(ReadWriteLock):
    """
    Fair reader-writer lock that prevents writer starvation.
    Uses a ticket system to ensure fairness.
    """
    
    def __init__(self):
        self._read_count = 0
        self._write_count = 0
        self._waiting_writers = 0
        self._lock = threading.RLock()
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)
        self._readers = set()
        self._writer = None
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock fairly."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            # Wait until no writers and no waiting writers
            if not self._read_ready.wait_for(
                lambda: self._write_count == 0 and self._waiting_writers == 0,
                timeout=timeout
            ):
                return False
            
            self._read_count += 1
            self._readers.add(current_thread)
            return True
    
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire a write lock fairly."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            self._waiting_writers += 1
            try:
                # Wait until no readers or writers
                if not self._write_ready.wait_for(
                    lambda: self._read_count == 0 and self._write_count == 0,
                    timeout=timeout
                ):
                    return False
                
                self._write_count = 1
                self._writer = current_thread
                return True
            finally:
                self._waiting_writers -= 1
    
    def release_read(self):
        """Release a read lock."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            if current_thread not in self._readers:
                raise RuntimeError("Thread does not hold a read lock")
            
            self._read_count -= 1
            self._readers.remove(current_thread)
            
            # Notify waiting writers if no more readers
            if self._read_count == 0:
                self._write_ready.notify()
    
    def release_write(self):
        """Release a write lock."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            if self._writer != current_thread:
                raise RuntimeError("Thread does not hold the write lock")
            
            self._write_count = 0
            self._writer = None
            
            # Notify waiting writers first (fairness), then readers
            self._write_ready.notify()
            self._read_ready.notify_all()


class UpgradableReadWriteLock(ReadWriteLock):
    """
    Reader-writer lock that supports upgrading read locks to write locks.
    Prevents deadlocks during lock upgrades.
    """
    
    def __init__(self):
        self._read_count = 0
        self._write_count = 0
        self._upgrade_count = 0
        self._lock = threading.RLock()
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)
        self._upgrade_ready = threading.Condition(self._lock)
        
        self._readers = set()
        self._writer = None
        self._upgrader = None  # Thread that has upgrade lock
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            # Wait until no writers
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
            # Wait until no readers, writers, or upgraders
            if not self._write_ready.wait_for(
                lambda: (self._read_count == 0 and 
                        self._write_count == 0 and 
                        self._upgrade_count == 0),
                timeout=timeout
            ):
                return False
            
            self._write_count = 1
            self._writer = current_thread
            return True
    
    def acquire_upgrade(self, timeout: Optional[float] = None) -> bool:
        """Acquire an upgradable read lock."""
        current_thread = threading.current_thread()
        
        with self._upgrade_ready:
            # Wait until no writers or upgraders
            if not self._upgrade_ready.wait_for(
                lambda: self._write_count == 0 and self._upgrade_count == 0,
                timeout=timeout
            ):
                return False
            
            self._upgrade_count = 1
            self._upgrader = current_thread
            self._read_count += 1
            self._readers.add(current_thread)
            return True
    
    def upgrade_to_write(self, timeout: Optional[float] = None) -> bool:
        """Upgrade an upgradable lock to a write lock."""
        current_thread = threading.current_thread()
        
        if self._upgrader != current_thread:
            raise RuntimeError("Thread does not hold an upgradable lock")
        
        with self._write_ready:
            # Wait until only this thread has a read lock
            if not self._write_ready.wait_for(
                lambda: self._read_count == 1 and current_thread in self._readers,
                timeout=timeout
            ):
                return False
            
            # Convert upgrade lock to write lock
            self._upgrade_count = 0
            self._upgrader = None
            self._read_count = 0
            self._readers.remove(current_thread)
            
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
            
            # Notify waiting writers/upgraders
            if self._read_count == 0:
                self._write_ready.notify()
            elif self._read_count == 1 and self._upgrader:
                self._write_ready.notify()  # Notify upgrader
    
    def release_write(self):
        """Release a write lock."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            if self._writer != current_thread:
                raise RuntimeError("Thread does not hold the write lock")
            
            self._write_count = 0
            self._writer = None
            
            # Notify all waiting threads
            self._write_ready.notify_all()
            self._read_ready.notify_all()
            self._upgrade_ready.notify_all()
    
    def release_upgrade(self):
        """Release an upgradable lock."""
        current_thread = threading.current_thread()
        
        with self._upgrade_ready:
            if self._upgrader != current_thread:
                raise RuntimeError("Thread does not hold an upgradable lock")
            
            self._upgrade_count = 0
            self._upgrader = None
            self._read_count -= 1
            self._readers.remove(current_thread)
            
            # Notify waiting threads
            self._upgrade_ready.notify_all()
            if self._read_count == 0:
                self._write_ready.notify()
    
    @contextmanager
    def upgrade_lock(self, timeout: Optional[float] = None):
        """Context manager for upgradable lock."""
        if self.acquire_upgrade(timeout):
            try:
                yield self
            finally:
                self.release_upgrade()
        else:
            raise TimeoutError("Failed to acquire upgradable lock")


class WritePreferringReadWriteLock(ReadWriteLock):
    """
    Reader-writer lock that gives preference to writers.
    Prevents reader starvation of writers.
    """
    
    def __init__(self):
        self._read_count = 0
        self._write_count = 0
        self._waiting_writers = 0
        self._lock = threading.RLock()
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)
        self._no_writers = threading.Condition(self._lock)
        
        self._readers = set()
        self._writer = None
    
    def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """Acquire a read lock (writers have preference)."""
        current_thread = threading.current_thread()
        
        with self._no_writers:
            # Wait until no writers or waiting writers
            if not self._no_writers.wait_for(
                lambda: self._write_count == 0 and self._waiting_writers == 0,
                timeout=timeout
            ):
                return False
        
        with self._read_ready:
            self._read_count += 1
            self._readers.add(current_thread)
            return True
    
    def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """Acquire a write lock (has preference over readers)."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            self._waiting_writers += 1
            try:
                # Wait until no readers or writers
                if not self._write_ready.wait_for(
                    lambda: self._read_count == 0 and self._write_count == 0,
                    timeout=timeout
                ):
                    return False
                
                self._write_count = 1
                self._writer = current_thread
                return True
            finally:
                self._waiting_writers -= 1
                if self._waiting_writers == 0:
                    with self._no_writers:
                        self._no_writers.notify_all()
    
    def release_read(self):
        """Release a read lock."""
        current_thread = threading.current_thread()
        
        with self._read_ready:
            if current_thread not in self._readers:
                raise RuntimeError("Thread does not hold a read lock")
            
            self._read_count -= 1
            self._readers.remove(current_thread)
            
            # Notify waiting writers if no more readers
            if self._read_count == 0:
                with self._write_ready:
                    self._write_ready.notify()
    
    def release_write(self):
        """Release a write lock."""
        current_thread = threading.current_thread()
        
        with self._write_ready:
            if self._writer != current_thread:
                raise RuntimeError("Thread does not hold the write lock")
            
            self._write_count = 0
            self._writer = None
            
            # Notify waiting writers first, then readers
            self._write_ready.notify()


# Example usage and demonstrations
def demo_basic_reader_writer():
    """Demonstrate basic reader-writer lock."""
    print("=== Basic Reader-Writer Lock Demo ===")
    
    lock = BasicReadWriteLock()
    shared_data = {"value": 0}
    
    def reader(reader_id: int):
        for i in range(3):
            with lock.read_lock():
                value = shared_data["value"]
                print(f"Reader {reader_id}: Read value {value}")
                time.sleep(0.1)
    
    def writer(writer_id: int):
        for i in range(2):
            with lock.write_lock():
                shared_data["value"] += 1
                print(f"Writer {writer_id}: Wrote value {shared_data['value']}")
                time.sleep(0.2)
    
    # Start multiple readers and writers
    threads = []
    
    # Start readers
    for i in range(3):
        t = threading.Thread(target=reader, args=(i,))
        threads.append(t)
        t.start()
    
    # Start writers
    for i in range(2):
        t = threading.Thread(target=writer, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    print(f"Final stats: {lock.get_stats()}")
    print(f"Final value: {shared_data['value']}")


def demo_upgradable_lock():
    """Demonstrate upgradable reader-writer lock."""
    print("\n=== Upgradable Reader-Writer Lock Demo ===")
    
    lock = UpgradableReadWriteLock()
    shared_data = {"items": []}
    
    def processor():
        with lock.upgrade_lock():
            # Read current state
            current_items = len(shared_data["items"])
            print(f"Processor: Current items count: {current_items}")
            
            # Decide if update is needed
            if current_items < 5:
                print("Processor: Upgrading to write lock...")
                if lock.upgrade_to_write():
                    # Perform write operation
                    shared_data["items"].append(f"item-{current_items}")
                    print(f"Processor: Added item, new count: {len(shared_data['items'])}")
                else:
                    print("Processor: Failed to upgrade lock")
    
    def reader(reader_id: int):
        with lock.read_lock():
            count = len(shared_data["items"])
            print(f"Reader {reader_id}: Items count: {count}")
            time.sleep(0.1)
    
    # Run processor and readers
    threads = []
    
    # Start processor
    for i in range(3):
        t = threading.Thread(target=processor)
        threads.append(t)
        t.start()
        time.sleep(0.05)
    
    # Start readers
    for i in range(2):
        t = threading.Thread(target=reader, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    print(f"Final items: {shared_data['items']}")


def demo_fair_lock():
    """Demonstrate fair reader-writer lock."""
    print("\n=== Fair Reader-Writer Lock Demo ===")
    
    lock = FairReadWriteLock()
    shared_data = {"counter": 0}
    
    def continuous_reader(reader_id: int):
        for i in range(5):
            with lock.read_lock():
                value = shared_data["counter"]
                print(f"Reader {reader_id}: Read {value}")
                time.sleep(0.05)
    
    def writer(writer_id: int):
        time.sleep(0.1)  # Let readers start first
        with lock.write_lock():
            shared_data["counter"] += 10
            print(f"Writer {writer_id}: Incremented to {shared_data['counter']}")
            time.sleep(0.1)
    
    # Start continuous readers and a writer
    threads = []
    
    # Start readers
    for i in range(3):
        t = threading.Thread(target=continuous_reader, args=(i,))
        threads.append(t)
        t.start()
    
    # Start writer (should not be starved)
    t = threading.Thread(target=writer, args=(0,))
    threads.append(t)
    t.start()
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    print(f"Final counter: {shared_data['counter']}")


if __name__ == "__main__":
    demo_basic_reader_writer()
    demo_upgradable_lock()
    demo_fair_lock()
