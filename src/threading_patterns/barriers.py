"""
Barrier Synchronization Patterns

Implementation of various barrier synchronization primitives.
"""

import threading
import time
from typing import Optional, Callable


class CyclicBarrier:
    """
    Synchronization barrier that allows a set of threads to wait for each other.
    """
    
    def __init__(self, parties: int, action: Optional[Callable] = None):
        self.parties = parties
        self.action = action
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._count = 0
        self._generation = 0
        self._broken = False
    
    def wait(self, timeout: Optional[float] = None) -> int:
        """Wait for all parties to arrive at the barrier."""
        with self._condition:
            if self._broken:
                raise RuntimeError("Barrier is broken")
            
            generation = self._generation
            index = self._count
            self._count += 1
            
            try:
                if self._count == self.parties:
                    # Last thread to arrive
                    self._next_generation()
                    if self.action:
                        self.action()
                    return index
                
                # Wait for other threads
                while self._count < self.parties and generation == self._generation:
                    if not self._condition.wait(timeout):
                        self._broken = True
                        self._condition.notify_all()
                        raise TimeoutError("Barrier wait timed out")
                
                if self._broken:
                    raise RuntimeError("Barrier is broken")
                
                return index
            
            except:
                self._broken = True
                self._condition.notify_all()
                raise
    
    def _next_generation(self):
        """Move to the next generation."""
        self._count = 0
        self._generation += 1
        self._condition.notify_all()
    
    def reset(self):
        """Reset the barrier to its initial state."""
        with self._condition:
            if self._count > 0:
                self._broken = True
                self._condition.notify_all()
            self._next_generation()
            self._broken = False
    
    @property
    def is_broken(self) -> bool:
        """Check if the barrier is broken."""
        return self._broken
    
    @property
    def number_waiting(self) -> int:
        """Get the number of threads currently waiting."""
        return self._count


class CountDownLatch:
    """
    Synchronization primitive that allows threads to wait until a count reaches zero.
    """
    
    def __init__(self, count: int):
        if count < 0:
            raise ValueError("Count cannot be negative")
        
        self._count = count
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
    
    def count_down(self):
        """Decrement the count."""
        with self._condition:
            if self._count > 0:
                self._count -= 1
                if self._count == 0:
                    self._condition.notify_all()
    
    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait until the count reaches zero."""
        with self._condition:
            while self._count > 0:
                if not self._condition.wait(timeout):
                    return False
            return True
    
    @property
    def count(self) -> int:
        """Get the current count."""
        return self._count


class Phaser:
    """
    Reusable synchronization barrier similar to CyclicBarrier but more flexible.
    """
    
    def __init__(self, parties: int = 0):
        self._parties = parties
        self._arrived = 0
        self._phase = 0
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._terminated = False
    
    def register(self) -> int:
        """Register a new party."""
        with self._lock:
            if self._terminated:
                raise RuntimeError("Phaser is terminated")
            self._parties += 1
            return self._phase
    
    def arrive(self) -> int:
        """Arrive at the phaser without waiting."""
        with self._lock:
            if self._terminated:
                raise RuntimeError("Phaser is terminated")
            
            phase = self._phase
            self._arrived += 1
            
            if self._arrived >= self._parties:
                self._advance_phase()
            
            return phase
    
    def arrive_and_wait_advance(self, timeout: Optional[float] = None) -> int:
        """Arrive and wait for all parties to arrive."""
        with self._lock:
            if self._terminated:
                raise RuntimeError("Phaser is terminated")
            
            phase = self._phase
            self._arrived += 1
            
            if self._arrived >= self._parties:
                self._advance_phase()
                return phase
            
            # Wait for phase to advance
            while self._phase == phase and not self._terminated:
                if not self._condition.wait(timeout):
                    raise TimeoutError("Phaser wait timed out")
            
            return phase
    
    def _advance_phase(self):
        """Advance to the next phase."""
        self._phase += 1
        self._arrived = 0
        self._condition.notify_all()
    
    def force_termination(self):
        """Force termination of the phaser."""
        with self._lock:
            self._terminated = True
            self._condition.notify_all()
    
    @property
    def phase(self) -> int:
        """Get the current phase number."""
        return self._phase
    
    @property
    def registered_parties(self) -> int:
        """Get the number of registered parties."""
        return self._parties
    
    @property
    def arrived_parties(self) -> int:
        """Get the number of arrived parties in current phase."""
        return self._arrived
    
    @property
    def is_terminated(self) -> bool:
        """Check if the phaser is terminated."""
        return self._terminated
