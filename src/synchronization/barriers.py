"""
Barrier Synchronization Patterns

Implementation of various barrier and latch synchronization primitives.
"""

import threading
import time
import asyncio
from typing import Optional, List, Dict, Any, Callable, Set
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import logging
import weakref


class BarrierState(Enum):
    """Barrier states."""
    WAITING = "waiting"
    BROKEN = "broken"
    RESET = "reset"


@dataclass
class BarrierStats:
    """Barrier statistics."""
    parties: int
    waiting_count: int = 0
    generation: int = 0
    total_waits: int = 0
    total_timeouts: int = 0
    total_breaks: int = 0
    average_wait_time: float = 0.0


class BrokenBarrierError(Exception):
    """Exception raised when barrier is broken."""
    pass


class CyclicBarrier:
    """
    Cyclic barrier that allows a set of threads to wait for each other.
    """
    
    def __init__(self, parties: int, barrier_action: Optional[Callable[[], None]] = None):
        if parties <= 0:
            raise ValueError("Parties must be positive")
        
        self.parties = parties
        self.barrier_action = barrier_action
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._generation = 0
        self._count = 0
        self._broken = False
        self._stats = BarrierStats(parties)
    
    def wait(self, timeout: Optional[float] = None) -> int:
        """
        Wait for all parties to reach the barrier.
        Returns the arrival index (0 for last thread).
        """
        start_time = time.time()
        
        with self._condition:
            if self._broken:
                raise BrokenBarrierError("Barrier is broken")
            
            generation = self._generation
            index = self._count
            self._count += 1
            self._stats.waiting_count += 1
            self._stats.total_waits += 1
            
            try:
                if self._count == self.parties:
                    # Last thread to arrive
                    self._next_generation()
                    return 0
                else:
                    # Wait for other threads
                    while self._generation == generation and not self._broken:
                        if not self._condition.wait(timeout=timeout):
                            # Timeout
                            self._stats.total_timeouts += 1
                            self._break_barrier()
                            raise TimeoutError("Barrier wait timed out")
                    
                    if self._broken:
                        raise BrokenBarrierError("Barrier was broken while waiting")
                    
                    return self.parties - index - 1
            
            finally:
                self._stats.waiting_count -= 1
                
                # Update wait time statistics
                wait_time = time.time() - start_time
                self._stats.average_wait_time = (
                    (self._stats.average_wait_time * (self._stats.total_waits - 1) + wait_time) /
                    self._stats.total_waits
                )
    
    def _next_generation(self):
        """Advance to next generation."""
        # Execute barrier action if provided
        if self.barrier_action:
            try:
                self.barrier_action()
            except Exception as e:
                self._break_barrier()
                raise e
        
        # Reset for next cycle
        self._count = 0
        self._generation += 1
        self._stats.generation = self._generation
        self._condition.notify_all()
    
    def _break_barrier(self):
        """Break the barrier."""
        self._broken = True
        self._stats.total_breaks += 1
        self._condition.notify_all()
    
    def reset(self):
        """Reset the barrier to initial state."""
        with self._condition:
            if self._count > 0:
                self._break_barrier()
            
            self._broken = False
            self._count = 0
            self._generation += 1
            self._stats.generation = self._generation
    
    def is_broken(self) -> bool:
        """Check if barrier is broken."""
        with self._lock:
            return self._broken
    
    def get_number_waiting(self) -> int:
        """Get number of threads currently waiting."""
        with self._lock:
            return self._count
    
    def get_parties(self) -> int:
        """Get number of parties required."""
        return self.parties
    
    def get_stats(self) -> BarrierStats:
        """Get barrier statistics."""
        with self._lock:
            return BarrierStats(
                parties=self.parties,
                waiting_count=self._count,
                generation=self._generation,
                total_waits=self._stats.total_waits,
                total_timeouts=self._stats.total_timeouts,
                total_breaks=self._stats.total_breaks,
                average_wait_time=self._stats.average_wait_time
            )


class CountDownLatch:
    """
    Count-down latch that allows threads to wait until a count reaches zero.
    """
    
    def __init__(self, count: int):
        if count < 0:
            raise ValueError("Count must be non-negative")
        
        self._count = count
        self._initial_count = count
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._total_waits = 0
        self._total_timeouts = 0
        self._average_wait_time = 0.0
    
    def await_latch(self, timeout: Optional[float] = None) -> bool:
        """
        Wait until the latch count reaches zero.
        Returns True if count reached zero, False if timeout.
        """
        start_time = time.time()
        
        with self._condition:
            self._total_waits += 1
            
            try:
                while self._count > 0:
                    if not self._condition.wait(timeout=timeout):
                        self._total_timeouts += 1
                        return False
                
                return True
            
            finally:
                # Update wait time statistics
                wait_time = time.time() - start_time
                self._average_wait_time = (
                    (self._average_wait_time * (self._total_waits - 1) + wait_time) /
                    self._total_waits
                )
    
    def count_down(self):
        """Decrement the count by one."""
        with self._condition:
            if self._count > 0:
                self._count -= 1
                if self._count == 0:
                    self._condition.notify_all()
    
    def get_count(self) -> int:
        """Get current count."""
        with self._lock:
            return self._count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get latch statistics."""
        with self._lock:
            return {
                'initial_count': self._initial_count,
                'current_count': self._count,
                'total_waits': self._total_waits,
                'total_timeouts': self._total_timeouts,
                'average_wait_time': self._average_wait_time
            }


class PhaseType(Enum):
    """Phase types for Phaser."""
    REGISTRATION = "registration"
    SYNCHRONIZATION = "synchronization"
    TERMINATION = "termination"


@dataclass
class PhaseStats:
    """Phaser phase statistics."""
    phase_number: int
    registered_parties: int
    arrived_parties: int
    unarrived_parties: int
    phase_type: PhaseType


class Phaser:
    """
    Flexible synchronization barrier with dynamic party registration.
    """
    
    def __init__(self, parties: int = 0, parent: Optional['Phaser'] = None):
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._phase = 0
        self._parties = parties
        self._arrived = 0
        self._terminated = False
        self._parent = parent
        self._children: Set['Phaser'] = set()
        self._total_phases = 0
        self._total_arrivals = 0
        
        if parent:
            parent._add_child(self)
    
    def register(self) -> int:
        """Register a new party and return current phase."""
        with self._condition:
            if self._terminated:
                raise RuntimeError("Phaser is terminated")
            
            self._parties += 1
            return self._phase
    
    def bulk_register(self, parties: int) -> int:
        """Register multiple parties and return current phase."""
        if parties < 0:
            raise ValueError("Parties must be non-negative")
        
        with self._condition:
            if self._terminated:
                raise RuntimeError("Phaser is terminated")
            
            self._parties += parties
            return self._phase
    
    def arrive(self) -> int:
        """
        Arrive at current phase without waiting.
        Returns current phase number.
        """
        with self._condition:
            if self._terminated:
                return self._phase
            
            if self._arrived >= self._parties:
                raise RuntimeError("Too many arrivals")
            
            self._arrived += 1
            self._total_arrivals += 1
            
            if self._arrived == self._parties:
                # All parties arrived, advance phase
                self._advance_phase()
            
            return self._phase
    
    def arrive_and_await_advance(self, timeout: Optional[float] = None) -> int:
        """
        Arrive at current phase and wait for advance.
        Returns the phase number upon advance.
        """
        start_time = time.time()
        
        with self._condition:
            if self._terminated:
                return self._phase
            
            current_phase = self._phase
            self.arrive()  # This may advance the phase
            
            # Wait for phase advance if we didn't cause it
            while self._phase == current_phase and not self._terminated:
                remaining_timeout = None
                if timeout is not None:
                    elapsed = time.time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    if remaining_timeout <= 0:
                        raise TimeoutError("Phase advance timed out")
                
                if not self._condition.wait(timeout=remaining_timeout):
                    raise TimeoutError("Phase advance timed out")
            
            return self._phase
    
    def arrive_and_deregister(self) -> int:
        """
        Arrive at current phase and deregister.
        Returns current phase number.
        """
        with self._condition:
            if self._terminated:
                return self._phase
            
            if self._parties <= 0:
                raise RuntimeError("No parties to deregister")
            
            self._arrived += 1
            self._parties -= 1
            self._total_arrivals += 1
            
            if self._parties == 0:
                # No more parties, terminate
                self._terminate()
            elif self._arrived == self._parties:
                # All remaining parties arrived, advance phase
                self._advance_phase()
            
            return self._phase
    
    def await_advance(self, phase: int, timeout: Optional[float] = None) -> int:
        """
        Wait for phase to advance beyond given phase number.
        Returns the phase number upon advance.
        """
        start_time = time.time()
        
        with self._condition:
            if self._terminated or self._phase > phase:
                return self._phase
            
            while self._phase == phase and not self._terminated:
                remaining_timeout = None
                if timeout is not None:
                    elapsed = time.time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    if remaining_timeout <= 0:
                        raise TimeoutError("Phase advance timed out")
                
                if not self._condition.wait(timeout=remaining_timeout):
                    raise TimeoutError("Phase advance timed out")
            
            return self._phase
    
    def _advance_phase(self):
        """Advance to next phase."""
        self._phase += 1
        self._arrived = 0
        self._total_phases += 1
        
        # Notify parent if exists
        if self._parent:
            self._parent.arrive()
        
        # Notify all waiting threads
        self._condition.notify_all()
    
    def _terminate(self):
        """Terminate the phaser."""
        self._terminated = True
        
        # Terminate all children
        for child in self._children:
            child._terminate()
        
        # Remove from parent
        if self._parent:
            self._parent._remove_child(self)
        
        self._condition.notify_all()
    
    def _add_child(self, child: 'Phaser'):
        """Add a child phaser."""
        with self._lock:
            self._children.add(child)
    
    def _remove_child(self, child: 'Phaser'):
        """Remove a child phaser."""
        with self._lock:
            self._children.discard(child)
    
    def force_termination(self):
        """Force termination of the phaser."""
        with self._condition:
            self._terminate()
    
    def get_phase(self) -> int:
        """Get current phase number."""
        with self._lock:
            return self._phase
    
    def get_registered_parties(self) -> int:
        """Get number of registered parties."""
        with self._lock:
            return self._parties
    
    def get_arrived_parties(self) -> int:
        """Get number of arrived parties in current phase."""
        with self._lock:
            return self._arrived
    
    def get_unarrived_parties(self) -> int:
        """Get number of unarrived parties in current phase."""
        with self._lock:
            return self._parties - self._arrived
    
    def is_terminated(self) -> bool:
        """Check if phaser is terminated."""
        with self._lock:
            return self._terminated
    
    def get_stats(self) -> PhaseStats:
        """Get current phase statistics."""
        with self._lock:
            phase_type = PhaseType.TERMINATION if self._terminated else PhaseType.SYNCHRONIZATION
            
            return PhaseStats(
                phase_number=self._phase,
                registered_parties=self._parties,
                arrived_parties=self._arrived,
                unarrived_parties=self._parties - self._arrived,
                phase_type=phase_type
            )
    
    def get_total_stats(self) -> Dict[str, Any]:
        """Get total statistics."""
        with self._lock:
            return {
                'total_phases': self._total_phases,
                'total_arrivals': self._total_arrivals,
                'current_phase': self._phase,
                'registered_parties': self._parties,
                'terminated': self._terminated,
                'children_count': len(self._children)
            }


class AsyncBarrier:
    """
    Async barrier for asyncio applications.
    """
    
    def __init__(self, parties: int):
        if parties <= 0:
            raise ValueError("Parties must be positive")
        
        self.parties = parties
        self._count = 0
        self._generation = 0
        self._broken = False
        self._waiters = []
        self._lock = asyncio.Lock()
    
    async def wait(self, timeout: Optional[float] = None) -> int:
        """Wait for all parties to reach the barrier."""
        async with self._lock:
            if self._broken:
                raise BrokenBarrierError("Barrier is broken")
            
            generation = self._generation
            index = self._count
            self._count += 1
            
            if self._count == self.parties:
                # Last coroutine to arrive
                await self._next_generation()
                return 0
            else:
                # Wait for other coroutines
                future = asyncio.Future()
                self._waiters.append((generation, future))
                
                try:
                    if timeout is None:
                        await future
                    else:
                        await asyncio.wait_for(future, timeout=timeout)
                    
                    if self._broken:
                        raise BrokenBarrierError("Barrier was broken while waiting")
                    
                    return self.parties - index - 1
                
                except asyncio.TimeoutError:
                    await self._break_barrier()
                    raise TimeoutError("Barrier wait timed out")
    
    async def _next_generation(self):
        """Advance to next generation."""
        self._count = 0
        self._generation += 1
        
        # Notify all waiters from current generation
        current_waiters = [future for gen, future in self._waiters if gen == self._generation - 1]
        self._waiters = [(gen, future) for gen, future in self._waiters if gen != self._generation - 1]
        
        for future in current_waiters:
            if not future.done():
                future.set_result(None)
    
    async def _break_barrier(self):
        """Break the barrier."""
        self._broken = True
        
        # Cancel all waiting futures
        for _, future in self._waiters:
            if not future.done():
                future.cancel()
        
        self._waiters.clear()
    
    def is_broken(self) -> bool:
        """Check if barrier is broken."""
        return self._broken
    
    def get_number_waiting(self) -> int:
        """Get number of coroutines currently waiting."""
        return self._count
    
    def get_parties(self) -> int:
        """Get number of parties required."""
        return self.parties


# Example usage and demonstrations
def demo_cyclic_barrier():
    """Demonstrate cyclic barrier."""
    print("=== Cyclic Barrier Demo ===")
    
    def barrier_action():
        print("All threads reached barrier - executing barrier action!")
    
    barrier = CyclicBarrier(3, barrier_action)
    results = []
    
    def worker(worker_id: int):
        for round_num in range(2):
            print(f"Worker {worker_id} doing work in round {round_num}")
            time.sleep(0.1 * worker_id)  # Simulate different work times
            
            try:
                arrival_index = barrier.wait(timeout=2.0)
                results.append(f"Worker {worker_id} round {round_num}: index {arrival_index}")
                print(f"Worker {worker_id} passed barrier in round {round_num} (index: {arrival_index})")
            except (BrokenBarrierError, TimeoutError) as e:
                results.append(f"Worker {worker_id} round {round_num}: {type(e).__name__}")
                print(f"Worker {worker_id} barrier error: {e}")
    
    # Start workers
    threads = []
    for i in range(3):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Results: {results}")
    print(f"Barrier stats: {barrier.get_stats()}")


def demo_countdown_latch():
    """Demonstrate countdown latch."""
    print("\n=== CountDown Latch Demo ===")
    
    latch = CountDownLatch(3)
    results = []
    
    def worker(worker_id: int):
        print(f"Worker {worker_id} starting work")
        time.sleep(0.5 + 0.1 * worker_id)  # Simulate work
        print(f"Worker {worker_id} finished work")
        latch.count_down()
        results.append(f"Worker {worker_id} completed")
    
    def waiter():
        print("Waiter waiting for all workers to complete")
        if latch.await_latch(timeout=3.0):
            print("Waiter: All workers completed!")
            results.append("Waiter: All completed")
        else:
            print("Waiter: Timeout!")
            results.append("Waiter: Timeout")
    
    # Start workers
    threads = []
    for i in range(3):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Start waiter
    waiter_thread = threading.Thread(target=waiter)
    waiter_thread.start()
    threads.append(waiter_thread)
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Results: {results}")
    print(f"Latch stats: {latch.get_stats()}")


def demo_phaser():
    """Demonstrate phaser."""
    print("\n=== Phaser Demo ===")
    
    phaser = Phaser(0)  # Start with no parties
    results = []
    
    def worker(worker_id: int, phases: int):
        # Register with phaser
        current_phase = phaser.register()
        print(f"Worker {worker_id} registered at phase {current_phase}")
        
        for phase_num in range(phases):
            print(f"Worker {worker_id} working in phase {phase_num}")
            time.sleep(0.1 * (worker_id + 1))  # Simulate work
            
            try:
                if phase_num == phases - 1:
                    # Last phase - deregister
                    final_phase = phaser.arrive_and_deregister()
                    results.append(f"Worker {worker_id} deregistered at phase {final_phase}")
                    print(f"Worker {worker_id} deregistered at phase {final_phase}")
                else:
                    # Continue to next phase
                    next_phase = phaser.arrive_and_await_advance(timeout=2.0)
                    results.append(f"Worker {worker_id} advanced to phase {next_phase}")
                    print(f"Worker {worker_id} advanced to phase {next_phase}")
            except TimeoutError:
                results.append(f"Worker {worker_id} timeout in phase {phase_num}")
                print(f"Worker {worker_id} timeout in phase {phase_num}")
                break
    
    # Start workers with different phase counts
    threads = []
    for i in range(3):
        phases = 2 + i  # Different number of phases per worker
        thread = threading.Thread(target=worker, args=(i, phases))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Results: {results}")
    print(f"Phaser stats: {phaser.get_stats()}")
    print(f"Total stats: {phaser.get_total_stats()}")


async def demo_async_barrier():
    """Demonstrate async barrier."""
    print("\n=== Async Barrier Demo ===")
    
    barrier = AsyncBarrier(3)
    results = []
    
    async def async_worker(worker_id: int):
        print(f"Async worker {worker_id} starting")
        await asyncio.sleep(0.1 * worker_id)  # Simulate async work
        
        try:
            arrival_index = await barrier.wait(timeout=2.0)
            results.append(f"Async worker {worker_id}: index {arrival_index}")
            print(f"Async worker {worker_id} passed barrier (index: {arrival_index})")
        except (BrokenBarrierError, TimeoutError) as e:
            results.append(f"Async worker {worker_id}: {type(e).__name__}")
            print(f"Async worker {worker_id} barrier error: {e}")
    
    # Run async workers
    await asyncio.gather(*[async_worker(i) for i in range(3)])
    
    print(f"Async results: {results}")


async def main():
    """Run all barrier demonstrations."""
    demo_cyclic_barrier()
    demo_countdown_latch()
    demo_phaser()
    await demo_async_barrier()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
