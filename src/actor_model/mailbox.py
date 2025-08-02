"""
Actor Mailbox Patterns

Different mailbox implementations for actor message handling.
"""

import asyncio
import time
from typing import Any, Optional, List, Callable, TypeVar, Generic
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import heapq
import logging

from .actor_system import Message


T = TypeVar('T')


class MailboxType(Enum):
    """Types of mailboxes."""
    UNBOUNDED = "unbounded"
    BOUNDED = "bounded"
    PRIORITY = "priority"
    STASH = "stash"
    BALANCING = "balancing"


@dataclass
class MailboxStats:
    """Mailbox statistics."""
    total_messages: int = 0
    processed_messages: int = 0
    dropped_messages: int = 0
    current_size: int = 0
    max_size_reached: int = 0
    average_processing_time: float = 0.0


class Mailbox(ABC, Generic[T]):
    """Abstract base class for actor mailboxes."""
    
    def __init__(self):
        self.stats = MailboxStats()
        self._closed = False
    
    @abstractmethod
    async def enqueue(self, message: T) -> bool:
        """Enqueue a message. Returns True if successful."""
        pass
    
    @abstractmethod
    async def dequeue(self) -> Optional[T]:
        """Dequeue a message. Returns None if empty."""
        pass
    
    @abstractmethod
    def size(self) -> int:
        """Get current mailbox size."""
        pass
    
    @abstractmethod
    def is_empty(self) -> bool:
        """Check if mailbox is empty."""
        pass
    
    def close(self):
        """Close the mailbox."""
        self._closed = True
    
    def is_closed(self) -> bool:
        """Check if mailbox is closed."""
        return self._closed
    
    def get_stats(self) -> MailboxStats:
        """Get mailbox statistics."""
        self.stats.current_size = self.size()
        return self.stats


class UnboundedMailbox(Mailbox[T]):
    """
    Unbounded FIFO mailbox using asyncio.Queue.
    """
    
    def __init__(self):
        super().__init__()
        self._queue = asyncio.Queue()
    
    async def enqueue(self, message: T) -> bool:
        """Enqueue a message."""
        if self._closed:
            return False
        
        await self._queue.put(message)
        self.stats.total_messages += 1
        
        # Update max size
        current_size = self.size()
        if current_size > self.stats.max_size_reached:
            self.stats.max_size_reached = current_size
        
        return True
    
    async def dequeue(self) -> Optional[T]:
        """Dequeue a message."""
        if self._closed and self._queue.empty():
            return None
        
        try:
            message = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            self.stats.processed_messages += 1
            return message
        except asyncio.TimeoutError:
            return None
    
    def size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty()


class BoundedMailbox(Mailbox[T]):
    """
    Bounded FIFO mailbox with configurable size limit.
    """
    
    def __init__(self, max_size: int, drop_policy: str = "drop_new"):
        super().__init__()
        self.max_size = max_size
        self.drop_policy = drop_policy  # "drop_new", "drop_old", "block"
        self._queue = asyncio.Queue(maxsize=max_size if drop_policy == "block" else 0)
        self._messages: List[T] = []
    
    async def enqueue(self, message: T) -> bool:
        """Enqueue a message with size limit."""
        if self._closed:
            return False
        
        if self.drop_policy == "block":
            # Use bounded queue that blocks
            await self._queue.put(message)
            self.stats.total_messages += 1
            return True
        
        # Handle drop policies manually
        if len(self._messages) >= self.max_size:
            if self.drop_policy == "drop_new":
                # Drop the new message
                self.stats.dropped_messages += 1
                return False
            elif self.drop_policy == "drop_old":
                # Drop the oldest message
                if self._messages:
                    self._messages.pop(0)
                    self.stats.dropped_messages += 1
        
        self._messages.append(message)
        self.stats.total_messages += 1
        
        # Update max size
        current_size = len(self._messages)
        if current_size > self.stats.max_size_reached:
            self.stats.max_size_reached = current_size
        
        return True
    
    async def dequeue(self) -> Optional[T]:
        """Dequeue a message."""
        if self.drop_policy == "block":
            if self._closed and self._queue.empty():
                return None
            
            try:
                message = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                self.stats.processed_messages += 1
                return message
            except asyncio.TimeoutError:
                return None
        
        # Handle manual queue
        if not self._messages:
            return None
        
        message = self._messages.pop(0)
        self.stats.processed_messages += 1
        return message
    
    def size(self) -> int:
        """Get current queue size."""
        if self.drop_policy == "block":
            return self._queue.qsize()
        return len(self._messages)
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        if self.drop_policy == "block":
            return self._queue.empty()
        return len(self._messages) == 0


@dataclass
class PriorityMessage:
    """Message wrapper with priority."""
    priority: int
    timestamp: float
    message: Any
    
    def __lt__(self, other):
        # Lower priority number = higher priority
        if self.priority != other.priority:
            return self.priority < other.priority
        # If same priority, use timestamp (FIFO)
        return self.timestamp < other.timestamp


class PriorityMailbox(Mailbox[T]):
    """
    Priority mailbox using a heap queue.
    """
    
    def __init__(self, max_size: Optional[int] = None):
        super().__init__()
        self.max_size = max_size
        self._heap: List[PriorityMessage] = []
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)
    
    async def enqueue(self, message: T, priority: int = 0) -> bool:
        """Enqueue a message with priority."""
        if self._closed:
            return False
        
        async with self._lock:
            # Check size limit
            if self.max_size and len(self._heap) >= self.max_size:
                self.stats.dropped_messages += 1
                return False
            
            # Create priority message
            priority_msg = PriorityMessage(
                priority=priority,
                timestamp=time.time(),
                message=message
            )
            
            # Add to heap
            heapq.heappush(self._heap, priority_msg)
            self.stats.total_messages += 1
            
            # Update max size
            current_size = len(self._heap)
            if current_size > self.stats.max_size_reached:
                self.stats.max_size_reached = current_size
            
            # Notify waiting consumers
            self._not_empty.notify()
        
        return True
    
    async def dequeue(self) -> Optional[T]:
        """Dequeue highest priority message."""
        async with self._not_empty:
            # Wait for messages if empty
            while not self._heap and not self._closed:
                try:
                    await asyncio.wait_for(self._not_empty.wait(), timeout=0.1)
                except asyncio.TimeoutError:
                    return None
            
            if not self._heap:
                return None
            
            # Get highest priority message
            priority_msg = heapq.heappop(self._heap)
            self.stats.processed_messages += 1
            return priority_msg.message
    
    def size(self) -> int:
        """Get current heap size."""
        return len(self._heap)
    
    def is_empty(self) -> bool:
        """Check if heap is empty."""
        return len(self._heap) == 0


class StashMailbox(Mailbox[T]):
    """
    Mailbox with stashing capability for selective message processing.
    """
    
    def __init__(self, base_mailbox: Mailbox[T]):
        super().__init__()
        self.base_mailbox = base_mailbox
        self._stash: List[T] = []
        self._lock = asyncio.Lock()
    
    async def enqueue(self, message: T) -> bool:
        """Enqueue a message to base mailbox."""
        result = await self.base_mailbox.enqueue(message)
        if result:
            self.stats.total_messages += 1
        return result
    
    async def dequeue(self) -> Optional[T]:
        """Dequeue from base mailbox."""
        message = await self.base_mailbox.dequeue()
        if message:
            self.stats.processed_messages += 1
        return message
    
    async def stash(self, message: T):
        """Stash a message for later processing."""
        async with self._lock:
            self._stash.append(message)
    
    async def unstash_all(self):
        """Unstash all messages back to the mailbox."""
        async with self._lock:
            for message in reversed(self._stash):  # Maintain order
                await self.base_mailbox.enqueue(message)
            self._stash.clear()
    
    async def unstash_one(self) -> bool:
        """Unstash one message. Returns True if a message was unstashed."""
        async with self._lock:
            if self._stash:
                message = self._stash.pop(0)
                await self.base_mailbox.enqueue(message)
                return True
            return False
    
    def stash_size(self) -> int:
        """Get number of stashed messages."""
        return len(self._stash)
    
    def size(self) -> int:
        """Get total size (base + stash)."""
        return self.base_mailbox.size() + len(self._stash)
    
    def is_empty(self) -> bool:
        """Check if both base and stash are empty."""
        return self.base_mailbox.is_empty() and len(self._stash) == 0
    
    def close(self):
        """Close both mailboxes."""
        super().close()
        self.base_mailbox.close()


class BalancingMailbox(Mailbox[T]):
    """
    Mailbox that balances load across multiple workers.
    """
    
    def __init__(self, num_workers: int = 4):
        super().__init__()
        self.num_workers = num_workers
        self._queues = [asyncio.Queue() for _ in range(num_workers)]
        self._round_robin_index = 0
        self._lock = asyncio.Lock()
    
    async def enqueue(self, message: T) -> bool:
        """Enqueue message using round-robin distribution."""
        if self._closed:
            return False
        
        async with self._lock:
            # Round-robin distribution
            queue_index = self._round_robin_index % self.num_workers
            self._round_robin_index += 1
        
        await self._queues[queue_index].put(message)
        self.stats.total_messages += 1
        
        # Update max size
        current_size = self.size()
        if current_size > self.stats.max_size_reached:
            self.stats.max_size_reached = current_size
        
        return True
    
    async def dequeue(self, worker_id: int = 0) -> Optional[T]:
        """Dequeue message for specific worker."""
        if worker_id >= self.num_workers:
            return None
        
        queue = self._queues[worker_id]
        
        if self._closed and queue.empty():
            return None
        
        try:
            message = await asyncio.wait_for(queue.get(), timeout=0.1)
            self.stats.processed_messages += 1
            return message
        except asyncio.TimeoutError:
            return None
    
    async def dequeue_any(self) -> Optional[T]:
        """Dequeue from any non-empty queue."""
        for i in range(self.num_workers):
            queue = self._queues[i]
            if not queue.empty():
                try:
                    message = queue.get_nowait()
                    self.stats.processed_messages += 1
                    return message
                except asyncio.QueueEmpty:
                    continue
        return None
    
    def size(self) -> int:
        """Get total size across all queues."""
        return sum(queue.qsize() for queue in self._queues)
    
    def is_empty(self) -> bool:
        """Check if all queues are empty."""
        return all(queue.empty() for queue in self._queues)
    
    def get_queue_sizes(self) -> List[int]:
        """Get sizes of individual queues."""
        return [queue.qsize() for queue in self._queues]


class MailboxFactory:
    """Factory for creating different types of mailboxes."""
    
    @staticmethod
    def create_mailbox(mailbox_type: MailboxType, **kwargs) -> Mailbox:
        """Create a mailbox of the specified type."""
        if mailbox_type == MailboxType.UNBOUNDED:
            return UnboundedMailbox()
        
        elif mailbox_type == MailboxType.BOUNDED:
            max_size = kwargs.get('max_size', 100)
            drop_policy = kwargs.get('drop_policy', 'drop_new')
            return BoundedMailbox(max_size, drop_policy)
        
        elif mailbox_type == MailboxType.PRIORITY:
            max_size = kwargs.get('max_size')
            return PriorityMailbox(max_size)
        
        elif mailbox_type == MailboxType.STASH:
            base_type = kwargs.get('base_type', MailboxType.UNBOUNDED)
            base_mailbox = MailboxFactory.create_mailbox(base_type, **kwargs)
            return StashMailbox(base_mailbox)
        
        elif mailbox_type == MailboxType.BALANCING:
            num_workers = kwargs.get('num_workers', 4)
            return BalancingMailbox(num_workers)
        
        else:
            raise ValueError(f"Unknown mailbox type: {mailbox_type}")


# Example usage and demonstrations
async def demo_unbounded_mailbox():
    """Demonstrate unbounded mailbox."""
    print("=== Unbounded Mailbox Demo ===")
    
    mailbox = UnboundedMailbox()
    
    # Enqueue messages
    for i in range(5):
        await mailbox.enqueue(f"message-{i}")
    
    print(f"Enqueued 5 messages, size: {mailbox.size()}")
    
    # Dequeue messages
    while not mailbox.is_empty():
        message = await mailbox.dequeue()
        print(f"Dequeued: {message}")
    
    stats = mailbox.get_stats()
    print(f"Stats: {stats.total_messages} total, {stats.processed_messages} processed")


async def demo_priority_mailbox():
    """Demonstrate priority mailbox."""
    print("\n=== Priority Mailbox Demo ===")
    
    mailbox = PriorityMailbox()
    
    # Enqueue messages with different priorities
    await mailbox.enqueue("low priority", priority=10)
    await mailbox.enqueue("high priority", priority=1)
    await mailbox.enqueue("medium priority", priority=5)
    await mailbox.enqueue("highest priority", priority=0)
    
    print(f"Enqueued 4 messages with priorities, size: {mailbox.size()}")
    
    # Dequeue in priority order
    while not mailbox.is_empty():
        message = await mailbox.dequeue()
        print(f"Dequeued: {message}")


async def demo_stash_mailbox():
    """Demonstrate stash mailbox."""
    print("\n=== Stash Mailbox Demo ===")
    
    base_mailbox = UnboundedMailbox()
    stash_mailbox = StashMailbox(base_mailbox)
    
    # Enqueue some messages
    for i in range(5):
        await stash_mailbox.enqueue(f"message-{i}")
    
    # Process some messages and stash others
    for _ in range(3):
        message = await stash_mailbox.dequeue()
        if message and "2" in message:
            # Stash message-2 for later
            await stash_mailbox.stash(message)
            print(f"Stashed: {message}")
        else:
            print(f"Processed: {message}")
    
    print(f"Stash size: {stash_mailbox.stash_size()}")
    
    # Unstash and process
    await stash_mailbox.unstash_all()
    print("Unstashed all messages")
    
    while not stash_mailbox.is_empty():
        message = await stash_mailbox.dequeue()
        print(f"Final processing: {message}")


async def demo_balancing_mailbox():
    """Demonstrate balancing mailbox."""
    print("\n=== Balancing Mailbox Demo ===")
    
    mailbox = BalancingMailbox(num_workers=3)
    
    # Enqueue messages (distributed round-robin)
    for i in range(9):
        await mailbox.enqueue(f"task-{i}")
    
    print(f"Enqueued 9 tasks, queue sizes: {mailbox.get_queue_sizes()}")
    
    # Process from different workers
    for worker_id in range(3):
        for _ in range(3):
            message = await mailbox.dequeue(worker_id)
            if message:
                print(f"Worker {worker_id} processed: {message}")


async def main():
    """Run all mailbox demonstrations."""
    await demo_unbounded_mailbox()
    await demo_priority_mailbox()
    await demo_stash_mailbox()
    await demo_balancing_mailbox()


if __name__ == "__main__":
    asyncio.run(main())
