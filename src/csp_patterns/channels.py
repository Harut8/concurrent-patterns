"""
CSP Channels Implementation

Channels are the primary communication mechanism in CSP.
They provide synchronous and asynchronous communication between processes.
"""

import asyncio
import time
from typing import Any, Optional, List, AsyncIterator, TypeVar, Generic
from abc import ABC, abstractmethod
from enum import Enum
import weakref
import logging


T = TypeVar('T')


class ChannelState(Enum):
    """Channel states."""
    OPEN = "open"
    CLOSED = "closed"


class ChannelClosedError(Exception):
    """Raised when attempting to use a closed channel."""
    pass


class Channel(Generic[T], ABC):
    """
    Abstract base class for all channels.
    Defines the interface for CSP-style communication.
    """
    
    def __init__(self, name: Optional[str] = None):
        self.name = name or f"Channel-{id(self)}"
        self.state = ChannelState.OPEN
        self._send_count = 0
        self._receive_count = 0
        self._created_at = time.time()
    
    @abstractmethod
    async def send(self, value: T, timeout: Optional[float] = None) -> bool:
        """Send a value through the channel."""
        pass
    
    @abstractmethod
    async def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive a value from the channel."""
        pass
    
    @abstractmethod
    def close(self):
        """Close the channel."""
        pass
    
    @abstractmethod
    def is_closed(self) -> bool:
        """Check if the channel is closed."""
        pass
    
    def __aiter__(self) -> AsyncIterator[T]:
        """Make channel async iterable."""
        return self
    
    async def __anext__(self) -> T:
        """Async iterator implementation."""
        value = await self.receive()
        if value is None:
            raise StopAsyncIteration
        return value
    
    def get_stats(self) -> dict:
        """Get channel statistics."""
        return {
            "name": self.name,
            "state": self.state.value,
            "send_count": self._send_count,
            "receive_count": self._receive_count,
            "created_at": self._created_at,
            "age": time.time() - self._created_at,
        }


class SynchronousChannel(Channel[T]):
    """
    Synchronous channel (unbuffered).
    Send operations block until a receiver is ready.
    """
    
    def __init__(self, name: Optional[str] = None):
        super().__init__(name)
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._receive_queue: asyncio.Queue = asyncio.Queue()
        self._pending_sends: List[asyncio.Future] = []
        self._pending_receives: List[asyncio.Future] = []
        self._lock = asyncio.Lock()
    
    async def send(self, value: T, timeout: Optional[float] = None) -> bool:
        """Send a value synchronously."""
        if self.is_closed():
            raise ChannelClosedError("Cannot send on closed channel")
        
        async with self._lock:
            # Check if there's a waiting receiver
            if self._pending_receives:
                receiver_future = self._pending_receives.pop(0)
                if not receiver_future.cancelled():
                    receiver_future.set_result(value)
                    self._send_count += 1
                    return True
            
            # No receiver available, wait for one
            sender_future = asyncio.Future()
            sender_future.add_done_callback(lambda f: self._cleanup_sender(f))
            self._pending_sends.append(sender_future)
            
            # Store the value with the future
            sender_future._channel_value = value
        
        try:
            await asyncio.wait_for(sender_future, timeout=timeout)
            self._send_count += 1
            return True
        except asyncio.TimeoutError:
            async with self._lock:
                if sender_future in self._pending_sends:
                    self._pending_sends.remove(sender_future)
            return False
    
    async def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive a value synchronously."""
        if self.is_closed() and not self._pending_sends:
            return None
        
        async with self._lock:
            # Check if there's a waiting sender
            if self._pending_sends:
                sender_future = self._pending_sends.pop(0)
                if not sender_future.cancelled():
                    value = getattr(sender_future, '_channel_value', None)
                    sender_future.set_result(True)
                    self._receive_count += 1
                    return value
            
            # No sender available, wait for one
            if self.is_closed():
                return None
            
            receiver_future = asyncio.Future()
            receiver_future.add_done_callback(lambda f: self._cleanup_receiver(f))
            self._pending_receives.append(receiver_future)
        
        try:
            value = await asyncio.wait_for(receiver_future, timeout=timeout)
            self._receive_count += 1
            return value
        except asyncio.TimeoutError:
            async with self._lock:
                if receiver_future in self._pending_receives:
                    self._pending_receives.remove(receiver_future)
            return None
    
    def _cleanup_sender(self, future):
        """Clean up cancelled sender futures."""
        if future.cancelled():
            try:
                self._pending_sends.remove(future)
            except ValueError:
                pass
    
    def _cleanup_receiver(self, future):
        """Clean up cancelled receiver futures."""
        if future.cancelled():
            try:
                self._pending_receives.remove(future)
            except ValueError:
                pass
    
    def close(self):
        """Close the synchronous channel."""
        self.state = ChannelState.CLOSED
        
        # Cancel all pending operations
        for future in self._pending_sends + self._pending_receives:
            if not future.done():
                future.cancel()
        
        self._pending_sends.clear()
        self._pending_receives.clear()
    
    def is_closed(self) -> bool:
        """Check if channel is closed."""
        return self.state == ChannelState.CLOSED


class BufferedChannel(Channel[T]):
    """
    Buffered channel with fixed capacity.
    Send operations block only when buffer is full.
    """
    
    def __init__(self, capacity: int, name: Optional[str] = None):
        super().__init__(name)
        self.capacity = capacity
        self._buffer: asyncio.Queue = asyncio.Queue(maxsize=capacity)
    
    async def send(self, value: T, timeout: Optional[float] = None) -> bool:
        """Send a value to the buffered channel."""
        if self.is_closed():
            raise ChannelClosedError("Cannot send on closed channel")
        
        try:
            await asyncio.wait_for(self._buffer.put(value), timeout=timeout)
            self._send_count += 1
            return True
        except asyncio.TimeoutError:
            return False
    
    async def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive a value from the buffered channel."""
        if self.is_closed() and self._buffer.empty():
            return None
        
        try:
            value = await asyncio.wait_for(self._buffer.get(), timeout=timeout)
            self._receive_count += 1
            return value
        except asyncio.TimeoutError:
            return None
    
    def send_nowait(self, value: T) -> bool:
        """Send without waiting (non-blocking)."""
        if self.is_closed():
            raise ChannelClosedError("Cannot send on closed channel")
        
        try:
            self._buffer.put_nowait(value)
            self._send_count += 1
            return True
        except asyncio.QueueFull:
            return False
    
    def receive_nowait(self) -> Optional[T]:
        """Receive without waiting (non-blocking)."""
        if self.is_closed() and self._buffer.empty():
            return None
        
        try:
            value = self._buffer.get_nowait()
            self._receive_count += 1
            return value
        except asyncio.QueueEmpty:
            return None
    
    def close(self):
        """Close the buffered channel."""
        self.state = ChannelState.CLOSED
    
    def is_closed(self) -> bool:
        """Check if channel is closed."""
        return self.state == ChannelState.CLOSED
    
    def size(self) -> int:
        """Get current buffer size."""
        return self._buffer.qsize()
    
    def is_full(self) -> bool:
        """Check if buffer is full."""
        return self._buffer.full()
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._buffer.empty()


class UnboundedChannel(Channel[T]):
    """
    Unbounded channel with unlimited capacity.
    Send operations never block.
    """
    
    def __init__(self, name: Optional[str] = None):
        super().__init__(name)
        self._buffer: asyncio.Queue = asyncio.Queue()
    
    async def send(self, value: T, timeout: Optional[float] = None) -> bool:
        """Send a value to the unbounded channel."""
        if self.is_closed():
            raise ChannelClosedError("Cannot send on closed channel")
        
        await self._buffer.put(value)
        self._send_count += 1
        return True
    
    async def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive a value from the unbounded channel."""
        if self.is_closed() and self._buffer.empty():
            return None
        
        try:
            value = await asyncio.wait_for(self._buffer.get(), timeout=timeout)
            self._receive_count += 1
            return value
        except asyncio.TimeoutError:
            return None
    
    def send_nowait(self, value: T) -> bool:
        """Send without waiting (always succeeds for unbounded)."""
        if self.is_closed():
            raise ChannelClosedError("Cannot send on closed channel")
        
        self._buffer.put_nowait(value)
        self._send_count += 1
        return True
    
    def receive_nowait(self) -> Optional[T]:
        """Receive without waiting."""
        if self.is_closed() and self._buffer.empty():
            return None
        
        try:
            value = self._buffer.get_nowait()
            self._receive_count += 1
            return value
        except asyncio.QueueEmpty:
            return None
    
    def close(self):
        """Close the unbounded channel."""
        self.state = ChannelState.CLOSED
    
    def is_closed(self) -> bool:
        """Check if channel is closed."""
        return self.state == ChannelState.CLOSED
    
    def size(self) -> int:
        """Get current buffer size."""
        return self._buffer.qsize()


class BroadcastChannel(Channel[T]):
    """
    Broadcast channel that sends messages to multiple receivers.
    Each message is delivered to all active receivers.
    """
    
    def __init__(self, name: Optional[str] = None):
        super().__init__(name)
        self._receivers: List[asyncio.Queue] = []
        self._receiver_refs: List[weakref.ReferenceType] = []
        self._lock = asyncio.Lock()
    
    async def send(self, value: T, timeout: Optional[float] = None) -> bool:
        """Broadcast a value to all receivers."""
        if self.is_closed():
            raise ChannelClosedError("Cannot send on closed channel")
        
        async with self._lock:
            # Clean up dead receivers
            self._cleanup_receivers()
            
            if not self._receivers:
                return True  # No receivers, but send is "successful"
            
            # Send to all receivers
            send_tasks = []
            for receiver_queue in self._receivers:
                task = asyncio.create_task(receiver_queue.put(value))
                send_tasks.append(task)
            
            try:
                await asyncio.wait_for(
                    asyncio.gather(*send_tasks, return_exceptions=True),
                    timeout=timeout
                )
                self._send_count += 1
                return True
            except asyncio.TimeoutError:
                # Cancel pending sends
                for task in send_tasks:
                    if not task.done():
                        task.cancel()
                return False
    
    async def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive from the broadcast channel (creates a new receiver)."""
        receiver_queue = asyncio.Queue()
        
        async with self._lock:
            if self.is_closed():
                return None
            
            self._receivers.append(receiver_queue)
            # Store weak reference for cleanup
            self._receiver_refs.append(weakref.ref(receiver_queue))
        
        try:
            value = await asyncio.wait_for(receiver_queue.get(), timeout=timeout)
            self._receive_count += 1
            return value
        except asyncio.TimeoutError:
            return None
        finally:
            # Remove this receiver
            async with self._lock:
                try:
                    self._receivers.remove(receiver_queue)
                except ValueError:
                    pass
    
    def create_receiver(self) -> 'BroadcastReceiver':
        """Create a dedicated receiver for this broadcast channel."""
        return BroadcastReceiver(self)
    
    def _cleanup_receivers(self):
        """Remove dead receiver references."""
        alive_receivers = []
        alive_refs = []
        
        for receiver, ref in zip(self._receivers, self._receiver_refs):
            if ref() is not None:  # Receiver is still alive
                alive_receivers.append(receiver)
                alive_refs.append(ref)
        
        self._receivers = alive_receivers
        self._receiver_refs = alive_refs
    
    def close(self):
        """Close the broadcast channel."""
        self.state = ChannelState.CLOSED
        
        # Close all receiver queues
        for receiver_queue in self._receivers:
            # Signal closure by putting None
            try:
                receiver_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
    
    def is_closed(self) -> bool:
        """Check if channel is closed."""
        return self.state == ChannelState.CLOSED
    
    def receiver_count(self) -> int:
        """Get number of active receivers."""
        self._cleanup_receivers()
        return len(self._receivers)


class BroadcastReceiver:
    """Dedicated receiver for broadcast channels."""
    
    def __init__(self, broadcast_channel: BroadcastChannel):
        self.broadcast_channel = broadcast_channel
        self._queue = asyncio.Queue()
        self._registered = False
    
    async def receive(self, timeout: Optional[float] = None) -> Optional[T]:
        """Receive a message from the broadcast channel."""
        if not self._registered:
            await self._register()
        
        try:
            value = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            if value is None:  # Channel closed signal
                return None
            return value
        except asyncio.TimeoutError:
            return None
    
    async def _register(self):
        """Register this receiver with the broadcast channel."""
        async with self.broadcast_channel._lock:
            if not self.broadcast_channel.is_closed():
                self.broadcast_channel._receivers.append(self._queue)
                self.broadcast_channel._receiver_refs.append(weakref.ref(self._queue))
                self._registered = True
    
    def close(self):
        """Close this receiver."""
        if self._registered:
            try:
                self.broadcast_channel._receivers.remove(self._queue)
            except ValueError:
                pass
            self._registered = False


# Utility functions for channel creation
def make_channel(capacity: Optional[int] = None, name: Optional[str] = None) -> Channel:
    """
    Factory function to create channels.
    
    Args:
        capacity: None for synchronous, 0 for unbounded, >0 for buffered
        name: Optional channel name
    """
    if capacity is None:
        return SynchronousChannel(name)
    elif capacity == 0:
        return UnboundedChannel(name)
    else:
        return BufferedChannel(capacity, name)


def make_broadcast_channel(name: Optional[str] = None) -> BroadcastChannel:
    """Create a broadcast channel."""
    return BroadcastChannel(name)


# Example usage and demonstrations
async def demo_synchronous_channel():
    """Demonstrate synchronous channel communication."""
    print("=== Synchronous Channel Demo ===")
    
    ch = SynchronousChannel("sync-demo")
    
    async def sender():
        for i in range(5):
            success = await ch.send(f"message-{i}")
            print(f"Sent: message-{i}, success: {success}")
            await asyncio.sleep(0.1)
        ch.close()
    
    async def receiver():
        while True:
            msg = await ch.receive(timeout=2.0)
            if msg is None:
                break
            print(f"Received: {msg}")
            await asyncio.sleep(0.2)
    
    # Run sender and receiver concurrently
    await asyncio.gather(sender(), receiver())
    print(f"Channel stats: {ch.get_stats()}")


async def demo_buffered_channel():
    """Demonstrate buffered channel communication."""
    print("\n=== Buffered Channel Demo ===")
    
    ch = BufferedChannel(3, "buffered-demo")
    
    # Send multiple messages quickly
    for i in range(5):
        success = await ch.send(f"buffered-{i}", timeout=0.1)
        print(f"Sent buffered-{i}: {success}, buffer size: {ch.size()}")
    
    # Receive messages
    while not ch.is_empty():
        msg = ch.receive_nowait()
        print(f"Received: {msg}")
    
    ch.close()
    print(f"Channel stats: {ch.get_stats()}")


async def demo_broadcast_channel():
    """Demonstrate broadcast channel communication."""
    print("\n=== Broadcast Channel Demo ===")
    
    ch = BroadcastChannel("broadcast-demo")
    
    # Create multiple receivers
    receivers = [ch.create_receiver() for _ in range(3)]
    
    async def sender():
        for i in range(3):
            await ch.send(f"broadcast-{i}")
            print(f"Broadcasted: broadcast-{i}")
            await asyncio.sleep(0.1)
        ch.close()
    
    async def receiver_task(receiver_id, receiver):
        while True:
            msg = await receiver.receive(timeout=2.0)
            if msg is None:
                break
            print(f"Receiver {receiver_id} got: {msg}")
    
    # Start all receivers and sender
    tasks = [receiver_task(i, recv) for i, recv in enumerate(receivers)]
    tasks.append(sender())
    
    await asyncio.gather(*tasks)
    print(f"Channel stats: {ch.get_stats()}")


async def main():
    """Run all channel demonstrations."""
    await demo_synchronous_channel()
    await demo_buffered_channel()
    await demo_broadcast_channel()


if __name__ == "__main__":
    asyncio.run(main())
