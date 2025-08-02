"""
Async Producer-Consumer Pattern Implementations

Asynchronous implementations of producer-consumer patterns using asyncio.
Includes bounded queues, priority queues, and backpressure handling.
"""

import asyncio
import time
import random
from typing import Any, Optional, Callable, List, AsyncGenerator, TypeVar, Generic
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import logging


T = TypeVar('T')


@dataclass
class AsyncWorkItem(Generic[T]):
    """Represents an async work item with metadata."""
    data: T
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3
    
    def __lt__(self, other):
        # Higher priority number = higher priority
        if self.priority != other.priority:
            return self.priority > other.priority
        return self.timestamp < other.timestamp


class BackpressureStrategy(Enum):
    """Strategies for handling backpressure in async queues."""
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    RAISE_EXCEPTION = "raise_exception"


class AsyncBoundedQueue(Generic[T]):
    """
    Async bounded queue with configurable backpressure handling.
    Provides flow control for producer-consumer scenarios.
    """
    
    def __init__(self, maxsize: int, 
                 backpressure_strategy: BackpressureStrategy = BackpressureStrategy.BLOCK):
        self.maxsize = maxsize
        self.backpressure_strategy = backpressure_strategy
        self._queue = asyncio.Queue(maxsize=maxsize)
        self._closed = False
        self._put_waiters = []
        self._get_waiters = []
        self._items_put = 0
        self._items_got = 0
    
    async def put(self, item: T, timeout: Optional[float] = None) -> bool:
        """Put an item into the queue with backpressure handling."""
        if self._closed:
            raise RuntimeError("Queue is closed")
        
        if self.backpressure_strategy == BackpressureStrategy.BLOCK:
            try:
                await asyncio.wait_for(self._queue.put(item), timeout=timeout)
                self._items_put += 1
                return True
            except asyncio.TimeoutError:
                return False
        
        elif self.backpressure_strategy == BackpressureStrategy.RAISE_EXCEPTION:
            if self._queue.full():
                raise asyncio.QueueFull("Queue is full")
            await self._queue.put(item)
            self._items_put += 1
            return True
        
        elif self.backpressure_strategy == BackpressureStrategy.DROP_OLDEST:
            if self._queue.full():
                try:
                    self._queue.get_nowait()  # Drop oldest
                except asyncio.QueueEmpty:
                    pass
            await self._queue.put(item)
            self._items_put += 1
            return True
        
        elif self.backpressure_strategy == BackpressureStrategy.DROP_NEWEST:
            if self._queue.full():
                return False  # Drop the new item
            await self._queue.put(item)
            self._items_put += 1
            return True
    
    async def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get an item from the queue."""
        if self._closed and self._queue.empty():
            return None
        
        try:
            item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            self._items_got += 1
            return item
        except asyncio.TimeoutError:
            return None
    
    def put_nowait(self, item: T) -> bool:
        """Put an item without waiting."""
        if self._closed:
            return False
        
        try:
            self._queue.put_nowait(item)
            self._items_put += 1
            return True
        except asyncio.QueueFull:
            if self.backpressure_strategy == BackpressureStrategy.DROP_OLDEST:
                try:
                    self._queue.get_nowait()
                    self._queue.put_nowait(item)
                    self._items_put += 1
                    return True
                except asyncio.QueueEmpty:
                    return False
            return False
    
    def get_nowait(self) -> Optional[T]:
        """Get an item without waiting."""
        try:
            item = self._queue.get_nowait()
            self._items_got += 1
            return item
        except asyncio.QueueEmpty:
            return None
    
    def close(self):
        """Close the queue."""
        self._closed = True
    
    def qsize(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty()
    
    def full(self) -> bool:
        """Check if queue is full."""
        return self._queue.full()
    
    def get_stats(self) -> dict:
        """Get queue statistics."""
        return {
            "maxsize": self.maxsize,
            "current_size": self.qsize(),
            "items_put": self._items_put,
            "items_got": self._items_got,
            "backpressure_strategy": self.backpressure_strategy.value,
            "is_closed": self._closed,
        }


class AsyncPriorityQueue(Generic[T]):
    """
    Async priority queue for ordered task processing.
    Higher priority items are processed first.
    """
    
    def __init__(self, maxsize: int = 0):
        self._queue = asyncio.PriorityQueue(maxsize=maxsize)
        self._closed = False
        self._item_counter = 0
    
    async def put(self, item: AsyncWorkItem[T], timeout: Optional[float] = None):
        """Put a work item with priority."""
        if self._closed:
            raise RuntimeError("Queue is closed")
        
        # Add counter to ensure FIFO for same priority
        priority_item = (item.priority, self._item_counter, item)
        self._item_counter += 1
        
        await asyncio.wait_for(self._queue.put(priority_item), timeout=timeout)
    
    async def get(self, timeout: Optional[float] = None) -> Optional[AsyncWorkItem[T]]:
        """Get the highest priority work item."""
        if self._closed and self._queue.empty():
            return None
        
        try:
            _, _, item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            return item
        except asyncio.TimeoutError:
            return None
    
    def close(self):
        """Close the priority queue."""
        self._closed = True
    
    def qsize(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()


class AsyncProducer:
    """Generic async producer that generates work items."""
    
    def __init__(self, producer_id: int, queue: AsyncBoundedQueue,
                 item_generator: Callable[[], Any], production_rate: float = 1.0):
        self.producer_id = producer_id
        self.queue = queue
        self.item_generator = item_generator
        self.production_rate = production_rate
        self.items_produced = 0
        self.running = False
    
    async def start(self, duration: Optional[float] = None, item_count: Optional[int] = None):
        """Start producing items."""
        self.running = True
        start_time = time.time()
        
        try:
            while self.running:
                # Check termination conditions
                if duration and (time.time() - start_time) >= duration:
                    break
                if item_count and self.items_produced >= item_count:
                    break
                
                # Generate and put item
                item = self.item_generator()
                success = await self.queue.put(item, timeout=1.0)
                
                if success:
                    self.items_produced += 1
                    print(f"Producer {self.producer_id}: Produced item {self.items_produced} -> {item}")
                else:
                    print(f"Producer {self.producer_id}: Failed to put item (queue full or timeout)")
                
                # Rate limiting
                if self.production_rate > 0:
                    await asyncio.sleep(1.0 / self.production_rate)
                
        except Exception as e:
            print(f"Producer {self.producer_id}: Error - {e}")
        finally:
            self.running = False
            print(f"Producer {self.producer_id}: Finished. Produced {self.items_produced} items")
    
    def stop(self):
        """Stop the producer."""
        self.running = False


class AsyncConsumer:
    """Generic async consumer that processes work items."""
    
    def __init__(self, consumer_id: int, queue: AsyncBoundedQueue,
                 item_processor: Callable[[Any], Any], processing_time: float = 0.1):
        self.consumer_id = consumer_id
        self.queue = queue
        self.item_processor = item_processor
        self.processing_time = processing_time
        self.items_consumed = 0
        self.running = False
    
    async def start(self, duration: Optional[float] = None):
        """Start consuming items."""
        self.running = True
        start_time = time.time()
        
        try:
            while self.running:
                # Check termination condition
                if duration and (time.time() - start_time) >= duration:
                    break
                
                # Get and process item
                item = await self.queue.get(timeout=1.0)
                if item is None:
                    continue
                
                # Process the item
                if asyncio.iscoroutinefunction(self.item_processor):
                    result = await self.item_processor(item)
                else:
                    result = self.item_processor(item)
                
                self.items_consumed += 1
                print(f"Consumer {self.consumer_id}: Processed item {self.items_consumed} -> {result}")
                
                # Simulate processing time
                if self.processing_time > 0:
                    await asyncio.sleep(self.processing_time)
                
        except Exception as e:
            print(f"Consumer {self.consumer_id}: Error - {e}")
        finally:
            self.running = False
            print(f"Consumer {self.consumer_id}: Finished. Consumed {self.items_consumed} items")
    
    def stop(self):
        """Stop the consumer."""
        self.running = False


class AsyncProducerConsumer:
    """
    Main coordinator for async producer-consumer pattern.
    Manages multiple async producers and consumers.
    """
    
    def __init__(self, buffer_capacity: int = 10,
                 backpressure_strategy: BackpressureStrategy = BackpressureStrategy.BLOCK):
        self.buffer = AsyncBoundedQueue(buffer_capacity, backpressure_strategy)
        self.producers: List[AsyncProducer] = []
        self.consumers: List[AsyncConsumer] = []
        self.running = False
    
    def add_producer(self, item_generator: Callable[[], Any], 
                    production_rate: float = 1.0) -> AsyncProducer:
        """Add a producer with specified item generator and rate."""
        producer_id = len(self.producers)
        producer = AsyncProducer(producer_id, self.buffer, item_generator, production_rate)
        self.producers.append(producer)
        return producer
    
    def add_consumer(self, item_processor: Callable[[Any], Any],
                    processing_time: float = 0.1) -> AsyncConsumer:
        """Add a consumer with specified item processor."""
        consumer_id = len(self.consumers)
        consumer = AsyncConsumer(consumer_id, self.buffer, item_processor, processing_time)
        self.consumers.append(consumer)
        return consumer
    
    async def run_simulation(self, duration: float = 10.0, 
                           producers: int = 2, consumers: int = 2):
        """Run a complete simulation with specified parameters."""
        # Add default producers and consumers if none exist
        if not self.producers:
            for _ in range(producers):
                self.add_producer(
                    lambda: f"data-{random.randint(1, 1000)}",
                    production_rate=2.0
                )
        
        if not self.consumers:
            for _ in range(consumers):
                self.add_consumer(
                    lambda item: f"processed-{item}",
                    processing_time=0.2
                )
        
        # Start all producers and consumers
        tasks = []
        
        for producer in self.producers:
            task = asyncio.create_task(producer.start(duration=duration))
            tasks.append(task)
        
        for consumer in self.consumers:
            task = asyncio.create_task(consumer.start(duration=duration + 2))
            tasks.append(task)
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close the buffer
        self.buffer.close()
    
    def get_statistics(self) -> dict:
        """Get comprehensive statistics."""
        total_produced = sum(p.items_produced for p in self.producers)
        total_consumed = sum(c.items_consumed for c in self.consumers)
        
        return {
            "producers": len(self.producers),
            "consumers": len(self.consumers),
            "total_produced": total_produced,
            "total_consumed": total_consumed,
            "buffer_stats": self.buffer.get_stats(),
        }


class AsyncBatchProcessor:
    """
    Processes items in batches for improved efficiency.
    Useful for database operations or API calls.
    """
    
    def __init__(self, batch_size: int = 10, batch_timeout: float = 1.0):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._queue = asyncio.Queue()
        self._batch = []
        self._last_batch_time = time.time()
        self._processing = False
    
    async def add_item(self, item: T):
        """Add an item for batch processing."""
        await self._queue.put(item)
    
    async def process_batches(self, batch_processor: Callable[[List[T]], Any]):
        """Start processing items in batches."""
        self._processing = True
        
        while self._processing:
            try:
                # Try to fill a batch
                while len(self._batch) < self.batch_size:
                    try:
                        # Calculate remaining timeout
                        elapsed = time.time() - self._last_batch_time
                        remaining_timeout = max(0, self.batch_timeout - elapsed)
                        
                        if remaining_timeout <= 0:
                            break
                        
                        item = await asyncio.wait_for(
                            self._queue.get(), 
                            timeout=remaining_timeout
                        )
                        self._batch.append(item)
                        
                    except asyncio.TimeoutError:
                        break
                
                # Process the batch if we have items
                if self._batch:
                    if asyncio.iscoroutinefunction(batch_processor):
                        await batch_processor(self._batch.copy())
                    else:
                        batch_processor(self._batch.copy())
                    
                    print(f"Processed batch of {len(self._batch)} items")
                    self._batch.clear()
                    self._last_batch_time = time.time()
                
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.01)
                
            except Exception as e:
                print(f"Batch processing error: {e}")
    
    def stop(self):
        """Stop batch processing."""
        self._processing = False


# Example usage and demonstrations
async def demo_async_producer_consumer():
    """Demonstrate basic async producer-consumer pattern."""
    print("=== Async Producer-Consumer Demo ===")
    
    pc = AsyncProducerConsumer(
        buffer_capacity=5,
        backpressure_strategy=BackpressureStrategy.DROP_OLDEST
    )
    
    # Run simulation
    await pc.run_simulation(duration=5.0, producers=2, consumers=3)
    
    # Print statistics
    stats = pc.get_statistics()
    print(f"Statistics: {stats}")


async def demo_priority_queue():
    """Demonstrate async priority queue."""
    print("\n=== Async Priority Queue Demo ===")
    
    pq = AsyncPriorityQueue(maxsize=10)
    
    # Add items with different priorities
    items = [
        AsyncWorkItem("Low priority task", priority=1),
        AsyncWorkItem("High priority task", priority=5),
        AsyncWorkItem("Medium priority task", priority=3),
        AsyncWorkItem("Critical task", priority=10),
        AsyncWorkItem("Another low priority", priority=1),
    ]
    
    # Put items
    for item in items:
        await pq.put(item)
        print(f"Added: {item.data} (priority: {item.priority})")
    
    # Get items (should come out in priority order)
    print("\nProcessing in priority order:")
    while pq.qsize() > 0:
        item = await pq.get()
        print(f"Processing: {item.data} (priority: {item.priority})")


async def demo_batch_processor():
    """Demonstrate async batch processor."""
    print("\n=== Async Batch Processor Demo ===")
    
    processor = AsyncBatchProcessor(batch_size=3, batch_timeout=2.0)
    
    async def batch_handler(batch):
        """Process a batch of items."""
        print(f"Processing batch: {batch}")
        await asyncio.sleep(0.5)  # Simulate batch processing time
    
    # Start batch processing
    process_task = asyncio.create_task(processor.process_batches(batch_handler))
    
    # Add items over time
    for i in range(8):
        await processor.add_item(f"item-{i}")
        print(f"Added item-{i}")
        await asyncio.sleep(0.3)
    
    # Stop processing
    await asyncio.sleep(3)  # Let final batch process
    processor.stop()
    await process_task


async def main():
    """Run all demonstrations."""
    await demo_async_producer_consumer()
    await demo_priority_queue()
    await demo_batch_processor()


if __name__ == "__main__":
    asyncio.run(main())
