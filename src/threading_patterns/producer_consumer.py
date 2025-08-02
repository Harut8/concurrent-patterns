"""
Producer-Consumer Pattern Implementations

Various implementations of the classic producer-consumer pattern using threading.
Includes bounded buffers, priority queues, and multiple producer/consumer scenarios.
"""

import threading
import queue
import time
import random
from typing import Any, Optional, Callable, List
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class WorkItem:
    """Represents a work item with priority and data."""
    data: Any
    priority: int = 0
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()
    
    def __lt__(self, other):
        return self.priority < other.priority


class BoundedBuffer:
    """
    Thread-safe bounded buffer implementation using condition variables.
    Classic solution to producer-consumer problem.
    """
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = []
        self.lock = threading.Lock()
        self.not_full = threading.Condition(self.lock)
        self.not_empty = threading.Condition(self.lock)
        self.closed = False
    
    def put(self, item: Any, timeout: Optional[float] = None) -> bool:
        """Put an item into the buffer. Blocks if buffer is full."""
        with self.not_full:
            if not self.not_full.wait_for(
                lambda: len(self.buffer) < self.capacity or self.closed,
                timeout=timeout
            ):
                return False
            
            if self.closed:
                return False
            
            self.buffer.append(item)
            self.not_empty.notify()
            return True
    
    def get(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Get an item from the buffer. Blocks if buffer is empty."""
        with self.not_empty:
            if not self.not_empty.wait_for(
                lambda: len(self.buffer) > 0 or self.closed,
                timeout=timeout
            ):
                return None
            
            if len(self.buffer) == 0 and self.closed:
                return None
            
            item = self.buffer.pop(0)
            self.not_full.notify()
            return item
    
    def close(self):
        """Close the buffer, waking up all waiting threads."""
        with self.lock:
            self.closed = True
            self.not_full.notify_all()
            self.not_empty.notify_all()
    
    def size(self) -> int:
        """Get current buffer size."""
        with self.lock:
            return len(self.buffer)
    
    def is_full(self) -> bool:
        """Check if buffer is full."""
        with self.lock:
            return len(self.buffer) >= self.capacity
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        with self.lock:
            return len(self.buffer) == 0


class Producer(threading.Thread):
    """Generic producer thread that generates work items."""
    
    def __init__(self, buffer: BoundedBuffer, producer_id: int, 
                 item_count: int, item_generator: Callable[[], Any]):
        super().__init__(name=f"Producer-{producer_id}")
        self.buffer = buffer
        self.producer_id = producer_id
        self.item_count = item_count
        self.item_generator = item_generator
        self.items_produced = 0
        self.daemon = True
    
    def run(self):
        """Produce items and put them in the buffer."""
        try:
            for i in range(self.item_count):
                item = self.item_generator()
                if not self.buffer.put(item, timeout=1.0):
                    print(f"Producer {self.producer_id}: Timeout putting item {i}")
                    break
                
                self.items_produced += 1
                print(f"Producer {self.producer_id}: Produced item {i} -> {item}")
                
                # Simulate variable production time
                time.sleep(random.uniform(0.01, 0.1))
                
        except Exception as e:
            print(f"Producer {self.producer_id}: Error - {e}")
        finally:
            print(f"Producer {self.producer_id}: Finished. Produced {self.items_produced} items")


class Consumer(threading.Thread):
    """Generic consumer thread that processes work items."""
    
    def __init__(self, buffer: BoundedBuffer, consumer_id: int,
                 item_processor: Callable[[Any], None]):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.buffer = buffer
        self.consumer_id = consumer_id
        self.item_processor = item_processor
        self.items_consumed = 0
        self.daemon = True
    
    def run(self):
        """Consume items from the buffer and process them."""
        try:
            while True:
                item = self.buffer.get(timeout=1.0)
                if item is None:
                    break
                
                self.item_processor(item)
                self.items_consumed += 1
                print(f"Consumer {self.consumer_id}: Consumed item -> {item}")
                
                # Simulate variable processing time
                time.sleep(random.uniform(0.02, 0.15))
                
        except Exception as e:
            print(f"Consumer {self.consumer_id}: Error - {e}")
        finally:
            print(f"Consumer {self.consumer_id}: Finished. Consumed {self.items_consumed} items")


class PriorityProducerConsumer:
    """
    Producer-Consumer pattern with priority queue.
    Higher priority items are consumed first.
    """
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.queue = queue.PriorityQueue(maxsize=capacity)
        self.closed = False
        self.lock = threading.Lock()
    
    def put(self, item: WorkItem, timeout: Optional[float] = None) -> bool:
        """Put a work item with priority."""
        try:
            self.queue.put(item, timeout=timeout)
            return True
        except queue.Full:
            return False
    
    def get(self, timeout: Optional[float] = None) -> Optional[WorkItem]:
        """Get the highest priority work item."""
        try:
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def close(self):
        """Signal that no more items will be added."""
        with self.lock:
            self.closed = True
    
    def task_done(self):
        """Mark a task as done."""
        self.queue.task_done()
    
    def join(self):
        """Wait for all tasks to be completed."""
        self.queue.join()


class ProducerConsumer:
    """
    Main coordinator class for producer-consumer pattern.
    Manages multiple producers and consumers with various configurations.
    """
    
    def __init__(self, buffer_capacity: int = 10):
        self.buffer = BoundedBuffer(buffer_capacity)
        self.producers: List[Producer] = []
        self.consumers: List[Consumer] = []
        self.running = False
    
    def add_producer(self, item_count: int, item_generator: Callable[[], Any]) -> Producer:
        """Add a producer with specified item count and generator."""
        producer_id = len(self.producers)
        producer = Producer(self.buffer, producer_id, item_count, item_generator)
        self.producers.append(producer)
        return producer
    
    def add_consumer(self, item_processor: Callable[[Any], None]) -> Consumer:
        """Add a consumer with specified item processor."""
        consumer_id = len(self.consumers)
        consumer = Consumer(self.buffer, consumer_id, item_processor)
        self.consumers.append(consumer)
        return consumer
    
    def start(self):
        """Start all producers and consumers."""
        if self.running:
            return
        
        self.running = True
        
        # Start all producers
        for producer in self.producers:
            producer.start()
        
        # Start all consumers
        for consumer in self.consumers:
            consumer.start()
    
    def wait_for_completion(self, timeout: Optional[float] = None):
        """Wait for all producers to finish, then stop consumers."""
        # Wait for all producers to finish
        for producer in self.producers:
            producer.join(timeout)
        
        # Give consumers time to process remaining items
        time.sleep(0.5)
        
        # Close the buffer to signal consumers to stop
        self.buffer.close()
        
        # Wait for all consumers to finish
        for consumer in self.consumers:
            consumer.join(timeout)
        
        self.running = False
    
    def get_statistics(self) -> dict:
        """Get production and consumption statistics."""
        total_produced = sum(p.items_produced for p in self.producers)
        total_consumed = sum(c.items_consumed for c in self.consumers)
        
        return {
            "producers": len(self.producers),
            "consumers": len(self.consumers),
            "total_produced": total_produced,
            "total_consumed": total_consumed,
            "buffer_size": self.buffer.size(),
            "buffer_capacity": self.buffer.capacity,
        }


# Example usage and demonstrations
def demo_basic_producer_consumer():
    """Demonstrate basic producer-consumer pattern."""
    print("=== Basic Producer-Consumer Demo ===")
    
    pc = ProducerConsumer(buffer_capacity=5)
    
    # Add producers
    pc.add_producer(10, lambda: f"data-{random.randint(1, 100)}")
    pc.add_producer(8, lambda: f"item-{random.randint(1, 100)}")
    
    # Add consumers
    pc.add_consumer(lambda item: print(f"Processing: {item}"))
    pc.add_consumer(lambda item: print(f"Handling: {item}"))
    
    # Run the simulation
    pc.start()
    pc.wait_for_completion(timeout=10)
    
    # Print statistics
    stats = pc.get_statistics()
    print(f"Statistics: {stats}")


def demo_priority_producer_consumer():
    """Demonstrate priority-based producer-consumer pattern."""
    print("\n=== Priority Producer-Consumer Demo ===")
    
    ppc = PriorityProducerConsumer(capacity=10)
    
    def producer_func():
        for i in range(5):
            priority = random.randint(1, 5)
            item = WorkItem(data=f"task-{i}", priority=priority)
            ppc.put(item)
            print(f"Produced: {item.data} with priority {item.priority}")
            time.sleep(0.1)
    
    def consumer_func():
        while True:
            item = ppc.get(timeout=1.0)
            if item is None:
                break
            print(f"Consumed: {item.data} (priority: {item.priority})")
            ppc.task_done()
            time.sleep(0.2)
    
    # Start producer and consumer threads
    producer_thread = threading.Thread(target=producer_func)
    consumer_thread = threading.Thread(target=consumer_func)
    
    producer_thread.start()
    consumer_thread.start()
    
    producer_thread.join()
    ppc.close()
    consumer_thread.join()


if __name__ == "__main__":
    demo_basic_producer_consumer()
    demo_priority_producer_consumer()
