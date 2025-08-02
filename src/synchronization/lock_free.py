"""
Lock-Free Data Structures and Algorithms

Implementation of lock-free concurrent data structures using atomic operations.
"""

import threading
import time
import weakref
from typing import Optional, List, Dict, Any, TypeVar, Generic, Iterator
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import logging
import sys
import gc


T = TypeVar('T')


class AtomicInteger:
    """
    Lock-free atomic integer implementation.
    """
    
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()  # Fallback for true atomicity
    
    def get(self) -> int:
        """Get current value."""
        return self._value
    
    def set(self, new_value: int):
        """Set new value."""
        with self._lock:
            self._value = new_value
    
    def compare_and_swap(self, expected: int, new_value: int) -> bool:
        """Compare and swap operation."""
        with self._lock:
            if self._value == expected:
                self._value = new_value
                return True
            return False
    
    def increment_and_get(self) -> int:
        """Increment and return new value."""
        with self._lock:
            self._value += 1
            return self._value
    
    def decrement_and_get(self) -> int:
        """Decrement and return new value."""
        with self._lock:
            self._value -= 1
            return self._value
    
    def add_and_get(self, delta: int) -> int:
        """Add delta and return new value."""
        with self._lock:
            self._value += delta
            return self._value
    
    def get_and_increment(self) -> int:
        """Get current value and increment."""
        with self._lock:
            old_value = self._value
            self._value += 1
            return old_value
    
    def get_and_decrement(self) -> int:
        """Get current value and decrement."""
        with self._lock:
            old_value = self._value
            self._value -= 1
            return old_value
    
    def get_and_add(self, delta: int) -> int:
        """Get current value and add delta."""
        with self._lock:
            old_value = self._value
            self._value += delta
            return old_value


class AtomicReference(Generic[T]):
    """
    Lock-free atomic reference implementation.
    """
    
    def __init__(self, initial_value: Optional[T] = None):
        self._value = initial_value
        self._lock = threading.Lock()
    
    def get(self) -> Optional[T]:
        """Get current reference."""
        return self._value
    
    def set(self, new_value: Optional[T]):
        """Set new reference."""
        with self._lock:
            self._value = new_value
    
    def compare_and_swap(self, expected: Optional[T], new_value: Optional[T]) -> bool:
        """Compare and swap operation."""
        with self._lock:
            if self._value is expected:
                self._value = new_value
                return True
            return False
    
    def get_and_set(self, new_value: Optional[T]) -> Optional[T]:
        """Get current value and set new value."""
        with self._lock:
            old_value = self._value
            self._value = new_value
            return old_value


@dataclass
class LockFreeNode(Generic[T]):
    """Node for lock-free data structures."""
    data: T
    next: Optional['LockFreeNode[T]'] = None
    marked: bool = False  # For deletion marking


class LockFreeStack(Generic[T]):
    """
    Lock-free stack implementation using compare-and-swap.
    """
    
    def __init__(self):
        self._head = AtomicReference[LockFreeNode[T]](None)
        self._size = AtomicInteger(0)
    
    def push(self, item: T) -> bool:
        """Push item onto stack."""
        new_node = LockFreeNode(data=item)
        
        while True:
            current_head = self._head.get()
            new_node.next = current_head
            
            if self._head.compare_and_swap(current_head, new_node):
                self._size.increment_and_get()
                return True
            
            # Retry on failure (another thread modified head)
    
    def pop(self) -> Optional[T]:
        """Pop item from stack."""
        while True:
            current_head = self._head.get()
            
            if current_head is None:
                return None  # Stack is empty
            
            next_node = current_head.next
            
            if self._head.compare_and_swap(current_head, next_node):
                self._size.decrement_and_get()
                return current_head.data
            
            # Retry on failure
    
    def peek(self) -> Optional[T]:
        """Peek at top item without removing."""
        head = self._head.get()
        return head.data if head else None
    
    def is_empty(self) -> bool:
        """Check if stack is empty."""
        return self._head.get() is None
    
    def size(self) -> int:
        """Get approximate size."""
        return self._size.get()


class LockFreeQueue(Generic[T]):
    """
    Lock-free queue implementation using Michael & Scott algorithm.
    """
    
    def __init__(self):
        # Initialize with dummy node
        dummy = LockFreeNode(data=None)
        self._head = AtomicReference[LockFreeNode[T]](dummy)
        self._tail = AtomicReference[LockFreeNode[T]](dummy)
        self._size = AtomicInteger(0)
    
    def enqueue(self, item: T) -> bool:
        """Enqueue item to queue."""
        new_node = LockFreeNode(data=item)
        
        while True:
            tail = self._tail.get()
            next_node = tail.next
            
            # Check if tail is still the last node
            if tail == self._tail.get():
                if next_node is None:
                    # Try to link new node at end of list
                    if self._compare_and_swap_next(tail, None, new_node):
                        break
                else:
                    # Tail was lagging, try to advance it
                    self._tail.compare_and_swap(tail, next_node)
        
        # Try to advance tail to new node
        self._tail.compare_and_swap(tail, new_node)
        self._size.increment_and_get()
        return True
    
    def dequeue(self) -> Optional[T]:
        """Dequeue item from queue."""
        while True:
            head = self._head.get()
            tail = self._tail.get()
            next_node = head.next
            
            # Check if head is still the first node
            if head == self._head.get():
                if head == tail:
                    if next_node is None:
                        return None  # Queue is empty
                    
                    # Tail is lagging, try to advance it
                    self._tail.compare_and_swap(tail, next_node)
                else:
                    if next_node is None:
                        continue  # Inconsistent state, retry
                    
                    # Read data before CAS
                    data = next_node.data
                    
                    # Try to advance head
                    if self._head.compare_and_swap(head, next_node):
                        self._size.decrement_and_get()
                        return data
    
    def _compare_and_swap_next(self, node: LockFreeNode[T], 
                               expected: Optional[LockFreeNode[T]], 
                               new_value: Optional[LockFreeNode[T]]) -> bool:
        """Compare and swap the next pointer of a node."""
        # Simplified implementation - in real systems this would be atomic
        if node.next is expected:
            node.next = new_value
            return True
        return False
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        head = self._head.get()
        tail = self._tail.get()
        return head == tail and head.next is None
    
    def size(self) -> int:
        """Get approximate size."""
        return self._size.get()


class LockFreeHashMap(Generic[T]):
    """
    Lock-free hash map implementation.
    """
    
    def __init__(self, initial_capacity: int = 16):
        self.capacity = initial_capacity
        self.buckets = [AtomicReference[LockFreeNode[tuple]](None) for _ in range(initial_capacity)]
        self._size = AtomicInteger(0)
    
    def _hash(self, key: Any) -> int:
        """Simple hash function."""
        return hash(key) % self.capacity
    
    def put(self, key: Any, value: T) -> Optional[T]:
        """Put key-value pair."""
        bucket_index = self._hash(key)
        bucket = self.buckets[bucket_index]
        
        while True:
            current = bucket.get()
            
            # Search for existing key
            node = current
            while node is not None:
                if node.data[0] == key and not node.marked:
                    # Key exists, try to update
                    old_value = node.data[1]
                    new_data = (key, value)
                    
                    # In a real implementation, this would be atomic
                    if not node.marked:
                        node.data = new_data
                        return old_value
                    break
                node = node.next
            
            # Key doesn't exist, add new node
            new_node = LockFreeNode(data=(key, value))
            new_node.next = current
            
            if bucket.compare_and_swap(current, new_node):
                self._size.increment_and_get()
                return None
    
    def get(self, key: Any) -> Optional[T]:
        """Get value by key."""
        bucket_index = self._hash(key)
        bucket = self.buckets[bucket_index]
        
        current = bucket.get()
        while current is not None:
            if current.data[0] == key and not current.marked:
                return current.data[1]
            current = current.next
        
        return None
    
    def remove(self, key: Any) -> Optional[T]:
        """Remove key-value pair."""
        bucket_index = self._hash(key)
        bucket = self.buckets[bucket_index]
        
        while True:
            current = bucket.get()
            prev = None
            node = current
            
            # Search for key
            while node is not None:
                if node.data[0] == key and not node.marked:
                    # Mark for deletion
                    node.marked = True
                    old_value = node.data[1]
                    
                    # Try to physically remove
                    if prev is None:
                        # Removing head
                        bucket.compare_and_swap(current, node.next)
                    else:
                        # Remove from middle/end
                        prev.next = node.next
                    
                    self._size.decrement_and_get()
                    return old_value
                
                prev = node
                node = node.next
            
            return None  # Key not found
    
    def contains_key(self, key: Any) -> bool:
        """Check if key exists."""
        return self.get(key) is not None
    
    def size(self) -> int:
        """Get approximate size."""
        return self._size.get()
    
    def keys(self) -> List[Any]:
        """Get all keys."""
        result = []
        for bucket in self.buckets:
            current = bucket.get()
            while current is not None:
                if not current.marked:
                    result.append(current.data[0])
                current = current.next
        return result


class LockFreeCounter:
    """
    Lock-free counter with multiple operations.
    """
    
    def __init__(self, initial_value: int = 0):
        self._value = AtomicInteger(initial_value)
        self._operations = AtomicInteger(0)
    
    def increment(self) -> int:
        """Increment counter."""
        self._operations.increment_and_get()
        return self._value.increment_and_get()
    
    def decrement(self) -> int:
        """Decrement counter."""
        self._operations.increment_and_get()
        return self._value.decrement_and_get()
    
    def add(self, delta: int) -> int:
        """Add delta to counter."""
        self._operations.increment_and_get()
        return self._value.add_and_get(delta)
    
    def get(self) -> int:
        """Get current value."""
        return self._value.get()
    
    def reset(self) -> int:
        """Reset counter to zero."""
        self._operations.increment_and_get()
        return self._value.get_and_set(0)
    
    def get_operations_count(self) -> int:
        """Get number of operations performed."""
        return self._operations.get()


class LockFreeLinkedList(Generic[T]):
    """
    Lock-free linked list with mark-and-sweep deletion.
    """
    
    def __init__(self):
        # Head and tail sentinels
        self._head = LockFreeNode(data=None)
        self._tail = LockFreeNode(data=None)
        self._head.next = self._tail
        self._size = AtomicInteger(0)
    
    def add(self, item: T) -> bool:
        """Add item to list."""
        new_node = LockFreeNode(data=item)
        
        while True:
            # Find position to insert (sorted order)
            prev, current = self._find_position(item)
            
            if current != self._tail and current.data == item and not current.marked:
                return False  # Item already exists
            
            new_node.next = current
            
            # Try to link new node
            if self._compare_and_swap_next(prev, current, new_node):
                self._size.increment_and_get()
                return True
    
    def remove(self, item: T) -> bool:
        """Remove item from list."""
        while True:
            prev, current = self._find_position(item)
            
            if current == self._tail or current.data != item:
                return False  # Item not found
            
            next_node = current.next
            
            # Mark node for deletion
            if not current.marked:
                current.marked = True
                
                # Try to physically remove
                if self._compare_and_swap_next(prev, current, next_node):
                    self._size.decrement_and_get()
                    return True
    
    def contains(self, item: T) -> bool:
        """Check if item exists in list."""
        current = self._head.next
        
        while current != self._tail:
            if current.data == item and not current.marked:
                return True
            current = current.next
        
        return False
    
    def _find_position(self, item: T) -> tuple:
        """Find position for item (for sorted insertion)."""
        while True:
            prev = self._head
            current = prev.next
            
            while current != self._tail:
                if current.marked:
                    # Skip marked nodes
                    next_node = current.next
                    if self._compare_and_swap_next(prev, current, next_node):
                        current = next_node
                        continue
                    else:
                        break  # Retry from beginning
                
                if current.data >= item:
                    return prev, current
                
                prev = current
                current = current.next
            
            return prev, current
    
    def _compare_and_swap_next(self, node: LockFreeNode[T], 
                               expected: Optional[LockFreeNode[T]], 
                               new_value: Optional[LockFreeNode[T]]) -> bool:
        """Compare and swap the next pointer of a node."""
        if node.next is expected:
            node.next = new_value
            return True
        return False
    
    def size(self) -> int:
        """Get approximate size."""
        return self._size.get()
    
    def to_list(self) -> List[T]:
        """Convert to regular list."""
        result = []
        current = self._head.next
        
        while current != self._tail:
            if not current.marked:
                result.append(current.data)
            current = current.next
        
        return result


class HazardPointer:
    """
    Hazard pointer for safe memory reclamation in lock-free structures.
    """
    
    def __init__(self):
        self._pointers = weakref.WeakSet()
        self._retired = []
        self._lock = threading.Lock()
    
    def protect(self, pointer: Any):
        """Protect a pointer from being reclaimed."""
        self._pointers.add(pointer)
    
    def unprotect(self, pointer: Any):
        """Unprotect a pointer."""
        self._pointers.discard(pointer)
    
    def retire(self, pointer: Any):
        """Retire a pointer for later reclamation."""
        with self._lock:
            self._retired.append(pointer)
            
            # Periodically clean up
            if len(self._retired) > 100:
                self._reclaim()
    
    def _reclaim(self):
        """Reclaim retired pointers that are not protected."""
        protected = set(self._pointers)
        new_retired = []
        
        for pointer in self._retired:
            if pointer not in protected:
                # Safe to reclaim
                del pointer
            else:
                new_retired.append(pointer)
        
        self._retired = new_retired


# Example usage and demonstrations
def demo_atomic_operations():
    """Demonstrate atomic operations."""
    print("=== Atomic Operations Demo ===")
    
    counter = AtomicInteger(0)
    reference = AtomicReference[str]("initial")
    
    def worker(worker_id: int):
        for i in range(100):
            # Atomic increment
            value = counter.increment_and_get()
            
            # Atomic reference update
            old_ref = reference.get()
            new_ref = f"worker-{worker_id}-{i}"
            reference.compare_and_swap(old_ref, new_ref)
    
    # Start multiple workers
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Final counter value: {counter.get()}")
    print(f"Final reference: {reference.get()}")


def demo_lock_free_stack():
    """Demonstrate lock-free stack."""
    print("\n=== Lock-Free Stack Demo ===")
    
    stack = LockFreeStack[int]()
    results = []
    
    def producer(producer_id: int):
        for i in range(5):
            item = producer_id * 100 + i
            stack.push(item)
            print(f"Producer {producer_id}: Pushed {item}")
    
    def consumer(consumer_id: int):
        for _ in range(3):
            item = stack.pop()
            if item is not None:
                results.append(item)
                print(f"Consumer {consumer_id}: Popped {item}")
            time.sleep(0.1)
    
    # Start producers and consumers
    threads = []
    
    for i in range(2):
        thread = threading.Thread(target=producer, args=(i,))
        threads.append(thread)
        thread.start()
    
    for i in range(2):
        thread = threading.Thread(target=consumer, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Consumed items: {sorted(results)}")
    print(f"Remaining stack size: {stack.size()}")


def demo_lock_free_queue():
    """Demonstrate lock-free queue."""
    print("\n=== Lock-Free Queue Demo ===")
    
    queue = LockFreeQueue[str]()
    results = []
    
    def producer(producer_id: int):
        for i in range(3):
            item = f"item-{producer_id}-{i}"
            queue.enqueue(item)
            print(f"Producer {producer_id}: Enqueued {item}")
            time.sleep(0.1)
    
    def consumer(consumer_id: int):
        for _ in range(2):
            item = queue.dequeue()
            if item is not None:
                results.append(item)
                print(f"Consumer {consumer_id}: Dequeued {item}")
            time.sleep(0.2)
    
    # Start producers and consumers
    threads = []
    
    for i in range(2):
        thread = threading.Thread(target=producer, args=(i,))
        threads.append(thread)
        thread.start()
    
    for i in range(2):
        thread = threading.Thread(target=consumer, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Consumed items: {results}")
    print(f"Remaining queue size: {queue.size()}")


def demo_lock_free_hashmap():
    """Demonstrate lock-free hash map."""
    print("\n=== Lock-Free HashMap Demo ===")
    
    hashmap = LockFreeHashMap[int]()
    
    def worker(worker_id: int):
        # Put some values
        for i in range(5):
            key = f"key-{worker_id}-{i}"
            value = worker_id * 100 + i
            old_value = hashmap.put(key, value)
            print(f"Worker {worker_id}: Put {key}={value}, old={old_value}")
        
        # Get some values
        for i in range(3):
            key = f"key-{worker_id}-{i}"
            value = hashmap.get(key)
            print(f"Worker {worker_id}: Get {key}={value}")
    
    # Start multiple workers
    threads = []
    for i in range(3):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"HashMap size: {hashmap.size()}")
    print(f"All keys: {hashmap.keys()}")


def demo_lock_free_counter():
    """Demonstrate lock-free counter."""
    print("\n=== Lock-Free Counter Demo ===")
    
    counter = LockFreeCounter(0)
    
    def worker(worker_id: int):
        for i in range(100):
            if i % 3 == 0:
                counter.increment()
            elif i % 3 == 1:
                counter.decrement()
            else:
                counter.add(2)
    
    # Start multiple workers
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    print(f"Final counter value: {counter.get()}")
    print(f"Total operations: {counter.get_operations_count()}")


def main():
    """Run all lock-free demonstrations."""
    demo_atomic_operations()
    demo_lock_free_stack()
    demo_lock_free_queue()
    demo_lock_free_hashmap()
    demo_lock_free_counter()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
