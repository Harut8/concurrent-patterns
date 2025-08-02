"""
Shared Memory Patterns

Shared memory implementations for efficient inter-process communication.
"""

import multiprocessing as mp
import mmap
import struct
import time
import os
import tempfile
from typing import Any, Optional, List, Dict, Union, Callable, TypeVar
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import threading
import numpy as np
import logging


T = TypeVar('T')


class SharedMemoryType(Enum):
    """Types of shared memory."""
    VALUE = "value"
    ARRAY = "array"
    NUMPY_ARRAY = "numpy_array"
    MEMORY_MAP = "memory_map"
    CUSTOM_STRUCT = "custom_struct"


@dataclass
class SharedMemoryInfo:
    """Information about shared memory segment."""
    name: str
    size: int
    memory_type: SharedMemoryType
    created_at: float
    access_count: int = 0


class SharedMemoryManager:
    """
    Manager for shared memory segments.
    """
    
    def __init__(self):
        self.segments: Dict[str, Any] = {}
        self.info: Dict[str, SharedMemoryInfo] = {}
        self.lock = mp.Lock()
    
    def create_value(self, name: str, typecode: str, value: Any) -> mp.Value:
        """Create a shared value."""
        with self.lock:
            if name in self.segments:
                raise ValueError(f"Shared memory '{name}' already exists")
            
            shared_value = mp.Value(typecode, value)
            self.segments[name] = shared_value
            self.info[name] = SharedMemoryInfo(
                name=name,
                size=struct.calcsize(typecode),
                memory_type=SharedMemoryType.VALUE,
                created_at=time.time()
            )
            
            return shared_value
    
    def create_array(self, name: str, typecode: str, size_or_initializer: Union[int, List]) -> mp.Array:
        """Create a shared array."""
        with self.lock:
            if name in self.segments:
                raise ValueError(f"Shared memory '{name}' already exists")
            
            shared_array = mp.Array(typecode, size_or_initializer)
            self.segments[name] = shared_array
            
            if isinstance(size_or_initializer, int):
                size = size_or_initializer * struct.calcsize(typecode)
            else:
                size = len(size_or_initializer) * struct.calcsize(typecode)
            
            self.info[name] = SharedMemoryInfo(
                name=name,
                size=size,
                memory_type=SharedMemoryType.ARRAY,
                created_at=time.time()
            )
            
            return shared_array
    
    def create_numpy_array(self, name: str, shape: tuple, dtype: np.dtype) -> np.ndarray:
        """Create a shared numpy array."""
        with self.lock:
            if name in self.segments:
                raise ValueError(f"Shared memory '{name}' already exists")
            
            # Calculate size
            size = np.prod(shape) * np.dtype(dtype).itemsize
            
            # Create raw shared memory
            shared_mem = mp.RawArray('b', int(size))
            
            # Create numpy array view
            np_array = np.frombuffer(shared_mem, dtype=dtype).reshape(shape)
            
            self.segments[name] = (shared_mem, np_array)
            self.info[name] = SharedMemoryInfo(
                name=name,
                size=size,
                memory_type=SharedMemoryType.NUMPY_ARRAY,
                created_at=time.time()
            )
            
            return np_array
    
    def get(self, name: str) -> Optional[Any]:
        """Get a shared memory segment."""
        with self.lock:
            if name in self.segments:
                self.info[name].access_count += 1
                segment = self.segments[name]
                
                # Return numpy array if it's a numpy shared memory
                if self.info[name].memory_type == SharedMemoryType.NUMPY_ARRAY:
                    return segment[1]  # Return the numpy array view
                
                return segment
            return None
    
    def remove(self, name: str) -> bool:
        """Remove a shared memory segment."""
        with self.lock:
            if name in self.segments:
                del self.segments[name]
                del self.info[name]
                return True
            return False
    
    def list_segments(self) -> Dict[str, SharedMemoryInfo]:
        """List all shared memory segments."""
        with self.lock:
            return self.info.copy()


class SharedBuffer:
    """
    Circular buffer in shared memory for producer-consumer patterns.
    """
    
    def __init__(self, name: str, size: int, item_size: int = 8):
        self.name = name
        self.size = size
        self.item_size = item_size
        self.buffer_size = size * item_size
        
        # Create shared memory for buffer
        self.buffer = mp.RawArray('b', self.buffer_size)
        
        # Control variables
        self.head = mp.Value('i', 0)      # Write position
        self.tail = mp.Value('i', 0)      # Read position
        self.count = mp.Value('i', 0)     # Number of items
        
        # Synchronization
        self.lock = mp.Lock()
        self.not_empty = mp.Condition(self.lock)
        self.not_full = mp.Condition(self.lock)
    
    def put(self, data: bytes, timeout: Optional[float] = None) -> bool:
        """Put data into the buffer."""
        if len(data) > self.item_size:
            raise ValueError(f"Data size {len(data)} exceeds item size {self.item_size}")
        
        with self.not_full:
            # Wait for space
            while self.count.value >= self.size:
                if not self.not_full.wait(timeout=timeout):
                    return False
            
            # Write data
            start_pos = self.head.value * self.item_size
            padded_data = data.ljust(self.item_size, b'\x00')
            
            for i, byte in enumerate(padded_data):
                self.buffer[start_pos + i] = byte
            
            # Update head and count
            self.head.value = (self.head.value + 1) % self.size
            self.count.value += 1
            
            # Notify waiting consumers
            self.not_empty.notify()
            
            return True
    
    def get(self, timeout: Optional[float] = None) -> Optional[bytes]:
        """Get data from the buffer."""
        with self.not_empty:
            # Wait for data
            while self.count.value == 0:
                if not self.not_empty.wait(timeout=timeout):
                    return None
            
            # Read data
            start_pos = self.tail.value * self.item_size
            data = bytes(self.buffer[start_pos:start_pos + self.item_size])
            
            # Remove padding
            data = data.rstrip(b'\x00')
            
            # Update tail and count
            self.tail.value = (self.tail.value + 1) % self.size
            self.count.value -= 1
            
            # Notify waiting producers
            self.not_full.notify()
            
            return data
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        with self.lock:
            return self.count.value == 0
    
    def is_full(self) -> bool:
        """Check if buffer is full."""
        with self.lock:
            return self.count.value >= self.size
    
    def size_info(self) -> Dict[str, int]:
        """Get buffer size information."""
        with self.lock:
            return {
                'capacity': self.size,
                'count': self.count.value,
                'free': self.size - self.count.value
            }


class MemoryMappedFile:
    """
    Memory-mapped file for shared data access.
    """
    
    def __init__(self, filename: str, size: int, create: bool = True):
        self.filename = filename
        self.size = size
        self.file = None
        self.mmap = None
        
        if create:
            self._create_file()
        else:
            self._open_file()
    
    def _create_file(self):
        """Create and initialize the memory-mapped file."""
        # Create file with specified size
        with open(self.filename, 'wb') as f:
            f.write(b'\x00' * self.size)
        
        # Open for memory mapping
        self.file = open(self.filename, 'r+b')
        self.mmap = mmap.mmap(self.file.fileno(), self.size)
    
    def _open_file(self):
        """Open existing memory-mapped file."""
        self.file = open(self.filename, 'r+b')
        self.mmap = mmap.mmap(self.file.fileno(), 0)
    
    def write(self, offset: int, data: bytes):
        """Write data at specified offset."""
        if offset + len(data) > self.size:
            raise ValueError("Write would exceed file size")
        
        self.mmap[offset:offset + len(data)] = data
        self.mmap.flush()
    
    def read(self, offset: int, length: int) -> bytes:
        """Read data from specified offset."""
        if offset + length > self.size:
            raise ValueError("Read would exceed file size")
        
        return self.mmap[offset:offset + length]
    
    def write_struct(self, offset: int, format_str: str, *values):
        """Write structured data."""
        data = struct.pack(format_str, *values)
        self.write(offset, data)
    
    def read_struct(self, offset: int, format_str: str) -> tuple:
        """Read structured data."""
        size = struct.calcsize(format_str)
        data = self.read(offset, size)
        return struct.unpack(format_str, data)
    
    def close(self):
        """Close the memory-mapped file."""
        if self.mmap:
            self.mmap.close()
        if self.file:
            self.file.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SharedMatrix:
    """
    Shared matrix for mathematical computations across processes.
    """
    
    def __init__(self, name: str, rows: int, cols: int, dtype: np.dtype = np.float64):
        self.name = name
        self.rows = rows
        self.cols = cols
        self.dtype = dtype
        
        # Create shared memory for matrix data
        self.size = rows * cols * np.dtype(dtype).itemsize
        self.raw_array = mp.RawArray('b', self.size)
        
        # Create numpy array view
        self.matrix = np.frombuffer(self.raw_array, dtype=dtype).reshape((rows, cols))
        
        # Synchronization for thread-safe operations
        self.lock = mp.Lock()
    
    def set_element(self, row: int, col: int, value: float):
        """Set a single element."""
        with self.lock:
            self.matrix[row, col] = value
    
    def get_element(self, row: int, col: int) -> float:
        """Get a single element."""
        with self.lock:
            return self.matrix[row, col]
    
    def set_row(self, row: int, values: np.ndarray):
        """Set an entire row."""
        with self.lock:
            self.matrix[row, :] = values
    
    def get_row(self, row: int) -> np.ndarray:
        """Get an entire row."""
        with self.lock:
            return self.matrix[row, :].copy()
    
    def set_column(self, col: int, values: np.ndarray):
        """Set an entire column."""
        with self.lock:
            self.matrix[:, col] = values
    
    def get_column(self, col: int) -> np.ndarray:
        """Get an entire column."""
        with self.lock:
            return self.matrix[:, col].copy()
    
    def fill(self, value: float):
        """Fill matrix with a value."""
        with self.lock:
            self.matrix.fill(value)
    
    def copy_from(self, source: np.ndarray):
        """Copy data from another array."""
        if source.shape != (self.rows, self.cols):
            raise ValueError("Shape mismatch")
        
        with self.lock:
            self.matrix[:] = source
    
    def copy_to(self, destination: np.ndarray):
        """Copy data to another array."""
        if destination.shape != (self.rows, self.cols):
            raise ValueError("Shape mismatch")
        
        with self.lock:
            destination[:] = self.matrix
    
    def get_shape(self) -> tuple:
        """Get matrix shape."""
        return (self.rows, self.cols)
    
    def sum(self) -> float:
        """Calculate sum of all elements."""
        with self.lock:
            return float(np.sum(self.matrix))
    
    def mean(self) -> float:
        """Calculate mean of all elements."""
        with self.lock:
            return float(np.mean(self.matrix))


# Example worker functions
def matrix_worker(shared_matrix: SharedMatrix, start_row: int, end_row: int, worker_id: int):
    """Worker that processes a range of matrix rows."""
    print(f"Worker {worker_id} processing rows {start_row} to {end_row}")
    
    for row in range(start_row, end_row):
        # Simulate computation - fill row with worker_id
        values = np.full(shared_matrix.cols, worker_id, dtype=shared_matrix.dtype)
        shared_matrix.set_row(row, values)
        time.sleep(0.01)  # Simulate work
    
    print(f"Worker {worker_id} completed")


def buffer_producer(shared_buffer: SharedBuffer, producer_id: int, num_items: int):
    """Producer that puts items into shared buffer."""
    for i in range(num_items):
        data = f"item-{producer_id}-{i}".encode()
        
        if shared_buffer.put(data, timeout=2.0):
            print(f"Producer {producer_id} put: item-{i}")
        else:
            print(f"Producer {producer_id} timeout on item-{i}")
        
        time.sleep(0.1)


def buffer_consumer(shared_buffer: SharedBuffer, consumer_id: int):
    """Consumer that gets items from shared buffer."""
    while True:
        data = shared_buffer.get(timeout=2.0)
        if data:
            print(f"Consumer {consumer_id} got: {data.decode()}")
        else:
            print(f"Consumer {consumer_id} timeout")
            break


# Example usage and demonstrations
def demo_shared_values_arrays():
    """Demonstrate shared values and arrays."""
    print("=== Shared Values and Arrays Demo ===")
    
    manager = SharedMemoryManager()
    
    # Create shared value
    counter = manager.create_value("counter", 'i', 0)
    
    # Create shared array
    data_array = manager.create_array("data", 'f', [1.0, 2.0, 3.0, 4.0, 5.0])
    
    def worker(shared_counter, shared_array, worker_id):
        # Increment counter
        with shared_counter.get_lock():
            shared_counter.value += 1
            print(f"Worker {worker_id} incremented counter to {shared_counter.value}")
        
        # Modify array
        with shared_array.get_lock():
            shared_array[worker_id % len(shared_array)] *= 2
            print(f"Worker {worker_id} modified array: {list(shared_array[:])}")
    
    # Create worker processes
    processes = []
    for i in range(3):
        p = mp.Process(target=worker, args=(counter, data_array, i))
        processes.append(p)
        p.start()
    
    # Wait for completion
    for p in processes:
        p.join()
    
    print(f"Final counter: {counter.value}")
    print(f"Final array: {list(data_array[:])}")
    
    # List segments
    segments = manager.list_segments()
    for name, info in segments.items():
        print(f"Segment {name}: {info}")


def demo_shared_matrix():
    """Demonstrate shared matrix processing."""
    print("\n=== Shared Matrix Demo ===")
    
    # Create shared matrix
    matrix = SharedMatrix("test_matrix", rows=6, cols=4)
    matrix.fill(0.0)
    
    # Create worker processes to fill different parts
    processes = []
    rows_per_worker = 2
    
    for i in range(3):
        start_row = i * rows_per_worker
        end_row = start_row + rows_per_worker
        p = mp.Process(target=matrix_worker, args=(matrix, start_row, end_row, i + 1))
        processes.append(p)
        p.start()
    
    # Wait for completion
    for p in processes:
        p.join()
    
    # Print results
    print("Final matrix:")
    for row in range(matrix.rows):
        row_data = matrix.get_row(row)
        print(f"Row {row}: {row_data}")
    
    print(f"Matrix sum: {matrix.sum()}")
    print(f"Matrix mean: {matrix.mean()}")


def demo_shared_buffer():
    """Demonstrate shared circular buffer."""
    print("\n=== Shared Buffer Demo ===")
    
    # Create shared buffer
    buffer = SharedBuffer("test_buffer", size=5, item_size=32)
    
    # Create producer and consumer processes
    producer1 = mp.Process(target=buffer_producer, args=(buffer, 1, 5))
    producer2 = mp.Process(target=buffer_producer, args=(buffer, 2, 5))
    consumer1 = mp.Process(target=buffer_consumer, args=(buffer, 1))
    consumer2 = mp.Process(target=buffer_consumer, args=(buffer, 2))
    
    # Start processes
    producer1.start()
    producer2.start()
    consumer1.start()
    consumer2.start()
    
    # Wait for producers to finish
    producer1.join()
    producer2.join()
    
    # Give consumers time to finish
    consumer1.join(timeout=3)
    consumer2.join(timeout=3)
    
    # Terminate if still running
    if consumer1.is_alive():
        consumer1.terminate()
    if consumer2.is_alive():
        consumer2.terminate()
    
    print(f"Final buffer info: {buffer.size_info()}")


def demo_memory_mapped_file():
    """Demonstrate memory-mapped file."""
    print("\n=== Memory-Mapped File Demo ===")
    
    filename = tempfile.mktemp(suffix='.dat')
    
    try:
        # Create memory-mapped file
        with MemoryMappedFile(filename, size=1024) as mmf:
            # Write some data
            mmf.write_struct(0, 'iii', 42, 100, 200)
            mmf.write(12, b"Hello, shared memory!")
            
            # Read data back
            values = mmf.read_struct(0, 'iii')
            text = mmf.read(12, 21).decode().rstrip('\x00')
            
            print(f"Read values: {values}")
            print(f"Read text: {text}")
    
    finally:
        # Clean up
        if os.path.exists(filename):
            os.unlink(filename)


def main():
    """Run all shared memory demonstrations."""
    demo_shared_values_arrays()
    demo_shared_matrix()
    demo_shared_buffer()
    demo_memory_mapped_file()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
