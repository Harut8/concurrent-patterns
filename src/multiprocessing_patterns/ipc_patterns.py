"""
Inter-Process Communication (IPC) Patterns

Various IPC mechanisms for multiprocessing communication.
"""

import multiprocessing as mp
import time
import pickle
import threading
from typing import Any, Optional, Dict, List, Callable, Union
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import logging
import queue
import socket
import struct


class IPCType(Enum):
    """Types of IPC mechanisms."""
    QUEUE = "queue"
    PIPE = "pipe"
    SHARED_MEMORY = "shared_memory"
    SOCKET = "socket"
    FILE = "file"
    SEMAPHORE = "semaphore"
    EVENT = "event"


@dataclass
class IPCMessage:
    """Standard IPC message format."""
    sender_id: str
    receiver_id: str
    message_type: str
    data: Any
    timestamp: float
    correlation_id: Optional[str] = None


class IPCChannel(ABC):
    """Abstract base class for IPC channels."""
    
    @abstractmethod
    def send(self, message: IPCMessage, timeout: Optional[float] = None) -> bool:
        """Send a message through the channel."""
        pass
    
    @abstractmethod
    def receive(self, timeout: Optional[float] = None) -> Optional[IPCMessage]:
        """Receive a message from the channel."""
        pass
    
    @abstractmethod
    def close(self):
        """Close the channel."""
        pass
    
    @abstractmethod
    def is_closed(self) -> bool:
        """Check if channel is closed."""
        pass


class QueueChannel(IPCChannel):
    """
    IPC channel using multiprocessing.Queue.
    """
    
    def __init__(self, maxsize: int = 0):
        self.queue = mp.Queue(maxsize=maxsize)
        self._closed = False
    
    def send(self, message: IPCMessage, timeout: Optional[float] = None) -> bool:
        """Send message via queue."""
        if self._closed:
            return False
        
        try:
            if timeout is None:
                self.queue.put(message)
            else:
                self.queue.put(message, timeout=timeout)
            return True
        except queue.Full:
            return False
        except Exception as e:
            logging.error(f"Queue send error: {e}")
            return False
    
    def receive(self, timeout: Optional[float] = None) -> Optional[IPCMessage]:
        """Receive message from queue."""
        if self._closed:
            return None
        
        try:
            if timeout is None:
                return self.queue.get()
            else:
                return self.queue.get(timeout=timeout)
        except queue.Empty:
            return None
        except Exception as e:
            logging.error(f"Queue receive error: {e}")
            return None
    
    def close(self):
        """Close the queue."""
        self._closed = True
        # Note: multiprocessing.Queue doesn't have a close method
    
    def is_closed(self) -> bool:
        """Check if queue is closed."""
        return self._closed


class PipeChannel(IPCChannel):
    """
    IPC channel using multiprocessing.Pipe.
    """
    
    def __init__(self, duplex: bool = True):
        self.conn1, self.conn2 = mp.Pipe(duplex=duplex)
        self.send_conn = self.conn1
        self.recv_conn = self.conn2 if duplex else self.conn1
        self._closed = False
    
    def send(self, message: IPCMessage, timeout: Optional[float] = None) -> bool:
        """Send message via pipe."""
        if self._closed:
            return False
        
        try:
            if timeout is None:
                self.send_conn.send(message)
            else:
                # Pipe doesn't support timeout directly
                self.send_conn.send(message)
            return True
        except Exception as e:
            logging.error(f"Pipe send error: {e}")
            return False
    
    def receive(self, timeout: Optional[float] = None) -> Optional[IPCMessage]:
        """Receive message from pipe."""
        if self._closed:
            return None
        
        try:
            if timeout is None:
                if self.recv_conn.poll():
                    return self.recv_conn.recv()
            else:
                if self.recv_conn.poll(timeout):
                    return self.recv_conn.recv()
            return None
        except Exception as e:
            logging.error(f"Pipe receive error: {e}")
            return None
    
    def close(self):
        """Close the pipe."""
        self._closed = True
        try:
            self.conn1.close()
            self.conn2.close()
        except Exception:
            pass
    
    def is_closed(self) -> bool:
        """Check if pipe is closed."""
        return self._closed
    
    def get_other_end(self):
        """Get the other end of the pipe for the receiving process."""
        return self.recv_conn


class SocketChannel(IPCChannel):
    """
    IPC channel using Unix domain sockets.
    """
    
    def __init__(self, socket_path: str, is_server: bool = True):
        self.socket_path = socket_path
        self.is_server = is_server
        self.socket = None
        self.connection = None
        self._closed = False
        
        if is_server:
            self._setup_server()
        else:
            self._setup_client()
    
    def _setup_server(self):
        """Setup server socket."""
        try:
            import os
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
            
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.bind(self.socket_path)
            self.socket.listen(1)
            
        except Exception as e:
            logging.error(f"Socket server setup error: {e}")
    
    def _setup_client(self):
        """Setup client socket."""
        try:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.connect(self.socket_path)
            self.connection = self.socket
        except Exception as e:
            logging.error(f"Socket client setup error: {e}")
    
    def accept_connection(self):
        """Accept a client connection (server only)."""
        if self.is_server and self.socket:
            self.connection, _ = self.socket.accept()
    
    def send(self, message: IPCMessage, timeout: Optional[float] = None) -> bool:
        """Send message via socket."""
        if self._closed or not self.connection:
            return False
        
        try:
            # Serialize message
            data = pickle.dumps(message)
            size = len(data)
            
            # Send size first, then data
            self.connection.sendall(struct.pack('!I', size))
            self.connection.sendall(data)
            return True
        except Exception as e:
            logging.error(f"Socket send error: {e}")
            return False
    
    def receive(self, timeout: Optional[float] = None) -> Optional[IPCMessage]:
        """Receive message from socket."""
        if self._closed or not self.connection:
            return None
        
        try:
            if timeout:
                self.connection.settimeout(timeout)
            
            # Receive size first
            size_data = self.connection.recv(4)
            if len(size_data) < 4:
                return None
            
            size = struct.unpack('!I', size_data)[0]
            
            # Receive data
            data = b''
            while len(data) < size:
                chunk = self.connection.recv(size - len(data))
                if not chunk:
                    return None
                data += chunk
            
            # Deserialize message
            message = pickle.loads(data)
            return message
            
        except socket.timeout:
            return None
        except Exception as e:
            logging.error(f"Socket receive error: {e}")
            return None
    
    def close(self):
        """Close the socket."""
        self._closed = True
        try:
            if self.connection:
                self.connection.close()
            if self.socket:
                self.socket.close()
            
            # Clean up socket file
            if self.is_server:
                import os
                if os.path.exists(self.socket_path):
                    os.unlink(self.socket_path)
        except Exception:
            pass
    
    def is_closed(self) -> bool:
        """Check if socket is closed."""
        return self._closed


class MessageBroker:
    """
    Message broker for routing messages between processes.
    """
    
    def __init__(self):
        self.channels: Dict[str, IPCChannel] = {}
        self.subscribers: Dict[str, List[str]] = {}  # topic -> list of process_ids
        self.running = False
        self.message_queue = mp.Queue()
    
    def register_channel(self, process_id: str, channel: IPCChannel):
        """Register a channel for a process."""
        self.channels[process_id] = channel
    
    def subscribe(self, process_id: str, topic: str):
        """Subscribe a process to a topic."""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if process_id not in self.subscribers[topic]:
            self.subscribers[topic].append(process_id)
    
    def unsubscribe(self, process_id: str, topic: str):
        """Unsubscribe a process from a topic."""
        if topic in self.subscribers:
            self.subscribers[topic] = [pid for pid in self.subscribers[topic] if pid != process_id]
    
    def publish(self, topic: str, message: IPCMessage):
        """Publish a message to a topic."""
        if topic in self.subscribers:
            for process_id in self.subscribers[topic]:
                if process_id in self.channels:
                    channel = self.channels[process_id]
                    channel.send(message)
    
    def route_message(self, message: IPCMessage):
        """Route a message to its destination."""
        if message.receiver_id in self.channels:
            channel = self.channels[message.receiver_id]
            channel.send(message)
        else:
            logging.warning(f"No channel found for receiver: {message.receiver_id}")
    
    def start_broker(self):
        """Start the message broker."""
        self.running = True
        
        def broker_loop():
            while self.running:
                try:
                    message = self.message_queue.get(timeout=1.0)
                    self.route_message(message)
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f"Broker error: {e}")
        
        broker_thread = threading.Thread(target=broker_loop)
        broker_thread.daemon = True
        broker_thread.start()
    
    def stop_broker(self):
        """Stop the message broker."""
        self.running = False


class SharedCounter:
    """
    Shared counter using multiprocessing synchronization.
    """
    
    def __init__(self, initial_value: int = 0):
        self.value = mp.Value('i', initial_value)
        self.lock = mp.Lock()
    
    def increment(self, amount: int = 1) -> int:
        """Increment the counter."""
        with self.lock:
            self.value.value += amount
            return self.value.value
    
    def decrement(self, amount: int = 1) -> int:
        """Decrement the counter."""
        with self.lock:
            self.value.value -= amount
            return self.value.value
    
    def get(self) -> int:
        """Get the current value."""
        with self.lock:
            return self.value.value
    
    def set(self, value: int):
        """Set the counter value."""
        with self.lock:
            self.value.value = value


class ProcessBarrier:
    """
    Barrier synchronization for multiple processes.
    """
    
    def __init__(self, num_processes: int):
        self.num_processes = num_processes
        self.count = mp.Value('i', 0)
        self.generation = mp.Value('i', 0)
        self.lock = mp.Lock()
        self.condition = mp.Condition(self.lock)
    
    def wait(self, timeout: Optional[float] = None):
        """Wait for all processes to reach the barrier."""
        with self.condition:
            current_generation = self.generation.value
            self.count.value += 1
            
            if self.count.value == self.num_processes:
                # Last process to arrive
                self.count.value = 0
                self.generation.value += 1
                self.condition.notify_all()
            else:
                # Wait for other processes
                while self.generation.value == current_generation:
                    if not self.condition.wait(timeout=timeout):
                        raise TimeoutError("Barrier wait timed out")


# Example worker processes
def producer_process(channel: IPCChannel, process_id: str, num_items: int):
    """Producer process that sends messages."""
    for i in range(num_items):
        message = IPCMessage(
            sender_id=process_id,
            receiver_id="consumer",
            message_type="data",
            data=f"item-{i}",
            timestamp=time.time()
        )
        
        if channel.send(message):
            print(f"Producer {process_id} sent: item-{i}")
        else:
            print(f"Producer {process_id} failed to send: item-{i}")
        
        time.sleep(0.1)


def consumer_process(channel: IPCChannel, process_id: str):
    """Consumer process that receives messages."""
    while True:
        message = channel.receive(timeout=2.0)
        if message:
            print(f"Consumer {process_id} received: {message.data} from {message.sender_id}")
        else:
            print(f"Consumer {process_id} timeout")
            break


def worker_process(shared_counter: SharedCounter, barrier: ProcessBarrier, worker_id: int):
    """Worker process that increments shared counter."""
    print(f"Worker {worker_id} starting")
    
    # Do some work
    for i in range(5):
        count = shared_counter.increment()
        print(f"Worker {worker_id} incremented counter to {count}")
        time.sleep(0.1)
    
    # Wait at barrier
    print(f"Worker {worker_id} waiting at barrier")
    barrier.wait()
    print(f"Worker {worker_id} passed barrier")


# Example usage and demonstrations
def demo_queue_ipc():
    """Demonstrate queue-based IPC."""
    print("=== Queue IPC Demo ===")
    
    # Create queue channel
    channel = QueueChannel(maxsize=10)
    
    # Create processes
    producer = mp.Process(target=producer_process, args=(channel, "producer-1", 5))
    consumer = mp.Process(target=consumer_process, args=(channel, "consumer-1"))
    
    # Start processes
    producer.start()
    consumer.start()
    
    # Wait for completion
    producer.join()
    consumer.join(timeout=5)
    
    if consumer.is_alive():
        consumer.terminate()
    
    channel.close()
    print("Queue IPC demo completed")


def demo_pipe_ipc():
    """Demonstrate pipe-based IPC."""
    print("\n=== Pipe IPC Demo ===")
    
    # Create pipe channel
    channel = PipeChannel(duplex=True)
    other_end = channel.get_other_end()
    
    def pipe_worker(conn, worker_id):
        for i in range(3):
            message = IPCMessage(
                sender_id=f"worker-{worker_id}",
                receiver_id="main",
                message_type="result",
                data=f"result-{i}",
                timestamp=time.time()
            )
            conn.send(message)
            time.sleep(0.1)
        conn.close()
    
    # Create worker process
    worker = mp.Process(target=pipe_worker, args=(other_end, 1))
    worker.start()
    
    # Receive messages
    for _ in range(3):
        message = channel.receive(timeout=2.0)
        if message:
            print(f"Main received: {message.data} from {message.sender_id}")
    
    worker.join()
    channel.close()
    print("Pipe IPC demo completed")


def demo_shared_synchronization():
    """Demonstrate shared counter and barrier."""
    print("\n=== Shared Synchronization Demo ===")
    
    # Create shared objects
    counter = SharedCounter(0)
    barrier = ProcessBarrier(3)
    
    # Create worker processes
    workers = []
    for i in range(3):
        worker = mp.Process(target=worker_process, args=(counter, barrier, i))
        workers.append(worker)
        worker.start()
    
    # Wait for all workers
    for worker in workers:
        worker.join()
    
    print(f"Final counter value: {counter.get()}")
    print("Shared synchronization demo completed")


def main():
    """Run all IPC demonstrations."""
    demo_queue_ipc()
    demo_pipe_ipc()
    demo_shared_synchronization()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
