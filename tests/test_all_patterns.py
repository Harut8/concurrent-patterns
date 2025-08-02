"""
Comprehensive Test Suite for Concurrency Patterns

Tests all implemented concurrency patterns to ensure correctness and reliability.
"""

import pytest
import asyncio
import threading
import time
import multiprocessing as mp
from typing import List, Any
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import patterns to test
from threading_patterns.producer_consumer import ProducerConsumer, BoundedBuffer
from threading_patterns.thread_pool import BackpressureThreadPool, PriorityThreadPool
from asyncio_patterns.async_producer_consumer import AsyncProducerConsumer, AsyncBoundedQueue
from actor_model.actor_system import ActorSystem, Actor, UserMessage
from csp_patterns.channels import SynchronousChannel, BufferedChannel, BroadcastChannel
from synchronization.reader_writer import BasicReadWriteLock, FairReadWriteLock
from multiprocessing_patterns.process_pool import ProcessPool


class TestThreadingPatterns:
    """Test threading-based concurrency patterns."""
    
    def test_bounded_buffer(self):
        """Test bounded buffer implementation."""
        buffer = BoundedBuffer(capacity=3)
        
        # Test basic put/get
        assert buffer.put("item1", timeout=1.0) == True
        assert buffer.put("item2", timeout=1.0) == True
        assert buffer.put("item3", timeout=1.0) == True
        
        # Buffer should be full
        assert buffer.is_full() == True
        assert buffer.put("item4", timeout=0.1) == False
        
        # Test get
        assert buffer.get(timeout=1.0) == "item1"
        assert buffer.get(timeout=1.0) == "item2"
        assert buffer.size() == 1
        
        # Test close
        buffer.close()
        assert buffer.get(timeout=0.1) is None
    
    def test_producer_consumer(self):
        """Test producer-consumer pattern."""
        pc = ProducerConsumer(buffer_capacity=5)
        
        # Add producers and consumers
        pc.add_producer(10, lambda: f"data-{time.time()}")
        pc.add_consumer(lambda item: f"processed-{item}")
        
        # Run simulation
        pc.start()
        pc.wait_for_completion(timeout=5.0)
        
        # Check statistics
        stats = pc.get_statistics()
        assert stats["total_produced"] > 0
        assert stats["total_consumed"] > 0
        assert stats["producers"] == 1
        assert stats["consumers"] == 1
    
    def test_backpressure_thread_pool(self):
        """Test backpressure thread pool."""
        pool = BackpressureThreadPool(max_workers=2, max_queue_size=5)
        
        def simple_task(x):
            time.sleep(0.01)
            return x * 2
        
        # Submit tasks
        futures = []
        for i in range(10):
            future = pool.submit(simple_task, i)
            if future:
                futures.append(future)
        
        # Wait for results
        results = []
        for future in futures:
            try:
                result = future.result(timeout=2.0)
                results.append(result)
            except Exception:
                pass
        
        assert len(results) > 0
        
        # Check stats
        stats = pool.get_stats()
        assert stats["max_workers"] == 2
        assert stats["tasks_submitted"] > 0
        
        pool.shutdown()
    
    def test_priority_thread_pool(self):
        """Test priority thread pool."""
        pool = PriorityThreadPool(max_workers=2)
        
        def task_with_id(task_id):
            return f"completed-{task_id}"
        
        # Submit tasks with different priorities
        futures = []
        for i in range(5):
            priority = 5 - i  # Higher number = higher priority
            future = pool.submit(task_with_id, i, priority=priority)
            futures.append((i, future))
        
        # Collect results
        results = []
        for task_id, future in futures:
            result = future.result(timeout=2.0)
            results.append((task_id, result))
        
        assert len(results) == 5
        pool.shutdown()


class TestAsyncioPatterns:
    """Test asyncio-based concurrency patterns."""
    
    @pytest.mark.asyncio
    async def test_async_bounded_queue(self):
        """Test async bounded queue."""
        from asyncio_patterns.async_producer_consumer import BackpressureStrategy
        
        queue = AsyncBoundedQueue(maxsize=3, backpressure_strategy=BackpressureStrategy.BLOCK)
        
        # Test basic operations
        assert await queue.put("item1") == True
        assert await queue.put("item2") == True
        assert await queue.put("item3") == True
        
        # Test get
        item = await queue.get()
        assert item == "item1"
        
        # Test stats
        stats = queue.get_stats()
        assert stats["maxsize"] == 3
        assert stats["items_put"] == 3
        assert stats["items_got"] == 1
        
        queue.close()
    
    @pytest.mark.asyncio
    async def test_async_producer_consumer(self):
        """Test async producer-consumer pattern."""
        pc = AsyncProducerConsumer(buffer_capacity=10)
        
        # Run simulation
        await pc.run_simulation(duration=2.0, producers=2, consumers=2)
        
        # Check statistics
        stats = pc.get_statistics()
        assert stats["producers"] == 2
        assert stats["consumers"] == 2
        assert stats["total_produced"] > 0
        assert stats["total_consumed"] > 0


class TestActorModel:
    """Test actor model implementation."""
    
    class TestActor(Actor):
        def __init__(self):
            super().__init__()
            self.received_messages = []
        
        async def receive(self, message):
            if isinstance(message, UserMessage):
                self.received_messages.append(message.content)
                return f"processed-{message.content}"
            return None
    
    @pytest.mark.asyncio
    async def test_actor_system(self):
        """Test basic actor system functionality."""
        system = ActorSystem("test-system")
        await system.start()
        
        try:
            # Create actor
            actor_ref = system.actor_of(self.TestActor, "test-actor")
            
            # Send messages
            response = await actor_ref.ask("hello")
            assert response == "processed-hello"
            
            await actor_ref.tell("world")
            
            # Check system stats
            stats = system.get_actor_stats()
            assert stats["total_actors"] == 1
            assert stats["is_running"] == True
            
        finally:
            await system.shutdown()
    
    @pytest.mark.asyncio
    async def test_actor_communication(self):
        """Test actor-to-actor communication."""
        system = ActorSystem("comm-test")
        await system.start()
        
        try:
            # Create multiple actors
            actor1_ref = system.actor_of(self.TestActor, "actor1")
            actor2_ref = system.actor_of(self.TestActor, "actor2")
            
            # Test communication
            response1 = await actor1_ref.ask("message1")
            response2 = await actor2_ref.ask("message2")
            
            assert response1 == "processed-message1"
            assert response2 == "processed-message2"
            
        finally:
            await system.shutdown()


class TestCSPPatterns:
    """Test CSP (Communicating Sequential Processes) patterns."""
    
    @pytest.mark.asyncio
    async def test_synchronous_channel(self):
        """Test synchronous channel communication."""
        ch = SynchronousChannel("test-sync")
        
        async def sender():
            for i in range(3):
                success = await ch.send(f"msg-{i}")
                assert success == True
            ch.close()
        
        async def receiver():
            messages = []
            while True:
                msg = await ch.receive(timeout=1.0)
                if msg is None:
                    break
                messages.append(msg)
            return messages
        
        # Run sender and receiver concurrently
        sender_task = asyncio.create_task(sender())
        receiver_task = asyncio.create_task(receiver())
        
        await sender_task
        messages = await receiver_task
        
        assert len(messages) == 3
        assert messages[0] == "msg-0"
        
        # Check stats
        stats = ch.get_stats()
        assert stats["send_count"] == 3
        assert stats["receive_count"] == 3
    
    @pytest.mark.asyncio
    async def test_buffered_channel(self):
        """Test buffered channel communication."""
        ch = BufferedChannel(capacity=3, name="test-buffered")
        
        # Send without receiver (should work due to buffer)
        assert await ch.send("item1") == True
        assert await ch.send("item2") == True
        assert await ch.send("item3") == True
        
        # Buffer should be full
        assert ch.is_full() == True
        
        # Receive items
        item1 = await ch.receive()
        item2 = await ch.receive()
        
        assert item1 == "item1"
        assert item2 == "item2"
        assert ch.size() == 1
        
        ch.close()
    
    @pytest.mark.asyncio
    async def test_broadcast_channel(self):
        """Test broadcast channel communication."""
        ch = BroadcastChannel("test-broadcast")
        
        # Create receivers
        receiver1 = ch.create_receiver()
        receiver2 = ch.create_receiver()
        
        async def sender():
            await asyncio.sleep(0.1)  # Let receivers register
            for i in range(3):
                await ch.send(f"broadcast-{i}")
            ch.close()
        
        async def receiver_task(receiver, receiver_id):
            messages = []
            while True:
                msg = await receiver.receive(timeout=2.0)
                if msg is None:
                    break
                messages.append(msg)
            return receiver_id, messages
        
        # Run sender and receivers
        tasks = [
            asyncio.create_task(sender()),
            asyncio.create_task(receiver_task(receiver1, 1)),
            asyncio.create_task(receiver_task(receiver2, 2))
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Check that both receivers got all messages
        _, messages1 = results[1]
        _, messages2 = results[2]
        
        assert len(messages1) == 3
        assert len(messages2) == 3
        assert messages1 == messages2


class TestSynchronization:
    """Test synchronization primitives."""
    
    def test_basic_reader_writer_lock(self):
        """Test basic reader-writer lock."""
        lock = BasicReadWriteLock()
        shared_data = {"value": 0}
        results = {"reads": [], "writes": []}
        
        def reader(reader_id):
            for i in range(3):
                with lock.read_lock():
                    value = shared_data["value"]
                    results["reads"].append((reader_id, value))
                    time.sleep(0.01)
        
        def writer(writer_id):
            for i in range(2):
                with lock.write_lock():
                    shared_data["value"] += 1
                    results["writes"].append((writer_id, shared_data["value"]))
                    time.sleep(0.02)
        
        # Start threads
        threads = []
        
        # Start readers
        for i in range(2):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()
        
        # Start writer
        t = threading.Thread(target=writer, args=(0,))
        threads.append(t)
        t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        assert len(results["reads"]) > 0
        assert len(results["writes"]) > 0
        assert shared_data["value"] > 0
    
    def test_fair_reader_writer_lock(self):
        """Test fair reader-writer lock."""
        lock = FairReadWriteLock()
        
        # Test basic acquire/release
        assert lock.acquire_read(timeout=1.0) == True
        assert lock.acquire_read(timeout=1.0) == True  # Multiple readers OK
        
        lock.release_read()
        lock.release_read()
        
        assert lock.acquire_write(timeout=1.0) == True
        lock.release_write()


class TestMultiprocessing:
    """Test multiprocessing patterns."""
    
    def test_process_pool(self):
        """Test process pool implementation."""
        def cpu_task(n):
            result = 0
            for i in range(n):
                result += i
            return result
        
        pool = ProcessPool(min_workers=2, max_workers=4)
        pool.start()
        
        try:
            # Submit tasks
            task_ids = []
            for i in range(5):
                task_id = pool.submit(cpu_task, 1000 * (i + 1))
                task_ids.append(task_id)
            
            # Collect results
            results = []
            for _ in range(len(task_ids)):
                result = pool.get_result(timeout=5.0)
                if result and result.success:
                    results.append(result)
            
            assert len(results) > 0
            
            # Check stats
            stats = pool.get_stats()
            assert stats["total_workers"] >= 2
            assert stats["tasks_submitted"] == 5
            
        finally:
            pool.shutdown()


class TestIntegration:
    """Integration tests combining multiple patterns."""
    
    @pytest.mark.asyncio
    async def test_mixed_concurrency(self):
        """Test combining different concurrency approaches."""
        # Use threading for CPU work and asyncio for coordination
        thread_pool = BackpressureThreadPool(max_workers=2, max_queue_size=10)
        
        def cpu_work(n):
            return sum(i * i for i in range(n))
        
        async def coordinator():
            # Submit CPU work to thread pool
            futures = []
            for i in range(5):
                future = thread_pool.submit(cpu_work, 1000)
                if future:
                    futures.append(future)
            
            # Wait for results asynchronously
            results = []
            for future in futures:
                try:
                    result = future.result(timeout=2.0)
                    results.append(result)
                except Exception:
                    pass
            
            return results
        
        try:
            results = await coordinator()
            assert len(results) > 0
        finally:
            thread_pool.shutdown()
    
    def test_pattern_composition(self):
        """Test composing multiple patterns together."""
        # Combine producer-consumer with thread pool
        pc = ProducerConsumer(buffer_capacity=10)
        thread_pool = BackpressureThreadPool(max_workers=2, max_queue_size=5)
        
        def process_item(item):
            # Simulate processing
            time.sleep(0.01)
            return f"processed-{item}"
        
        # Producer generates work for thread pool
        def producer_func():
            return f"work-{time.time()}"
        
        def consumer_func(item):
            # Submit to thread pool for processing
            future = thread_pool.submit(process_item, item)
            if future:
                return future.result(timeout=1.0)
            return None
        
        try:
            pc.add_producer(5, producer_func)
            pc.add_consumer(consumer_func)
            
            pc.start()
            pc.wait_for_completion(timeout=3.0)
            
            stats = pc.get_statistics()
            assert stats["total_produced"] > 0
            
        finally:
            thread_pool.shutdown()


# Test configuration
pytest_plugins = ['pytest_asyncio']


def test_all_imports():
    """Test that all modules can be imported without errors."""
    # This test ensures all our patterns are importable
    try:
        from threading_patterns import *
        from asyncio_patterns import *
        from actor_model import *
        from csp_patterns import *
        from synchronization import *
        from multiprocessing_patterns import *
        from reactive_patterns import *
    except ImportError as e:
        pytest.fail(f"Import error: {e}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
