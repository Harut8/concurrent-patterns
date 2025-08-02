"""
CSP Design Patterns

Common CSP (Communicating Sequential Processes) design patterns and idioms.
"""

import asyncio
import time
import random
from typing import List, Dict, Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging

from .channels import Channel, BufferedChannel, BroadcastChannel
from .processes import Process, ProcessNetwork
from .select import Select, select, receive_case, send_case, timeout_case


T = TypeVar('T')


# Pattern 1: Fan-Out / Fan-In
class FanOutPattern:
    """
    Fan-out pattern: distribute work from one source to multiple workers.
    """
    
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.input_channel = BufferedChannel(capacity=10)
        self.worker_channels = [BufferedChannel(capacity=5) for _ in range(num_workers)]
        self.output_channel = BufferedChannel(capacity=20)
        self.network = ProcessNetwork("fan-out-pattern")
    
    async def setup(self, worker_func: Callable[[Any], Any]):
        """Setup the fan-out pattern with worker function."""
        
        # Distributor process
        class Distributor(Process):
            def __init__(self, input_ch, worker_channels):
                super().__init__("distributor")
                self.input_ch = input_ch
                self.worker_channels = worker_channels
                self.round_robin_index = 0
            
            async def run(self):
                async for work in self.input_ch:
                    # Round-robin distribution
                    worker_ch = self.worker_channels[self.round_robin_index]
                    await worker_ch.send(work)
                    self.round_robin_index = (self.round_robin_index + 1) % len(self.worker_channels)
                
                # Close worker channels
                for ch in self.worker_channels:
                    await ch.close()
        
        # Worker process
        class Worker(Process):
            def __init__(self, worker_id, input_ch, output_ch, func):
                super().__init__(f"worker-{worker_id}")
                self.input_ch = input_ch
                self.output_ch = output_ch
                self.func = func
            
            async def run(self):
                async for work in self.input_ch:
                    if asyncio.iscoroutinefunction(self.func):
                        result = await self.func(work)
                    else:
                        result = self.func(work)
                    await self.output_ch.send(result)
        
        # Create processes
        distributor = Distributor(self.input_channel, self.worker_channels)
        self.network.add_process(distributor)
        
        for i, worker_ch in enumerate(self.worker_channels):
            worker = Worker(i, worker_ch, self.output_channel, worker_func)
            self.network.add_process(worker)
    
    async def start(self):
        """Start the fan-out pattern."""
        await self.network.start()
    
    async def stop(self):
        """Stop the fan-out pattern."""
        await self.network.stop()
    
    async def submit_work(self, work: Any):
        """Submit work to the pattern."""
        await self.input_channel.send(work)
    
    async def get_result(self) -> Any:
        """Get a result from the pattern."""
        return await self.output_channel.receive()
    
    async def close_input(self):
        """Close the input channel."""
        await self.input_channel.close()


class FanInPattern:
    """
    Fan-in pattern: collect results from multiple sources into one stream.
    """
    
    def __init__(self, num_sources: int = 4):
        self.num_sources = num_sources
        self.source_channels = [BufferedChannel(capacity=5) for _ in range(num_sources)]
        self.output_channel = BufferedChannel(capacity=20)
        self.network = ProcessNetwork("fan-in-pattern")
    
    async def setup(self):
        """Setup the fan-in pattern."""
        
        class Collector(Process):
            def __init__(self, source_channels, output_ch):
                super().__init__("collector")
                self.source_channels = source_channels
                self.output_ch = output_ch
            
            async def run(self):
                # Use select to receive from any source channel
                while True:
                    cases = [receive_case(ch) for ch in self.source_channels if not ch.is_closed()]
                    
                    if not cases:
                        break
                    
                    select_stmt = Select(*cases)
                    result = await select_stmt.execute()
                    
                    if result.result and not isinstance(result.result, str) or "closed" not in str(result.result):
                        await self.output_ch.send(result.result)
        
        collector = Collector(self.source_channels, self.output_channel)
        self.network.add_process(collector)
    
    async def start(self):
        """Start the fan-in pattern."""
        await self.network.start()
    
    async def stop(self):
        """Stop the fan-in pattern."""
        await self.network.stop()
    
    async def submit_to_source(self, source_id: int, data: Any):
        """Submit data to a specific source."""
        if 0 <= source_id < len(self.source_channels):
            await self.source_channels[source_id].send(data)
    
    async def get_result(self) -> Any:
        """Get a result from the pattern."""
        return await self.output_channel.receive()
    
    async def close_sources(self):
        """Close all source channels."""
        for ch in self.source_channels:
            await ch.close()


# Pattern 2: Pipeline
class PipelineStage(ABC):
    """Abstract pipeline stage."""
    
    @abstractmethod
    async def process(self, data: Any) -> Any:
        """Process data in this stage."""
        pass


class Pipeline:
    """
    Pipeline pattern: sequential processing through multiple stages.
    """
    
    def __init__(self, stages: List[PipelineStage]):
        self.stages = stages
        self.channels = []
        self.network = ProcessNetwork("pipeline")
        
        # Create channels between stages
        for i in range(len(stages) + 1):
            self.channels.append(BufferedChannel(capacity=5))
    
    async def setup(self):
        """Setup the pipeline."""
        
        class StageProcess(Process):
            def __init__(self, stage, input_ch, output_ch, stage_id):
                super().__init__(f"stage-{stage_id}")
                self.stage = stage
                self.input_ch = input_ch
                self.output_ch = output_ch
            
            async def run(self):
                async for data in self.input_ch:
                    result = await self.stage.process(data)
                    await self.output_ch.send(result)
                await self.output_ch.close()
        
        # Create stage processes
        for i, stage in enumerate(self.stages):
            stage_process = StageProcess(stage, self.channels[i], self.channels[i + 1], i)
            self.network.add_process(stage_process)
    
    async def start(self):
        """Start the pipeline."""
        await self.network.start()
    
    async def stop(self):
        """Stop the pipeline."""
        await self.network.stop()
    
    async def submit(self, data: Any):
        """Submit data to the pipeline."""
        await self.channels[0].send(data)
    
    async def get_result(self) -> Any:
        """Get result from the pipeline."""
        return await self.channels[-1].receive()
    
    async def close_input(self):
        """Close the input channel."""
        await self.channels[0].close()


# Pattern 3: Worker Pool
class WorkerPool:
    """
    Worker pool pattern: manage a pool of workers processing tasks.
    """
    
    def __init__(self, num_workers: int, worker_func: Callable[[Any], Any]):
        self.num_workers = num_workers
        self.worker_func = worker_func
        self.task_channel = BufferedChannel(capacity=20)
        self.result_channel = BufferedChannel(capacity=20)
        self.network = ProcessNetwork("worker-pool")
    
    async def setup(self):
        """Setup the worker pool."""
        
        class Worker(Process):
            def __init__(self, worker_id, task_ch, result_ch, func):
                super().__init__(f"worker-{worker_id}")
                self.task_ch = task_ch
                self.result_ch = result_ch
                self.func = func
                self.processed_count = 0
            
            async def run(self):
                async for task in self.task_ch:
                    try:
                        if asyncio.iscoroutinefunction(self.func):
                            result = await self.func(task)
                        else:
                            result = self.func(task)
                        
                        await self.result_ch.send({
                            'task': task,
                            'result': result,
                            'worker_id': self.name,
                            'success': True
                        })
                        self.processed_count += 1
                        
                    except Exception as e:
                        await self.result_ch.send({
                            'task': task,
                            'error': str(e),
                            'worker_id': self.name,
                            'success': False
                        })
        
        # Create worker processes
        for i in range(self.num_workers):
            worker = Worker(i, self.task_channel, self.result_channel, self.worker_func)
            self.network.add_process(worker)
    
    async def start(self):
        """Start the worker pool."""
        await self.network.start()
    
    async def stop(self):
        """Stop the worker pool."""
        await self.network.stop()
    
    async def submit_task(self, task: Any):
        """Submit a task to the pool."""
        await self.task_channel.send(task)
    
    async def get_result(self) -> Dict[str, Any]:
        """Get a result from the pool."""
        return await self.result_channel.receive()
    
    async def close_tasks(self):
        """Close the task channel."""
        await self.task_channel.close()


# Pattern 4: Publish-Subscribe
class PubSubBroker:
    """
    Publish-Subscribe pattern using CSP channels.
    """
    
    def __init__(self):
        self.subscribers: Dict[str, List[Channel]] = {}
        self.message_channel = BufferedChannel(capacity=50)
        self.network = ProcessNetwork("pubsub-broker")
    
    async def setup(self):
        """Setup the pub-sub broker."""
        
        class Broker(Process):
            def __init__(self, message_ch, subscribers):
                super().__init__("broker")
                self.message_ch = message_ch
                self.subscribers = subscribers
            
            async def run(self):
                async for message in self.message_ch:
                    topic = message.get('topic')
                    data = message.get('data')
                    
                    if topic in self.subscribers:
                        # Send to all subscribers of this topic
                        for subscriber_ch in self.subscribers[topic]:
                            try:
                                await subscriber_ch.send(data)
                            except Exception as e:
                                logging.error(f"Failed to send to subscriber: {e}")
        
        broker = Broker(self.message_channel, self.subscribers)
        self.network.add_process(broker)
    
    async def start(self):
        """Start the broker."""
        await self.network.start()
    
    async def stop(self):
        """Stop the broker."""
        await self.network.stop()
    
    def subscribe(self, topic: str, subscriber_channel: Channel):
        """Subscribe a channel to a topic."""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(subscriber_channel)
    
    def unsubscribe(self, topic: str, subscriber_channel: Channel):
        """Unsubscribe a channel from a topic."""
        if topic in self.subscribers:
            self.subscribers[topic] = [ch for ch in self.subscribers[topic] if ch != subscriber_channel]
            if not self.subscribers[topic]:
                del self.subscribers[topic]
    
    async def publish(self, topic: str, data: Any):
        """Publish data to a topic."""
        message = {'topic': topic, 'data': data}
        await self.message_channel.send(message)


# Pattern 5: Request-Response
class RequestResponsePattern:
    """
    Request-Response pattern with correlation IDs.
    """
    
    def __init__(self):
        self.request_channel = BufferedChannel(capacity=10)
        self.response_channels: Dict[str, Channel] = {}
        self.network = ProcessNetwork("request-response")
    
    async def setup(self, handler_func: Callable[[Any], Any]):
        """Setup the request-response pattern."""
        
        class RequestHandler(Process):
            def __init__(self, request_ch, response_channels, handler):
                super().__init__("request-handler")
                self.request_ch = request_ch
                self.response_channels = response_channels
                self.handler = handler
            
            async def run(self):
                async for request in self.request_ch:
                    correlation_id = request.get('correlation_id')
                    data = request.get('data')
                    
                    try:
                        if asyncio.iscoroutinefunction(self.handler):
                            result = await self.handler(data)
                        else:
                            result = self.handler(data)
                        
                        response = {'correlation_id': correlation_id, 'result': result, 'success': True}
                    except Exception as e:
                        response = {'correlation_id': correlation_id, 'error': str(e), 'success': False}
                    
                    # Send response to the appropriate channel
                    if correlation_id in self.response_channels:
                        await self.response_channels[correlation_id].send(response)
        
        handler = RequestHandler(self.request_channel, self.response_channels, handler_func)
        self.network.add_process(handler)
    
    async def start(self):
        """Start the pattern."""
        await self.network.start()
    
    async def stop(self):
        """Stop the pattern."""
        await self.network.stop()
    
    async def request(self, data: Any, timeout: float = 5.0) -> Any:
        """Make a request and wait for response."""
        correlation_id = f"req-{int(time.time() * 1000000)}-{random.randint(1000, 9999)}"
        response_channel = BufferedChannel(capacity=1)
        
        # Register response channel
        self.response_channels[correlation_id] = response_channel
        
        try:
            # Send request
            request = {'correlation_id': correlation_id, 'data': data}
            await self.request_channel.send(request)
            
            # Wait for response with timeout
            select_stmt = Select(
                receive_case(response_channel),
                timeout_case(timeout)
            )
            
            result = await select_stmt.execute()
            
            if "timeout" in result.case_description.lower():
                raise TimeoutError(f"Request timed out after {timeout}s")
            
            response = result.result
            if response['success']:
                return response['result']
            else:
                raise Exception(response['error'])
        
        finally:
            # Clean up response channel
            self.response_channels.pop(correlation_id, None)


# Example usage and demonstrations
async def demo_fan_out_fan_in():
    """Demonstrate fan-out and fan-in patterns."""
    print("=== Fan-Out / Fan-In Demo ===")
    
    # Fan-out demo
    fan_out = FanOutPattern(num_workers=3)
    
    def square_work(x):
        time.sleep(0.1)  # Simulate work
        return x ** 2
    
    await fan_out.setup(square_work)
    await fan_out.start()
    
    # Submit work
    for i in range(10):
        await fan_out.submit_work(i)
    
    await fan_out.close_input()
    
    # Collect results
    results = []
    for _ in range(10):
        try:
            result = await asyncio.wait_for(fan_out.get_result(), timeout=2.0)
            results.append(result)
        except asyncio.TimeoutError:
            break
    
    print(f"Fan-out results: {sorted(results)}")
    await fan_out.stop()


async def demo_pipeline():
    """Demonstrate pipeline pattern."""
    print("\n=== Pipeline Demo ===")
    
    class AddStage(PipelineStage):
        def __init__(self, value):
            self.value = value
        
        async def process(self, data):
            return data + self.value
    
    class MultiplyStage(PipelineStage):
        def __init__(self, factor):
            self.factor = factor
        
        async def process(self, data):
            return data * self.factor
    
    # Create pipeline: add 10, then multiply by 2
    pipeline = Pipeline([AddStage(10), MultiplyStage(2)])
    await pipeline.setup()
    await pipeline.start()
    
    # Submit data
    for i in range(5):
        await pipeline.submit(i)
    
    await pipeline.close_input()
    
    # Get results
    results = []
    for _ in range(5):
        try:
            result = await asyncio.wait_for(pipeline.get_result(), timeout=1.0)
            results.append(result)
        except asyncio.TimeoutError:
            break
    
    print(f"Pipeline results: {results}")
    await pipeline.stop()


async def demo_worker_pool():
    """Demonstrate worker pool pattern."""
    print("\n=== Worker Pool Demo ===")
    
    def process_task(task):
        # Simulate work
        time.sleep(0.1)
        return f"processed-{task}"
    
    pool = WorkerPool(num_workers=3, worker_func=process_task)
    await pool.setup()
    await pool.start()
    
    # Submit tasks
    for i in range(8):
        await pool.submit_task(f"task-{i}")
    
    await pool.close_tasks()
    
    # Get results
    results = []
    for _ in range(8):
        try:
            result = await asyncio.wait_for(pool.get_result(), timeout=2.0)
            results.append(result)
        except asyncio.TimeoutError:
            break
    
    print(f"Worker pool processed {len(results)} tasks")
    await pool.stop()


async def demo_pubsub():
    """Demonstrate publish-subscribe pattern."""
    print("\n=== Pub-Sub Demo ===")
    
    broker = PubSubBroker()
    await broker.setup()
    await broker.start()
    
    # Create subscriber channels
    news_subscriber = BufferedChannel(capacity=5)
    sports_subscriber = BufferedChannel(capacity=5)
    
    # Subscribe to topics
    broker.subscribe("news", news_subscriber)
    broker.subscribe("sports", sports_subscriber)
    
    # Publish messages
    await broker.publish("news", "Breaking news!")
    await broker.publish("sports", "Team wins championship!")
    await broker.publish("news", "Weather update")
    
    # Read messages
    news_msg = await news_subscriber.receive()
    sports_msg = await sports_subscriber.receive()
    news_msg2 = await news_subscriber.receive()
    
    print(f"News: {news_msg}, {news_msg2}")
    print(f"Sports: {sports_msg}")
    
    await broker.stop()


async def main():
    """Run all pattern demonstrations."""
    await demo_fan_out_fan_in()
    await demo_pipeline()
    await demo_worker_pool()
    await demo_pubsub()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
