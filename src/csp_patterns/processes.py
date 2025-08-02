"""
CSP Process Patterns

Process abstractions for Communicating Sequential Processes.
"""

import asyncio
import time
from typing import AsyncIterator, Optional, Callable, Any, List, Dict, Set
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import logging

from .channels import Channel, SynchronousChannel, BufferedChannel


class ProcessState(Enum):
    """Process states."""
    CREATED = "created"
    RUNNING = "running"
    BLOCKED = "blocked"
    TERMINATED = "terminated"
    ERROR = "error"


@dataclass
class ProcessInfo:
    """Process information."""
    process_id: str
    name: str
    state: ProcessState
    created_at: float
    started_at: Optional[float] = None
    terminated_at: Optional[float] = None
    error: Optional[Exception] = None


class Process(ABC):
    """
    Abstract base class for CSP processes.
    """
    
    def __init__(self, name: str = None):
        self.name = name or f"process-{id(self)}"
        self.process_id = f"pid-{int(time.time() * 1000000)}"
        self.state = ProcessState.CREATED
        self.created_at = time.time()
        self.started_at = None
        self.terminated_at = None
        self.error = None
        self._task: Optional[asyncio.Task] = None
        self._channels: Set[Channel] = set()
    
    @abstractmethod
    async def run(self):
        """Main process logic - override this method."""
        pass
    
    async def start(self) -> asyncio.Task:
        """Start the process."""
        if self.state != ProcessState.CREATED:
            raise RuntimeError(f"Process {self.name} already started")
        
        self.state = ProcessState.RUNNING
        self.started_at = time.time()
        
        async def _run_wrapper():
            try:
                await self.run()
                self.state = ProcessState.TERMINATED
            except Exception as e:
                self.state = ProcessState.ERROR
                self.error = e
                logging.error(f"Process {self.name} failed: {e}")
                raise
            finally:
                self.terminated_at = time.time()
        
        self._task = asyncio.create_task(_run_wrapper())
        return self._task
    
    async def stop(self, timeout: Optional[float] = None):
        """Stop the process."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        self.state = ProcessState.TERMINATED
        self.terminated_at = time.time()
    
    async def wait(self):
        """Wait for process to complete."""
        if self._task:
            await self._task
    
    def is_running(self) -> bool:
        """Check if process is running."""
        return self.state == ProcessState.RUNNING
    
    def is_terminated(self) -> bool:
        """Check if process is terminated."""
        return self.state in (ProcessState.TERMINATED, ProcessState.ERROR)
    
    def get_info(self) -> ProcessInfo:
        """Get process information."""
        return ProcessInfo(
            process_id=self.process_id,
            name=self.name,
            state=self.state,
            created_at=self.created_at,
            started_at=self.started_at,
            terminated_at=self.terminated_at,
            error=self.error
        )
    
    def add_channel(self, channel: Channel):
        """Add a channel to this process."""
        self._channels.add(channel)
    
    def remove_channel(self, channel: Channel):
        """Remove a channel from this process."""
        self._channels.discard(channel)


class ProducerProcess(Process):
    """
    Producer process that sends data to a channel.
    """
    
    def __init__(self, channel: Channel, data_generator: Callable[[], AsyncIterator], name: str = None):
        super().__init__(name or "producer")
        self.channel = channel
        self.data_generator = data_generator
        self.add_channel(channel)
    
    async def run(self):
        """Produce data and send to channel."""
        try:
            async for data in self.data_generator():
                await self.channel.send(data)
                logging.debug(f"Producer {self.name} sent: {data}")
        finally:
            await self.channel.close()
            logging.info(f"Producer {self.name} finished")


class ConsumerProcess(Process):
    """
    Consumer process that receives data from a channel.
    """
    
    def __init__(self, channel: Channel, processor: Callable[[Any], None], name: str = None):
        super().__init__(name or "consumer")
        self.channel = channel
        self.processor = processor
        self.add_channel(channel)
    
    async def run(self):
        """Consume data from channel."""
        try:
            async for data in self.channel:
                if asyncio.iscoroutinefunction(self.processor):
                    await self.processor(data)
                else:
                    self.processor(data)
                logging.debug(f"Consumer {self.name} processed: {data}")
        except Exception as e:
            logging.error(f"Consumer {self.name} error: {e}")
            raise
        finally:
            logging.info(f"Consumer {self.name} finished")


class FilterProcess(Process):
    """
    Filter process that filters data between channels.
    """
    
    def __init__(self, input_channel: Channel, output_channel: Channel, 
                 predicate: Callable[[Any], bool], name: str = None):
        super().__init__(name or "filter")
        self.input_channel = input_channel
        self.output_channel = output_channel
        self.predicate = predicate
        self.add_channel(input_channel)
        self.add_channel(output_channel)
    
    async def run(self):
        """Filter data from input to output channel."""
        try:
            async for data in self.input_channel:
                if self.predicate(data):
                    await self.output_channel.send(data)
                    logging.debug(f"Filter {self.name} passed: {data}")
                else:
                    logging.debug(f"Filter {self.name} filtered: {data}")
        finally:
            await self.output_channel.close()
            logging.info(f"Filter {self.name} finished")


class TransformProcess(Process):
    """
    Transform process that transforms data between channels.
    """
    
    def __init__(self, input_channel: Channel, output_channel: Channel,
                 transformer: Callable[[Any], Any], name: str = None):
        super().__init__(name or "transform")
        self.input_channel = input_channel
        self.output_channel = output_channel
        self.transformer = transformer
        self.add_channel(input_channel)
        self.add_channel(output_channel)
    
    async def run(self):
        """Transform data from input to output channel."""
        try:
            async for data in self.input_channel:
                if asyncio.iscoroutinefunction(self.transformer):
                    transformed = await self.transformer(data)
                else:
                    transformed = self.transformer(data)
                
                await self.output_channel.send(transformed)
                logging.debug(f"Transform {self.name} transformed: {data} -> {transformed}")
        finally:
            await self.output_channel.close()
            logging.info(f"Transform {self.name} finished")


class SplitterProcess(Process):
    """
    Splitter process that splits data to multiple output channels.
    """
    
    def __init__(self, input_channel: Channel, output_channels: List[Channel], name: str = None):
        super().__init__(name or "splitter")
        self.input_channel = input_channel
        self.output_channels = output_channels
        self.add_channel(input_channel)
        for channel in output_channels:
            self.add_channel(channel)
    
    async def run(self):
        """Split data to multiple output channels."""
        try:
            async for data in self.input_channel:
                # Send to all output channels
                for channel in self.output_channels:
                    await channel.send(data)
                logging.debug(f"Splitter {self.name} split: {data}")
        finally:
            # Close all output channels
            for channel in self.output_channels:
                await channel.close()
            logging.info(f"Splitter {self.name} finished")


class MergerProcess(Process):
    """
    Merger process that merges data from multiple input channels.
    """
    
    def __init__(self, input_channels: List[Channel], output_channel: Channel, name: str = None):
        super().__init__(name or "merger")
        self.input_channels = input_channels
        self.output_channel = output_channel
        for channel in input_channels:
            self.add_channel(channel)
        self.add_channel(output_channel)
    
    async def run(self):
        """Merge data from multiple input channels."""
        try:
            # Create tasks for each input channel
            async def read_channel(channel: Channel):
                async for data in channel:
                    await self.output_channel.send(data)
                    logging.debug(f"Merger {self.name} merged: {data}")
            
            # Run all channel readers concurrently
            tasks = [asyncio.create_task(read_channel(channel)) for channel in self.input_channels]
            await asyncio.gather(*tasks)
        finally:
            await self.output_channel.close()
            logging.info(f"Merger {self.name} finished")


class ProcessNetwork:
    """
    Network of connected CSP processes.
    """
    
    def __init__(self, name: str = None):
        self.name = name or "network"
        self.processes: Dict[str, Process] = {}
        self.channels: Dict[str, Channel] = {}
        self.running_tasks: List[asyncio.Task] = []
    
    def add_process(self, process: Process) -> Process:
        """Add a process to the network."""
        if process.name in self.processes:
            raise ValueError(f"Process {process.name} already exists")
        
        self.processes[process.name] = process
        return process
    
    def add_channel(self, name: str, channel: Channel) -> Channel:
        """Add a channel to the network."""
        if name in self.channels:
            raise ValueError(f"Channel {name} already exists")
        
        self.channels[name] = channel
        return channel
    
    def get_process(self, name: str) -> Optional[Process]:
        """Get a process by name."""
        return self.processes.get(name)
    
    def get_channel(self, name: str) -> Optional[Channel]:
        """Get a channel by name."""
        return self.channels.get(name)
    
    async def start(self):
        """Start all processes in the network."""
        self.running_tasks = []
        
        for process in self.processes.values():
            task = await process.start()
            self.running_tasks.append(task)
        
        logging.info(f"Started {len(self.processes)} processes in network {self.name}")
    
    async def stop(self, timeout: Optional[float] = None):
        """Stop all processes in the network."""
        # Stop all processes
        stop_tasks = []
        for process in self.processes.values():
            stop_tasks.append(asyncio.create_task(process.stop(timeout)))
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        # Cancel running tasks
        for task in self.running_tasks:
            if not task.done():
                task.cancel()
        
        if self.running_tasks:
            await asyncio.gather(*self.running_tasks, return_exceptions=True)
        
        self.running_tasks.clear()
        logging.info(f"Stopped network {self.name}")
    
    async def wait(self):
        """Wait for all processes to complete."""
        if self.running_tasks:
            await asyncio.gather(*self.running_tasks, return_exceptions=True)
    
    def get_network_info(self) -> Dict[str, Any]:
        """Get network information."""
        return {
            "name": self.name,
            "processes": {name: process.get_info() for name, process in self.processes.items()},
            "channels": {name: {"type": type(channel).__name__} for name, channel in self.channels.items()},
            "running_tasks": len(self.running_tasks)
        }


# Example usage and demonstrations
async def demo_producer_consumer():
    """Demonstrate producer-consumer processes."""
    print("=== Producer-Consumer Processes Demo ===")
    
    # Create channel
    channel = BufferedChannel(capacity=5)
    
    # Create data generator
    async def generate_data():
        for i in range(10):
            yield f"data-{i}"
            await asyncio.sleep(0.1)
    
    # Create data processor
    def process_data(data):
        print(f"Processed: {data}")
    
    # Create processes
    producer = ProducerProcess(channel, generate_data, "producer-1")
    consumer = ConsumerProcess(channel, process_data, "consumer-1")
    
    # Create network
    network = ProcessNetwork("producer-consumer-demo")
    network.add_process(producer)
    network.add_process(consumer)
    network.add_channel("main", channel)
    
    # Run network
    await network.start()
    await network.wait()
    
    print("Producer-consumer demo completed")


async def demo_pipeline():
    """Demonstrate process pipeline."""
    print("\n=== Process Pipeline Demo ===")
    
    # Create channels
    input_channel = BufferedChannel(capacity=5)
    filtered_channel = BufferedChannel(capacity=5)
    output_channel = BufferedChannel(capacity=5)
    
    # Create data generator
    async def generate_numbers():
        for i in range(20):
            yield i
            await asyncio.sleep(0.05)
    
    # Create processes
    producer = ProducerProcess(input_channel, generate_numbers, "number-producer")
    filter_proc = FilterProcess(input_channel, filtered_channel, lambda x: x % 2 == 0, "even-filter")
    transform_proc = TransformProcess(filtered_channel, output_channel, lambda x: x ** 2, "square-transform")
    consumer = ConsumerProcess(output_channel, lambda x: print(f"Result: {x}"), "result-consumer")
    
    # Create network
    network = ProcessNetwork("pipeline-demo")
    network.add_process(producer)
    network.add_process(filter_proc)
    network.add_process(transform_proc)
    network.add_process(consumer)
    
    # Run network
    await network.start()
    await network.wait()
    
    print("Pipeline demo completed")


async def demo_splitter_merger():
    """Demonstrate splitter and merger processes."""
    print("\n=== Splitter-Merger Demo ===")
    
    # Create channels
    input_channel = BufferedChannel(capacity=5)
    branch1_channel = BufferedChannel(capacity=5)
    branch2_channel = BufferedChannel(capacity=5)
    output_channel = BufferedChannel(capacity=5)
    
    # Create data generator
    async def generate_tasks():
        for i in range(10):
            yield f"task-{i}"
            await asyncio.sleep(0.1)
    
    # Create processes
    producer = ProducerProcess(input_channel, generate_tasks, "task-producer")
    splitter = SplitterProcess(input_channel, [branch1_channel, branch2_channel], "task-splitter")
    
    # Transform processes for each branch
    transform1 = TransformProcess(branch1_channel, BufferedChannel(capacity=5), 
                                lambda x: f"branch1-{x}", "transform1")
    transform2 = TransformProcess(branch2_channel, BufferedChannel(capacity=5), 
                                lambda x: f"branch2-{x}", "transform2")
    
    # Merger
    merger = MergerProcess([transform1.output_channel, transform2.output_channel], 
                          output_channel, "result-merger")
    
    # Consumer
    consumer = ConsumerProcess(output_channel, lambda x: print(f"Final: {x}"), "final-consumer")
    
    # Create network
    network = ProcessNetwork("splitter-merger-demo")
    network.add_process(producer)
    network.add_process(splitter)
    network.add_process(transform1)
    network.add_process(transform2)
    network.add_process(merger)
    network.add_process(consumer)
    
    # Run network
    await network.start()
    await network.wait()
    
    print("Splitter-merger demo completed")


async def main():
    """Run all process demonstrations."""
    await demo_producer_consumer()
    await demo_pipeline()
    await demo_splitter_merger()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
