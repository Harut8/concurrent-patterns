"""
CSP Select Patterns

Select statement implementations for non-deterministic choice in CSP.
"""

import asyncio
import random
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging

from .channels import Channel


class SelectCase(ABC):
    """Abstract base class for select cases."""
    
    @abstractmethod
    async def is_ready(self) -> bool:
        """Check if this case is ready for execution."""
        pass
    
    @abstractmethod
    async def execute(self) -> Any:
        """Execute this case."""
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """Get description of this case."""
        pass


class SendCase(SelectCase):
    """Select case for sending to a channel."""
    
    def __init__(self, channel: Channel, value: Any, callback: Optional[Callable] = None):
        self.channel = channel
        self.value = value
        self.callback = callback
    
    async def is_ready(self) -> bool:
        """Check if channel is ready for sending."""
        return not self.channel.is_closed() and not self.channel.is_full()
    
    async def execute(self) -> Any:
        """Send value to channel."""
        await self.channel.send(self.value)
        result = f"Sent {self.value} to {self.channel}"
        
        if self.callback:
            if asyncio.iscoroutinefunction(self.callback):
                await self.callback(self.value)
            else:
                self.callback(self.value)
        
        return result
    
    def get_description(self) -> str:
        return f"send {self.value} -> {self.channel}"


class ReceiveCase(SelectCase):
    """Select case for receiving from a channel."""
    
    def __init__(self, channel: Channel, callback: Optional[Callable[[Any], None]] = None):
        self.channel = channel
        self.callback = callback
    
    async def is_ready(self) -> bool:
        """Check if channel has data to receive."""
        return not self.channel.is_empty() or self.channel.is_closed()
    
    async def execute(self) -> Any:
        """Receive value from channel."""
        try:
            value = await self.channel.receive()
            
            if self.callback:
                if asyncio.iscoroutinefunction(self.callback):
                    await self.callback(value)
                else:
                    self.callback(value)
            
            return value
        except Exception as e:
            return f"Channel closed: {e}"
    
    def get_description(self) -> str:
        return f"receive <- {self.channel}"


class TimeoutCase(SelectCase):
    """Select case for timeout."""
    
    def __init__(self, timeout: float, callback: Optional[Callable] = None):
        self.timeout = timeout
        self.callback = callback
        self.start_time = None
    
    async def is_ready(self) -> bool:
        """Check if timeout has elapsed."""
        if self.start_time is None:
            self.start_time = asyncio.get_event_loop().time()
            return False
        
        elapsed = asyncio.get_event_loop().time() - self.start_time
        return elapsed >= self.timeout
    
    async def execute(self) -> Any:
        """Execute timeout callback."""
        if self.callback:
            if asyncio.iscoroutinefunction(self.callback):
                await self.callback()
            else:
                self.callback()
        
        return f"Timeout after {self.timeout}s"
    
    def get_description(self) -> str:
        return f"timeout {self.timeout}s"


class DefaultCase(SelectCase):
    """Default case for select (non-blocking)."""
    
    def __init__(self, callback: Optional[Callable] = None):
        self.callback = callback
    
    async def is_ready(self) -> bool:
        """Default case is always ready."""
        return True
    
    async def execute(self) -> Any:
        """Execute default callback."""
        if self.callback:
            if asyncio.iscoroutinefunction(self.callback):
                await self.callback()
            else:
                self.callback()
        
        return "Default case executed"
    
    def get_description(self) -> str:
        return "default"


@dataclass
class SelectResult:
    """Result of a select operation."""
    case_index: int
    case_description: str
    result: Any
    execution_time: float


class Select:
    """
    Select statement for non-deterministic choice among channel operations.
    """
    
    def __init__(self, *cases: SelectCase):
        self.cases = list(cases)
        self.has_default = any(isinstance(case, DefaultCase) for case in cases)
    
    def add_case(self, case: SelectCase):
        """Add a case to the select statement."""
        self.cases.append(case)
        if isinstance(case, DefaultCase):
            self.has_default = True
    
    async def execute(self, shuffle: bool = True) -> SelectResult:
        """
        Execute the select statement.
        
        Args:
            shuffle: Whether to randomize case order for fairness
        """
        start_time = asyncio.get_event_loop().time()
        
        # Create list of cases with indices
        indexed_cases = list(enumerate(self.cases))
        
        if shuffle:
            random.shuffle(indexed_cases)
        
        # If we have a default case, try other cases first
        if self.has_default:
            non_default_cases = [(i, case) for i, case in indexed_cases 
                               if not isinstance(case, DefaultCase)]
            default_cases = [(i, case) for i, case in indexed_cases 
                           if isinstance(case, DefaultCase)]
            
            # Try non-default cases first
            for original_index, case in non_default_cases:
                if await case.is_ready():
                    result = await case.execute()
                    execution_time = asyncio.get_event_loop().time() - start_time
                    
                    return SelectResult(
                        case_index=original_index,
                        case_description=case.get_description(),
                        result=result,
                        execution_time=execution_time
                    )
            
            # If no other case is ready, execute default
            if default_cases:
                original_index, case = default_cases[0]
                result = await case.execute()
                execution_time = asyncio.get_event_loop().time() - start_time
                
                return SelectResult(
                    case_index=original_index,
                    case_description=case.get_description(),
                    result=result,
                    execution_time=execution_time
                )
        
        # No default case - wait for any case to become ready
        while True:
            for original_index, case in indexed_cases:
                if await case.is_ready():
                    result = await case.execute()
                    execution_time = asyncio.get_event_loop().time() - start_time
                    
                    return SelectResult(
                        case_index=original_index,
                        case_description=case.get_description(),
                        result=result,
                        execution_time=execution_time
                    )
            
            # Small delay to prevent busy waiting
            await asyncio.sleep(0.001)


class SelectBuilder:
    """Builder for creating select statements."""
    
    def __init__(self):
        self.cases: List[SelectCase] = []
    
    def send(self, channel: Channel, value: Any, callback: Optional[Callable] = None):
        """Add a send case."""
        self.cases.append(SendCase(channel, value, callback))
        return self
    
    def receive(self, channel: Channel, callback: Optional[Callable[[Any], None]] = None):
        """Add a receive case."""
        self.cases.append(ReceiveCase(channel, callback))
        return self
    
    def timeout(self, seconds: float, callback: Optional[Callable] = None):
        """Add a timeout case."""
        self.cases.append(TimeoutCase(seconds, callback))
        return self
    
    def default(self, callback: Optional[Callable] = None):
        """Add a default case."""
        self.cases.append(DefaultCase(callback))
        return self
    
    def build(self) -> Select:
        """Build the select statement."""
        return Select(*self.cases)


# Utility functions
def select(*cases: SelectCase) -> Select:
    """Create a select statement with the given cases."""
    return Select(*cases)


def send_case(channel: Channel, value: Any, callback: Optional[Callable] = None) -> SendCase:
    """Create a send case."""
    return SendCase(channel, value, callback)


def receive_case(channel: Channel, callback: Optional[Callable[[Any], None]] = None) -> ReceiveCase:
    """Create a receive case."""
    return ReceiveCase(channel, callback)


def timeout_case(seconds: float, callback: Optional[Callable] = None) -> TimeoutCase:
    """Create a timeout case."""
    return TimeoutCase(seconds, callback)


def default_case(callback: Optional[Callable] = None) -> DefaultCase:
    """Create a default case."""
    return DefaultCase(callback)


# Advanced select patterns
class SelectLoop:
    """
    Continuous select loop for handling multiple channels.
    """
    
    def __init__(self, *cases: SelectCase):
        self.cases = list(cases)
        self.running = False
        self.iteration_count = 0
        self.results: List[SelectResult] = []
    
    async def run(self, max_iterations: Optional[int] = None):
        """Run the select loop."""
        self.running = True
        self.iteration_count = 0
        
        try:
            while self.running:
                if max_iterations and self.iteration_count >= max_iterations:
                    break
                
                select_stmt = Select(*self.cases)
                result = await select_stmt.execute()
                self.results.append(result)
                self.iteration_count += 1
                
                logging.debug(f"Select iteration {self.iteration_count}: {result.case_description}")
                
        except Exception as e:
            logging.error(f"Select loop error: {e}")
            raise
        finally:
            self.running = False
    
    def stop(self):
        """Stop the select loop."""
        self.running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get select loop statistics."""
        if not self.results:
            return {"iterations": 0, "case_distribution": {}}
        
        case_counts = {}
        total_time = 0.0
        
        for result in self.results:
            case_counts[result.case_description] = case_counts.get(result.case_description, 0) + 1
            total_time += result.execution_time
        
        return {
            "iterations": self.iteration_count,
            "case_distribution": case_counts,
            "average_execution_time": total_time / len(self.results) if self.results else 0,
            "total_execution_time": total_time
        }


# Example usage and demonstrations
async def demo_basic_select():
    """Demonstrate basic select functionality."""
    print("=== Basic Select Demo ===")
    
    from .channels import BufferedChannel
    
    # Create channels
    ch1 = BufferedChannel(capacity=1)
    ch2 = BufferedChannel(capacity=1)
    
    # Pre-fill one channel
    await ch1.send("hello")
    
    # Create select statement
    select_stmt = select(
        receive_case(ch1, lambda x: print(f"Received from ch1: {x}")),
        receive_case(ch2, lambda x: print(f"Received from ch2: {x}")),
        timeout_case(1.0, lambda: print("Timeout occurred"))
    )
    
    # Execute select
    result = await select_stmt.execute()
    print(f"Selected: {result.case_description} -> {result.result}")


async def demo_select_with_default():
    """Demonstrate select with default case."""
    print("\n=== Select with Default Demo ===")
    
    from .channels import BufferedChannel
    
    # Create empty channels
    ch1 = BufferedChannel(capacity=1)
    ch2 = BufferedChannel(capacity=1)
    
    # Create select with default
    select_stmt = select(
        receive_case(ch1),
        receive_case(ch2),
        default_case(lambda: print("No channels ready, executing default"))
    )
    
    # Execute select (should hit default)
    result = await select_stmt.execute()
    print(f"Selected: {result.case_description}")


async def demo_select_builder():
    """Demonstrate select builder pattern."""
    print("\n=== Select Builder Demo ===")
    
    from .channels import BufferedChannel
    
    # Create channels
    input_ch = BufferedChannel(capacity=2)
    output_ch = BufferedChannel(capacity=2)
    
    # Pre-fill input channel
    await input_ch.send("data1")
    await input_ch.send("data2")
    
    # Build select statement
    select_stmt = (SelectBuilder()
                   .receive(input_ch, lambda x: print(f"Processing: {x}"))
                   .send(output_ch, "processed", lambda x: print(f"Sent: {x}"))
                   .timeout(0.5, lambda: print("Operation timed out"))
                   .build())
    
    # Execute multiple times
    for i in range(3):
        result = await select_stmt.execute()
        print(f"Iteration {i+1}: {result.case_description}")


async def demo_select_loop():
    """Demonstrate select loop."""
    print("\n=== Select Loop Demo ===")
    
    from .channels import BufferedChannel
    
    # Create channels
    work_ch = BufferedChannel(capacity=5)
    result_ch = BufferedChannel(capacity=5)
    
    # Fill work channel
    for i in range(5):
        await work_ch.send(f"task-{i}")
    
    # Create select loop
    loop = SelectLoop(
        receive_case(work_ch, lambda x: print(f"Processing work: {x}")),
        receive_case(result_ch, lambda x: print(f"Got result: {x}")),
        timeout_case(2.0, lambda: print("No activity"))
    )
    
    # Run loop for limited iterations
    await loop.run(max_iterations=7)
    
    # Print statistics
    stats = loop.get_stats()
    print(f"Loop stats: {stats}")


async def main():
    """Run all select demonstrations."""
    await demo_basic_select()
    await demo_select_with_default()
    await demo_select_builder()
    await demo_select_loop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
