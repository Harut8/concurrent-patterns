# Communicating Sequential Processes (CSP) Patterns

## Table of Contents
1. [Introduction](#introduction)
2. [Theoretical Foundations](#theoretical-foundations)
3. [Channel Communication](#channel-communication)
4. [Process Composition](#process-composition)
5. [Select Operations](#select-operations)
6. [Process Networks](#process-networks)
7. [Deadlock Prevention](#deadlock-prevention)
8. [Performance Patterns](#performance-patterns)

## Introduction

Communicating Sequential Processes (CSP) is a formal language for describing patterns of interaction in concurrent systems. Developed by Tony Hoare in 1978, CSP provides a mathematical framework for modeling concurrent systems through process composition and channel-based communication.

### Core Principles

1. **Sequential Processes**: Each process executes sequentially internally
2. **Synchronous Communication**: Processes communicate via synchronous channels
3. **Composition**: Complex systems built from simple process combinations
4. **Deterministic Choice**: Select operations provide controlled non-determinism
5. **Deadlock Freedom**: Mathematical tools for deadlock analysis and prevention

### CSP vs Other Models

| Aspect | CSP | Actor Model | Shared Memory |
|--------|-----|-------------|---------------|
| Communication | Synchronous Channels | Async Messages | Shared Variables |
| Process Identity | Anonymous | Named Actors | Thread IDs |
| Buffering | Unbuffered (sync) | Buffered Mailboxes | N/A |
| Composition | Algebraic | Hierarchical | Ad-hoc |
| Deadlock Analysis | Formal Methods | Runtime Detection | Manual Analysis |

## Theoretical Foundations

### Process Algebra

CSP processes are defined using algebraic notation:

```
P ::= STOP                    // Deadlock process
    | SKIP                    // Successful termination
    | a → P                   // Prefix (action then process)
    | P □ Q                   // External choice
    | P ⊓ Q                   // Internal choice
    | P ||| Q                 // Interleaving
    | P || Q                  // Parallel composition
    | P \ A                   // Hiding
    | f(P)                    // Renaming
```

### Channel Types

#### Synchronous Channels
- Rendezvous communication (send/receive must happen simultaneously)
- No buffering - direct handoff between processes
- Provides strong synchronization guarantees

#### Buffered Channels
- Asynchronous communication up to buffer capacity
- Decouples sender and receiver timing
- Trade-off between performance and memory usage

## Channel Communication

### Basic Patterns

#### Producer-Consumer
```python
async def producer(channel, items):
    """Producer process that sends items to channel"""
    for item in items:
        await channel.send(item)
        print(f"Produced: {item}")
    channel.close()

async def consumer(channel, process_func):
    """Consumer process that receives and processes items"""
    try:
        while True:
            item = await channel.recv()
            result = await process_func(item)
            print(f"Consumed: {item} -> {result}")
    except ChannelClosedError:
        print("Consumer finished")
```

#### Request-Response
```python
class RequestResponseChannel:
    """Channel pair for request-response communication"""
    def __init__(self):
        self.request_channel = SyncChannel()
        self.response_channel = SyncChannel()
    
    async def send_request(self, request):
        """Send request and wait for response"""
        await self.request_channel.send(request)
        return await self.response_channel.recv()
    
    async def recv_request(self):
        """Receive request"""
        return await self.request_channel.recv()
    
    async def send_response(self, response):
        """Send response"""
        await self.response_channel.send(response)
```

### Channel Multiplexing

#### Fan-Out Pattern
```python
async def fan_out(input_channel, output_channels):
    """Distribute input to multiple output channels"""
    try:
        while True:
            value = await input_channel.recv()
            
            # Send to all output channels concurrently
            send_tasks = [
                channel.send(value) for channel in output_channels
            ]
            await asyncio.gather(*send_tasks)
            
    except ChannelClosedError:
        for channel in output_channels:
            channel.close()
```

#### Fan-In Pattern
```python
async def fan_in(input_channels, output_channel):
    """Merge multiple input channels into single output"""
    async def forward_channel(input_ch):
        try:
            while True:
                value = await input_ch.recv()
                await output_channel.send(value)
        except ChannelClosedError:
            pass
    
    # Start forwarding tasks
    forward_tasks = [
        asyncio.create_task(forward_channel(ch))
        for ch in input_channels
    ]
    
    await asyncio.gather(*forward_tasks)
    output_channel.close()
```

## Process Composition

### Sequential Composition
```python
class SequentialProcess:
    """Process that executes sub-processes sequentially"""
    def __init__(self, processes):
        self.processes = processes
    
    async def run(self, input_channel, output_channel):
        """Execute processes in sequence"""
        channels = [input_channel]
        
        # Create intermediate channels
        for i in range(len(self.processes) - 1):
            channels.append(BufferedChannel())
        
        channels.append(output_channel)
        
        # Start all processes
        tasks = []
        for i, process in enumerate(self.processes):
            task = asyncio.create_task(
                process.run(channels[i], channels[i + 1])
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks)
```

### Parallel Composition
```python
class ParallelProcess:
    """Process that executes sub-processes in parallel"""
    def __init__(self, processes):
        self.processes = processes
    
    async def run(self, input_channels, output_channels):
        """Execute processes in parallel"""
        tasks = []
        for i, process in enumerate(self.processes):
            task = asyncio.create_task(
                process.run(input_channels[i], output_channels[i])
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks)
```

## Select Operations

### Basic Select
```python
async def select(*cases, default=None, timeout=None):
    """
    Select operation for non-deterministic choice
    Returns (case_index, result) or (None, None) for default/timeout
    """
    # Check for immediately ready cases
    ready_cases = []
    for i, case in enumerate(cases):
        if case.operation == 'recv':
            value, ready = case.channel.try_recv()
            if ready:
                ready_cases.append((i, value))
        elif case.operation == 'send':
            ready = case.channel.try_send(case.value)
            if ready:
                ready_cases.append((i, None))
    
    # If any cases are ready, choose randomly
    if ready_cases:
        import random
        return random.choice(ready_cases)
    
    # If default is provided, return immediately
    if default is not None:
        return None, default()
    
    # Wait for first case to become ready
    case_tasks = [
        asyncio.create_task(wait_for_case(i, case))
        for i, case in enumerate(cases)
    ]
    
    try:
        if timeout:
            done, pending = await asyncio.wait(
                case_tasks, 
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if not done:  # Timeout
                for task in pending:
                    task.cancel()
                return None, None
            
            result_task = done.pop()
            result = await result_task
            
            for task in pending:
                task.cancel()
            
            return result
        else:
            done, pending = await asyncio.wait(
                case_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            result_task = done.pop()
            result = await result_task
            
            for task in pending:
                task.cancel()
            
            return result
    
    except Exception:
        for task in case_tasks:
            task.cancel()
        raise
```

### Advanced Select Patterns

#### Load Balancer
```python
async def load_balancer(input_ch, worker_channels):
    """Distribute work to available workers using select"""
    try:
        while True:
            work_item = await input_ch.recv()
            
            # Create send cases for all worker channels
            send_cases = [
                SelectCase(ch, 'send', work_item)
                for ch in worker_channels
            ]
            
            # Select first available worker
            case_index, _ = await select(*send_cases)
            
            if case_index is not None:
                print(f"Sent work to worker {case_index}")
            
    except ChannelClosedError:
        for ch in worker_channels:
            ch.close()
```

## Process Networks

### Pipeline Networks
```python
class PipelineNetwork:
    """Network of processes connected in pipeline"""
    def __init__(self, stages):
        self.stages = stages
        self.channels = []
    
    async def run(self, input_data):
        """Execute pipeline with input data"""
        # Create channels between stages
        self.channels = [BufferedChannel() for _ in range(len(self.stages) + 1)]
        
        # Start all stages
        stage_tasks = []
        for i, stage in enumerate(self.stages):
            task = asyncio.create_task(
                stage.run(self.channels[i], self.channels[i + 1])
            )
            stage_tasks.append(task)
        
        # Feed input data
        input_task = asyncio.create_task(
            self.feed_input(input_data, self.channels[0])
        )
        
        # Collect output
        output_task = asyncio.create_task(
            self.collect_output(self.channels[-1])
        )
        
        await asyncio.gather(input_task, *stage_tasks)
        results = await output_task
        
        return results
    
    async def feed_input(self, data, input_ch):
        """Feed input data to pipeline"""
        for item in data:
            await input_ch.send(item)
        input_ch.close()
    
    async def collect_output(self, output_ch):
        """Collect output from pipeline"""
        results = []
        try:
            while True:
                result = await output_ch.recv()
                results.append(result)
        except ChannelClosedError:
            pass
        return results
```

## Deadlock Prevention

### Deadlock Detection
```python
class DeadlockDetector:
    """Detect potential deadlocks in CSP systems"""
    def __init__(self):
        self.wait_graph = {}  # process -> set of processes it waits for
    
    def add_wait_edge(self, waiting_process, waited_process):
        """Add edge to wait graph"""
        if waiting_process not in self.wait_graph:
            self.wait_graph[waiting_process] = set()
        self.wait_graph[waiting_process].add(waited_process)
    
    def detect_cycle(self):
        """Detect cycles in wait graph (potential deadlock)"""
        visited = set()
        rec_stack = set()
        
        def dfs(node):
            if node in rec_stack:
                return True  # Cycle detected
            if node in visited:
                return False
            
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in self.wait_graph.get(node, []):
                if dfs(neighbor):
                    return True
            
            rec_stack.remove(node)
            return False
        
        for process in self.wait_graph:
            if process not in visited:
                if dfs(process):
                    return True
        
        return False
```

### Prevention Strategies

#### Channel Ordering
```python
class OrderedChannelManager:
    """Prevent deadlock through channel ordering"""
    def __init__(self):
        self.channel_order = {}
        self.next_order = 0
    
    def create_channel(self):
        """Create channel with unique order"""
        channel = BufferedChannel()
        self.channel_order[channel] = self.next_order
        self.next_order += 1
        return channel
    
    async def multi_send(self, channel_value_pairs):
        """Send to multiple channels in order to prevent deadlock"""
        # Sort by channel order
        sorted_pairs = sorted(
            channel_value_pairs,
            key=lambda pair: self.channel_order[pair[0]]
        )
        
        # Send in order
        for channel, value in sorted_pairs:
            await channel.send(value)
```

#### Timeout-Based Prevention
```python
async def safe_multi_channel_op(operations, timeout=5.0):
    """Perform multiple channel operations with timeout"""
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*operations),
            timeout=timeout
        )
        return results
    except asyncio.TimeoutError:
        for op in operations:
            if hasattr(op, 'cancel'):
                op.cancel()
        raise DeadlockTimeoutError("Potential deadlock detected")
```

## Performance Patterns

### Buffering Strategies
- **Fixed Buffering**: Constant buffer size for predictable memory usage
- **Adaptive Buffering**: Dynamic buffer sizing based on load patterns
- **Bounded Buffering**: Limits to prevent memory exhaustion
- **Ring Buffering**: Circular buffers for continuous data streams

### Load Balancing
- **Round Robin**: Distribute work evenly across workers
- **Least Loaded**: Send work to worker with smallest queue
- **Random**: Random distribution for simplicity
- **Weighted**: Distribute based on worker capabilities

### Backpressure Handling
- **Blocking**: Block sender when receiver is overwhelmed
- **Dropping**: Drop messages when buffers are full
- **Sampling**: Process only subset of messages under load
- **Batching**: Group messages to reduce overhead

---

*CSP provides a mathematically rigorous foundation for concurrent system design. Understanding these patterns enables the construction of deadlock-free, composable, and analyzable concurrent systems.*
