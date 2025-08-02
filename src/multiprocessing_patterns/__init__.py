"""
Multiprocessing Patterns

This module contains multiprocessing-based concurrency patterns including
process pools, IPC mechanisms, shared memory, MapReduce, and parallel algorithms.
"""

from .process_pool import (
    DynamicProcessPool,
    ProcessPool,
    WorkerProcess,
    TaskResult,
)

from .ipc_patterns import (
    IPCChannel,
    QueueChannel,
    PipeChannel,
    SocketChannel,
    MessageBroker,
    SharedCounter,
    ProcessBarrier,
    IPCMessage,
    IPCType,
)

from .shared_memory import (
    SharedMemoryManager,
    SharedBuffer,
    MemoryMappedFile,
    SharedMatrix,
    SharedMemoryType,
    SharedMemoryInfo,
)

from .map_reduce import (
    MapReduceFramework,
    MapTask,
    ReduceTask,
    MapReduceResult,
    Partitioner,
    HashPartitioner,
    RangePartitioner,
    WordCountExample,
    InvertedIndexExample,
    TopKExample,
    StreamingMapReduce,
)

from .parallel_algorithms import (
    ParallelSorting,
    ParallelSearch,
    ParallelNumerical,
    ParallelGraphAlgorithms,
    AlgorithmResult,
    PerformanceMeasurer,
)

__all__ = [
    # Process Pool
    'DynamicProcessPool',
    'ProcessPool',
    'WorkerProcess',
    'TaskResult',

    # IPC Patterns
    'IPCChannel',
    'QueueChannel',
    'PipeChannel',
    'SocketChannel',
    'MessageBroker',
    'SharedCounter',
    'ProcessBarrier',
    'IPCMessage',
    'IPCType',
    
    # Shared Memory
    'SharedMemoryManager',
    'SharedBuffer',
    'MemoryMappedFile',
    'SharedMatrix',
    'SharedMemoryType',
    'SharedMemoryInfo',
    
    # MapReduce
    'MapReduceFramework',
    'MapTask',
    'ReduceTask',
    'MapReduceResult',
    'Partitioner',
    'HashPartitioner',
    'RangePartitioner',
    'WordCountExample',
    'InvertedIndexExample',
    'TopKExample',
    'StreamingMapReduce',
    
    # Parallel Algorithms
    'ParallelSorting',
    'ParallelSearch',
    'ParallelNumerical',
    'ParallelGraphAlgorithms',
    'AlgorithmResult',
    'PerformanceMeasurer',
]
