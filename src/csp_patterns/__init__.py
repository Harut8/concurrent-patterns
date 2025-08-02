"""
CSP (Communicating Sequential Processes) Patterns

This module implements CSP concurrency patterns with channels, processes,
select statements, and common CSP design patterns.
"""

from .channels import (
    Channel,
    SynchronousChannel,
    BufferedChannel,
    UnboundedChannel,
    BroadcastChannel,
)

from .processes import (
    Process,
    ProcessState,
    ProcessInfo,
    ProducerProcess,
    ConsumerProcess,
    FilterProcess,
    TransformProcess,
    SplitterProcess,
    MergerProcess,
    ProcessNetwork,
)

from .select import (
    Select,
    SelectCase,
    SendCase,
    ReceiveCase,
    TimeoutCase,
    DefaultCase,
    SelectResult,
    SelectBuilder,
    SelectLoop,
    select,
    send_case,
    receive_case,
    timeout_case,
    default_case,
)

from .patterns import (
    FanOutPattern,
    FanInPattern,
    PipelineStage,
    Pipeline,
    WorkerPool,
    PubSubBroker,
    RequestResponsePattern,
)

__all__ = [
    # Channels
    'Channel',
    'SynchronousChannel',
    'BufferedChannel',
    'UnboundedChannel',
    'BroadcastChannel',
    
    # Processes
    'Process',
    'ProcessState',
    'ProcessInfo',
    'ProducerProcess',
    'ConsumerProcess',
    'FilterProcess',
    'TransformProcess',
    'SplitterProcess',
    'MergerProcess',
    'ProcessNetwork',
    
    # Select
    'Select',
    'SelectCase',
    'SendCase',
    'ReceiveCase',
    'TimeoutCase',
    'DefaultCase',
    'SelectResult',
    'SelectBuilder',
    'SelectLoop',
    'select',
    'send_case',
    'receive_case',
    'timeout_case',
    'default_case',
    
    # Patterns
    'FanOutPattern',
    'FanInPattern',
    'PipelineStage',
    'Pipeline',
    'WorkerPool',
    'PubSubBroker',
    'RequestResponsePattern',
]
