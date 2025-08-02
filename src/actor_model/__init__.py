"""
Actor Model Implementation

This module provides a complete actor model implementation with supervision,
mailboxes, and common actor patterns.
"""

from .actor_system import (
    Actor,
    ActorRef,
    ActorSystem,
    Message,
    SystemMessage,
    StartMessage,
    StopMessage,
)

from .supervisor import (
    Supervisor,
    DynamicSupervisor,
    SupervisorStrategy,
    RestartPolicy,
    SupervisorConfig,
    ChildSpec,
    SupervisorMessage,
    StartChild,
    StopChild,
    RestartChild,
    ChildTerminated,
)

from .mailbox import (
    Mailbox,
    UnboundedMailbox,
    BoundedMailbox,
    PriorityMailbox,
    StashMailbox,
    BalancingMailbox,
    MailboxType,
    MailboxFactory,
    MailboxStats,
)

from .patterns import (
    RequestResponseActor,
    WorkerPoolManager,
    Worker,
    EventBus,
    EventSubscriber,
    CircuitBreakerActor,
    SagaActor,
    SagaStep,
    RequestResponseMessage,
    ResponseMessage,
    WorkMessage,
    WorkResult,
    SubscribeMessage,
    UnsubscribeMessage,
    EventMessage,
)

__all__ = [
    # Core actor system
    'Actor',
    'ActorRef',
    'ActorSystem',
    'Message',
    'SystemMessage',
    'StartMessage',
    'StopMessage',

    # Supervision
    'Supervisor',
    'DynamicSupervisor',
    'SupervisorStrategy',
    'RestartPolicy',
    'SupervisorConfig',
    'ChildSpec',
    'SupervisorMessage',
    'StartChild',
    'StopChild',
    'RestartChild',
    'ChildTerminated',
    
    # Mailboxes
    'Mailbox',
    'UnboundedMailbox',
    'BoundedMailbox',
    'PriorityMailbox',
    'StashMailbox',
    'BalancingMailbox',
    'MailboxType',
    'MailboxFactory',
    'MailboxStats',
    
    # Patterns
    'RequestResponseActor',
    'WorkerPoolManager',
    'Worker',
    'EventBus',
    'EventSubscriber',
    'CircuitBreakerActor',
    'SagaActor',
    'SagaStep',
    'RequestResponseMessage',
    'ResponseMessage',
    'WorkMessage',
    'WorkResult',
    'SubscribeMessage',
    'UnsubscribeMessage',
    'EventMessage',
]
