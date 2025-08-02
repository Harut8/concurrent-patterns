"""
Actor Supervisor Patterns

Supervisor implementations for fault tolerance and actor lifecycle management.
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional, Callable, Any, Set
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod

from .actor_system import Actor, ActorRef, Message


class SupervisorStrategy(Enum):
    """Supervisor restart strategies."""
    ONE_FOR_ONE = "one_for_one"           # Restart only the failed actor
    ONE_FOR_ALL = "one_for_all"           # Restart all supervised actors
    REST_FOR_ONE = "rest_for_one"         # Restart failed actor and all started after it
    ESCALATE = "escalate"                 # Escalate failure to parent supervisor


class RestartPolicy(Enum):
    """Actor restart policies."""
    PERMANENT = "permanent"               # Always restart
    TEMPORARY = "temporary"               # Never restart
    TRANSIENT = "transient"              # Restart only on abnormal termination


@dataclass
class SupervisorConfig:
    """Supervisor configuration."""
    strategy: SupervisorStrategy = SupervisorStrategy.ONE_FOR_ONE
    max_restarts: int = 3
    within_time_period: float = 60.0  # seconds
    restart_delay: float = 1.0        # seconds
    escalation_timeout: float = 5.0   # seconds


@dataclass
class ChildSpec:
    """Child actor specification."""
    actor_class: type
    args: tuple = ()
    kwargs: dict = None
    restart_policy: RestartPolicy = RestartPolicy.PERMANENT
    shutdown_timeout: float = 5.0
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class SupervisorMessage(Message):
    """Base class for supervisor messages."""
    pass


class StartChild(SupervisorMessage):
    """Message to start a child actor."""
    def __init__(self, child_spec: ChildSpec, child_id: str):
        super().__init__()
        self.child_spec = child_spec
        self.child_id = child_id


class StopChild(SupervisorMessage):
    """Message to stop a child actor."""
    def __init__(self, child_id: str):
        super().__init__()
        self.child_id = child_id


class RestartChild(SupervisorMessage):
    """Message to restart a child actor."""
    def __init__(self, child_id: str):
        super().__init__()
        self.child_id = child_id


class ChildTerminated(SupervisorMessage):
    """Message indicating a child actor has terminated."""
    def __init__(self, child_id: str, actor_ref: ActorRef, reason: Optional[Exception] = None):
        super().__init__()
        self.child_id = child_id
        self.actor_ref = actor_ref
        self.reason = reason


@dataclass
class RestartRecord:
    """Record of actor restart attempts."""
    timestamp: float
    child_id: str
    reason: Optional[Exception] = None


class Supervisor(Actor):
    """
    Supervisor actor that manages child actors with fault tolerance.
    """
    
    def __init__(self, config: SupervisorConfig = None):
        super().__init__()
        self.config = config or SupervisorConfig()
        self.children: Dict[str, ActorRef] = {}
        self.child_specs: Dict[str, ChildSpec] = {}
        self.restart_records: List[RestartRecord] = []
        self.starting_children: Set[str] = set()
        self.stopping_children: Set[str] = set()
    
    async def receive(self, message: Message):
        """Handle supervisor messages."""
        if isinstance(message, StartChild):
            await self._handle_start_child(message)
        elif isinstance(message, StopChild):
            await self._handle_stop_child(message)
        elif isinstance(message, RestartChild):
            await self._handle_restart_child(message)
        elif isinstance(message, ChildTerminated):
            await self._handle_child_terminated(message)
        else:
            await super().receive(message)
    
    async def _handle_start_child(self, message: StartChild):
        """Start a child actor."""
        child_id = message.child_id
        child_spec = message.child_spec
        
        if child_id in self.children:
            logging.warning(f"Child {child_id} already exists")
            return
        
        if child_id in self.starting_children:
            logging.warning(f"Child {child_id} is already starting")
            return
        
        try:
            self.starting_children.add(child_id)
            
            # Create and start child actor
            child_actor = child_spec.actor_class(*child_spec.args, **child_spec.kwargs)
            child_ref = await self.actor_system.start_actor(child_actor, name=child_id)
            
            # Store child reference and spec
            self.children[child_id] = child_ref
            self.child_specs[child_id] = child_spec
            
            # Monitor child for termination
            await self._monitor_child(child_id, child_ref)
            
            logging.info(f"Started child actor {child_id}")
            
        except Exception as e:
            logging.error(f"Failed to start child {child_id}: {e}")
        finally:
            self.starting_children.discard(child_id)
    
    async def _handle_stop_child(self, message: StopChild):
        """Stop a child actor."""
        child_id = message.child_id
        
        if child_id not in self.children:
            logging.warning(f"Child {child_id} not found")
            return
        
        if child_id in self.stopping_children:
            logging.warning(f"Child {child_id} is already stopping")
            return
        
        try:
            self.stopping_children.add(child_id)
            child_ref = self.children[child_id]
            child_spec = self.child_specs[child_id]
            
            # Stop the child actor
            await self.actor_system.stop_actor(child_ref, timeout=child_spec.shutdown_timeout)
            
            # Clean up
            del self.children[child_id]
            del self.child_specs[child_id]
            
            logging.info(f"Stopped child actor {child_id}")
            
        except Exception as e:
            logging.error(f"Failed to stop child {child_id}: {e}")
        finally:
            self.stopping_children.discard(child_id)
    
    async def _handle_restart_child(self, message: RestartChild):
        """Restart a child actor."""
        child_id = message.child_id
        
        if child_id not in self.child_specs:
            logging.warning(f"Child spec for {child_id} not found")
            return
        
        # Stop existing child if running
        if child_id in self.children:
            await self._handle_stop_child(StopChild(child_id))
        
        # Wait for restart delay
        await asyncio.sleep(self.config.restart_delay)
        
        # Start child again
        child_spec = self.child_specs[child_id]
        await self._handle_start_child(StartChild(child_spec, child_id))
    
    async def _handle_child_terminated(self, message: ChildTerminated):
        """Handle child actor termination."""
        child_id = message.child_id
        reason = message.reason
        
        logging.info(f"Child {child_id} terminated: {reason}")
        
        # Clean up child reference
        if child_id in self.children:
            del self.children[child_id]
        
        # Check if we should restart based on policy
        if child_id not in self.child_specs:
            return
        
        child_spec = self.child_specs[child_id]
        should_restart = self._should_restart(child_spec.restart_policy, reason)
        
        if should_restart and self._can_restart(child_id):
            # Record restart attempt
            self.restart_records.append(RestartRecord(
                timestamp=time.time(),
                child_id=child_id,
                reason=reason
            ))
            
            # Apply supervisor strategy
            await self._apply_strategy(child_id, reason)
        else:
            # Remove child spec if not restarting
            if child_id in self.child_specs:
                del self.child_specs[child_id]
    
    def _should_restart(self, policy: RestartPolicy, reason: Optional[Exception]) -> bool:
        """Determine if child should be restarted based on policy."""
        if policy == RestartPolicy.PERMANENT:
            return True
        elif policy == RestartPolicy.TEMPORARY:
            return False
        elif policy == RestartPolicy.TRANSIENT:
            # Restart only on abnormal termination (exceptions)
            return reason is not None
        return False
    
    def _can_restart(self, child_id: str) -> bool:
        """Check if child can be restarted based on restart limits."""
        current_time = time.time()
        cutoff_time = current_time - self.config.within_time_period
        
        # Count recent restarts for this child
        recent_restarts = [
            record for record in self.restart_records
            if record.child_id == child_id and record.timestamp > cutoff_time
        ]
        
        return len(recent_restarts) < self.config.max_restarts
    
    async def _apply_strategy(self, failed_child_id: str, reason: Optional[Exception]):
        """Apply supervisor strategy for handling child failure."""
        strategy = self.config.strategy
        
        if strategy == SupervisorStrategy.ONE_FOR_ONE:
            # Restart only the failed child
            await self._restart_child(failed_child_id)
            
        elif strategy == SupervisorStrategy.ONE_FOR_ALL:
            # Restart all children
            await self._restart_all_children()
            
        elif strategy == SupervisorStrategy.REST_FOR_ONE:
            # Restart failed child and all started after it
            await self._restart_from_child(failed_child_id)
            
        elif strategy == SupervisorStrategy.ESCALATE:
            # Escalate to parent supervisor
            await self._escalate_failure(failed_child_id, reason)
    
    async def _restart_child(self, child_id: str):
        """Restart a specific child."""
        await self.tell(self.actor_ref, RestartChild(child_id))
    
    async def _restart_all_children(self):
        """Restart all children."""
        child_ids = list(self.child_specs.keys())
        
        # Stop all children first
        for child_id in child_ids:
            if child_id in self.children:
                await self._handle_stop_child(StopChild(child_id))
        
        # Wait for restart delay
        await asyncio.sleep(self.config.restart_delay)
        
        # Start all children
        for child_id in child_ids:
            child_spec = self.child_specs[child_id]
            await self._handle_start_child(StartChild(child_spec, child_id))
    
    async def _restart_from_child(self, failed_child_id: str):
        """Restart failed child and all children started after it."""
        # For simplicity, restart all children
        # In a real implementation, you'd track start order
        await self._restart_all_children()
    
    async def _escalate_failure(self, child_id: str, reason: Optional[Exception]):
        """Escalate failure to parent supervisor."""
        if self.parent_ref:
            # Send termination message to parent
            await self.tell(self.parent_ref, ChildTerminated(
                child_id=self.name or "supervisor",
                actor_ref=self.actor_ref,
                reason=reason
            ))
        else:
            logging.error(f"No parent to escalate failure of {child_id}: {reason}")
    
    async def _monitor_child(self, child_id: str, child_ref: ActorRef):
        """Monitor a child actor for termination."""
        async def monitor():
            try:
                # Wait for child to terminate
                await child_ref.wait_for_termination()
                
                # Notify supervisor of termination
                await self.tell(self.actor_ref, ChildTerminated(
                    child_id=child_id,
                    actor_ref=child_ref
                ))
            except Exception as e:
                # Notify supervisor of error
                await self.tell(self.actor_ref, ChildTerminated(
                    child_id=child_id,
                    actor_ref=child_ref,
                    reason=e
                ))
        
        # Start monitoring task
        asyncio.create_task(monitor())
    
    async def start_child(self, child_spec: ChildSpec, child_id: str):
        """Public method to start a child actor."""
        await self.tell(self.actor_ref, StartChild(child_spec, child_id))
    
    async def stop_child(self, child_id: str):
        """Public method to stop a child actor."""
        await self.tell(self.actor_ref, StopChild(child_id))
    
    def get_children(self) -> Dict[str, ActorRef]:
        """Get all child actor references."""
        return self.children.copy()
    
    def get_child(self, child_id: str) -> Optional[ActorRef]:
        """Get a specific child actor reference."""
        return self.children.get(child_id)


class DynamicSupervisor(Supervisor):
    """
    Dynamic supervisor that can start/stop children on demand.
    """
    
    def __init__(self, config: SupervisorConfig = None):
        super().__init__(config)
        self.child_counter = 0
    
    async def start_child_dynamic(self, actor_class: type, *args, **kwargs) -> str:
        """Start a child actor dynamically and return its ID."""
        self.child_counter += 1
        child_id = f"child-{self.child_counter}"
        
        child_spec = ChildSpec(
            actor_class=actor_class,
            args=args,
            kwargs=kwargs,
            restart_policy=RestartPolicy.TEMPORARY  # Don't restart dynamic children
        )
        
        await self.start_child(child_spec, child_id)
        return child_id


# Example worker actors for demonstration
class WorkerActor(Actor):
    """Example worker actor."""
    
    def __init__(self, worker_id: str, fail_after: Optional[int] = None):
        super().__init__()
        self.worker_id = worker_id
        self.fail_after = fail_after
        self.message_count = 0
    
    async def receive(self, message: Message):
        """Process work messages."""
        self.message_count += 1
        
        if hasattr(message, 'work_data'):
            logging.info(f"Worker {self.worker_id} processing: {message.work_data}")
            
            # Simulate work
            await asyncio.sleep(0.1)
            
            # Simulate failure if configured
            if self.fail_after and self.message_count >= self.fail_after:
                raise RuntimeError(f"Worker {self.worker_id} failed after {self.message_count} messages")
        
        await super().receive(message)


class WorkMessage(Message):
    """Work message for worker actors."""
    def __init__(self, work_data: Any):
        super().__init__()
        self.work_data = work_data


# Example usage and demonstrations
async def demo_basic_supervisor():
    """Demonstrate basic supervisor functionality."""
    print("=== Basic Supervisor Demo ===")
    
    from .actor_system import ActorSystem
    
    # Create actor system
    system = ActorSystem()
    await system.start()
    
    try:
        # Create supervisor with one-for-one strategy
        config = SupervisorConfig(
            strategy=SupervisorStrategy.ONE_FOR_ONE,
            max_restarts=2,
            within_time_period=10.0
        )
        supervisor = Supervisor(config)
        supervisor_ref = await system.start_actor(supervisor, name="supervisor")
        
        # Start some worker children
        for i in range(3):
            child_spec = ChildSpec(
                actor_class=WorkerActor,
                args=(f"worker-{i}",),
                restart_policy=RestartPolicy.PERMANENT
            )
            await supervisor.start_child(child_spec, f"worker-{i}")
        
        # Wait for children to start
        await asyncio.sleep(1)
        
        # Send work to children
        children = supervisor.get_children()
        for i, (child_id, child_ref) in enumerate(children.items()):
            await system.tell(child_ref, WorkMessage(f"task-{i}"))
        
        print(f"Started {len(children)} worker children")
        
        # Wait a bit
        await asyncio.sleep(2)
        
    finally:
        await system.stop()


async def demo_fault_tolerance():
    """Demonstrate supervisor fault tolerance."""
    print("\n=== Fault Tolerance Demo ===")
    
    from .actor_system import ActorSystem
    
    # Create actor system
    system = ActorSystem()
    await system.start()
    
    try:
        # Create supervisor
        config = SupervisorConfig(
            strategy=SupervisorStrategy.ONE_FOR_ONE,
            max_restarts=3,
            within_time_period=10.0,
            restart_delay=0.5
        )
        supervisor = Supervisor(config)
        supervisor_ref = await system.start_actor(supervisor, name="supervisor")
        
        # Start a worker that will fail after 2 messages
        child_spec = ChildSpec(
            actor_class=WorkerActor,
            args=("failing-worker", 2),  # Fail after 2 messages
            restart_policy=RestartPolicy.PERMANENT
        )
        await supervisor.start_child(child_spec, "failing-worker")
        
        # Wait for child to start
        await asyncio.sleep(1)
        
        # Send messages that will cause failure
        child_ref = supervisor.get_child("failing-worker")
        if child_ref:
            for i in range(5):
                await system.tell(child_ref, WorkMessage(f"task-{i}"))
                await asyncio.sleep(0.2)
                
                # Update child ref after restart
                child_ref = supervisor.get_child("failing-worker")
                if not child_ref:
                    break
        
        print("Sent messages to failing worker - supervisor should restart it")
        
        # Wait to see restart behavior
        await asyncio.sleep(3)
        
    finally:
        await system.stop()


async def demo_dynamic_supervisor():
    """Demonstrate dynamic supervisor."""
    print("\n=== Dynamic Supervisor Demo ===")
    
    from .actor_system import ActorSystem
    
    # Create actor system
    system = ActorSystem()
    await system.start()
    
    try:
        # Create dynamic supervisor
        supervisor = DynamicSupervisor()
        supervisor_ref = await system.start_actor(supervisor, name="dynamic-supervisor")
        
        # Start children dynamically
        child_ids = []
        for i in range(3):
            child_id = await supervisor.start_child_dynamic(
                WorkerActor, f"dynamic-worker-{i}"
            )
            child_ids.append(child_id)
        
        # Wait for children to start
        await asyncio.sleep(1)
        
        # Send work to dynamic children
        for child_id in child_ids:
            child_ref = supervisor.get_child(child_id)
            if child_ref:
                await system.tell(child_ref, WorkMessage(f"dynamic-task-{child_id}"))
        
        print(f"Started {len(child_ids)} dynamic children")
        
        # Stop some children
        await supervisor.stop_child(child_ids[0])
        await asyncio.sleep(1)
        
        remaining_children = supervisor.get_children()
        print(f"Remaining children: {len(remaining_children)}")
        
    finally:
        await system.stop()


async def main():
    """Run all supervisor demonstrations."""
    await demo_basic_supervisor()
    await demo_fault_tolerance()
    await demo_dynamic_supervisor()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
