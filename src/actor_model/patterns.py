"""
Actor Model Design Patterns

Common actor patterns and architectural solutions.
"""

import asyncio
import time
import random
from typing import Dict, List, Optional, Any, Callable, TypeVar, Generic, Set
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import logging

from .actor_system import Actor, ActorRef, Message, ActorSystem
from .supervisor import Supervisor, ChildSpec, RestartPolicy


T = TypeVar('T')
R = TypeVar('R')


# Message types for patterns
class RequestResponseMessage(Message):
    """Base class for request-response pattern."""
    def __init__(self, request_id: str, sender: ActorRef):
        super().__init__()
        self.request_id = request_id
        self.sender = sender


class ResponseMessage(Message):
    """Response message."""
    def __init__(self, request_id: str, result: Any, error: Optional[Exception] = None):
        super().__init__()
        self.request_id = request_id
        self.result = result
        self.error = error


class WorkMessage(Message):
    """Work message for worker patterns."""
    def __init__(self, work_id: str, data: Any):
        super().__init__()
        self.work_id = work_id
        self.data = data


class WorkResult(Message):
    """Work result message."""
    def __init__(self, work_id: str, result: Any, worker_id: str):
        super().__init__()
        self.work_id = work_id
        self.result = result
        self.worker_id = worker_id


class SubscribeMessage(Message):
    """Subscribe to events."""
    def __init__(self, subscriber: ActorRef, event_type: str):
        super().__init__()
        self.subscriber = subscriber
        self.event_type = event_type


class UnsubscribeMessage(Message):
    """Unsubscribe from events."""
    def __init__(self, subscriber: ActorRef, event_type: str):
        super().__init__()
        self.subscriber = subscriber
        self.event_type = event_type


class EventMessage(Message):
    """Event notification."""
    def __init__(self, event_type: str, data: Any):
        super().__init__()
        self.event_type = event_type
        self.data = data


# Pattern 1: Request-Response Pattern
class RequestResponseActor(Actor):
    """
    Actor that handles request-response interactions with timeout.
    """
    
    def __init__(self, timeout: float = 5.0):
        super().__init__()
        self.timeout = timeout
        self.pending_requests: Dict[str, asyncio.Future] = {}
    
    async def ask(self, target: ActorRef, message: Message, timeout: Optional[float] = None) -> Any:
        """Send request and wait for response."""
        request_id = f"req-{int(time.time() * 1000000)}-{random.randint(1000, 9999)}"
        timeout = timeout or self.timeout
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        # Send request
        request_msg = RequestResponseMessage(request_id, self.actor_ref)
        request_msg.original_message = message
        await self.tell(target, request_msg)
        
        try:
            # Wait for response
            response = await asyncio.wait_for(future, timeout=timeout)
            if response.error:
                raise response.error
            return response.result
        except asyncio.TimeoutError:
            raise TimeoutError(f"Request {request_id} timed out after {timeout}s")
        finally:
            self.pending_requests.pop(request_id, None)
    
    async def receive(self, message: Message):
        """Handle incoming messages."""
        if isinstance(message, RequestResponseMessage):
            # Handle request
            try:
                result = await self.handle_request(message.original_message)
                response = ResponseMessage(message.request_id, result)
            except Exception as e:
                response = ResponseMessage(message.request_id, None, e)
            
            await self.tell(message.sender, response)
        
        elif isinstance(message, ResponseMessage):
            # Handle response
            future = self.pending_requests.get(message.request_id)
            if future and not future.done():
                future.set_result(message)
        
        else:
            await super().receive(message)
    
    async def handle_request(self, message: Message) -> Any:
        """Override this to handle specific requests."""
        return f"Processed: {message}"


# Pattern 2: Worker Pool Pattern
class WorkerPoolManager(Actor):
    """
    Manages a pool of worker actors for parallel processing.
    """
    
    def __init__(self, worker_class: type, pool_size: int = 4):
        super().__init__()
        self.worker_class = worker_class
        self.pool_size = pool_size
        self.workers: List[ActorRef] = []
        self.work_queue: List[WorkMessage] = []
        self.busy_workers: Set[ActorRef] = set()
        self.work_results: Dict[str, Any] = {}
        self.round_robin_index = 0
    
    async def start(self):
        """Start worker pool."""
        await super().start()
        
        # Create worker actors
        for i in range(self.pool_size):
            worker = self.worker_class(f"worker-{i}")
            worker_ref = await self.actor_system.start_actor(worker, name=f"worker-{i}")
            self.workers.append(worker_ref)
    
    async def receive(self, message: Message):
        """Handle work distribution and results."""
        if isinstance(message, WorkMessage):
            await self._handle_work(message)
        
        elif isinstance(message, WorkResult):
            await self._handle_work_result(message)
        
        else:
            await super().receive(message)
    
    async def _handle_work(self, work: WorkMessage):
        """Distribute work to available workers."""
        # Try to find available worker
        available_worker = None
        for worker in self.workers:
            if worker not in self.busy_workers:
                available_worker = worker
                break
        
        if available_worker:
            # Send work to available worker
            self.busy_workers.add(available_worker)
            await self.tell(available_worker, work)
        else:
            # Queue work for later
            self.work_queue.append(work)
    
    async def _handle_work_result(self, result: WorkResult):
        """Handle completed work."""
        # Find worker that completed the work
        worker_ref = None
        for worker in self.workers:
            if hasattr(worker, 'name') and worker.name == result.worker_id:
                worker_ref = worker
                break
        
        if worker_ref:
            self.busy_workers.discard(worker_ref)
            
            # Process queued work if any
            if self.work_queue:
                next_work = self.work_queue.pop(0)
                self.busy_workers.add(worker_ref)
                await self.tell(worker_ref, next_work)
        
        # Store result
        self.work_results[result.work_id] = result.result
        logging.info(f"Work {result.work_id} completed by {result.worker_id}")
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get worker pool statistics."""
        return {
            "total_workers": len(self.workers),
            "busy_workers": len(self.busy_workers),
            "queued_work": len(self.work_queue),
            "completed_work": len(self.work_results)
        }


class Worker(Actor):
    """Base worker actor."""
    
    def __init__(self, worker_id: str):
        super().__init__()
        self.worker_id = worker_id
    
    async def receive(self, message: Message):
        """Process work messages."""
        if isinstance(message, WorkMessage):
            try:
                result = await self.process_work(message.data)
                work_result = WorkResult(message.work_id, result, self.worker_id)
                
                # Send result back to manager
                if self.parent_ref:
                    await self.tell(self.parent_ref, work_result)
                
            except Exception as e:
                logging.error(f"Worker {self.worker_id} failed to process work: {e}")
        
        else:
            await super().receive(message)
    
    async def process_work(self, data: Any) -> Any:
        """Override this to implement specific work processing."""
        # Simulate work
        await asyncio.sleep(random.uniform(0.1, 0.5))
        return f"Processed {data} by {self.worker_id}"


# Pattern 3: Event Bus Pattern
class EventBus(Actor):
    """
    Centralized event bus for publish-subscribe messaging.
    """
    
    def __init__(self):
        super().__init__()
        self.subscribers: Dict[str, Set[ActorRef]] = {}
    
    async def receive(self, message: Message):
        """Handle subscriptions and event publishing."""
        if isinstance(message, SubscribeMessage):
            await self._handle_subscribe(message)
        
        elif isinstance(message, UnsubscribeMessage):
            await self._handle_unsubscribe(message)
        
        elif isinstance(message, EventMessage):
            await self._handle_event(message)
        
        else:
            await super().receive(message)
    
    async def _handle_subscribe(self, message: SubscribeMessage):
        """Subscribe actor to event type."""
        event_type = message.event_type
        subscriber = message.subscriber
        
        if event_type not in self.subscribers:
            self.subscribers[event_type] = set()
        
        self.subscribers[event_type].add(subscriber)
        logging.info(f"Actor subscribed to {event_type}")
    
    async def _handle_unsubscribe(self, message: UnsubscribeMessage):
        """Unsubscribe actor from event type."""
        event_type = message.event_type
        subscriber = message.subscriber
        
        if event_type in self.subscribers:
            self.subscribers[event_type].discard(subscriber)
            if not self.subscribers[event_type]:
                del self.subscribers[event_type]
        
        logging.info(f"Actor unsubscribed from {event_type}")
    
    async def _handle_event(self, message: EventMessage):
        """Publish event to all subscribers."""
        event_type = message.event_type
        subscribers = self.subscribers.get(event_type, set())
        
        # Send event to all subscribers
        for subscriber in subscribers.copy():  # Copy to avoid modification during iteration
            try:
                await self.tell(subscriber, message)
            except Exception as e:
                logging.error(f"Failed to send event to subscriber: {e}")
                # Remove failed subscriber
                self.subscribers[event_type].discard(subscriber)
        
        logging.info(f"Published {event_type} event to {len(subscribers)} subscribers")


class EventSubscriber(Actor):
    """Base class for event subscribers."""
    
    def __init__(self, event_bus: ActorRef):
        super().__init__()
        self.event_bus = event_bus
        self.subscribed_events: Set[str] = set()
    
    async def subscribe(self, event_type: str):
        """Subscribe to an event type."""
        if event_type not in self.subscribed_events:
            self.subscribed_events.add(event_type)
            await self.tell(self.event_bus, SubscribeMessage(self.actor_ref, event_type))
    
    async def unsubscribe(self, event_type: str):
        """Unsubscribe from an event type."""
        if event_type in self.subscribed_events:
            self.subscribed_events.remove(event_type)
            await self.tell(self.event_bus, UnsubscribeMessage(self.actor_ref, event_type))
    
    async def receive(self, message: Message):
        """Handle events and other messages."""
        if isinstance(message, EventMessage):
            await self.handle_event(message)
        else:
            await super().receive(message)
    
    async def handle_event(self, event: EventMessage):
        """Override this to handle specific events."""
        logging.info(f"Received event {event.event_type}: {event.data}")


# Pattern 4: Circuit Breaker Pattern
class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    timeout: float = 60.0
    success_threshold: int = 3


class CircuitBreakerActor(Actor):
    """
    Circuit breaker pattern for fault tolerance.
    """
    
    def __init__(self, target: ActorRef, config: CircuitBreakerConfig = None):
        super().__init__()
        self.target = target
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
    
    async def receive(self, message: Message):
        """Handle requests with circuit breaker logic."""
        if isinstance(message, RequestResponseMessage):
            await self._handle_request(message)
        else:
            await super().receive(message)
    
    async def _handle_request(self, request: RequestResponseMessage):
        """Process request through circuit breaker."""
        if self.state == CircuitBreakerState.OPEN:
            # Check if timeout has passed
            if time.time() - self.last_failure_time > self.config.timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
            else:
                # Circuit is open, reject request
                error_response = ResponseMessage(
                    request.request_id, 
                    None, 
                    Exception("Circuit breaker is OPEN")
                )
                await self.tell(request.sender, error_response)
                return
        
        try:
            # Forward request to target
            response = await self._forward_request(request)
            
            # Handle successful response
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = 0
            
            await self.tell(request.sender, response)
        
        except Exception as e:
            # Handle failure
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            
            error_response = ResponseMessage(request.request_id, None, e)
            await self.tell(request.sender, error_response)
    
    async def _forward_request(self, request: RequestResponseMessage) -> ResponseMessage:
        """Forward request to target and wait for response."""
        # This is a simplified version - in practice you'd need proper request forwarding
        await self.tell(self.target, request.original_message)
        
        # Simulate response (in real implementation, you'd wait for actual response)
        await asyncio.sleep(0.1)
        return ResponseMessage(request.request_id, "Success")


# Pattern 5: Saga Pattern
class SagaStep:
    """A step in a saga transaction."""
    
    def __init__(self, action: Callable, compensation: Callable):
        self.action = action
        self.compensation = compensation


class SagaActor(Actor):
    """
    Saga pattern for distributed transactions.
    """
    
    def __init__(self, steps: List[SagaStep]):
        super().__init__()
        self.steps = steps
        self.completed_steps: List[int] = []
        self.saga_state = "pending"
    
    async def execute_saga(self) -> bool:
        """Execute the saga transaction."""
        try:
            # Execute all steps
            for i, step in enumerate(self.steps):
                await step.action()
                self.completed_steps.append(i)
            
            self.saga_state = "completed"
            logging.info("Saga completed successfully")
            return True
        
        except Exception as e:
            logging.error(f"Saga failed at step {len(self.completed_steps)}: {e}")
            await self._compensate()
            return False
    
    async def _compensate(self):
        """Execute compensation actions for completed steps."""
        self.saga_state = "compensating"
        
        # Execute compensations in reverse order
        for step_index in reversed(self.completed_steps):
            try:
                await self.steps[step_index].compensation()
                logging.info(f"Compensated step {step_index}")
            except Exception as e:
                logging.error(f"Compensation failed for step {step_index}: {e}")
        
        self.saga_state = "compensated"
        logging.info("Saga compensation completed")


# Example usage and demonstrations
async def demo_request_response():
    """Demonstrate request-response pattern."""
    print("=== Request-Response Pattern Demo ===")
    
    system = ActorSystem()
    await system.start()
    
    try:
        # Create request-response actors
        server = RequestResponseActor()
        client = RequestResponseActor()
        
        server_ref = await system.start_actor(server, name="server")
        client_ref = await system.start_actor(client, name="client")
        
        # Make request
        response = await client.ask(server_ref, Message("Hello Server"))
        print(f"Response: {response}")
        
    finally:
        await system.stop()


async def demo_worker_pool():
    """Demonstrate worker pool pattern."""
    print("\n=== Worker Pool Pattern Demo ===")
    
    system = ActorSystem()
    await system.start()
    
    try:
        # Create worker pool
        pool_manager = WorkerPoolManager(Worker, pool_size=3)
        manager_ref = await system.start_actor(pool_manager, name="pool-manager")
        
        # Submit work
        for i in range(10):
            work = WorkMessage(f"work-{i}", f"task-{i}")
            await system.tell(manager_ref, work)
        
        # Wait for work to complete
        await asyncio.sleep(2)
        
        stats = pool_manager.get_pool_stats()
        print(f"Pool stats: {stats}")
        
    finally:
        await system.stop()


async def demo_event_bus():
    """Demonstrate event bus pattern."""
    print("\n=== Event Bus Pattern Demo ===")
    
    system = ActorSystem()
    await system.start()
    
    try:
        # Create event bus
        event_bus = EventBus()
        bus_ref = await system.start_actor(event_bus, name="event-bus")
        
        # Create subscribers
        subscriber1 = EventSubscriber(bus_ref)
        subscriber2 = EventSubscriber(bus_ref)
        
        sub1_ref = await system.start_actor(subscriber1, name="subscriber1")
        sub2_ref = await system.start_actor(subscriber2, name="subscriber2")
        
        # Subscribe to events
        await subscriber1.subscribe("user.created")
        await subscriber2.subscribe("user.created")
        await subscriber1.subscribe("order.placed")
        
        # Publish events
        await system.tell(bus_ref, EventMessage("user.created", {"user_id": 123}))
        await system.tell(bus_ref, EventMessage("order.placed", {"order_id": 456}))
        
        await asyncio.sleep(1)
        
    finally:
        await system.stop()


async def main():
    """Run all pattern demonstrations."""
    await demo_request_response()
    await demo_worker_pool()
    await demo_event_bus()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
