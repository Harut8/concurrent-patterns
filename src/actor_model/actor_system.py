"""
Core Actor System Implementation

Implements the fundamental actor model with:
- Actor lifecycle management
- Message passing
- Actor references
- System supervision
"""

import asyncio
import uuid
import weakref
from typing import Any, Dict, Optional, Callable, Type, List, Union
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import logging
import time


class ActorState(Enum):
    """Actor lifecycle states."""
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class Message:
    """Base message class for actor communication."""
    sender: Optional['ActorRef'] = None
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()


@dataclass
class SystemMessage(Message):
    """System-level messages for actor lifecycle."""
    pass


@dataclass
class StartMessage(SystemMessage):
    """Message to start an actor."""
    pass


@dataclass
class StopMessage(SystemMessage):
    """Message to stop an actor."""
    pass


@dataclass
class RestartMessage(SystemMessage):
    """Message to restart an actor."""
    pass


@dataclass
class UserMessage(Message):
    """User-defined message."""
    content: Any = None


class ActorRef:
    """
    Reference to an actor that provides location transparency.
    All communication with actors goes through ActorRef.
    """
    
    def __init__(self, actor_id: str, actor_system: 'ActorSystem'):
        self.actor_id = actor_id
        self.actor_system = actor_system
        self._path = f"/user/{actor_id}"
    
    async def tell(self, message: Any, sender: Optional['ActorRef'] = None):
        """Send a message to the actor (fire-and-forget)."""
        if not isinstance(message, Message):
            message = UserMessage(content=message, sender=sender)
        else:
            message.sender = sender
        
        await self.actor_system._deliver_message(self.actor_id, message)
    
    async def ask(self, message: Any, timeout: float = 5.0) -> Any:
        """Send a message and wait for a response."""
        response_id = str(uuid.uuid4())
        future = asyncio.Future()
        
        # Register future for response
        self.actor_system._pending_asks[response_id] = future
        
        # Create ask message
        ask_message = AskMessage(
            content=message,
            response_id=response_id,
            sender=None  # Will be set by system
        )
        
        try:
            await self.tell(ask_message)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self.actor_system._pending_asks.pop(response_id, None)
            raise
        finally:
            self.actor_system._pending_asks.pop(response_id, None)
    
    def __str__(self):
        return f"ActorRef({self._path})"
    
    def __eq__(self, other):
        return isinstance(other, ActorRef) and self.actor_id == other.actor_id


@dataclass
class AskMessage(Message):
    """Message for ask pattern (request-response)."""
    content: Any = None
    response_id: str = ""


@dataclass
class ResponseMessage(Message):
    """Response message for ask pattern."""
    response_id: str = ""
    content: Any = None


class Actor(ABC):
    """
    Base actor class. All actors must inherit from this class.
    Actors process messages sequentially and maintain private state.
    """
    
    def __init__(self):
        self.actor_id: str = ""
        self.actor_ref: Optional[ActorRef] = None
        self.actor_system: Optional['ActorSystem'] = None
        self.state: ActorState = ActorState.CREATED
        self._mailbox: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._children: Dict[str, ActorRef] = {}
        self._parent: Optional[ActorRef] = None
        self._stopping = False
    
    async def pre_start(self):
        """Called before the actor starts processing messages."""
        pass
    
    async def post_stop(self):
        """Called after the actor stops processing messages."""
        pass
    
    async def pre_restart(self, reason: Exception):
        """Called before the actor restarts."""
        await self.post_stop()
    
    async def post_restart(self, reason: Exception):
        """Called after the actor restarts."""
        await self.pre_start()
    
    @abstractmethod
    async def receive(self, message: Message) -> Optional[Any]:
        """
        Process a received message.
        Return a value for ask pattern, None for tell pattern.
        """
        pass
    
    async def _process_messages(self):
        """Main message processing loop."""
        try:
            self.state = ActorState.STARTING
            await self.pre_start()
            self.state = ActorState.RUNNING
            
            while not self._stopping:
                try:
                    message = await self._mailbox.get()
                    
                    # Handle system messages
                    if isinstance(message, StopMessage):
                        break
                    elif isinstance(message, RestartMessage):
                        await self._handle_restart(message)
                        continue
                    
                    # Process user message
                    response = await self.receive(message)
                    
                    # Handle ask pattern response
                    if isinstance(message, AskMessage) and response is not None:
                        response_msg = ResponseMessage(
                            response_id=message.response_id,
                            content=response,
                            sender=self.actor_ref
                        )
                        await self.actor_system._handle_response(response_msg)
                    
                except Exception as e:
                    await self._handle_error(e, message)
        
        except Exception as e:
            self.state = ActorState.FAILED
            logging.error(f"Actor {self.actor_id} failed: {e}")
        
        finally:
            self.state = ActorState.STOPPING
            await self.post_stop()
            self.state = ActorState.STOPPED
    
    async def _handle_error(self, error: Exception, message: Message):
        """Handle errors during message processing."""
        logging.error(f"Actor {self.actor_id} error processing {message}: {error}")
        # In a full implementation, this would consult supervision strategy
    
    async def _handle_restart(self, message: RestartMessage):
        """Handle actor restart."""
        try:
            await self.pre_restart(Exception("Restart requested"))
            # Reset state but keep actor_id and references
            await self.post_restart(Exception("Restart requested"))
        except Exception as e:
            logging.error(f"Actor {self.actor_id} restart failed: {e}")
    
    def stop(self):
        """Request the actor to stop."""
        self._stopping = True
        if self._mailbox:
            try:
                self._mailbox.put_nowait(StopMessage())
            except asyncio.QueueFull:
                pass


class ActorSystem:
    """
    Actor system that manages actor lifecycle and message delivery.
    Provides the runtime environment for actors.
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self._actors: Dict[str, Actor] = {}
        self._actor_refs: Dict[str, ActorRef] = {}
        self._pending_asks: Dict[str, asyncio.Future] = {}
        self._running = False
        self._guardian_actor: Optional[ActorRef] = None
    
    async def start(self):
        """Start the actor system."""
        if self._running:
            return
        
        self._running = True
        logging.info(f"Actor system '{self.name}' started")
    
    async def shutdown(self, timeout: float = 10.0):
        """Shutdown the actor system gracefully."""
        if not self._running:
            return
        
        self._running = False
        
        # Stop all actors
        stop_tasks = []
        for actor in self._actors.values():
            actor.stop()
            if actor._task:
                stop_tasks.append(actor._task)
        
        # Wait for all actors to stop
        if stop_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logging.warning("Some actors did not stop gracefully")
        
        # Cancel pending asks
        for future in self._pending_asks.values():
            if not future.done():
                future.cancel()
        
        self._actors.clear()
        self._actor_refs.clear()
        self._pending_asks.clear()
        
        logging.info(f"Actor system '{self.name}' shutdown")
    
    def actor_of(self, actor_class: Type[Actor], name: Optional[str] = None) -> ActorRef:
        """Create a new actor and return its reference."""
        if not self._running:
            raise RuntimeError("Actor system is not running")
        
        # Generate unique actor ID
        actor_id = name or f"{actor_class.__name__}_{uuid.uuid4().hex[:8]}"
        if actor_id in self._actors:
            raise ValueError(f"Actor with ID '{actor_id}' already exists")
        
        # Create actor instance
        actor = actor_class()
        actor.actor_id = actor_id
        actor.actor_system = self
        
        # Create actor reference
        actor_ref = ActorRef(actor_id, self)
        actor.actor_ref = actor_ref
        
        # Register actor
        self._actors[actor_id] = actor
        self._actor_refs[actor_id] = actor_ref
        
        # Start actor task
        actor._task = asyncio.create_task(actor._process_messages())
        
        logging.info(f"Created actor: {actor_ref}")
        return actor_ref
    
    async def _deliver_message(self, actor_id: str, message: Message):
        """Deliver a message to an actor."""
        actor = self._actors.get(actor_id)
        if not actor:
            logging.warning(f"Actor {actor_id} not found for message delivery")
            return
        
        if actor.state not in [ActorState.RUNNING, ActorState.STARTING]:
            logging.warning(f"Actor {actor_id} is not running, dropping message")
            return
        
        try:
            await actor._mailbox.put(message)
        except Exception as e:
            logging.error(f"Failed to deliver message to {actor_id}: {e}")
    
    async def _handle_response(self, response: ResponseMessage):
        """Handle response message for ask pattern."""
        future = self._pending_asks.get(response.response_id)
        if future and not future.done():
            future.set_result(response.content)
    
    def get_actor_stats(self) -> Dict[str, Any]:
        """Get statistics about the actor system."""
        running_actors = sum(1 for actor in self._actors.values() 
                           if actor.state == ActorState.RUNNING)
        
        return {
            "system_name": self.name,
            "total_actors": len(self._actors),
            "running_actors": running_actors,
            "pending_asks": len(self._pending_asks),
            "is_running": self._running,
        }


# Example actor implementations
class EchoActor(Actor):
    """Simple actor that echoes back received messages."""
    
    async def receive(self, message: Message) -> Optional[Any]:
        if isinstance(message, UserMessage):
            content = message.content
            print(f"EchoActor received: {content}")
            return f"Echo: {content}"
        return None


class CounterActor(Actor):
    """Actor that maintains a counter state."""
    
    def __init__(self):
        super().__init__()
        self.count = 0
    
    async def receive(self, message: Message) -> Optional[Any]:
        if isinstance(message, UserMessage):
            command = message.content
            
            if command == "increment":
                self.count += 1
                return self.count
            elif command == "decrement":
                self.count -= 1
                return self.count
            elif command == "get":
                return self.count
            elif command == "reset":
                self.count = 0
                return 0
        
        return None


class CalculatorActor(Actor):
    """Actor that performs calculations."""
    
    async def receive(self, message: Message) -> Optional[Any]:
        if isinstance(message, UserMessage):
            operation = message.content
            
            if isinstance(operation, dict):
                op = operation.get("op")
                a = operation.get("a", 0)
                b = operation.get("b", 0)
                
                if op == "add":
                    return a + b
                elif op == "subtract":
                    return a - b
                elif op == "multiply":
                    return a * b
                elif op == "divide":
                    if b != 0:
                        return a / b
                    else:
                        raise ValueError("Division by zero")
        
        return None


# Example usage and demonstrations
async def demo_basic_actors():
    """Demonstrate basic actor functionality."""
    print("=== Basic Actor Demo ===")
    
    system = ActorSystem("demo")
    await system.start()
    
    try:
        # Create actors
        echo_ref = system.actor_of(EchoActor, "echo")
        counter_ref = system.actor_of(CounterActor, "counter")
        calc_ref = system.actor_of(CalculatorActor, "calculator")
        
        # Tell pattern (fire-and-forget)
        await echo_ref.tell("Hello, Actor!")
        await counter_ref.tell("increment")
        
        # Ask pattern (request-response)
        echo_response = await echo_ref.ask("Hello from ask!")
        print(f"Echo response: {echo_response}")
        
        count = await counter_ref.ask("get")
        print(f"Counter value: {count}")
        
        await counter_ref.tell("increment")
        await counter_ref.tell("increment")
        
        count = await counter_ref.ask("get")
        print(f"Counter after increments: {count}")
        
        # Calculator operations
        result = await calc_ref.ask({"op": "add", "a": 10, "b": 5})
        print(f"10 + 5 = {result}")
        
        result = await calc_ref.ask({"op": "multiply", "a": 7, "b": 8})
        print(f"7 * 8 = {result}")
        
        # System stats
        stats = system.get_actor_stats()
        print(f"System stats: {stats}")
        
    finally:
        await system.shutdown()


async def demo_actor_communication():
    """Demonstrate actor-to-actor communication."""
    print("\n=== Actor Communication Demo ===")
    
    class ForwarderActor(Actor):
        def __init__(self, target_ref: ActorRef):
            super().__init__()
            self.target_ref = target_ref
        
        async def receive(self, message: Message) -> Optional[Any]:
            if isinstance(message, UserMessage):
                print(f"Forwarder forwarding: {message.content}")
                response = await self.target_ref.ask(message.content)
                return f"Forwarded response: {response}"
            return None
    
    system = ActorSystem("communication")
    await system.start()
    
    try:
        # Create actors
        echo_ref = system.actor_of(EchoActor, "echo")
        
        # Create forwarder with reference to echo actor
        # Note: In a real system, this would be done differently
        # We'll create it manually for demo purposes
        forwarder = ForwarderActor(echo_ref)
        forwarder.actor_id = "forwarder"
        forwarder.actor_system = system
        forwarder_ref = ActorRef("forwarder", system)
        forwarder.actor_ref = forwarder_ref
        system._actors["forwarder"] = forwarder
        system._actor_refs["forwarder"] = forwarder_ref
        forwarder._task = asyncio.create_task(forwarder._process_messages())
        
        # Test forwarding
        response = await forwarder_ref.ask("Hello through forwarder!")
        print(f"Final response: {response}")
        
    finally:
        await system.shutdown()


async def main():
    """Run all demonstrations."""
    await demo_basic_actors()
    await demo_actor_communication()


if __name__ == "__main__":
    asyncio.run(main())
