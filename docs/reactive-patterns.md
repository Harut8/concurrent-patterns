# Reactive Programming Patterns

## Table of Contents
1. [Introduction](#introduction)
2. [Reactive Manifesto](#reactive-manifesto)
3. [Observable Streams](#observable-streams)
4. [Operators and Transformations](#operators-and-transformations)
5. [Backpressure Management](#backpressure-management)
6. [Error Handling](#error-handling)
7. [Schedulers and Threading](#schedulers-and-threading)
8. [Event-Driven Architecture](#event-driven-architecture)

## Introduction

Reactive Programming is a declarative programming paradigm concerned with data streams and the propagation of change. It provides a powerful abstraction for handling asynchronous data flows, making it particularly well-suited for modern applications dealing with user interfaces, real-time data, and distributed systems.

### Core Concepts

1. **Observables**: Streams of data that can be observed
2. **Observers**: Entities that react to observable emissions
3. **Operators**: Functions that transform, filter, or combine streams
4. **Schedulers**: Control where and when operations execute
5. **Subjects**: Act as both observable and observer

### Benefits

- **Composability**: Complex async flows from simple operators
- **Declarative**: What to do, not how to do it
- **Functional**: Immutable transformations
- **Backpressure**: Handle fast producers gracefully
- **Error Propagation**: Structured error handling

## Reactive Manifesto

The Reactive Manifesto defines four key traits of reactive systems:

### 1. Responsive
- Systems respond in a timely manner
- Responsiveness is the cornerstone of usability
- Focus on providing rapid and consistent response times

### 2. Resilient
- Systems stay responsive in the face of failure
- Resilience is achieved by replication, containment, isolation, and delegation
- Failures are contained within each component

### 3. Elastic
- Systems stay responsive under varying workload
- React to changes in input rate by increasing or decreasing resources
- No contention points or central bottlenecks

### 4. Message Driven
- Systems rely on asynchronous message-passing
- Establishes boundaries between components
- Ensures loose coupling, isolation, and location transparency

## Observable Streams

### Observable Types

#### Cold Observables
```python
class ColdObservable:
    """
    Cold observables start emitting when subscribed to.
    Each subscriber gets its own independent execution.
    """
    def __init__(self, producer_function):
        self.producer_function = producer_function
    
    def subscribe(self, observer):
        """Each subscription creates new execution"""
        return self.producer_function(observer)

# Example: HTTP request observable
def http_request_observable(url):
    def producer(observer):
        try:
            response = requests.get(url)
            observer.on_next(response)
            observer.on_completed()
        except Exception as e:
            observer.on_error(e)
    
    return ColdObservable(producer)
```

#### Hot Observables
```python
class HotObservable:
    """
    Hot observables emit regardless of subscribers.
    Subscribers share the same execution.
    """
    def __init__(self):
        self.observers = []
        self.is_emitting = False
    
    def subscribe(self, observer):
        """Join existing execution"""
        self.observers.append(observer)
        return Subscription(lambda: self.observers.remove(observer))
    
    def emit(self, value):
        """Emit to all current subscribers"""
        for observer in self.observers[:]:  # Copy to avoid modification during iteration
            try:
                observer.on_next(value)
            except Exception as e:
                observer.on_error(e)

# Example: Mouse movement observable
mouse_movements = HotObservable()

def on_mouse_move(event):
    mouse_movements.emit(MousePosition(event.x, event.y))
```

### Subject Types

#### PublishSubject
```python
class PublishSubject:
    """
    Emits to subscribers only items emitted after subscription.
    """
    def __init__(self):
        self.observers = []
        self.is_stopped = False
        self.error = None
    
    def subscribe(self, observer):
        if self.is_stopped:
            if self.error:
                observer.on_error(self.error)
            else:
                observer.on_completed()
            return EmptySubscription()
        
        self.observers.append(observer)
        return Subscription(lambda: self.observers.remove(observer))
    
    def on_next(self, value):
        if not self.is_stopped:
            for observer in self.observers[:]:
                observer.on_next(value)
    
    def on_error(self, error):
        if not self.is_stopped:
            self.is_stopped = True
            self.error = error
            for observer in self.observers[:]:
                observer.on_error(error)
            self.observers.clear()
    
    def on_completed(self):
        if not self.is_stopped:
            self.is_stopped = True
            for observer in self.observers[:]:
                observer.on_completed()
            self.observers.clear()
```

#### BehaviorSubject
```python
class BehaviorSubject:
    """
    Emits the most recent item and all subsequent items.
    Requires an initial value.
    """
    def __init__(self, initial_value):
        self.current_value = initial_value
        self.observers = []
        self.is_stopped = False
        self.error = None
    
    def subscribe(self, observer):
        if self.is_stopped:
            if self.error:
                observer.on_error(self.error)
            else:
                observer.on_next(self.current_value)
                observer.on_completed()
            return EmptySubscription()
        
        # Emit current value immediately
        observer.on_next(self.current_value)
        self.observers.append(observer)
        return Subscription(lambda: self.observers.remove(observer))
    
    def on_next(self, value):
        if not self.is_stopped:
            self.current_value = value
            for observer in self.observers[:]:
                observer.on_next(value)
    
    def get_value(self):
        """Get current value synchronously"""
        return self.current_value
```

#### ReplaySubject
```python
class ReplaySubject:
    """
    Emits all previously emitted items to new subscribers.
    Can be configured with buffer size and time window.
    """
    def __init__(self, buffer_size=None, window_time=None):
        self.buffer_size = buffer_size
        self.window_time = window_time
        self.buffer = []
        self.observers = []
        self.is_stopped = False
        self.error = None
    
    def subscribe(self, observer):
        if self.is_stopped:
            # Replay buffered items
            for item in self.get_valid_buffer():
                observer.on_next(item.value)
            
            if self.error:
                observer.on_error(self.error)
            else:
                observer.on_completed()
            return EmptySubscription()
        
        # Replay buffered items to new subscriber
        for item in self.get_valid_buffer():
            observer.on_next(item.value)
        
        self.observers.append(observer)
        return Subscription(lambda: self.observers.remove(observer))
    
    def on_next(self, value):
        if not self.is_stopped:
            # Add to buffer
            item = BufferItem(value, time.time())
            self.buffer.append(item)
            self.trim_buffer()
            
            # Emit to current subscribers
            for observer in self.observers[:]:
                observer.on_next(value)
    
    def get_valid_buffer(self):
        """Get buffer items within time window"""
        if self.window_time is None:
            return self.buffer
        
        cutoff_time = time.time() - self.window_time
        return [item for item in self.buffer if item.timestamp > cutoff_time]
    
    def trim_buffer(self):
        """Trim buffer to size and time limits"""
        # Trim by time
        if self.window_time is not None:
            cutoff_time = time.time() - self.window_time
            self.buffer = [item for item in self.buffer if item.timestamp > cutoff_time]
        
        # Trim by size
        if self.buffer_size is not None and len(self.buffer) > self.buffer_size:
            self.buffer = self.buffer[-self.buffer_size:]
```

## Operators and Transformations

### Transformation Operators

#### Map
```python
def map_operator(source_observable, transform_function):
    """Transform each emitted item using a function"""
    def subscribe(observer):
        def on_next(value):
            try:
                transformed = transform_function(value)
                observer.on_next(transformed)
            except Exception as e:
                observer.on_error(e)
        
        return source_observable.subscribe(Observer(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        ))
    
    return Observable(subscribe)

# Usage
numbers = Observable.range(1, 5)
squared = numbers.pipe(map_operator(lambda x: x * x))
```

#### FlatMap
```python
def flat_map_operator(source_observable, project_function):
    """Transform each item into an observable and flatten the result"""
    def subscribe(observer):
        active_subscriptions = []
        completed_count = 0
        source_completed = False
        
        def check_completion():
            if source_completed and completed_count == len(active_subscriptions):
                observer.on_completed()
        
        def on_next(value):
            try:
                inner_observable = project_function(value)
                
                def inner_on_completed():
                    nonlocal completed_count
                    completed_count += 1
                    check_completion()
                
                subscription = inner_observable.subscribe(Observer(
                    on_next=observer.on_next,
                    on_error=observer.on_error,
                    on_completed=inner_on_completed
                ))
                active_subscriptions.append(subscription)
            except Exception as e:
                observer.on_error(e)
        
        def on_completed():
            nonlocal source_completed
            source_completed = True
            check_completion()
        
        source_subscription = source_observable.subscribe(Observer(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=on_completed
        ))
        
        return CompositeSubscription([source_subscription] + active_subscriptions)
    
    return Observable(subscribe)
```

### Filtering Operators

#### Filter
```python
def filter_operator(source_observable, predicate):
    """Emit only items that pass the predicate test"""
    def subscribe(observer):
        def on_next(value):
            try:
                if predicate(value):
                    observer.on_next(value)
            except Exception as e:
                observer.on_error(e)
        
        return source_observable.subscribe(Observer(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        ))
    
    return Observable(subscribe)
```

#### Debounce
```python
def debounce_operator(source_observable, timeout_duration):
    """Emit item only after timeout duration without new emissions"""
    def subscribe(observer):
        last_emission_time = [0]
        pending_emission = [None]
        timer_subscription = [None]
        
        def emit_pending():
            if pending_emission[0] is not None:
                observer.on_next(pending_emission[0])
                pending_emission[0] = None
        
        def on_next(value):
            current_time = time.time()
            last_emission_time[0] = current_time
            pending_emission[0] = value
            
            # Cancel previous timer
            if timer_subscription[0]:
                timer_subscription[0].dispose()
            
            # Start new timer
            def timer_callback():
                if time.time() - last_emission_time[0] >= timeout_duration:
                    emit_pending()
            
            timer_subscription[0] = Timer(timeout_duration, timer_callback)
        
        return source_observable.subscribe(Observer(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=lambda: (emit_pending(), observer.on_completed())
        ))
    
    return Observable(subscribe)
```

### Combination Operators

#### Merge
```python
def merge_operator(*source_observables):
    """Merge multiple observables into one"""
    def subscribe(observer):
        subscriptions = []
        completed_count = [0]
        
        def check_completion():
            if completed_count[0] == len(source_observables):
                observer.on_completed()
        
        def create_on_completed():
            def on_completed():
                completed_count[0] += 1
                check_completion()
            return on_completed
        
        for source in source_observables:
            subscription = source.subscribe(Observer(
                on_next=observer.on_next,
                on_error=observer.on_error,
                on_completed=create_on_completed()
            ))
            subscriptions.append(subscription)
        
        return CompositeSubscription(subscriptions)
    
    return Observable(subscribe)
```

#### CombineLatest
```python
def combine_latest_operator(*source_observables, combiner_function=None):
    """Combine latest values from multiple observables"""
    def subscribe(observer):
        latest_values = [None] * len(source_observables)
        has_value = [False] * len(source_observables)
        completed_count = [0]
        
        def try_emit():
            if all(has_value):
                try:
                    if combiner_function:
                        result = combiner_function(*latest_values)
                    else:
                        result = tuple(latest_values)
                    observer.on_next(result)
                except Exception as e:
                    observer.on_error(e)
        
        def create_on_next(index):
            def on_next(value):
                latest_values[index] = value
                has_value[index] = True
                try_emit()
            return on_next
        
        def create_on_completed():
            def on_completed():
                completed_count[0] += 1
                if completed_count[0] == len(source_observables):
                    observer.on_completed()
            return on_completed
        
        subscriptions = []
        for i, source in enumerate(source_observables):
            subscription = source.subscribe(Observer(
                on_next=create_on_next(i),
                on_error=observer.on_error,
                on_completed=create_on_completed()
            ))
            subscriptions.append(subscription)
        
        return CompositeSubscription(subscriptions)
    
    return Observable(subscribe)
```

## Backpressure Management

### Backpressure Strategies

#### Buffer Strategy
```python
class BufferBackpressureStrategy:
    """Buffer items when consumer is slower than producer"""
    def __init__(self, buffer_size=1000, overflow_strategy='drop_oldest'):
        self.buffer_size = buffer_size
        self.overflow_strategy = overflow_strategy
        self.buffer = []
    
    def handle_item(self, item, emit_function):
        if len(self.buffer) < self.buffer_size:
            self.buffer.append(item)
        else:
            if self.overflow_strategy == 'drop_oldest':
                self.buffer.pop(0)
                self.buffer.append(item)
            elif self.overflow_strategy == 'drop_newest':
                pass  # Don't add the new item
            elif self.overflow_strategy == 'error':
                raise BackpressureError("Buffer overflow")
        
        # Try to emit buffered items
        while self.buffer:
            if emit_function(self.buffer[0]):
                self.buffer.pop(0)
            else:
                break  # Consumer not ready
```

#### Sample Strategy
```python
def sample_operator(source_observable, sample_interval):
    """Sample the latest value at regular intervals"""
    def subscribe(observer):
        latest_value = [None]
        has_value = [False]
        
        def emit_sample():
            if has_value[0]:
                observer.on_next(latest_value[0])
                has_value[0] = False
        
        # Set up sampling timer
        timer = Timer.interval(sample_interval, emit_sample)
        
        def on_next(value):
            latest_value[0] = value
            has_value[0] = True
        
        source_subscription = source_observable.subscribe(Observer(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        ))
        
        return CompositeSubscription([source_subscription, timer])
    
    return Observable(subscribe)
```

#### Throttle Strategy
```python
def throttle_operator(source_observable, throttle_duration):
    """Throttle emissions to at most one per duration"""
    def subscribe(observer):
        last_emission_time = [0]
        
        def on_next(value):
            current_time = time.time()
            if current_time - last_emission_time[0] >= throttle_duration:
                last_emission_time[0] = current_time
                observer.on_next(value)
        
        return source_observable.subscribe(Observer(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        ))
    
    return Observable(subscribe)
```

## Error Handling

### Error Recovery Operators

#### Retry
```python
def retry_operator(source_observable, max_retries=3):
    """Retry the source observable on error"""
    def subscribe(observer):
        retry_count = [0]
        
        def attempt_subscription():
            def on_error(error):
                if retry_count[0] < max_retries:
                    retry_count[0] += 1
                    # Retry after a delay
                    Timer(1.0, attempt_subscription).start()
                else:
                    observer.on_error(error)
            
            return source_observable.subscribe(Observer(
                on_next=observer.on_next,
                on_error=on_error,
                on_completed=observer.on_completed
            ))
        
        return attempt_subscription()
    
    return Observable(subscribe)
```

#### Catch
```python
def catch_operator(source_observable, error_handler):
    """Handle errors by switching to another observable"""
    def subscribe(observer):
        def on_error(error):
            try:
                recovery_observable = error_handler(error)
                recovery_observable.subscribe(observer)
            except Exception as e:
                observer.on_error(e)
        
        return source_observable.subscribe(Observer(
            on_next=observer.on_next,
            on_error=on_error,
            on_completed=observer.on_completed
        ))
    
    return Observable(subscribe)
```

## Schedulers and Threading

### Scheduler Types

#### ImmediateScheduler
```python
class ImmediateScheduler:
    """Execute work immediately on current thread"""
    def schedule(self, action, delay=0):
        if delay > 0:
            time.sleep(delay)
        action()
        return EmptySubscription()
```

#### ThreadPoolScheduler
```python
class ThreadPoolScheduler:
    """Execute work on thread pool"""
    def __init__(self, max_workers=None):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def schedule(self, action, delay=0):
        def delayed_action():
            if delay > 0:
                time.sleep(delay)
            action()
        
        future = self.executor.submit(delayed_action)
        return FutureSubscription(future)
```

#### EventLoopScheduler
```python
class EventLoopScheduler:
    """Execute work on asyncio event loop"""
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
    
    def schedule(self, action, delay=0):
        if delay > 0:
            handle = self.loop.call_later(delay, action)
        else:
            handle = self.loop.call_soon(action)
        
        return HandleSubscription(handle)
```

## Event-Driven Architecture

### Event Sourcing Pattern

```python
class EventStore:
    """Store for domain events"""
    def __init__(self):
        self.events = []
        self.event_subject = PublishSubject()
    
    def append_event(self, event):
        """Append event to store and notify subscribers"""
        self.events.append(event)
        self.event_subject.on_next(event)
    
    def get_events(self, aggregate_id=None, from_version=0):
        """Get events for aggregate or all events"""
        if aggregate_id:
            return [e for e in self.events 
                   if e.aggregate_id == aggregate_id and e.version >= from_version]
        return self.events[from_version:]
    
    def observe_events(self, event_type=None):
        """Get observable stream of events"""
        stream = self.event_subject
        if event_type:
            stream = stream.pipe(filter_operator(lambda e: isinstance(e, event_type)))
        return stream
```

### CQRS Pattern

```python
class CommandHandler:
    """Handle commands and generate events"""
    def __init__(self, event_store):
        self.event_store = event_store
    
    def handle(self, command):
        """Process command and generate events"""
        try:
            events = self.process_command(command)
            for event in events:
                self.event_store.append_event(event)
            return CommandResult(success=True, events=events)
        except Exception as e:
            return CommandResult(success=False, error=str(e))
    
    def process_command(self, command):
        """Override in subclasses"""
        raise NotImplementedError

class QueryHandler:
    """Handle queries using read models"""
    def __init__(self, read_model_store):
        self.read_model_store = read_model_store
    
    def handle(self, query):
        """Process query and return result"""
        return self.process_query(query)
    
    def process_query(self, query):
        """Override in subclasses"""
        raise NotImplementedError
```

---

*Reactive programming provides powerful abstractions for handling asynchronous data flows. Understanding these patterns is crucial for building responsive, resilient, and scalable systems in the modern computing landscape.*
