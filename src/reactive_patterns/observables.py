"""
Observable Patterns

Implementation of reactive observables for event-driven programming.
"""

import asyncio
import threading
import time
import weakref
from typing import Optional, List, Dict, Any, Callable, TypeVar, Generic, Iterator, Union
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import logging
import queue
from concurrent.futures import ThreadPoolExecutor


T = TypeVar('T')
R = TypeVar('R')


class ObservableState(Enum):
    """Observable states."""
    ACTIVE = "active"
    COMPLETED = "completed"
    ERROR = "error"
    DISPOSED = "disposed"


@dataclass
class Subscription:
    """Represents a subscription to an observable."""
    observer_id: str
    disposed: bool = False
    
    def dispose(self):
        """Dispose the subscription."""
        self.disposed = True


class Observer(ABC, Generic[T]):
    """
    Abstract observer interface.
    """
    
    @abstractmethod
    def on_next(self, value: T):
        """Called when observable emits a value."""
        pass
    
    @abstractmethod
    def on_error(self, error: Exception):
        """Called when observable encounters an error."""
        pass
    
    @abstractmethod
    def on_completed(self):
        """Called when observable completes."""
        pass


class SimpleObserver(Observer[T]):
    """
    Simple observer implementation with callback functions.
    """
    
    def __init__(self, 
                 on_next: Optional[Callable[[T], None]] = None,
                 on_error: Optional[Callable[[Exception], None]] = None,
                 on_completed: Optional[Callable[[], None]] = None):
        self._on_next = on_next or (lambda x: None)
        self._on_error = on_error or (lambda e: None)
        self._on_completed = on_completed or (lambda: None)
    
    def on_next(self, value: T):
        """Handle next value."""
        try:
            self._on_next(value)
        except Exception as e:
            self.on_error(e)
    
    def on_error(self, error: Exception):
        """Handle error."""
        self._on_error(error)
    
    def on_completed(self):
        """Handle completion."""
        self._on_completed()


class Observable(ABC, Generic[T]):
    """
    Abstract observable base class.
    """
    
    def __init__(self):
        self._observers: Dict[str, Observer[T]] = {}
        self._state = ObservableState.ACTIVE
        self._lock = threading.RLock()
        self._observer_counter = 0
    
    @abstractmethod
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Implementation-specific subscription logic."""
        pass
    
    def subscribe(self, observer: Observer[T]) -> Subscription:
        """Subscribe an observer to this observable."""
        with self._lock:
            if self._state == ObservableState.DISPOSED:
                raise ValueError("Observable is disposed")
            
            observer_id = f"observer_{self._observer_counter}"
            self._observer_counter += 1
            
            self._observers[observer_id] = observer
            subscription = Subscription(observer_id)
            
            # Call implementation-specific logic
            self._subscribe_impl(observer)
            
            return subscription
    
    def subscribe_simple(self, 
                        on_next: Optional[Callable[[T], None]] = None,
                        on_error: Optional[Callable[[Exception], None]] = None,
                        on_completed: Optional[Callable[[], None]] = None) -> Subscription:
        """Subscribe with simple callback functions."""
        observer = SimpleObserver(on_next, on_error, on_completed)
        return self.subscribe(observer)
    
    def unsubscribe(self, subscription: Subscription):
        """Unsubscribe an observer."""
        with self._lock:
            if subscription.observer_id in self._observers:
                del self._observers[subscription.observer_id]
                subscription.dispose()
    
    def _emit_next(self, value: T):
        """Emit next value to all observers."""
        with self._lock:
            if self._state != ObservableState.ACTIVE:
                return
            
            for observer in list(self._observers.values()):
                try:
                    observer.on_next(value)
                except Exception as e:
                    logging.error(f"Observer error: {e}")
    
    def _emit_error(self, error: Exception):
        """Emit error to all observers."""
        with self._lock:
            if self._state != ObservableState.ACTIVE:
                return
            
            self._state = ObservableState.ERROR
            
            for observer in list(self._observers.values()):
                try:
                    observer.on_error(error)
                except Exception as e:
                    logging.error(f"Observer error handling error: {e}")
    
    def _emit_completed(self):
        """Emit completion to all observers."""
        with self._lock:
            if self._state != ObservableState.ACTIVE:
                return
            
            self._state = ObservableState.COMPLETED
            
            for observer in list(self._observers.values()):
                try:
                    observer.on_completed()
                except Exception as e:
                    logging.error(f"Observer error handling completion: {e}")
    
    def dispose(self):
        """Dispose the observable."""
        with self._lock:
            self._state = ObservableState.DISPOSED
            self._observers.clear()
    
    # Operators
    def map(self, func: Callable[[T], R]) -> 'Observable[R]':
        """Transform values using a function."""
        return MapObservable(self, func)
    
    def filter(self, predicate: Callable[[T], bool]) -> 'Observable[T]':
        """Filter values using a predicate."""
        return FilterObservable(self, predicate)
    
    def take(self, count: int) -> 'Observable[T]':
        """Take only the first n values."""
        return TakeObservable(self, count)
    
    def skip(self, count: int) -> 'Observable[T]':
        """Skip the first n values."""
        return SkipObservable(self, count)
    
    def throttle(self, interval: float) -> 'Observable[T]':
        """Throttle emissions to at most one per interval."""
        return ThrottleObservable(self, interval)
    
    def debounce(self, interval: float) -> 'Observable[T]':
        """Debounce emissions, only emitting after silence."""
        return DebounceObservable(self, interval)


class ColdObservable(Observable[T]):
    """
    Cold observable that starts emitting when subscribed to.
    """
    
    def __init__(self, producer: Callable[[Observer[T]], None]):
        super().__init__()
        self._producer = producer
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Start producing values for this observer."""
        try:
            self._producer(observer)
        except Exception as e:
            observer.on_error(e)
        
        return Subscription(f"cold_{id(observer)}")


class HotObservable(Observable[T]):
    """
    Hot observable that emits values regardless of subscribers.
    """
    
    def __init__(self):
        super().__init__()
        self._is_emitting = False
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Hot observables don't need special subscription logic."""
        return Subscription(f"hot_{id(observer)}")
    
    def emit(self, value: T):
        """Manually emit a value."""
        self._emit_next(value)
    
    def error(self, error: Exception):
        """Manually emit an error."""
        self._emit_error(error)
    
    def complete(self):
        """Manually complete the observable."""
        self._emit_completed()


class IntervalObservable(Observable[int]):
    """
    Observable that emits incrementing integers at regular intervals.
    """
    
    def __init__(self, interval: float):
        super().__init__()
        self.interval = interval
        self._timer_thread = None
        self._stop_event = threading.Event()
        self._counter = 0
    
    def _subscribe_impl(self, observer: Observer[int]) -> Subscription:
        """Start the interval timer if not already running."""
        if self._timer_thread is None or not self._timer_thread.is_alive():
            self._start_timer()
        
        return Subscription(f"interval_{id(observer)}")
    
    def _start_timer(self):
        """Start the interval timer."""
        self._stop_event.clear()
        self._timer_thread = threading.Thread(target=self._timer_loop)
        self._timer_thread.daemon = True
        self._timer_thread.start()
    
    def _timer_loop(self):
        """Timer loop that emits values."""
        while not self._stop_event.wait(self.interval):
            if self._state == ObservableState.ACTIVE:
                self._emit_next(self._counter)
                self._counter += 1
            else:
                break
    
    def dispose(self):
        """Stop the timer and dispose."""
        self._stop_event.set()
        super().dispose()


class FromIterableObservable(Observable[T]):
    """
    Observable that emits values from an iterable.
    """
    
    def __init__(self, iterable: Iterator[T]):
        super().__init__()
        self.iterable = iterable
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Emit all values from the iterable."""
        def emit_values():
            try:
                for value in self.iterable:
                    if self._state != ObservableState.ACTIVE:
                        break
                    observer.on_next(value)
                
                if self._state == ObservableState.ACTIVE:
                    observer.on_completed()
            except Exception as e:
                observer.on_error(e)
        
        # Emit in a separate thread to avoid blocking
        thread = threading.Thread(target=emit_values)
        thread.daemon = True
        thread.start()
        
        return Subscription(f"iterable_{id(observer)}")


# Operator Observables
class MapObservable(Observable[R]):
    """Observable that maps values using a function."""
    
    def __init__(self, source: Observable[T], func: Callable[[T], R]):
        super().__init__()
        self.source = source
        self.func = func
    
    def _subscribe_impl(self, observer: Observer[R]) -> Subscription:
        """Subscribe to source and map values."""
        def on_next(value: T):
            try:
                mapped_value = self.func(value)
                observer.on_next(mapped_value)
            except Exception as e:
                observer.on_error(e)
        
        source_observer = SimpleObserver(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        )
        
        return self.source.subscribe(source_observer)


class FilterObservable(Observable[T]):
    """Observable that filters values using a predicate."""
    
    def __init__(self, source: Observable[T], predicate: Callable[[T], bool]):
        super().__init__()
        self.source = source
        self.predicate = predicate
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Subscribe to source and filter values."""
        def on_next(value: T):
            try:
                if self.predicate(value):
                    observer.on_next(value)
            except Exception as e:
                observer.on_error(e)
        
        source_observer = SimpleObserver(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        )
        
        return self.source.subscribe(source_observer)


class TakeObservable(Observable[T]):
    """Observable that takes only the first n values."""
    
    def __init__(self, source: Observable[T], count: int):
        super().__init__()
        self.source = source
        self.count = count
        self.taken = 0
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Subscribe to source and take only n values."""
        def on_next(value: T):
            if self.taken < self.count:
                observer.on_next(value)
                self.taken += 1
                
                if self.taken >= self.count:
                    observer.on_completed()
        
        source_observer = SimpleObserver(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        )
        
        return self.source.subscribe(source_observer)


class SkipObservable(Observable[T]):
    """Observable that skips the first n values."""
    
    def __init__(self, source: Observable[T], count: int):
        super().__init__()
        self.source = source
        self.count = count
        self.skipped = 0
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Subscribe to source and skip first n values."""
        def on_next(value: T):
            if self.skipped < self.count:
                self.skipped += 1
            else:
                observer.on_next(value)
        
        source_observer = SimpleObserver(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        )
        
        return self.source.subscribe(source_observer)


class ThrottleObservable(Observable[T]):
    """Observable that throttles emissions."""
    
    def __init__(self, source: Observable[T], interval: float):
        super().__init__()
        self.source = source
        self.interval = interval
        self.last_emit_time = 0
        self.lock = threading.Lock()
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Subscribe to source and throttle emissions."""
        def on_next(value: T):
            with self.lock:
                current_time = time.time()
                if current_time - self.last_emit_time >= self.interval:
                    self.last_emit_time = current_time
                    observer.on_next(value)
        
        source_observer = SimpleObserver(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        )
        
        return self.source.subscribe(source_observer)


class DebounceObservable(Observable[T]):
    """Observable that debounces emissions."""
    
    def __init__(self, source: Observable[T], interval: float):
        super().__init__()
        self.source = source
        self.interval = interval
        self.timer = None
        self.lock = threading.Lock()
        self.latest_value = None
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Subscribe to source and debounce emissions."""
        def on_next(value: T):
            with self.lock:
                self.latest_value = value
                
                # Cancel previous timer
                if self.timer:
                    self.timer.cancel()
                
                # Start new timer
                def emit_latest():
                    with self.lock:
                        if self.latest_value is not None:
                            observer.on_next(self.latest_value)
                            self.latest_value = None
                
                self.timer = threading.Timer(self.interval, emit_latest)
                self.timer.start()
        
        source_observer = SimpleObserver(
            on_next=on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed
        )
        
        return self.source.subscribe(source_observer)


class MergeObservable(Observable[T]):
    """Observable that merges multiple source observables."""
    
    def __init__(self, *sources: Observable[T]):
        super().__init__()
        self.sources = sources
        self.completed_count = 0
        self.lock = threading.Lock()
    
    def _subscribe_impl(self, observer: Observer[T]) -> Subscription:
        """Subscribe to all sources and merge their emissions."""
        subscriptions = []
        
        def on_next(value: T):
            observer.on_next(value)
        
        def on_error(error: Exception):
            observer.on_error(error)
        
        def on_completed():
            with self.lock:
                self.completed_count += 1
                if self.completed_count >= len(self.sources):
                    observer.on_completed()
        
        # Subscribe to all sources
        for source in self.sources:
            source_observer = SimpleObserver(
                on_next=on_next,
                on_error=on_error,
                on_completed=on_completed
            )
            subscription = source.subscribe(source_observer)
            subscriptions.append(subscription)
        
        # Return a composite subscription
        return Subscription(f"merge_{id(observer)}")


# Factory functions
def observable_from_iterable(iterable: Iterator[T]) -> Observable[T]:
    """Create observable from iterable."""
    return FromIterableObservable(iterable)


def observable_interval(interval: float) -> Observable[int]:
    """Create observable that emits at intervals."""
    return IntervalObservable(interval)


def observable_just(*values: T) -> Observable[T]:
    """Create observable that emits given values."""
    return FromIterableObservable(iter(values))


def observable_empty() -> Observable[T]:
    """Create observable that immediately completes."""
    def producer(observer: Observer[T]):
        observer.on_completed()
    
    return ColdObservable(producer)


def observable_error(error: Exception) -> Observable[T]:
    """Create observable that immediately errors."""
    def producer(observer: Observer[T]):
        observer.on_error(error)
    
    return ColdObservable(producer)


def observable_merge(*sources: Observable[T]) -> Observable[T]:
    """Merge multiple observables."""
    return MergeObservable(*sources)


# Example usage and demonstrations
def demo_cold_observable():
    """Demonstrate cold observable."""
    print("=== Cold Observable Demo ===")
    
    def producer(observer: Observer[int]):
        for i in range(5):
            observer.on_next(i)
            time.sleep(0.1)
        observer.on_completed()
    
    cold_obs = ColdObservable(producer)
    
    # Subscribe multiple observers
    results1 = []
    results2 = []
    
    sub1 = cold_obs.subscribe_simple(
        on_next=lambda x: results1.append(x),
        on_completed=lambda: print("Observer 1 completed")
    )
    
    time.sleep(0.3)  # Delay second subscription
    
    sub2 = cold_obs.subscribe_simple(
        on_next=lambda x: results2.append(x),
        on_completed=lambda: print("Observer 2 completed")
    )
    
    time.sleep(1)
    
    print(f"Observer 1 results: {results1}")
    print(f"Observer 2 results: {results2}")


def demo_hot_observable():
    """Demonstrate hot observable."""
    print("\n=== Hot Observable Demo ===")
    
    hot_obs = HotObservable[int]()
    
    results1 = []
    results2 = []
    
    # First subscriber
    sub1 = hot_obs.subscribe_simple(
        on_next=lambda x: results1.append(x)
    )
    
    # Emit some values
    hot_obs.emit(1)
    hot_obs.emit(2)
    
    # Second subscriber (will miss earlier values)
    sub2 = hot_obs.subscribe_simple(
        on_next=lambda x: results2.append(x)
    )
    
    # Emit more values
    hot_obs.emit(3)
    hot_obs.emit(4)
    
    print(f"Observer 1 results: {results1}")
    print(f"Observer 2 results: {results2}")


def demo_operators():
    """Demonstrate observable operators."""
    print("\n=== Observable Operators Demo ===")
    
    # Create source observable
    source = observable_from_iterable(range(10))
    
    # Apply operators
    filtered = source.filter(lambda x: x % 2 == 0)
    mapped = filtered.map(lambda x: x * 2)
    taken = mapped.take(3)
    
    results = []
    taken.subscribe_simple(
        on_next=lambda x: results.append(x),
        on_completed=lambda: print("Operators demo completed")
    )
    
    time.sleep(0.5)
    print(f"Filtered, mapped, and taken results: {results}")


def demo_interval_observable():
    """Demonstrate interval observable."""
    print("\n=== Interval Observable Demo ===")
    
    interval_obs = observable_interval(0.2)
    throttled = interval_obs.throttle(0.5).take(5)
    
    results = []
    subscription = throttled.subscribe_simple(
        on_next=lambda x: results.append(x),
        on_completed=lambda: print("Interval demo completed")
    )
    
    time.sleep(3)
    print(f"Throttled interval results: {results}")


def demo_merge_observable():
    """Demonstrate merge observable."""
    print("\n=== Merge Observable Demo ===")
    
    obs1 = observable_just(1, 2, 3)
    obs2 = observable_just(4, 5, 6)
    obs3 = observable_just(7, 8, 9)
    
    merged = observable_merge(obs1, obs2, obs3)
    
    results = []
    merged.subscribe_simple(
        on_next=lambda x: results.append(x),
        on_completed=lambda: print("Merge demo completed")
    )
    
    time.sleep(0.5)
    print(f"Merged results: {sorted(results)}")


def main():
    """Run all observable demonstrations."""
    demo_cold_observable()
    demo_hot_observable()
    demo_operators()
    demo_interval_observable()
    demo_merge_observable()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
