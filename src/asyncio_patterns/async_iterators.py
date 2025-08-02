"""
Async Iterator Patterns

Advanced async iterator implementations for stream processing and data transformation.
"""

import asyncio
import time
from typing import AsyncIterator, AsyncIterable, TypeVar, Generic, Callable, Optional, List, Any
from abc import ABC, abstractmethod
import logging


T = TypeVar('T')
U = TypeVar('U')


class AsyncBatchIterator(Generic[T]):
    """
    Async iterator that batches items from an async iterable.
    """
    
    def __init__(self, iterable: AsyncIterable[T], batch_size: int, timeout: Optional[float] = None):
        self.iterable = iterable
        self.batch_size = batch_size
        self.timeout = timeout
        self._iterator = None
    
    def __aiter__(self) -> AsyncIterator[List[T]]:
        return self
    
    async def __anext__(self) -> List[T]:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        batch = []
        start_time = time.time()
        
        while len(batch) < self.batch_size:
            try:
                # Calculate remaining timeout
                remaining_timeout = None
                if self.timeout:
                    elapsed = time.time() - start_time
                    remaining_timeout = max(0, self.timeout - elapsed)
                    if remaining_timeout <= 0:
                        break
                
                if remaining_timeout is not None:
                    item = await asyncio.wait_for(anext(self._iterator), timeout=remaining_timeout)
                else:
                    item = await anext(self._iterator)
                
                batch.append(item)
                
            except StopAsyncIteration:
                if batch:
                    return batch
                raise
            except asyncio.TimeoutError:
                if batch:
                    return batch
                raise StopAsyncIteration
        
        return batch


class AsyncChainIterator(Generic[T]):
    """
    Async iterator that chains multiple async iterables.
    """
    
    def __init__(self, *iterables: AsyncIterable[T]):
        self.iterables = iterables
        self._current_index = 0
        self._current_iterator = None
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self
    
    async def __anext__(self) -> T:
        while self._current_index < len(self.iterables):
            if self._current_iterator is None:
                self._current_iterator = aiter(self.iterables[self._current_index])
            
            try:
                return await anext(self._current_iterator)
            except StopAsyncIteration:
                self._current_iterator = None
                self._current_index += 1
        
        raise StopAsyncIteration


class AsyncFilterIterator(Generic[T]):
    """
    Async iterator that filters items based on a predicate.
    """
    
    def __init__(self, iterable: AsyncIterable[T], predicate: Callable[[T], bool]):
        self.iterable = iterable
        self.predicate = predicate
        self._iterator = None
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self
    
    async def __anext__(self) -> T:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        while True:
            item = await anext(self._iterator)
            if self.predicate(item):
                return item


class AsyncMapIterator(Generic[T, U]):
    """
    Async iterator that transforms items using a function.
    """
    
    def __init__(self, iterable: AsyncIterable[T], func: Callable[[T], U]):
        self.iterable = iterable
        self.func = func
        self._iterator = None
    
    def __aiter__(self) -> AsyncIterator[U]:
        return self
    
    async def __anext__(self) -> U:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        item = await anext(self._iterator)
        
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(item)
        else:
            return self.func(item)


class AsyncTakeIterator(Generic[T]):
    """
    Async iterator that takes only the first n items.
    """
    
    def __init__(self, iterable: AsyncIterable[T], count: int):
        self.iterable = iterable
        self.count = count
        self._taken = 0
        self._iterator = None
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self
    
    async def __anext__(self) -> T:
        if self._taken >= self.count:
            raise StopAsyncIteration
        
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        item = await anext(self._iterator)
        self._taken += 1
        return item


class AsyncSkipIterator(Generic[T]):
    """
    Async iterator that skips the first n items.
    """
    
    def __init__(self, iterable: AsyncIterable[T], count: int):
        self.iterable = iterable
        self.count = count
        self._skipped = 0
        self._iterator = None
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self
    
    async def __anext__(self) -> T:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        while self._skipped < self.count:
            await anext(self._iterator)
            self._skipped += 1
        
        return await anext(self._iterator)


class AsyncWindowIterator(Generic[T]):
    """
    Async iterator that creates sliding windows of items.
    """
    
    def __init__(self, iterable: AsyncIterable[T], window_size: int, step: int = 1):
        self.iterable = iterable
        self.window_size = window_size
        self.step = step
        self._buffer = []
        self._iterator = None
        self._exhausted = False
    
    def __aiter__(self) -> AsyncIterator[List[T]]:
        return self
    
    async def __anext__(self) -> List[T]:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        # Fill initial buffer
        while len(self._buffer) < self.window_size and not self._exhausted:
            try:
                item = await anext(self._iterator)
                self._buffer.append(item)
            except StopAsyncIteration:
                self._exhausted = True
                break
        
        if len(self._buffer) < self.window_size:
            raise StopAsyncIteration
        
        # Create window
        window = self._buffer[:self.window_size]
        
        # Advance buffer by step
        for _ in range(min(self.step, len(self._buffer))):
            self._buffer.pop(0)
        
        # Fill buffer for next window
        items_needed = min(self.step, self.window_size)
        for _ in range(items_needed):
            if self._exhausted:
                break
            try:
                item = await anext(self._iterator)
                self._buffer.append(item)
            except StopAsyncIteration:
                self._exhausted = True
                break
        
        return window


class AsyncGroupByIterator(Generic[T]):
    """
    Async iterator that groups consecutive items by a key function.
    """
    
    def __init__(self, iterable: AsyncIterable[T], key_func: Callable[[T], Any]):
        self.iterable = iterable
        self.key_func = key_func
        self._iterator = None
        self._current_key = object()  # Sentinel
        self._current_group = []
        self._exhausted = False
    
    def __aiter__(self) -> AsyncIterator[tuple[Any, List[T]]]:
        return self
    
    async def __anext__(self) -> tuple[Any, List[T]]:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        if self._exhausted and not self._current_group:
            raise StopAsyncIteration
        
        # If we have a current group, return it
        if self._current_group and not self._exhausted:
            group_key = self._current_key
            group_items = self._current_group.copy()
            self._current_group.clear()
            
            # Start building next group
            await self._build_next_group()
            
            return group_key, group_items
        
        # Build first group
        await self._build_next_group()
        
        if not self._current_group:
            raise StopAsyncIteration
        
        group_key = self._current_key
        group_items = self._current_group.copy()
        self._current_group.clear()
        
        return group_key, group_items
    
    async def _build_next_group(self):
        """Build the next group of items with the same key."""
        try:
            while True:
                item = await anext(self._iterator)
                key = self.key_func(item)
                
                if self._current_key is object() or key == self._current_key:
                    # Same group or first item
                    self._current_key = key
                    self._current_group.append(item)
                else:
                    # Different group, save for next iteration
                    self._next_item = item
                    self._next_key = key
                    break
        except StopAsyncIteration:
            self._exhausted = True


class AsyncRateLimitedIterator(Generic[T]):
    """
    Async iterator that rate-limits item consumption.
    """
    
    def __init__(self, iterable: AsyncIterable[T], rate: float):
        self.iterable = iterable
        self.rate = rate  # items per second
        self.interval = 1.0 / rate if rate > 0 else 0
        self._iterator = None
        self._last_yield = 0.0
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self
    
    async def __anext__(self) -> T:
        if self._iterator is None:
            self._iterator = aiter(self.iterable)
        
        # Rate limiting
        if self.interval > 0:
            current_time = time.time()
            time_since_last = current_time - self._last_yield
            
            if time_since_last < self.interval:
                await asyncio.sleep(self.interval - time_since_last)
            
            self._last_yield = time.time()
        
        return await anext(self._iterator)


class AsyncBufferedIterator(Generic[T]):
    """
    Async iterator that buffers items for smoother consumption.
    """
    
    def __init__(self, iterable: AsyncIterable[T], buffer_size: int = 10):
        self.iterable = iterable
        self.buffer_size = buffer_size
        self._buffer = asyncio.Queue(maxsize=buffer_size)
        self._producer_task = None
        self._finished = False
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self
    
    async def __anext__(self) -> T:
        if self._producer_task is None:
            self._producer_task = asyncio.create_task(self._producer())
        
        try:
            item = await asyncio.wait_for(self._buffer.get(), timeout=1.0)
            if item is StopAsyncIteration:
                raise StopAsyncIteration
            return item
        except asyncio.TimeoutError:
            if self._finished:
                raise StopAsyncIteration
            raise
    
    async def _producer(self):
        """Producer coroutine that fills the buffer."""
        try:
            async for item in self.iterable:
                await self._buffer.put(item)
        except Exception as e:
            logging.error(f"Producer error: {e}")
        finally:
            await self._buffer.put(StopAsyncIteration)
            self._finished = True


# Utility functions for creating async iterators
async def async_range(start: int, stop: Optional[int] = None, step: int = 1, delay: float = 0.0) -> AsyncIterator[int]:
    """Async version of range() with optional delay between items."""
    if stop is None:
        start, stop = 0, start
    
    current = start
    while (step > 0 and current < stop) or (step < 0 and current > stop):
        yield current
        current += step
        if delay > 0:
            await asyncio.sleep(delay)


async def async_enumerate(iterable: AsyncIterable[T], start: int = 0) -> AsyncIterator[tuple[int, T]]:
    """Async version of enumerate()."""
    index = start
    async for item in iterable:
        yield index, item
        index += 1


async def async_zip(*iterables: AsyncIterable) -> AsyncIterator[tuple]:
    """Async version of zip()."""
    iterators = [aiter(it) for it in iterables]
    
    while True:
        try:
            items = []
            for iterator in iterators:
                item = await anext(iterator)
                items.append(item)
            yield tuple(items)
        except StopAsyncIteration:
            break


# Example usage and demonstrations
async def demo_batch_iterator():
    """Demonstrate async batch iterator."""
    print("=== Async Batch Iterator Demo ===")
    
    # Create async range
    async_nums = async_range(1, 21, delay=0.1)
    
    # Batch into groups of 5
    batched = AsyncBatchIterator(async_nums, batch_size=5, timeout=1.0)
    
    async for batch in batched:
        print(f"Batch: {batch}")


async def demo_chain_iterator():
    """Demonstrate async chain iterator."""
    print("\n=== Async Chain Iterator Demo ===")
    
    # Create multiple async ranges
    range1 = async_range(1, 4)
    range2 = async_range(10, 13)
    range3 = async_range(20, 23)
    
    # Chain them together
    chained = AsyncChainIterator(range1, range2, range3)
    
    async for item in chained:
        print(f"Chained item: {item}")


async def demo_filter_map_iterator():
    """Demonstrate async filter and map iterators."""
    print("\n=== Async Filter/Map Iterator Demo ===")
    
    # Create async range
    async_nums = async_range(1, 11)
    
    # Filter even numbers
    evens = AsyncFilterIterator(async_nums, lambda x: x % 2 == 0)
    
    # Map to squares
    squares = AsyncMapIterator(evens, lambda x: x ** 2)
    
    async for square in squares:
        print(f"Even square: {square}")


async def demo_window_iterator():
    """Demonstrate async window iterator."""
    print("\n=== Async Window Iterator Demo ===")
    
    # Create async range
    async_nums = async_range(1, 11)
    
    # Create sliding windows of size 3
    windowed = AsyncWindowIterator(async_nums, window_size=3, step=1)
    
    async for window in windowed:
        print(f"Window: {window}")


async def demo_rate_limited_iterator():
    """Demonstrate rate-limited iterator."""
    print("\n=== Rate Limited Iterator Demo ===")
    
    # Create fast async range
    async_nums = async_range(1, 6)
    
    # Rate limit to 2 items per second
    rate_limited = AsyncRateLimitedIterator(async_nums, rate=2.0)
    
    start_time = time.time()
    async for item in rate_limited:
        elapsed = time.time() - start_time
        print(f"Item {item} at {elapsed:.1f}s")


async def demo_buffered_iterator():
    """Demonstrate buffered iterator."""
    print("\n=== Buffered Iterator Demo ===")
    
    # Create slow async range
    async_nums = async_range(1, 6, delay=0.2)
    
    # Buffer for smooth consumption
    buffered = AsyncBufferedIterator(async_nums, buffer_size=3)
    
    async for item in buffered:
        print(f"Buffered item: {item}")
        await asyncio.sleep(0.1)  # Consume faster than production


async def main():
    """Run all async iterator demonstrations."""
    await demo_batch_iterator()
    await demo_chain_iterator()
    await demo_filter_map_iterator()
    await demo_window_iterator()
    await demo_rate_limited_iterator()
    await demo_buffered_iterator()


if __name__ == "__main__":
    asyncio.run(main())
