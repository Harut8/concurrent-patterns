"""
Rate Limiting Patterns for Asyncio

Various rate limiting implementations for controlling request rates.
"""

import asyncio
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging


class RateLimiter(ABC):
    """Abstract base class for rate limiters."""
    
    @abstractmethod
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens from the rate limiter."""
        pass
    
    @abstractmethod
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting."""
        pass


class TokenBucketLimiter(RateLimiter):
    """
    Token bucket rate limiter.
    Allows bursts up to capacity, then limits to rate.
    """
    
    def __init__(self, rate: float, capacity: float):
        self.rate = rate  # tokens per second
        self.capacity = capacity  # maximum tokens
        self.tokens = capacity
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens, waiting if necessary."""
        async with self._lock:
            await self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            # Calculate wait time
            wait_time = (tokens - self.tokens) / self.rate
            await asyncio.sleep(wait_time)
            
            await self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            return False
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting."""
        current_time = time.time()
        time_passed = current_time - self.last_update
        self.tokens = min(self.capacity, self.tokens + time_passed * self.rate)
        self.last_update = current_time
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    async def _refill(self):
        """Refill tokens based on elapsed time."""
        current_time = time.time()
        time_passed = current_time - self.last_update
        self.tokens = min(self.capacity, self.tokens + time_passed * self.rate)
        self.last_update = current_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "rate": self.rate,
            "capacity": self.capacity,
            "current_tokens": self.tokens,
            "last_update": self.last_update
        }


class SlidingWindowLimiter(RateLimiter):
    """
    Sliding window rate limiter.
    Tracks requests in a sliding time window.
    """
    
    def __init__(self, max_requests: int, window_size: float):
        self.max_requests = max_requests
        self.window_size = window_size  # seconds
        self.requests = []  # list of request timestamps
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire permission to make requests."""
        async with self._lock:
            current_time = time.time()
            
            # Remove old requests outside the window
            cutoff_time = current_time - self.window_size
            self.requests = [req_time for req_time in self.requests if req_time > cutoff_time]
            
            if len(self.requests) + tokens <= self.max_requests:
                # Add new request timestamps
                for _ in range(tokens):
                    self.requests.append(current_time)
                return True
            
            # Calculate wait time until we can make the request
            if self.requests:
                oldest_request = min(self.requests)
                wait_time = oldest_request + self.window_size - current_time
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    return await self.acquire(tokens)
            
            return False
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without waiting."""
        current_time = time.time()
        cutoff_time = current_time - self.window_size
        self.requests = [req_time for req_time in self.requests if req_time > cutoff_time]
        
        if len(self.requests) + tokens <= self.max_requests:
            for _ in range(tokens):
                self.requests.append(current_time)
            return True
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        current_time = time.time()
        cutoff_time = current_time - self.window_size
        active_requests = [req for req in self.requests if req > cutoff_time]
        
        return {
            "max_requests": self.max_requests,
            "window_size": self.window_size,
            "current_requests": len(active_requests),
            "remaining_requests": self.max_requests - len(active_requests)
        }


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on success/failure rates.
    """
    
    def __init__(self, initial_rate: float, min_rate: float = 0.1, max_rate: float = 100.0):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 10.0  # seconds
        self._lock = asyncio.Lock()
        
        # Use token bucket as underlying mechanism
        self.token_bucket = TokenBucketLimiter(self.current_rate, self.current_rate * 2)
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens with adaptive rate adjustment."""
        async with self._lock:
            await self._maybe_adjust_rate()
            return await self.token_bucket.acquire(tokens)
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting."""
        return self.token_bucket.try_acquire(tokens)
    
    async def record_success(self):
        """Record a successful operation."""
        async with self._lock:
            self.success_count += 1
    
    async def record_failure(self):
        """Record a failed operation."""
        async with self._lock:
            self.failure_count += 1
    
    async def _maybe_adjust_rate(self):
        """Adjust rate based on success/failure ratio."""
        current_time = time.time()
        if current_time - self.last_adjustment < self.adjustment_interval:
            return
        
        total_operations = self.success_count + self.failure_count
        if total_operations == 0:
            return
        
        success_rate = self.success_count / total_operations
        
        if success_rate > 0.95:  # High success rate, increase rate
            new_rate = min(self.max_rate, self.current_rate * 1.1)
        elif success_rate < 0.8:  # Low success rate, decrease rate
            new_rate = max(self.min_rate, self.current_rate * 0.9)
        else:
            new_rate = self.current_rate
        
        if new_rate != self.current_rate:
            self.current_rate = new_rate
            self.token_bucket = TokenBucketLimiter(self.current_rate, self.current_rate * 2)
            logging.info(f"Adjusted rate to {self.current_rate:.2f} (success rate: {success_rate:.2f})")
        
        # Reset counters
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = current_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get adaptive rate limiter statistics."""
        total_ops = self.success_count + self.failure_count
        success_rate = self.success_count / max(total_ops, 1)
        
        return {
            "current_rate": self.current_rate,
            "min_rate": self.min_rate,
            "max_rate": self.max_rate,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": success_rate,
            "bucket_stats": self.token_bucket.get_stats()
        }


class LeakyBucketLimiter(RateLimiter):
    """
    Leaky bucket rate limiter.
    Smooths out bursts by processing requests at a constant rate.
    """
    
    def __init__(self, rate: float, capacity: int):
        self.rate = rate  # requests per second
        self.capacity = capacity  # maximum queue size
        self.queue = asyncio.Queue(maxsize=capacity)
        self.running = False
        self._processor_task = None
    
    async def start(self):
        """Start the leaky bucket processor."""
        if self.running:
            return
        
        self.running = True
        self._processor_task = asyncio.create_task(self._process_queue())
    
    async def stop(self):
        """Stop the leaky bucket processor."""
        self.running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Add request to the bucket."""
        if not self.running:
            await self.start()
        
        try:
            for _ in range(tokens):
                future = asyncio.Future()
                await self.queue.put(future)
                await future  # Wait for processing
            return True
        except asyncio.QueueFull:
            return False
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to add request without waiting."""
        try:
            for _ in range(tokens):
                future = asyncio.Future()
                self.queue.put_nowait(future)
            return True
        except asyncio.QueueFull:
            return False
    
    async def _process_queue(self):
        """Process requests at the specified rate."""
        interval = 1.0 / self.rate
        
        while self.running:
            try:
                future = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                future.set_result(True)
                await asyncio.sleep(interval)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"Leaky bucket processor error: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get leaky bucket statistics."""
        return {
            "rate": self.rate,
            "capacity": self.capacity,
            "queue_size": self.queue.qsize(),
            "running": self.running
        }


# Example usage and demonstrations
async def demo_token_bucket():
    """Demonstrate token bucket rate limiter."""
    print("=== Token Bucket Rate Limiter Demo ===")
    
    limiter = TokenBucketLimiter(rate=2.0, capacity=5.0)  # 2 tokens/sec, burst of 5
    
    async def make_request(request_id: int):
        start_time = time.time()
        await limiter.acquire()
        end_time = time.time()
        print(f"Request {request_id} completed after {end_time - start_time:.2f}s")
    
    # Make burst of requests
    tasks = [make_request(i) for i in range(8)]
    await asyncio.gather(*tasks)
    
    print(f"Final stats: {limiter.get_stats()}")


async def demo_sliding_window():
    """Demonstrate sliding window rate limiter."""
    print("\n=== Sliding Window Rate Limiter Demo ===")
    
    limiter = SlidingWindowLimiter(max_requests=3, window_size=2.0)  # 3 requests per 2 seconds
    
    async def make_request(request_id: int):
        start_time = time.time()
        success = await limiter.acquire()
        end_time = time.time()
        print(f"Request {request_id}: {'SUCCESS' if success else 'FAILED'} after {end_time - start_time:.2f}s")
    
    # Make requests
    tasks = [make_request(i) for i in range(6)]
    await asyncio.gather(*tasks)
    
    print(f"Final stats: {limiter.get_stats()}")


async def demo_adaptive_limiter():
    """Demonstrate adaptive rate limiter."""
    print("\n=== Adaptive Rate Limiter Demo ===")
    
    limiter = AdaptiveRateLimiter(initial_rate=1.0, min_rate=0.5, max_rate=5.0)
    
    # Simulate operations with varying success rates
    for phase in range(3):
        print(f"\nPhase {phase + 1}:")
        
        for i in range(10):
            await limiter.acquire()
            
            # Simulate different success rates per phase
            if phase == 0:  # High success rate
                if i < 9:
                    await limiter.record_success()
                else:
                    await limiter.record_failure()
            elif phase == 1:  # Low success rate
                if i < 5:
                    await limiter.record_success()
                else:
                    await limiter.record_failure()
            else:  # Medium success rate
                if i < 8:
                    await limiter.record_success()
                else:
                    await limiter.record_failure()
        
        await asyncio.sleep(11)  # Wait for adjustment
        stats = limiter.get_stats()
        print(f"Rate adjusted to: {stats['current_rate']:.2f}, Success rate: {stats['success_rate']:.2f}")


async def main():
    """Run all rate limiting demonstrations."""
    await demo_token_bucket()
    await demo_sliding_window()
    await demo_adaptive_limiter()


if __name__ == "__main__":
    asyncio.run(main())
