"""
Real-World Concurrency Examples

Practical examples demonstrating concurrency patterns in real scenarios:
- Web scraper with rate limiting
- Database connection pool
- Message queue system
- Real-time data processing
- Distributed task scheduler
"""

import asyncio
import aiohttp
import time
import random
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

# Import our patterns
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.asyncio_patterns.async_producer_consumer import AsyncProducerConsumer
from src.asyncio_patterns.rate_limiting import TokenBucketLimiter
from src.threading_patterns.thread_pool import BackpressureThreadPool
from src.actor_model.actor_system import ActorSystem, Actor, Message, UserMessage


@dataclass
class WebPage:
    """Represents a web page to scrape."""
    url: str
    priority: int = 0
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class ScrapedData:
    """Represents scraped data from a web page."""
    url: str
    title: str
    content_length: int
    status_code: int
    scraped_at: datetime
    processing_time: float


class RateLimitedWebScraper:
    """
    Web scraper with rate limiting and concurrent processing.
    Demonstrates async patterns with backpressure control.
    """
    
    def __init__(self, max_concurrent: int = 10, requests_per_second: float = 5.0):
        self.max_concurrent = max_concurrent
        self.rate_limiter = TokenBucketLimiter(
            rate=requests_per_second,
            capacity=requests_per_second * 2
        )
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.stats = {
            "pages_scraped": 0,
            "pages_failed": 0,
            "total_processing_time": 0.0,
            "start_time": None
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=self.max_concurrent)
        )
        self.stats["start_time"] = time.time()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def scrape_page(self, page: WebPage) -> Optional[ScrapedData]:
        """Scrape a single web page with rate limiting."""
        async with self.semaphore:  # Limit concurrent requests
            # Wait for rate limiter
            await self.rate_limiter.acquire()
            
            start_time = time.time()
            
            try:
                async with self.session.get(page.url) as response:
                    content = await response.text()
                    processing_time = time.time() - start_time
                    
                    # Extract title (simple implementation)
                    title = "Unknown"
                    if "<title>" in content:
                        start = content.find("<title>") + 7
                        end = content.find("</title>", start)
                        if end > start:
                            title = content[start:end].strip()
                    
                    scraped_data = ScrapedData(
                        url=page.url,
                        title=title,
                        content_length=len(content),
                        status_code=response.status,
                        scraped_at=datetime.now(),
                        processing_time=processing_time
                    )
                    
                    self.stats["pages_scraped"] += 1
                    self.stats["total_processing_time"] += processing_time
                    
                    return scraped_data
            
            except Exception as e:
                self.stats["pages_failed"] += 1
                logging.error(f"Failed to scrape {page.url}: {e}")
                return None
    
    async def scrape_urls(self, urls: List[str]) -> List[ScrapedData]:
        """Scrape multiple URLs concurrently."""
        pages = [WebPage(url=url, priority=i) for i, url in enumerate(urls)]
        
        # Create tasks for all pages
        tasks = [self.scrape_page(page) for page in pages]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        scraped_data = []
        for result in results:
            if isinstance(result, ScrapedData):
                scraped_data.append(result)
        
        return scraped_data
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        elapsed_time = time.time() - self.stats["start_time"] if self.stats["start_time"] else 0
        total_pages = self.stats["pages_scraped"] + self.stats["pages_failed"]
        
        return {
            **self.stats,
            "elapsed_time": elapsed_time,
            "pages_per_second": total_pages / max(elapsed_time, 0.001),
            "avg_processing_time": (
                self.stats["total_processing_time"] / max(self.stats["pages_scraped"], 1)
            ),
            "success_rate": (
                self.stats["pages_scraped"] / max(total_pages, 1)
            ),
        }


class DatabaseConnectionPool:
    """
    Simulated database connection pool using threading patterns.
    Demonstrates resource management and connection lifecycle.
    """
    
    def __init__(self, min_connections: int = 5, max_connections: int = 20):
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.pool = BackpressureThreadPool(
            max_workers=max_connections,
            max_queue_size=100
        )
        self.active_connections = 0
        self.connection_stats = {
            "created": 0,
            "destroyed": 0,
            "queries_executed": 0,
            "total_query_time": 0.0
        }
    
    def get_connection(self):
        """Get a database connection from the pool."""
        return DatabaseConnection(self)
    
    def execute_query(self, query: str, params: tuple = ()) -> Dict[str, Any]:
        """Execute a database query."""
        start_time = time.time()
        
        # Simulate database query execution
        time.sleep(random.uniform(0.01, 0.1))  # Simulate query time
        
        execution_time = time.time() - start_time
        self.connection_stats["queries_executed"] += 1
        self.connection_stats["total_query_time"] += execution_time
        
        # Return mock result
        return {
            "query": query,
            "params": params,
            "rows_affected": random.randint(0, 100),
            "execution_time": execution_time,
            "timestamp": datetime.now()
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        avg_query_time = (
            self.connection_stats["total_query_time"] / 
            max(self.connection_stats["queries_executed"], 1)
        )
        
        return {
            **self.connection_stats,
            "active_connections": self.active_connections,
            "pool_stats": self.pool.get_stats(),
            "avg_query_time": avg_query_time,
        }


class DatabaseConnection:
    """Simulated database connection."""
    
    def __init__(self, pool: DatabaseConnectionPool):
        self.pool = pool
        self.connection_id = random.randint(1000, 9999)
        self.created_at = time.time()
        self.pool.active_connections += 1
        self.pool.connection_stats["created"] += 1
    
    def execute(self, query: str, params: tuple = ()) -> Dict[str, Any]:
        """Execute a query on this connection."""
        return self.pool.execute_query(query, params)
    
    def close(self):
        """Close the database connection."""
        self.pool.active_connections -= 1
        self.pool.connection_stats["destroyed"] += 1
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MessageQueueActor(Actor):
    """Actor that implements a message queue."""
    
    def __init__(self):
        super().__init__()
        self.messages = []
        self.subscribers = set()
    
    async def receive(self, message: Message) -> Optional[Any]:
        if isinstance(message, UserMessage):
            command = message.content
            
            if isinstance(command, dict):
                action = command.get("action")
                
                if action == "publish":
                    msg_data = command.get("data")
                    self.messages.append({
                        "data": msg_data,
                        "timestamp": time.time(),
                        "sender": message.sender
                    })
                    
                    # Notify subscribers
                    for subscriber in self.subscribers:
                        await subscriber.tell({
                            "action": "message_received",
                            "data": msg_data
                        })
                    
                    return f"Published message: {msg_data}"
                
                elif action == "subscribe":
                    subscriber = message.sender
                    if subscriber:
                        self.subscribers.add(subscriber)
                        return f"Subscribed {subscriber}"
                
                elif action == "unsubscribe":
                    subscriber = message.sender
                    if subscriber in self.subscribers:
                        self.subscribers.remove(subscriber)
                        return f"Unsubscribed {subscriber}"
                
                elif action == "get_stats":
                    return {
                        "total_messages": len(self.messages),
                        "subscribers": len(self.subscribers),
                        "recent_messages": self.messages[-5:] if self.messages else []
                    }
        
        return None


class SubscriberActor(Actor):
    """Actor that subscribes to message queue."""
    
    def __init__(self, subscriber_id: str):
        super().__init__()
        self.subscriber_id = subscriber_id
        self.received_messages = []
    
    async def receive(self, message: Message) -> Optional[Any]:
        if isinstance(message, UserMessage):
            command = message.content
            
            if isinstance(command, dict):
                action = command.get("action")
                
                if action == "message_received":
                    data = command.get("data")
                    self.received_messages.append({
                        "data": data,
                        "received_at": time.time()
                    })
                    print(f"Subscriber {self.subscriber_id} received: {data}")
                    return "Message processed"
                
                elif action == "get_received":
                    return self.received_messages
        
        return None


class RealTimeDataProcessor:
    """
    Real-time data processing system using producer-consumer pattern.
    Simulates processing streaming data with backpressure handling.
    """
    
    def __init__(self, buffer_size: int = 100):
        self.processor = AsyncProducerConsumer(buffer_capacity=buffer_size)
        self.processed_data = []
        self.processing_stats = {
            "items_processed": 0,
            "processing_errors": 0,
            "total_processing_time": 0.0
        }
    
    async def data_generator(self) -> str:
        """Generate simulated real-time data."""
        return {
            "sensor_id": random.randint(1, 10),
            "value": random.uniform(0, 100),
            "timestamp": time.time(),
            "type": random.choice(["temperature", "humidity", "pressure"])
        }
    
    async def data_processor(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming data."""
        start_time = time.time()
        
        try:
            # Simulate data processing
            await asyncio.sleep(random.uniform(0.01, 0.05))
            
            processed = {
                **data,
                "processed_at": time.time(),
                "processed_value": data["value"] * 1.1,  # Simple transformation
                "status": "processed"
            }
            
            self.processed_data.append(processed)
            self.processing_stats["items_processed"] += 1
            
            processing_time = time.time() - start_time
            self.processing_stats["total_processing_time"] += processing_time
            
            return processed
        
        except Exception as e:
            self.processing_stats["processing_errors"] += 1
            logging.error(f"Data processing error: {e}")
            return {"error": str(e), "original_data": data}
    
    async def run_processing(self, duration: float = 10.0):
        """Run the real-time data processing simulation."""
        # Add data generator
        self.processor.add_producer(self.data_generator, production_rate=10.0)
        
        # Add data processors
        self.processor.add_consumer(self.data_processor, processing_time=0.02)
        self.processor.add_consumer(self.data_processor, processing_time=0.03)
        
        # Run the simulation
        await self.processor.run_simulation(duration=duration)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        avg_processing_time = (
            self.processing_stats["total_processing_time"] / 
            max(self.processing_stats["items_processed"], 1)
        )
        
        return {
            **self.processing_stats,
            "avg_processing_time": avg_processing_time,
            "recent_data": self.processed_data[-10:] if self.processed_data else [],
            "processor_stats": self.processor.get_statistics()
        }


# Example demonstrations
async def demo_web_scraper():
    """Demonstrate the rate-limited web scraper."""
    print("=== Rate-Limited Web Scraper Demo ===")
    
    # Sample URLs to scrape (using httpbin for testing)
    urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/html",
        "https://httpbin.org/json",
        "https://httpbin.org/xml",
        "https://httpbin.org/user-agent",
    ]
    
    async with RateLimitedWebScraper(max_concurrent=3, requests_per_second=2.0) as scraper:
        print(f"Scraping {len(urls)} URLs...")
        
        results = await scraper.scrape_urls(urls)
        
        print(f"Scraped {len(results)} pages successfully")
        for result in results:
            print(f"  {result.url}: {result.title} ({result.content_length} bytes)")
        
        stats = scraper.get_stats()
        print(f"Scraper stats: {stats}")


def demo_database_pool():
    """Demonstrate the database connection pool."""
    print("\n=== Database Connection Pool Demo ===")
    
    pool = DatabaseConnectionPool(min_connections=3, max_connections=10)
    
    def worker_task(worker_id: int):
        """Simulate database work."""
        for i in range(5):
            with pool.get_connection() as conn:
                result = conn.execute(
                    f"SELECT * FROM users WHERE id = ?", 
                    (worker_id * 10 + i,)
                )
                print(f"Worker {worker_id}: Query {i} completed in {result['execution_time']:.3f}s")
                time.sleep(0.1)
    
    # Submit worker tasks
    futures = []
    for i in range(5):
        future = pool.pool.submit(worker_task, i)
        futures.append(future)
    
    # Wait for completion
    for future in futures:
        future.result()
    
    stats = pool.get_stats()
    print(f"Database pool stats: {stats}")
    
    pool.pool.shutdown()


async def demo_message_queue():
    """Demonstrate the actor-based message queue."""
    print("\n=== Actor Message Queue Demo ===")
    
    system = ActorSystem("message-queue-demo")
    await system.start()
    
    try:
        # Create message queue actor
        queue_ref = system.actor_of(MessageQueueActor, "message-queue")
        
        # Create subscriber actors
        subscriber1_ref = system.actor_of(lambda: SubscriberActor("sub1"), "subscriber1")
        subscriber2_ref = system.actor_of(lambda: SubscriberActor("sub2"), "subscriber2")
        
        # Subscribe to queue
        await queue_ref.tell({"action": "subscribe"}, sender=subscriber1_ref)
        await queue_ref.tell({"action": "subscribe"}, sender=subscriber2_ref)
        
        # Publish messages
        messages = ["Hello", "World", "From", "Actor", "Queue"]
        for msg in messages:
            await queue_ref.tell({"action": "publish", "data": msg})
            await asyncio.sleep(0.1)
        
        # Get stats
        stats = await queue_ref.ask({"action": "get_stats"})
        print(f"Queue stats: {stats}")
        
        # Get received messages from subscribers
        sub1_messages = await subscriber1_ref.ask({"action": "get_received"})
        sub2_messages = await subscriber2_ref.ask({"action": "get_received"})
        
        print(f"Subscriber 1 received {len(sub1_messages)} messages")
        print(f"Subscriber 2 received {len(sub2_messages)} messages")
        
    finally:
        await system.shutdown()


async def demo_realtime_processor():
    """Demonstrate real-time data processing."""
    print("\n=== Real-Time Data Processor Demo ===")
    
    processor = RealTimeDataProcessor(buffer_size=50)
    
    print("Starting real-time data processing...")
    await processor.run_processing(duration=5.0)
    
    stats = processor.get_stats()
    print(f"Processing stats: {stats}")


async def main():
    """Run all real-world examples."""
    await demo_web_scraper()
    demo_database_pool()
    await demo_message_queue()
    await demo_realtime_processor()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    asyncio.run(main())
