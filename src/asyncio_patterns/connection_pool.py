"""
Connection Pool Patterns for Asyncio

Async connection pool implementations for managing database connections,
HTTP connections, and other resources.
"""

import asyncio
import aiohttp
import time
import weakref
from typing import Optional, Dict, Any, List, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import logging


T = TypeVar('T')


class ConnectionState(Enum):
    """Connection states."""
    IDLE = "idle"
    ACTIVE = "active"
    CLOSED = "closed"
    ERROR = "error"


@dataclass
class PoolStats:
    """Connection pool statistics."""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    failed_connections: int = 0
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    average_wait_time: float = 0.0
    pool_hits: int = 0
    pool_misses: int = 0


class Connection(ABC):
    """Abstract base class for pooled connections."""
    
    def __init__(self, connection_id: str):
        self.connection_id = connection_id
        self.state = ConnectionState.IDLE
        self.created_at = time.time()
        self.last_used = time.time()
        self.use_count = 0
        self.error_count = 0
    
    @abstractmethod
    async def connect(self):
        """Establish the connection."""
        pass
    
    @abstractmethod
    async def close(self):
        """Close the connection."""
        pass
    
    @abstractmethod
    async def ping(self) -> bool:
        """Check if connection is alive."""
        pass
    
    @abstractmethod
    async def execute(self, operation: Any) -> Any:
        """Execute an operation on this connection."""
        pass
    
    def mark_used(self):
        """Mark connection as used."""
        self.last_used = time.time()
        self.use_count += 1
    
    def mark_error(self):
        """Mark connection as having an error."""
        self.error_count += 1
        self.state = ConnectionState.ERROR
    
    def is_expired(self, max_age: float) -> bool:
        """Check if connection has expired."""
        return time.time() - self.created_at > max_age
    
    def is_idle_too_long(self, max_idle: float) -> bool:
        """Check if connection has been idle too long."""
        return time.time() - self.last_used > max_idle


class AsyncConnectionPool(Generic[T]):
    """
    Generic async connection pool with health checking and auto-scaling.
    """
    
    def __init__(self, 
                 connection_factory: Callable[[], T],
                 min_connections: int = 5,
                 max_connections: int = 20,
                 max_connection_age: float = 3600.0,  # 1 hour
                 max_idle_time: float = 300.0,        # 5 minutes
                 health_check_interval: float = 60.0): # 1 minute
        
        self.connection_factory = connection_factory
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_connection_age = max_connection_age
        self.max_idle_time = max_idle_time
        self.health_check_interval = health_check_interval
        
        self._connections: List[T] = []
        self._available = asyncio.Queue()
        self._in_use: Dict[str, T] = {}
        self._lock = asyncio.Lock()
        self._stats = PoolStats()
        self._closed = False
        self._health_check_task = None
        self._connection_counter = 0
    
    async def start(self):
        """Initialize the connection pool."""
        if self._health_check_task:
            return
        
        # Create minimum connections
        for _ in range(self.min_connections):
            await self._create_connection()
        
        # Start health check task
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logging.info(f"Connection pool started with {len(self._connections)} connections")
    
    async def close(self):
        """Close the connection pool and all connections."""
        if self._closed:
            return
        
        self._closed = True
        
        # Cancel health check
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        # Close all connections
        async with self._lock:
            for conn in self._connections:
                try:
                    await conn.close()
                except Exception as e:
                    logging.error(f"Error closing connection {conn.connection_id}: {e}")
            
            self._connections.clear()
            self._in_use.clear()
        
        logging.info("Connection pool closed")
    
    async def acquire(self, timeout: Optional[float] = None) -> T:
        """Acquire a connection from the pool."""
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        start_time = time.time()
        
        try:
            # Try to get an available connection
            try:
                conn = await asyncio.wait_for(self._available.get(), timeout=0.1)
                async with self._lock:
                    if conn.connection_id in self._in_use:
                        # Connection already in use, create new one
                        conn = await self._create_connection()
                    else:
                        self._in_use[conn.connection_id] = conn
                
                conn.state = ConnectionState.ACTIVE
                conn.mark_used()
                self._stats.pool_hits += 1
                
            except asyncio.TimeoutError:
                # No available connections, try to create new one
                async with self._lock:
                    if len(self._connections) < self.max_connections:
                        conn = await self._create_connection()
                        self._in_use[conn.connection_id] = conn
                        conn.state = ConnectionState.ACTIVE
                        conn.mark_used()
                        self._stats.pool_misses += 1
                    else:
                        # Wait for a connection to become available
                        conn = await asyncio.wait_for(self._available.get(), timeout=timeout)
                        async with self._lock:
                            self._in_use[conn.connection_id] = conn
                        conn.state = ConnectionState.ACTIVE
                        conn.mark_used()
                        self._stats.pool_hits += 1
            
            wait_time = time.time() - start_time
            self._stats.average_wait_time = (
                (self._stats.average_wait_time * self._stats.total_requests + wait_time) / 
                (self._stats.total_requests + 1)
            )
            self._stats.total_requests += 1
            
            return conn
        
        except Exception as e:
            self._stats.failed_requests += 1
            raise
    
    async def release(self, connection: T):
        """Release a connection back to the pool."""
        if self._closed:
            return
        
        async with self._lock:
            if connection.connection_id in self._in_use:
                del self._in_use[connection.connection_id]
                
                if connection.state == ConnectionState.ERROR:
                    # Remove errored connections
                    await self._remove_connection(connection)
                elif connection.is_expired(self.max_connection_age):
                    # Remove expired connections
                    await self._remove_connection(connection)
                else:
                    # Return healthy connection to pool
                    connection.state = ConnectionState.IDLE
                    await self._available.put(connection)
                    self._stats.successful_requests += 1
    
    async def _create_connection(self) -> T:
        """Create a new connection."""
        self._connection_counter += 1
        conn = self.connection_factory()
        conn.connection_id = f"conn-{self._connection_counter}"
        
        try:
            await conn.connect()
            self._connections.append(conn)
            self._stats.total_connections += 1
            logging.debug(f"Created connection {conn.connection_id}")
            return conn
        except Exception as e:
            logging.error(f"Failed to create connection: {e}")
            raise
    
    async def _remove_connection(self, connection: T):
        """Remove a connection from the pool."""
        try:
            await connection.close()
            if connection in self._connections:
                self._connections.remove(connection)
            self._stats.failed_connections += 1
            logging.debug(f"Removed connection {connection.connection_id}")
        except Exception as e:
            logging.error(f"Error removing connection {connection.connection_id}: {e}")
    
    async def _health_check_loop(self):
        """Periodic health check for connections."""
        while not self._closed:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Health check error: {e}")
    
    async def _health_check(self):
        """Perform health check on all connections."""
        async with self._lock:
            connections_to_remove = []
            
            for conn in self._connections:
                if conn.connection_id in self._in_use:
                    continue  # Skip connections in use
                
                try:
                    # Check if connection is still alive
                    if not await conn.ping():
                        connections_to_remove.append(conn)
                        continue
                    
                    # Check for expired connections
                    if conn.is_expired(self.max_connection_age):
                        connections_to_remove.append(conn)
                        continue
                    
                    # Check for idle connections
                    if (len(self._connections) > self.min_connections and 
                        conn.is_idle_too_long(self.max_idle_time)):
                        connections_to_remove.append(conn)
                        continue
                
                except Exception as e:
                    logging.error(f"Health check failed for {conn.connection_id}: {e}")
                    connections_to_remove.append(conn)
            
            # Remove unhealthy connections
            for conn in connections_to_remove:
                await self._remove_connection(conn)
            
            # Ensure minimum connections
            while len(self._connections) < self.min_connections:
                try:
                    await self._create_connection()
                except Exception as e:
                    logging.error(f"Failed to create minimum connection: {e}")
                    break
    
    def get_stats(self) -> PoolStats:
        """Get pool statistics."""
        self._stats.active_connections = len(self._in_use)
        self._stats.idle_connections = len(self._connections) - len(self._in_use)
        return self._stats


class HTTPConnectionPool(AsyncConnectionPool):
    """HTTP connection pool using aiohttp."""
    
    def __init__(self, base_url: str, **kwargs):
        self.base_url = base_url
        self.session_kwargs = kwargs.pop('session_kwargs', {})
        
        def connection_factory():
            return HTTPConnection(self.base_url, **self.session_kwargs)
        
        super().__init__(connection_factory, **kwargs)


class HTTPConnection(Connection):
    """HTTP connection wrapper."""
    
    def __init__(self, base_url: str, **session_kwargs):
        super().__init__("")
        self.base_url = base_url
        self.session_kwargs = session_kwargs
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def connect(self):
        """Create HTTP session."""
        self.session = aiohttp.ClientSession(**self.session_kwargs)
        self.state = ConnectionState.IDLE
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
        self.state = ConnectionState.CLOSED
    
    async def ping(self) -> bool:
        """Check if session is still valid."""
        if not self.session or self.session.closed:
            return False
        
        try:
            # Try a simple HEAD request
            async with self.session.head(self.base_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                return response.status < 500
        except Exception:
            return False
    
    async def execute(self, operation: Dict[str, Any]) -> aiohttp.ClientResponse:
        """Execute HTTP request."""
        if not self.session:
            raise RuntimeError("Connection not established")
        
        method = operation.get('method', 'GET')
        url = operation.get('url', '')
        kwargs = operation.get('kwargs', {})
        
        if not url.startswith('http'):
            url = f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
        
        return await self.session.request(method, url, **kwargs)


class DatabaseConnectionPool(AsyncConnectionPool):
    """Database connection pool (mock implementation)."""
    
    def __init__(self, connection_string: str, **kwargs):
        self.connection_string = connection_string
        
        def connection_factory():
            return DatabaseConnection(self.connection_string)
        
        super().__init__(connection_factory, **kwargs)


class DatabaseConnection(Connection):
    """Mock database connection."""
    
    def __init__(self, connection_string: str):
        super().__init__("")
        self.connection_string = connection_string
        self.connected = False
    
    async def connect(self):
        """Mock database connection."""
        await asyncio.sleep(0.1)  # Simulate connection time
        self.connected = True
        self.state = ConnectionState.IDLE
    
    async def close(self):
        """Close database connection."""
        self.connected = False
        self.state = ConnectionState.CLOSED
    
    async def ping(self) -> bool:
        """Check database connection."""
        if not self.connected:
            return False
        
        # Simulate ping
        await asyncio.sleep(0.01)
        return True
    
    async def execute(self, query: str) -> Dict[str, Any]:
        """Execute database query."""
        if not self.connected:
            raise RuntimeError("Database not connected")
        
        # Simulate query execution
        await asyncio.sleep(0.05)
        
        return {
            "query": query,
            "rows_affected": 1,
            "execution_time": 0.05,
            "connection_id": self.connection_id
        }


# Example usage and demonstrations
async def demo_http_pool():
    """Demonstrate HTTP connection pool."""
    print("=== HTTP Connection Pool Demo ===")
    
    pool = HTTPConnectionPool(
        base_url="https://httpbin.org",
        min_connections=2,
        max_connections=5
    )
    
    await pool.start()
    
    try:
        # Make concurrent HTTP requests
        async def make_request(request_id: int):
            conn = await pool.acquire()
            try:
                response = await conn.execute({
                    'method': 'GET',
                    'url': '/json',
                    'kwargs': {'timeout': aiohttp.ClientTimeout(total=10)}
                })
                print(f"Request {request_id}: Status {response.status}")
                response.close()
            finally:
                await pool.release(conn)
        
        # Make multiple concurrent requests
        tasks = [make_request(i) for i in range(10)]
        await asyncio.gather(*tasks)
        
        # Print statistics
        stats = pool.get_stats()
        print(f"Pool stats: Total connections: {stats.total_connections}")
        print(f"Active: {stats.active_connections}, Idle: {stats.idle_connections}")
        print(f"Requests: {stats.total_requests}, Success: {stats.successful_requests}")
        print(f"Pool hits: {stats.pool_hits}, Pool misses: {stats.pool_misses}")
        
    finally:
        await pool.close()


async def demo_database_pool():
    """Demonstrate database connection pool."""
    print("\n=== Database Connection Pool Demo ===")
    
    pool = DatabaseConnectionPool(
        connection_string="postgresql://user:pass@localhost/db",
        min_connections=3,
        max_connections=8
    )
    
    await pool.start()
    
    try:
        # Execute concurrent database operations
        async def execute_query(query_id: int):
            conn = await pool.acquire()
            try:
                result = await conn.execute(f"SELECT * FROM users WHERE id = {query_id}")
                print(f"Query {query_id}: {result['rows_affected']} rows affected")
            finally:
                await pool.release(conn)
        
        # Execute multiple queries
        tasks = [execute_query(i) for i in range(15)]
        await asyncio.gather(*tasks)
        
        # Print statistics
        stats = pool.get_stats()
        print(f"Database pool stats:")
        print(f"Total connections: {stats.total_connections}")
        print(f"Active: {stats.active_connections}, Idle: {stats.idle_connections}")
        print(f"Average wait time: {stats.average_wait_time:.3f}s")
        
    finally:
        await pool.close()


async def main():
    """Run all connection pool demonstrations."""
    await demo_http_pool()
    await demo_database_pool()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
