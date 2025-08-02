"""
Performance Comparison Benchmarks

Comprehensive benchmarks comparing different concurrency approaches:
- Threading vs Asyncio vs Multiprocessing
- Different queue implementations
- Lock performance comparisons
- Scalability tests
"""

import asyncio
import threading
import multiprocessing as mp
import time
import statistics
from typing import List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import psutil
import numpy as np
from dataclasses import dataclass
import json

# Import our patterns
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.threading_patterns.thread_pool import BackpressureThreadPool
from src.asyncio_patterns.async_producer_consumer import AsyncProducerConsumer
from src.multiprocessing_patterns.process_pool import ProcessPool


@dataclass
class BenchmarkResult:
    """Results of a benchmark test."""
    name: str
    approach: str
    duration: float
    throughput: float
    memory_usage: float
    cpu_usage: float
    tasks_completed: int
    errors: int
    additional_metrics: Dict[str, Any]


class PerformanceBenchmark:
    """Base class for performance benchmarks."""
    
    def __init__(self, name: str):
        self.name = name
        self.results: List[BenchmarkResult] = []
    
    def measure_resources(self, func: Callable, *args, **kwargs) -> tuple:
        """Measure resource usage during function execution."""
        process = psutil.Process()
        
        # Initial measurements
        start_time = time.time()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_cpu = process.cpu_percent()
        
        # Execute function
        result = func(*args, **kwargs)
        
        # Final measurements
        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB
        end_cpu = process.cpu_percent()
        
        duration = end_time - start_time
        memory_usage = end_memory - start_memory
        cpu_usage = (start_cpu + end_cpu) / 2
        
        return result, duration, memory_usage, cpu_usage
    
    def add_result(self, result: BenchmarkResult):
        """Add a benchmark result."""
        self.results.append(result)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get benchmark summary."""
        if not self.results:
            return {"name": self.name, "results": []}
        
        # Group by approach
        by_approach = {}
        for result in self.results:
            if result.approach not in by_approach:
                by_approach[result.approach] = []
            by_approach[result.approach].append(result)
        
        summary = {"name": self.name, "approaches": {}}
        
        for approach, results in by_approach.items():
            durations = [r.duration for r in results]
            throughputs = [r.throughput for r in results]
            
            summary["approaches"][approach] = {
                "runs": len(results),
                "avg_duration": statistics.mean(durations),
                "avg_throughput": statistics.mean(throughputs),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "std_duration": statistics.stdev(durations) if len(durations) > 1 else 0,
                "total_tasks": sum(r.tasks_completed for r in results),
                "total_errors": sum(r.errors for r in results),
            }
        
        return summary


class CPUIntensiveBenchmark(PerformanceBenchmark):
    """Benchmark CPU-intensive tasks across different concurrency models."""
    
    def __init__(self):
        super().__init__("CPU Intensive Tasks")
    
    def cpu_task(self, n: int) -> int:
        """CPU-intensive task for benchmarking."""
        result = 0
        for i in range(n):
            result += i ** 2
        return result
    
    def benchmark_threading(self, task_count: int = 100, workers: int = 4):
        """Benchmark threading approach."""
        def run_threading():
            with ThreadPoolExecutor(max_workers=workers) as executor:
                tasks = [executor.submit(self.cpu_task, 10000) for _ in range(task_count)]
                results = [task.result() for task in tasks]
                return len(results), 0  # completed, errors
        
        result, duration, memory, cpu = self.measure_resources(run_threading)
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name=f"Threading-{workers}workers",
            approach="threading",
            duration=duration,
            throughput=completed / duration,
            memory_usage=memory,
            cpu_usage=cpu,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={"workers": workers}
        ))
    
    def benchmark_multiprocessing(self, task_count: int = 100, workers: int = 4):
        """Benchmark multiprocessing approach."""
        def run_multiprocessing():
            with ProcessPoolExecutor(max_workers=workers) as executor:
                tasks = [executor.submit(self.cpu_task, 10000) for _ in range(task_count)]
                results = [task.result() for task in tasks]
                return len(results), 0
        
        result, duration, memory, cpu = self.measure_resources(run_multiprocessing)
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name=f"Multiprocessing-{workers}workers",
            approach="multiprocessing",
            duration=duration,
            throughput=completed / duration,
            memory_usage=memory,
            cpu_usage=cpu,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={"workers": workers}
        ))
    
    async def benchmark_asyncio(self, task_count: int = 100):
        """Benchmark asyncio approach (not ideal for CPU tasks)."""
        async def cpu_task_async(n: int) -> int:
            # Note: This won't be truly concurrent for CPU tasks
            return self.cpu_task(n)
        
        async def run_asyncio():
            tasks = [cpu_task_async(10000) for _ in range(task_count)]
            results = await asyncio.gather(*tasks)
            return len(results), 0
        
        start_time = time.time()
        result = await run_asyncio()
        duration = time.time() - start_time
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name="Asyncio",
            approach="asyncio",
            duration=duration,
            throughput=completed / duration,
            memory_usage=0,  # Not measured for async
            cpu_usage=0,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={}
        ))
    
    def run_all_benchmarks(self, task_count: int = 100):
        """Run all CPU-intensive benchmarks."""
        print(f"Running CPU-intensive benchmarks with {task_count} tasks...")
        
        # Test different worker counts
        for workers in [1, 2, 4, 8]:
            print(f"  Threading with {workers} workers...")
            self.benchmark_threading(task_count, workers)
            
            print(f"  Multiprocessing with {workers} workers...")
            self.benchmark_multiprocessing(task_count, workers)
        
        # Asyncio (for comparison)
        print("  Asyncio...")
        asyncio.run(self.benchmark_asyncio(task_count))


class IOIntensiveBenchmark(PerformanceBenchmark):
    """Benchmark I/O-intensive tasks across different concurrency models."""
    
    def __init__(self):
        super().__init__("I/O Intensive Tasks")
    
    def io_task(self, delay: float = 0.1) -> str:
        """Simulate I/O-intensive task."""
        time.sleep(delay)
        return f"completed-{time.time()}"
    
    async def io_task_async(self, delay: float = 0.1) -> str:
        """Async I/O-intensive task."""
        await asyncio.sleep(delay)
        return f"completed-{time.time()}"
    
    def benchmark_threading(self, task_count: int = 100, workers: int = 10):
        """Benchmark threading for I/O tasks."""
        def run_threading():
            with ThreadPoolExecutor(max_workers=workers) as executor:
                tasks = [executor.submit(self.io_task, 0.01) for _ in range(task_count)]
                results = [task.result() for task in tasks]
                return len(results), 0
        
        result, duration, memory, cpu = self.measure_resources(run_threading)
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name=f"Threading-{workers}workers",
            approach="threading",
            duration=duration,
            throughput=completed / duration,
            memory_usage=memory,
            cpu_usage=cpu,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={"workers": workers}
        ))
    
    async def benchmark_asyncio(self, task_count: int = 100):
        """Benchmark asyncio for I/O tasks."""
        async def run_asyncio():
            tasks = [self.io_task_async(0.01) for _ in range(task_count)]
            results = await asyncio.gather(*tasks)
            return len(results), 0
        
        start_time = time.time()
        result = await run_asyncio()
        duration = time.time() - start_time
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name="Asyncio",
            approach="asyncio",
            duration=duration,
            throughput=completed / duration,
            memory_usage=0,
            cpu_usage=0,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={}
        ))
    
    def run_all_benchmarks(self, task_count: int = 100):
        """Run all I/O-intensive benchmarks."""
        print(f"Running I/O-intensive benchmarks with {task_count} tasks...")
        
        # Test different worker counts for threading
        for workers in [5, 10, 20, 50]:
            print(f"  Threading with {workers} workers...")
            self.benchmark_threading(task_count, workers)
        
        # Asyncio
        print("  Asyncio...")
        asyncio.run(self.benchmark_asyncio(task_count))


class ProducerConsumerBenchmark(PerformanceBenchmark):
    """Benchmark different producer-consumer implementations."""
    
    def __init__(self):
        super().__init__("Producer-Consumer Patterns")
    
    def benchmark_threading_pc(self, items: int = 1000, buffer_size: int = 100):
        """Benchmark threading producer-consumer."""
        import queue
        
        def run_threading_pc():
            q = queue.Queue(maxsize=buffer_size)
            completed = [0]
            errors = [0]
            
            def producer():
                try:
                    for i in range(items):
                        q.put(f"item-{i}")
                except Exception:
                    errors[0] += 1
            
            def consumer():
                try:
                    while True:
                        try:
                            item = q.get(timeout=0.1)
                            completed[0] += 1
                            q.task_done()
                        except queue.Empty:
                            break
                except Exception:
                    errors[0] += 1
            
            # Start threads
            producer_thread = threading.Thread(target=producer)
            consumer_threads = [threading.Thread(target=consumer) for _ in range(3)]
            
            producer_thread.start()
            for t in consumer_threads:
                t.start()
            
            producer_thread.join()
            for t in consumer_threads:
                t.join()
            
            return completed[0], errors[0]
        
        result, duration, memory, cpu = self.measure_resources(run_threading_pc)
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name="Threading-Queue",
            approach="threading",
            duration=duration,
            throughput=completed / duration,
            memory_usage=memory,
            cpu_usage=cpu,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={"buffer_size": buffer_size}
        ))
    
    async def benchmark_asyncio_pc(self, items: int = 1000, buffer_size: int = 100):
        """Benchmark asyncio producer-consumer."""
        async def run_asyncio_pc():
            pc = AsyncProducerConsumer(buffer_capacity=buffer_size)
            
            # Add producer and consumers
            pc.add_producer(lambda: f"item-{time.time()}", production_rate=1000)
            pc.add_consumer(lambda x: f"processed-{x}", processing_time=0.001)
            pc.add_consumer(lambda x: f"processed-{x}", processing_time=0.001)
            
            # Run for short duration to process items
            await pc.run_simulation(duration=2.0)
            
            stats = pc.get_statistics()
            return stats["total_consumed"], 0
        
        start_time = time.time()
        result = await run_asyncio_pc()
        duration = time.time() - start_time
        completed, errors = result
        
        self.add_result(BenchmarkResult(
            name="Asyncio-Queue",
            approach="asyncio",
            duration=duration,
            throughput=completed / duration,
            memory_usage=0,
            cpu_usage=0,
            tasks_completed=completed,
            errors=errors,
            additional_metrics={"buffer_size": buffer_size}
        ))
    
    def run_all_benchmarks(self, items: int = 1000):
        """Run all producer-consumer benchmarks."""
        print(f"Running producer-consumer benchmarks with {items} items...")
        
        # Test different buffer sizes
        for buffer_size in [10, 50, 100, 500]:
            print(f"  Threading with buffer size {buffer_size}...")
            self.benchmark_threading_pc(items, buffer_size)
            
            print(f"  Asyncio with buffer size {buffer_size}...")
            asyncio.run(self.benchmark_asyncio_pc(items, buffer_size))


class ScalabilityBenchmark(PerformanceBenchmark):
    """Test scalability with increasing load."""
    
    def __init__(self):
        super().__init__("Scalability Tests")
    
    def benchmark_scalability(self, max_workers: int = 20):
        """Test how performance scales with worker count."""
        task_count = 200
        
        for workers in range(1, max_workers + 1, 2):
            print(f"  Testing with {workers} workers...")
            
            # Threading scalability
            def run_threading():
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    tasks = [executor.submit(time.sleep, 0.01) for _ in range(task_count)]
                    for task in tasks:
                        task.result()
                    return task_count, 0
            
            result, duration, memory, cpu = self.measure_resources(run_threading)
            completed, errors = result
            
            self.add_result(BenchmarkResult(
                name=f"Threading-{workers}workers",
                approach="threading",
                duration=duration,
                throughput=completed / duration,
                memory_usage=memory,
                cpu_usage=cpu,
                tasks_completed=completed,
                errors=errors,
                additional_metrics={"workers": workers}
            ))
    
    def run_all_benchmarks(self):
        """Run scalability benchmarks."""
        print("Running scalability benchmarks...")
        self.benchmark_scalability(mp.cpu_count() * 4)


class BenchmarkRunner:
    """Main benchmark runner that coordinates all tests."""
    
    def __init__(self):
        self.benchmarks: List[PerformanceBenchmark] = []
    
    def add_benchmark(self, benchmark: PerformanceBenchmark):
        """Add a benchmark to run."""
        self.benchmarks.append(benchmark)
    
    def run_all(self, save_results: bool = True):
        """Run all benchmarks."""
        print("Starting comprehensive performance benchmarks...")
        print(f"System info: {mp.cpu_count()} CPUs, {psutil.virtual_memory().total / 1024**3:.1f}GB RAM")
        print("=" * 60)
        
        for benchmark in self.benchmarks:
            print(f"\nRunning {benchmark.name}...")
            benchmark.run_all_benchmarks()
        
        # Generate report
        self.generate_report(save_results)
    
    def generate_report(self, save_results: bool = True):
        """Generate comprehensive benchmark report."""
        print("\n" + "=" * 60)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 60)
        
        all_results = {}
        
        for benchmark in self.benchmarks:
            summary = benchmark.get_summary()
            all_results[benchmark.name] = summary
            
            print(f"\n{benchmark.name}:")
            print("-" * 40)
            
            for approach, stats in summary["approaches"].items():
                print(f"  {approach}:")
                print(f"    Avg Duration: {stats['avg_duration']:.3f}s")
                print(f"    Avg Throughput: {stats['avg_throughput']:.1f} tasks/sec")
                print(f"    Total Tasks: {stats['total_tasks']}")
                print(f"    Errors: {stats['total_errors']}")
        
        # Save detailed results
        if save_results:
            timestamp = int(time.time())
            filename = f"benchmark_results_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(all_results, f, indent=2)
            
            print(f"\nDetailed results saved to: {filename}")
        
        # Performance recommendations
        self.generate_recommendations(all_results)
    
    def generate_recommendations(self, results: Dict[str, Any]):
        """Generate performance recommendations based on results."""
        print("\n" + "=" * 60)
        print("PERFORMANCE RECOMMENDATIONS")
        print("=" * 60)
        
        # CPU-intensive recommendations
        if "CPU Intensive Tasks" in results:
            cpu_results = results["CPU Intensive Tasks"]["approaches"]
            
            # Find best approach for CPU tasks
            best_cpu = min(cpu_results.items(), 
                          key=lambda x: x[1]["avg_duration"])
            
            print(f"\nFor CPU-intensive tasks:")
            print(f"  Best approach: {best_cpu[0]}")
            print(f"  Recommendation: Use multiprocessing for CPU-bound work")
        
        # I/O-intensive recommendations
        if "I/O Intensive Tasks" in results:
            io_results = results["I/O Intensive Tasks"]["approaches"]
            
            best_io = max(io_results.items(), 
                         key=lambda x: x[1]["avg_throughput"])
            
            print(f"\nFor I/O-intensive tasks:")
            print(f"  Best approach: {best_io[0]}")
            print(f"  Recommendation: Use asyncio for I/O-bound work")
        
        print(f"\nGeneral recommendations:")
        print(f"  - Use multiprocessing for CPU-intensive tasks")
        print(f"  - Use asyncio for I/O-intensive tasks")
        print(f"  - Use threading for mixed workloads")
        print(f"  - Consider worker count based on task type and system resources")


def main():
    """Run comprehensive benchmarks."""
    runner = BenchmarkRunner()
    
    # Add all benchmarks
    runner.add_benchmark(CPUIntensiveBenchmark())
    runner.add_benchmark(IOIntensiveBenchmark())
    runner.add_benchmark(ProducerConsumerBenchmark())
    runner.add_benchmark(ScalabilityBenchmark())
    
    # Run all benchmarks
    runner.run_all()


if __name__ == "__main__":
    main()
