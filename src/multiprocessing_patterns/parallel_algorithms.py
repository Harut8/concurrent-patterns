"""
Parallel Algorithms

Parallel implementations of common algorithms using multiprocessing.
"""

import multiprocessing as mp
import time
import math
import random
from typing import List, Any, Callable, Optional, Tuple, TypeVar
from dataclasses import dataclass
from abc import ABC, abstractmethod
import numpy as np
import logging


T = TypeVar('T')


@dataclass
class AlgorithmResult:
    """Result of a parallel algorithm execution."""
    result: Any
    execution_time: float
    num_processes: int
    speedup: Optional[float] = None
    efficiency: Optional[float] = None


class ParallelSorting:
    """
    Parallel sorting algorithms.
    """
    
    @staticmethod
    def merge_sort_parallel(data: List[T], num_processes: int = None) -> List[T]:
        """Parallel merge sort implementation."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if len(data) <= 1:
            return data
        
        # Sequential merge sort for small arrays
        if len(data) < 1000 or num_processes <= 1:
            return ParallelSorting._merge_sort_sequential(data)
        
        # Split data into chunks for parallel processing
        chunk_size = len(data) // num_processes
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        # Sort chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            sorted_chunks = pool.map(ParallelSorting._merge_sort_sequential, chunks)
        
        # Merge sorted chunks
        return ParallelSorting._merge_multiple(sorted_chunks)
    
    @staticmethod
    def _merge_sort_sequential(data: List[T]) -> List[T]:
        """Sequential merge sort."""
        if len(data) <= 1:
            return data
        
        mid = len(data) // 2
        left = ParallelSorting._merge_sort_sequential(data[:mid])
        right = ParallelSorting._merge_sort_sequential(data[mid:])
        
        return ParallelSorting._merge_two(left, right)
    
    @staticmethod
    def _merge_two(left: List[T], right: List[T]) -> List[T]:
        """Merge two sorted lists."""
        result = []
        i = j = 0
        
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1
        
        result.extend(left[i:])
        result.extend(right[j:])
        return result
    
    @staticmethod
    def _merge_multiple(sorted_lists: List[List[T]]) -> List[T]:
        """Merge multiple sorted lists."""
        while len(sorted_lists) > 1:
            new_lists = []
            for i in range(0, len(sorted_lists), 2):
                if i + 1 < len(sorted_lists):
                    merged = ParallelSorting._merge_two(sorted_lists[i], sorted_lists[i + 1])
                    new_lists.append(merged)
                else:
                    new_lists.append(sorted_lists[i])
            sorted_lists = new_lists
        
        return sorted_lists[0] if sorted_lists else []
    
    @staticmethod
    def quick_sort_parallel(data: List[T], num_processes: int = None, threshold: int = 1000) -> List[T]:
        """Parallel quick sort implementation."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if len(data) <= threshold or num_processes <= 1:
            return ParallelSorting._quick_sort_sequential(data)
        
        if len(data) <= 1:
            return data
        
        # Choose pivot
        pivot = data[len(data) // 2]
        
        # Partition data
        less = [x for x in data if x < pivot]
        equal = [x for x in data if x == pivot]
        greater = [x for x in data if x > pivot]
        
        # Sort partitions in parallel
        with mp.Pool(processes=2) as pool:
            if len(less) > threshold:
                future_less = pool.apply_async(
                    ParallelSorting.quick_sort_parallel, 
                    (less, num_processes // 2, threshold)
                )
            else:
                future_less = pool.apply_async(ParallelSorting._quick_sort_sequential, (less,))
            
            if len(greater) > threshold:
                future_greater = pool.apply_async(
                    ParallelSorting.quick_sort_parallel, 
                    (greater, num_processes // 2, threshold)
                )
            else:
                future_greater = pool.apply_async(ParallelSorting._quick_sort_sequential, (greater,))
            
            sorted_less = future_less.get()
            sorted_greater = future_greater.get()
        
        return sorted_less + equal + sorted_greater
    
    @staticmethod
    def _quick_sort_sequential(data: List[T]) -> List[T]:
        """Sequential quick sort."""
        if len(data) <= 1:
            return data
        
        pivot = data[len(data) // 2]
        less = [x for x in data if x < pivot]
        equal = [x for x in data if x == pivot]
        greater = [x for x in data if x > pivot]
        
        return (ParallelSorting._quick_sort_sequential(less) + 
                equal + 
                ParallelSorting._quick_sort_sequential(greater))


class ParallelSearch:
    """
    Parallel search algorithms.
    """
    
    @staticmethod
    def linear_search_parallel(data: List[T], target: T, num_processes: int = None) -> int:
        """Parallel linear search."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if len(data) == 0:
            return -1
        
        # Split data into chunks
        chunk_size = max(1, len(data) // num_processes)
        chunks = [(data[i:i + chunk_size], target, i) 
                 for i in range(0, len(data), chunk_size)]
        
        # Search chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            results = pool.map(ParallelSearch._search_chunk, chunks)
        
        # Find first occurrence
        for result in results:
            if result != -1:
                return result
        
        return -1
    
    @staticmethod
    def _search_chunk(args: Tuple[List[T], T, int]) -> int:
        """Search for target in a chunk."""
        chunk, target, offset = args
        for i, item in enumerate(chunk):
            if item == target:
                return offset + i
        return -1
    
    @staticmethod
    def binary_search_parallel(data: List[T], target: T, num_processes: int = None) -> int:
        """Parallel binary search (for sorted data)."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if len(data) == 0:
            return -1
        
        # For small arrays, use sequential search
        if len(data) < 1000 or num_processes <= 1:
            return ParallelSearch._binary_search_sequential(data, target, 0)
        
        # Split data into chunks
        chunk_size = max(1, len(data) // num_processes)
        chunks = []
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            chunks.append((chunk, target, i))
        
        # Search chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            results = pool.map(ParallelSearch._binary_search_chunk, chunks)
        
        # Find result
        for result in results:
            if result != -1:
                return result
        
        return -1
    
    @staticmethod
    def _binary_search_chunk(args: Tuple[List[T], T, int]) -> int:
        """Binary search in a chunk."""
        chunk, target, offset = args
        result = ParallelSearch._binary_search_sequential(chunk, target, 0)
        return offset + result if result != -1 else -1
    
    @staticmethod
    def _binary_search_sequential(data: List[T], target: T, offset: int) -> int:
        """Sequential binary search."""
        left, right = 0, len(data) - 1
        
        while left <= right:
            mid = (left + right) // 2
            if data[mid] == target:
                return mid
            elif data[mid] < target:
                left = mid + 1
            else:
                right = mid - 1
        
        return -1


class ParallelNumerical:
    """
    Parallel numerical algorithms.
    """
    
    @staticmethod
    def matrix_multiply_parallel(A: np.ndarray, B: np.ndarray, num_processes: int = None) -> np.ndarray:
        """Parallel matrix multiplication."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if A.shape[1] != B.shape[0]:
            raise ValueError("Matrix dimensions don't match for multiplication")
        
        rows_A, cols_A = A.shape
        rows_B, cols_B = B.shape
        
        # For small matrices, use sequential multiplication
        if rows_A < 100 or num_processes <= 1:
            return np.dot(A, B)
        
        # Split matrix A by rows
        rows_per_process = max(1, rows_A // num_processes)
        row_chunks = []
        
        for i in range(0, rows_A, rows_per_process):
            end_row = min(i + rows_per_process, rows_A)
            row_chunks.append((A[i:end_row], B, i))
        
        # Multiply chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            results = pool.map(ParallelNumerical._multiply_chunk, row_chunks)
        
        # Combine results
        return np.vstack(results)
    
    @staticmethod
    def _multiply_chunk(args: Tuple[np.ndarray, np.ndarray, int]) -> np.ndarray:
        """Multiply a chunk of rows with matrix B."""
        A_chunk, B, start_row = args
        return np.dot(A_chunk, B)
    
    @staticmethod
    def monte_carlo_pi(num_samples: int, num_processes: int = None) -> float:
        """Estimate π using parallel Monte Carlo method."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        samples_per_process = num_samples // num_processes
        
        # Run Monte Carlo simulation in parallel
        with mp.Pool(processes=num_processes) as pool:
            results = pool.map(ParallelNumerical._monte_carlo_chunk, 
                             [samples_per_process] * num_processes)
        
        # Combine results
        total_inside = sum(results)
        return 4.0 * total_inside / num_samples
    
    @staticmethod
    def _monte_carlo_chunk(num_samples: int) -> int:
        """Monte Carlo simulation chunk."""
        inside_circle = 0
        for _ in range(num_samples):
            x = random.uniform(-1, 1)
            y = random.uniform(-1, 1)
            if x*x + y*y <= 1:
                inside_circle += 1
        return inside_circle
    
    @staticmethod
    def parallel_sum(data: List[float], num_processes: int = None) -> float:
        """Parallel sum calculation."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if len(data) < 1000 or num_processes <= 1:
            return sum(data)
        
        # Split data into chunks
        chunk_size = max(1, len(data) // num_processes)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        # Sum chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            partial_sums = pool.map(sum, chunks)
        
        return sum(partial_sums)
    
    @staticmethod
    def parallel_reduce(data: List[T], operation: Callable[[T, T], T], 
                       identity: T, num_processes: int = None) -> T:
        """Parallel reduce operation."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        if len(data) == 0:
            return identity
        
        if len(data) == 1:
            return data[0]
        
        if len(data) < 1000 or num_processes <= 1:
            result = identity
            for item in data:
                result = operation(result, item)
            return result
        
        # Split data into chunks
        chunk_size = max(1, len(data) // num_processes)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        # Reduce chunks in parallel
        def reduce_chunk(chunk):
            result = identity
            for item in chunk:
                result = operation(result, item)
            return result
        
        with mp.Pool(processes=num_processes) as pool:
            partial_results = pool.map(reduce_chunk, chunks)
        
        # Combine partial results
        final_result = identity
        for partial in partial_results:
            final_result = operation(final_result, partial)
        
        return final_result


class ParallelGraphAlgorithms:
    """
    Parallel graph algorithms.
    """
    
    @staticmethod
    def parallel_bfs(graph: Dict[int, List[int]], start: int, num_processes: int = None) -> Dict[int, int]:
        """Parallel breadth-first search."""
        if num_processes is None:
            num_processes = mp.cpu_count()
        
        # For small graphs, use sequential BFS
        if len(graph) < 100 or num_processes <= 1:
            return ParallelGraphAlgorithms._sequential_bfs(graph, start)
        
        # Initialize
        distances = {start: 0}
        current_level = [start]
        level = 0
        
        while current_level:
            # Process current level in parallel
            next_level = []
            
            if len(current_level) > num_processes:
                # Split current level among processes
                chunk_size = max(1, len(current_level) // num_processes)
                chunks = [current_level[i:i + chunk_size] 
                         for i in range(0, len(current_level), chunk_size)]
                
                with mp.Pool(processes=num_processes) as pool:
                    chunk_results = pool.map(
                        lambda chunk: ParallelGraphAlgorithms._process_bfs_chunk(chunk, graph, distances, level + 1),
                        chunks
                    )
                
                # Combine results
                for chunk_next, chunk_distances in chunk_results:
                    next_level.extend(chunk_next)
                    distances.update(chunk_distances)
            else:
                # Process sequentially if too few nodes
                for node in current_level:
                    for neighbor in graph.get(node, []):
                        if neighbor not in distances:
                            distances[neighbor] = level + 1
                            next_level.append(neighbor)
            
            current_level = list(set(next_level))  # Remove duplicates
            level += 1
        
        return distances
    
    @staticmethod
    def _process_bfs_chunk(chunk: List[int], graph: Dict[int, List[int]], 
                          distances: Dict[int, int], new_level: int) -> Tuple[List[int], Dict[int, int]]:
        """Process a chunk of nodes in BFS."""
        next_level = []
        new_distances = {}
        
        for node in chunk:
            for neighbor in graph.get(node, []):
                if neighbor not in distances and neighbor not in new_distances:
                    new_distances[neighbor] = new_level
                    next_level.append(neighbor)
        
        return next_level, new_distances
    
    @staticmethod
    def _sequential_bfs(graph: Dict[int, List[int]], start: int) -> Dict[int, int]:
        """Sequential BFS implementation."""
        distances = {start: 0}
        queue = [start]
        
        while queue:
            node = queue.pop(0)
            for neighbor in graph.get(node, []):
                if neighbor not in distances:
                    distances[neighbor] = distances[node] + 1
                    queue.append(neighbor)
        
        return distances


# Performance measurement utilities
class PerformanceMeasurer:
    """Utility for measuring algorithm performance."""
    
    @staticmethod
    def measure_algorithm(algorithm: Callable, *args, **kwargs) -> AlgorithmResult:
        """Measure algorithm execution time and performance."""
        start_time = time.time()
        result = algorithm(*args, **kwargs)
        execution_time = time.time() - start_time
        
        num_processes = kwargs.get('num_processes', mp.cpu_count())
        
        return AlgorithmResult(
            result=result,
            execution_time=execution_time,
            num_processes=num_processes
        )
    
    @staticmethod
    def compare_sequential_parallel(sequential_func: Callable, parallel_func: Callable,
                                  *args, **kwargs) -> Tuple[AlgorithmResult, AlgorithmResult]:
        """Compare sequential and parallel versions of an algorithm."""
        # Measure sequential
        seq_result = PerformanceMeasurer.measure_algorithm(sequential_func, *args)
        
        # Measure parallel
        par_result = PerformanceMeasurer.measure_algorithm(parallel_func, *args, **kwargs)
        
        # Calculate speedup and efficiency
        if seq_result.execution_time > 0:
            par_result.speedup = seq_result.execution_time / par_result.execution_time
            par_result.efficiency = par_result.speedup / par_result.num_processes
        
        return seq_result, par_result


# Example usage and demonstrations
def demo_parallel_sorting():
    """Demonstrate parallel sorting algorithms."""
    print("=== Parallel Sorting Demo ===")
    
    # Generate test data
    data = [random.randint(1, 10000) for _ in range(10000)]
    
    # Test merge sort
    start_time = time.time()
    sorted_merge = ParallelSorting.merge_sort_parallel(data.copy(), num_processes=4)
    merge_time = time.time() - start_time
    
    # Test quick sort
    start_time = time.time()
    sorted_quick = ParallelSorting.quick_sort_parallel(data.copy(), num_processes=4)
    quick_time = time.time() - start_time
    
    # Verify correctness
    expected = sorted(data)
    merge_correct = sorted_merge == expected
    quick_correct = sorted_quick == expected
    
    print(f"Merge sort: {merge_time:.3f}s, Correct: {merge_correct}")
    print(f"Quick sort: {quick_time:.3f}s, Correct: {quick_correct}")


def demo_parallel_search():
    """Demonstrate parallel search algorithms."""
    print("\n=== Parallel Search Demo ===")
    
    # Generate test data
    data = list(range(100000))
    target = 75000
    
    # Linear search
    start_time = time.time()
    linear_result = ParallelSearch.linear_search_parallel(data, target, num_processes=4)
    linear_time = time.time() - start_time
    
    # Binary search
    start_time = time.time()
    binary_result = ParallelSearch.binary_search_parallel(data, target, num_processes=4)
    binary_time = time.time() - start_time
    
    print(f"Linear search: Found at index {linear_result}, Time: {linear_time:.4f}s")
    print(f"Binary search: Found at index {binary_result}, Time: {binary_time:.4f}s")


def demo_parallel_numerical():
    """Demonstrate parallel numerical algorithms."""
    print("\n=== Parallel Numerical Demo ===")
    
    # Matrix multiplication
    A = np.random.rand(500, 500)
    B = np.random.rand(500, 500)
    
    start_time = time.time()
    result_parallel = ParallelNumerical.matrix_multiply_parallel(A, B, num_processes=4)
    parallel_time = time.time() - start_time
    
    start_time = time.time()
    result_numpy = np.dot(A, B)
    numpy_time = time.time() - start_time
    
    # Check correctness
    correct = np.allclose(result_parallel, result_numpy)
    
    print(f"Matrix multiplication (500x500):")
    print(f"  Parallel: {parallel_time:.3f}s")
    print(f"  NumPy: {numpy_time:.3f}s")
    print(f"  Correct: {correct}")
    
    # Monte Carlo π estimation
    start_time = time.time()
    pi_estimate = ParallelNumerical.monte_carlo_pi(1000000, num_processes=4)
    pi_time = time.time() - start_time
    
    print(f"Monte Carlo π estimation:")
    print(f"  Estimated π: {pi_estimate:.6f}")
    print(f"  Error: {abs(pi_estimate - math.pi):.6f}")
    print(f"  Time: {pi_time:.3f}s")


def demo_parallel_graph():
    """Demonstrate parallel graph algorithms."""
    print("\n=== Parallel Graph Demo ===")
    
    # Create a simple graph
    graph = {
        0: [1, 2],
        1: [0, 3, 4],
        2: [0, 5],
        3: [1],
        4: [1, 6],
        5: [2],
        6: [4]
    }
    
    start_time = time.time()
    distances = ParallelGraphAlgorithms.parallel_bfs(graph, start=0, num_processes=2)
    bfs_time = time.time() - start_time
    
    print(f"BFS distances from node 0:")
    for node, distance in sorted(distances.items()):
        print(f"  Node {node}: distance {distance}")
    print(f"Time: {bfs_time:.4f}s")


def main():
    """Run all parallel algorithm demonstrations."""
    demo_parallel_sorting()
    demo_parallel_search()
    demo_parallel_numerical()
    demo_parallel_graph()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
