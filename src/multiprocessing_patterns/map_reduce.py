"""
MapReduce Patterns

MapReduce implementations for distributed parallel processing.
"""

import multiprocessing as mp
import time
import pickle
import os
import tempfile
from typing import Any, List, Dict, Callable, Optional, Tuple, Iterator, TypeVar
from dataclasses import dataclass
from abc import ABC, abstractmethod
from collections import defaultdict
import logging
import heapq


T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


@dataclass
class MapTask:
    """Map task definition."""
    task_id: str
    input_data: Any
    mapper_func: Callable[[Any], List[Tuple[Any, Any]]]


@dataclass
class ReduceTask:
    """Reduce task definition."""
    task_id: str
    key: Any
    values: List[Any]
    reducer_func: Callable[[Any, List[Any]], Any]


@dataclass
class MapReduceResult:
    """MapReduce job result."""
    job_id: str
    results: Dict[Any, Any]
    execution_time: float
    map_time: float
    reduce_time: float
    num_mappers: int
    num_reducers: int


class Partitioner(ABC):
    """Abstract partitioner for distributing keys to reducers."""
    
    @abstractmethod
    def partition(self, key: Any, num_partitions: int) -> int:
        """Return partition number for the given key."""
        pass


class HashPartitioner(Partitioner):
    """Hash-based partitioner."""
    
    def partition(self, key: Any, num_partitions: int) -> int:
        """Partition based on hash of key."""
        return hash(key) % num_partitions


class RangePartitioner(Partitioner):
    """Range-based partitioner for ordered keys."""
    
    def __init__(self, key_ranges: List[Tuple[Any, Any]]):
        self.key_ranges = key_ranges
    
    def partition(self, key: Any, num_partitions: int) -> int:
        """Partition based on key ranges."""
        for i, (start, end) in enumerate(self.key_ranges):
            if start <= key < end:
                return i
        return num_partitions - 1  # Default to last partition


class MapReduceFramework:
    """
    MapReduce framework for distributed parallel processing.
    """
    
    def __init__(self, num_mappers: int = None, num_reducers: int = None, 
                 partitioner: Partitioner = None, temp_dir: str = None):
        self.num_mappers = num_mappers or mp.cpu_count()
        self.num_reducers = num_reducers or mp.cpu_count()
        self.partitioner = partitioner or HashPartitioner()
        self.temp_dir = temp_dir or tempfile.gettempdir()
        
        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def run_job(self, job_id: str, input_data: List[Any], 
                mapper: Callable[[Any], List[Tuple[Any, Any]]],
                reducer: Callable[[Any, List[Any]], Any]) -> MapReduceResult:
        """
        Run a complete MapReduce job.
        """
        start_time = time.time()
        
        # Map phase
        map_start = time.time()
        intermediate_files = self._run_map_phase(job_id, input_data, mapper)
        map_time = time.time() - map_start
        
        # Shuffle phase
        shuffled_data = self._shuffle_phase(job_id, intermediate_files)
        
        # Reduce phase
        reduce_start = time.time()
        final_results = self._run_reduce_phase(job_id, shuffled_data, reducer)
        reduce_time = time.time() - reduce_start
        
        # Cleanup intermediate files
        self._cleanup_intermediate_files(intermediate_files)
        
        total_time = time.time() - start_time
        
        return MapReduceResult(
            job_id=job_id,
            results=final_results,
            execution_time=total_time,
            map_time=map_time,
            reduce_time=reduce_time,
            num_mappers=self.num_mappers,
            num_reducers=self.num_reducers
        )
    
    def _run_map_phase(self, job_id: str, input_data: List[Any], 
                      mapper: Callable[[Any], List[Tuple[Any, Any]]]) -> List[str]:
        """Run the map phase."""
        logging.info(f"Starting map phase for job {job_id}")
        
        # Split input data among mappers
        chunk_size = max(1, len(input_data) // self.num_mappers)
        input_chunks = [input_data[i:i + chunk_size] 
                       for i in range(0, len(input_data), chunk_size)]
        
        # Create map tasks
        map_tasks = []
        for i, chunk in enumerate(input_chunks):
            task = MapTask(
                task_id=f"{job_id}_map_{i}",
                input_data=chunk,
                mapper_func=mapper
            )
            map_tasks.append(task)
        
        # Run mappers in parallel
        with mp.Pool(processes=self.num_mappers) as pool:
            intermediate_files = pool.map(self._execute_map_task, map_tasks)
        
        # Flatten list of lists
        all_files = []
        for file_list in intermediate_files:
            all_files.extend(file_list)
        
        logging.info(f"Map phase completed. Generated {len(all_files)} intermediate files")
        return all_files
    
    def _execute_map_task(self, task: MapTask) -> List[str]:
        """Execute a single map task."""
        # Create intermediate files for each reducer
        intermediate_files = []
        file_handles = {}
        
        try:
            # Open files for each partition
            for partition in range(self.num_reducers):
                filename = os.path.join(
                    self.temp_dir, 
                    f"{task.task_id}_partition_{partition}.pkl"
                )
                file_handles[partition] = open(filename, 'wb')
                intermediate_files.append(filename)
            
            # Process input data
            for item in task.input_data:
                # Apply mapper function
                key_value_pairs = task.mapper_func(item)
                
                # Partition and write to appropriate files
                for key, value in key_value_pairs:
                    partition = self.partitioner.partition(key, self.num_reducers)
                    pickle.dump((key, value), file_handles[partition])
        
        finally:
            # Close all file handles
            for fh in file_handles.values():
                fh.close()
        
        return intermediate_files
    
    def _shuffle_phase(self, job_id: str, intermediate_files: List[str]) -> Dict[int, Dict[Any, List[Any]]]:
        """Shuffle and group intermediate data by key."""
        logging.info(f"Starting shuffle phase for job {job_id}")
        
        # Group files by partition
        partition_files = defaultdict(list)
        for filename in intermediate_files:
            # Extract partition number from filename
            parts = filename.split('_')
            partition = int(parts[-1].split('.')[0])
            partition_files[partition].append(filename)
        
        # Group data by key for each partition
        shuffled_data = {}
        for partition in range(self.num_reducers):
            key_groups = defaultdict(list)
            
            # Read all files for this partition
            for filename in partition_files[partition]:
                try:
                    with open(filename, 'rb') as f:
                        while True:
                            try:
                                key, value = pickle.load(f)
                                key_groups[key].append(value)
                            except EOFError:
                                break
                except FileNotFoundError:
                    continue
            
            shuffled_data[partition] = dict(key_groups)
        
        logging.info(f"Shuffle phase completed. Grouped data for {len(shuffled_data)} partitions")
        return shuffled_data
    
    def _run_reduce_phase(self, job_id: str, shuffled_data: Dict[int, Dict[Any, List[Any]]], 
                         reducer: Callable[[Any, List[Any]], Any]) -> Dict[Any, Any]:
        """Run the reduce phase."""
        logging.info(f"Starting reduce phase for job {job_id}")
        
        # Create reduce tasks
        reduce_tasks = []
        for partition, key_groups in shuffled_data.items():
            for key, values in key_groups.items():
                task = ReduceTask(
                    task_id=f"{job_id}_reduce_{partition}_{key}",
                    key=key,
                    values=values,
                    reducer_func=reducer
                )
                reduce_tasks.append(task)
        
        # Run reducers in parallel
        with mp.Pool(processes=self.num_reducers) as pool:
            results = pool.map(self._execute_reduce_task, reduce_tasks)
        
        # Combine results
        final_results = {}
        for key, value in results:
            final_results[key] = value
        
        logging.info(f"Reduce phase completed. Generated {len(final_results)} final results")
        return final_results
    
    def _execute_reduce_task(self, task: ReduceTask) -> Tuple[Any, Any]:
        """Execute a single reduce task."""
        result = task.reducer_func(task.key, task.values)
        return task.key, result
    
    def _cleanup_intermediate_files(self, intermediate_files: List[str]):
        """Clean up intermediate files."""
        for filename in intermediate_files:
            try:
                if os.path.exists(filename):
                    os.unlink(filename)
            except Exception as e:
                logging.warning(f"Failed to delete {filename}: {e}")


class WordCountExample:
    """Example: Word count using MapReduce."""
    
    @staticmethod
    def mapper(text: str) -> List[Tuple[str, int]]:
        """Map function for word count."""
        words = text.lower().split()
        return [(word.strip('.,!?;:"()[]'), 1) for word in words if word.strip('.,!?;:"()[]')]
    
    @staticmethod
    def reducer(word: str, counts: List[int]) -> int:
        """Reduce function for word count."""
        return sum(counts)


class InvertedIndexExample:
    """Example: Inverted index using MapReduce."""
    
    @staticmethod
    def mapper(doc_data: Tuple[str, str]) -> List[Tuple[str, str]]:
        """Map function for inverted index."""
        doc_id, content = doc_data
        words = content.lower().split()
        unique_words = set(word.strip('.,!?;:"()[]') for word in words)
        return [(word, doc_id) for word in unique_words if word]
    
    @staticmethod
    def reducer(word: str, doc_ids: List[str]) -> List[str]:
        """Reduce function for inverted index."""
        return sorted(list(set(doc_ids)))


class TopKExample:
    """Example: Top-K most frequent items using MapReduce."""
    
    def __init__(self, k: int):
        self.k = k
    
    def mapper(self, item: Any) -> List[Tuple[Any, int]]:
        """Map function for counting items."""
        return [(item, 1)]
    
    def reducer(self, item: Any, counts: List[int]) -> int:
        """Reduce function for summing counts."""
        return sum(counts)
    
    def find_top_k(self, data: List[Any]) -> List[Tuple[Any, int]]:
        """Find top-K most frequent items."""
        framework = MapReduceFramework(num_mappers=2, num_reducers=2)
        
        result = framework.run_job(
            job_id="top_k_job",
            input_data=data,
            mapper=self.mapper,
            reducer=self.reducer
        )
        
        # Sort by count and return top-K
        sorted_items = sorted(result.results.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:self.k]


class MatrixMultiplicationExample:
    """Example: Matrix multiplication using MapReduce."""
    
    @staticmethod
    def mapper(matrix_element: Tuple[str, int, int, float]) -> List[Tuple[Tuple[int, int], Tuple[str, int, float]]]:
        """Map function for matrix multiplication."""
        matrix_name, row, col, value = matrix_element
        
        if matrix_name == 'A':
            # For matrix A, emit for all possible columns in result matrix
            # Assuming we know the dimensions (this is simplified)
            return [((row, k), ('A', col, value)) for k in range(10)]  # Assume 10 columns in B
        else:  # matrix_name == 'B'
            # For matrix B, emit for all possible rows in result matrix
            return [((i, col), ('B', row, value)) for i in range(10)]  # Assume 10 rows in A
    
    @staticmethod
    def reducer(position: Tuple[int, int], values: List[Tuple[str, int, float]]) -> float:
        """Reduce function for matrix multiplication."""
        a_values = {}
        b_values = {}
        
        for matrix_name, index, value in values:
            if matrix_name == 'A':
                a_values[index] = value
            else:
                b_values[index] = value
        
        # Compute dot product
        result = 0.0
        for index in a_values:
            if index in b_values:
                result += a_values[index] * b_values[index]
        
        return result


# Streaming MapReduce for large datasets
class StreamingMapReduce:
    """
    Streaming MapReduce for processing large datasets that don't fit in memory.
    """
    
    def __init__(self, num_mappers: int = None, num_reducers: int = None):
        self.num_mappers = num_mappers or mp.cpu_count()
        self.num_reducers = num_reducers or mp.cpu_count()
        self.temp_dir = tempfile.mkdtemp()
    
    def process_stream(self, data_stream: Iterator[Any], 
                      mapper: Callable[[Any], List[Tuple[Any, Any]]],
                      reducer: Callable[[Any, List[Any]], Any],
                      batch_size: int = 1000) -> Dict[Any, Any]:
        """Process a data stream using MapReduce."""
        
        batch_num = 0
        all_intermediate_files = []
        
        # Process stream in batches
        batch = []
        for item in data_stream:
            batch.append(item)
            
            if len(batch) >= batch_size:
                # Process this batch
                job_id = f"stream_batch_{batch_num}"
                framework = MapReduceFramework(
                    num_mappers=self.num_mappers,
                    num_reducers=self.num_reducers,
                    temp_dir=self.temp_dir
                )
                
                # Run map phase only
                intermediate_files = framework._run_map_phase(job_id, batch, mapper)
                all_intermediate_files.extend(intermediate_files)
                
                batch = []
                batch_num += 1
        
        # Process remaining items
        if batch:
            job_id = f"stream_batch_{batch_num}"
            framework = MapReduceFramework(
                num_mappers=self.num_mappers,
                num_reducers=self.num_reducers,
                temp_dir=self.temp_dir
            )
            intermediate_files = framework._run_map_phase(job_id, batch, mapper)
            all_intermediate_files.extend(intermediate_files)
        
        # Final shuffle and reduce
        shuffled_data = framework._shuffle_phase("final", all_intermediate_files)
        final_results = framework._run_reduce_phase("final", shuffled_data, reducer)
        
        # Cleanup
        framework._cleanup_intermediate_files(all_intermediate_files)
        
        return final_results


# Example usage and demonstrations
def demo_word_count():
    """Demonstrate word count with MapReduce."""
    print("=== Word Count MapReduce Demo ===")
    
    # Sample text data
    documents = [
        "the quick brown fox jumps over the lazy dog",
        "the dog was lazy and the fox was quick",
        "brown fox and lazy dog are animals",
        "the quick brown fox is very fast",
        "lazy dog sleeps all day long"
    ]
    
    # Create MapReduce framework
    framework = MapReduceFramework(num_mappers=2, num_reducers=2)
    
    # Run word count job
    result = framework.run_job(
        job_id="word_count",
        input_data=documents,
        mapper=WordCountExample.mapper,
        reducer=WordCountExample.reducer
    )
    
    # Print results
    print(f"Word count results (execution time: {result.execution_time:.2f}s):")
    sorted_words = sorted(result.results.items(), key=lambda x: x[1], reverse=True)
    for word, count in sorted_words[:10]:  # Top 10 words
        print(f"  {word}: {count}")


def demo_inverted_index():
    """Demonstrate inverted index with MapReduce."""
    print("\n=== Inverted Index MapReduce Demo ===")
    
    # Sample documents
    documents = [
        ("doc1", "the quick brown fox jumps"),
        ("doc2", "the lazy dog sleeps"),
        ("doc3", "brown fox and lazy dog"),
        ("doc4", "quick brown fox runs fast"),
        ("doc5", "the dog is very lazy")
    ]
    
    # Create MapReduce framework
    framework = MapReduceFramework(num_mappers=2, num_reducers=2)
    
    # Run inverted index job
    result = framework.run_job(
        job_id="inverted_index",
        input_data=documents,
        mapper=InvertedIndexExample.mapper,
        reducer=InvertedIndexExample.reducer
    )
    
    # Print results
    print(f"Inverted index results (execution time: {result.execution_time:.2f}s):")
    for word, doc_list in sorted(result.results.items()):
        print(f"  {word}: {doc_list}")


def demo_top_k():
    """Demonstrate top-K with MapReduce."""
    print("\n=== Top-K MapReduce Demo ===")
    
    # Sample data with repeated items
    data = ['apple', 'banana', 'apple', 'cherry', 'banana', 'apple', 
            'date', 'banana', 'cherry', 'apple', 'elderberry', 'banana']
    
    # Find top-3 most frequent items
    top_k = TopKExample(k=3)
    results = top_k.find_top_k(data)
    
    print("Top-3 most frequent items:")
    for item, count in results:
        print(f"  {item}: {count}")


def demo_streaming_mapreduce():
    """Demonstrate streaming MapReduce."""
    print("\n=== Streaming MapReduce Demo ===")
    
    # Simulate a large data stream
    def data_generator():
        import random
        words = ['apple', 'banana', 'cherry', 'date', 'elderberry']
        for _ in range(10000):
            yield random.choice(words)
    
    # Create streaming MapReduce
    streaming_mr = StreamingMapReduce(num_mappers=2, num_reducers=2)
    
    # Process stream
    results = streaming_mr.process_stream(
        data_stream=data_generator(),
        mapper=WordCountExample.mapper,
        reducer=WordCountExample.reducer,
        batch_size=1000
    )
    
    print(f"Streaming word count results:")
    for word, count in sorted(results.items(), key=lambda x: x[1], reverse=True):
        print(f"  {word}: {count}")


def main():
    """Run all MapReduce demonstrations."""
    demo_word_count()
    demo_inverted_index()
    demo_top_k()
    demo_streaming_mapreduce()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
