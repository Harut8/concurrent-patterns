"""
Comprehensive Concurrency Patterns Collection

This package provides implementations of all major concurrency patterns
and solutions for multicore programming.
"""

__version__ = "0.1.0"
__author__ = "Concurrency Patterns Team"

# Core pattern modules
from . import threading_patterns
from . import asyncio_patterns
from . import multiprocessing_patterns
from . import actor_model
from . import csp_patterns
from . import reactive_patterns
from . import synchronization
from . import data_structures
from . import algorithms
from . import performance

__all__ = [
    "threading_patterns",
    "asyncio_patterns", 
    "multiprocessing_patterns",
    "actor_model",
    "csp_patterns",
    "reactive_patterns",
    "synchronization",
    "data_structures",
    "algorithms",
    "performance",
]
