"""
Base processing engine definitions and custom exceptions.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Callable


class EngineError(Exception):
    """Base exception for processing engine failures."""
    pass


class HadoopExecutionError(EngineError):
    """Exception raised when Hadoop MapReduce fails to execute workload."""
    pass


class PySparkExecutionError(EngineError):
    """Exception raised when PySpark fails to execute workload."""
    pass


class ProcessingEngineUnavailableError(EngineError):
    """Exception raised when neither Hadoop nor PySpark is available/functional."""
    pass


class BaseProcessingEngine(ABC):
    """Abstract Base Class for big data distributed processing engines."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def is_available(self) -> Tuple[bool, str]:
        """
        Check if the engine is genuinely available and operational.
        Returns:
            Tuple[bool, str]: (is_available, status_or_reason_message)
        """
        pass

    @abstractmethod
    def execute_analysis(
        self,
        job_id: str,
        dataset_id: str,
        file_path: str,
        columns: List[Any],
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Execute distributed analytics on the specified dataset.
        
        Args:
            job_id: Unique job identifier
            dataset_id: Dataset identifier
            file_path: Path to dataset file (CSV/etc.)
            columns: Confirmed column metadata
            progress_callback: Optional callback for reporting progress percentage and message
            
        Returns:
            Dict[str, Any]: Raw analytical results dictionary before normalization
        """
        pass
