"""
Job runner module dispatching asynchronous distributed data processing.
Coordinates execution via EngineManager (Hadoop MapReduce -> PySpark fallback -> Failure).
No in-memory Pandas/NumPy computation in the analytical path.
"""
import os
import sys
import time
import logging
from typing import Dict, Any, List, Optional
from models import ColumnInfo, DashboardAnalytics, AnalysisJobStatus
from processing.engine_manager import EngineManager
from processing.base_engine import ProcessingEngineUnavailableError, EngineError

logger = logging.getLogger("sales_analyzer.job_runner")

# In-memory stores tracking job status and results
JOB_STATUS_STORE: Dict[str, Dict[str, Any]] = {}
RESULT_STORE: Dict[str, DashboardAnalytics] = {}

# Central EngineManager instance
DEFAULT_ENGINE_MANAGER = EngineManager()


def is_hadoop_installed() -> bool:
    """Check if Hadoop CLI and environment are genuinely operational."""
    avail, _ = DEFAULT_ENGINE_MANAGER.hadoop_engine.is_available()
    return avail


def is_pyspark_installed() -> bool:
    """Check if PySpark engine is genuinely operational."""
    avail, _ = DEFAULT_ENGINE_MANAGER.pyspark_engine.is_available()
    return avail


def run_mapreduce_job_async(
    job_id: str,
    dataset_id: str,
    file_path: str,
    columns: List[ColumnInfo],
    engine_manager: Optional[EngineManager] = None
):
    """
    Background worker thread orchestrating distributed dataset analytics.
    Enforces Hadoop Priority 1 -> PySpark Priority 2 -> Clear Error Priority 3.
    """
    manager = engine_manager or DEFAULT_ENGINE_MANAGER
    start_time = time.time()

    active_key, active_display = manager.get_active_engine()
    JOB_STATUS_STORE[job_id] = {
        'job_id': job_id,
        'dataset_id': dataset_id,
        'status': 'queued',
        'progress_pct': 5,
        'current_step': f"Initializing distributed engine: {active_display}...",
        'error_message': None,
        'execution_time_seconds': None,
        'engine': active_display,
        'processing_engine': active_key,
        'engine_indication': f"[ACTIVE ENGINE: {active_display}]"
    }

    def update_progress(pct: int, step_desc: str):
        if job_id in JOB_STATUS_STORE:
            JOB_STATUS_STORE[job_id]['progress_pct'] = pct
            JOB_STATUS_STORE[job_id]['current_step'] = step_desc
            if pct >= 80:
                JOB_STATUS_STORE[job_id]['status'] = 'aggregating'
            elif pct >= 15:
                JOB_STATUS_STORE[job_id]['status'] = 'running_mapreduce'

    try:
        # Step 2: Distributed Execution via EngineManager
        analytics_result = manager.process_dataset(
            job_id=job_id,
            dataset_id=dataset_id,
            file_path=file_path,
            columns=columns,
            progress_callback=update_progress
        )

        elapsed = round(time.time() - start_time, 3)

        # Step 3: Cache result for frontend retrieval
        RESULT_STORE[dataset_id] = analytics_result

        # Step 4: Mark job completed
        engine_name = analytics_result.execution_stats.get('engine', 'Distributed Engine')
        proc_engine = analytics_result.execution_stats.get('processing_engine', 'hadoop')
        JOB_STATUS_STORE[job_id].update({
            'status': 'completed',
            'progress_pct': 100,
            'current_step': f"Job completed successfully using {engine_name}!",
            'execution_time_seconds': elapsed,
            'engine': engine_name,
            'processing_engine': proc_engine,
            'engine_indication': f"[ACTIVE ENGINE: {engine_name}]"
        })
        logger.info(f"Job {job_id} successfully completed for dataset {dataset_id} in {elapsed}s")

    except ProcessingEngineUnavailableError as peu:
        logger.error(f"Job {job_id} failed: {peu}")
        JOB_STATUS_STORE[job_id].update({
            'status': 'failed',
            'progress_pct': 0,
            'current_step': 'Distributed processing engine unavailable',
            'error_message': str(peu)
        })

    except Exception as e:
        logger.error(f"Job {job_id} encountered execution error: {e}", exc_info=True)
        JOB_STATUS_STORE[job_id].update({
            'status': 'failed',
            'progress_pct': 0,
            'current_step': 'Execution failed',
            'error_message': str(e)
        })
