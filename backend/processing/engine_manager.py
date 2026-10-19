"""
Central Big Data Processing Engine Manager.
Enforces engine selection priority:
  Priority 1: Apache Hadoop MapReduce
  Priority 2: Apache PySpark
  Priority 3: No processing engine available (Raises ProcessingEngineUnavailableError)
No silent fallback to Pandas or NumPy for core analytics.
"""
import time
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable

from processing.base_engine import (
    BaseProcessingEngine,
    HadoopExecutionError,
    PySparkExecutionError,
    ProcessingEngineUnavailableError
)
from processing.hadoop_engine import HadoopEngine
from processing.pyspark_engine import PySparkEngine
from processing.result_normalizer import normalize_to_dashboard_analytics
from models import DashboardAnalytics

logger = logging.getLogger("sales_analyzer.engine_manager")


class EngineManager:
    """Manages big data engine selection, capability probing, execution, and fallback."""

    def __init__(
        self,
        hadoop_engine: Optional[HadoopEngine] = None,
        pyspark_engine: Optional[PySparkEngine] = None
    ):
        self.hadoop_engine = hadoop_engine or HadoopEngine()
        self.pyspark_engine = pyspark_engine or PySparkEngine()

    def get_active_engine(self) -> Tuple[str, str]:
        """Determines which engine will be chosen based on current availability."""
        hadoop_ok, _ = self.hadoop_engine.is_available()
        if hadoop_ok:
            return "hadoop", "Apache Hadoop MapReduce"
        spark_ok, _ = self.pyspark_engine.is_available()
        if spark_ok:
            return "pyspark", "Apache PySpark (DataFrame & SQL Engine)"
        return "none", "None (Unavailable)"

    @property
    def engine_indication(self) -> str:
        """Concise indicator string showing active engine name."""
        _, display_name = self.get_active_engine()
        return f"[ENGINE: {display_name}]"

    def get_engine_status(self) -> Dict[str, Any]:
        """Probe and return the runtime capability of all registered processing engines with active engine indication."""
        active_key, active_display = self.get_active_engine()
        hadoop_ok, hadoop_msg = self.hadoop_engine.is_available()
        spark_ok, spark_msg = self.pyspark_engine.is_available()
        return {
            'active_engine': active_key,
            'active_engine_name': active_display,
            'engine_indication': f"[ACTIVE ENGINE: {active_display}]",
            'hadoop': {
                'available': hadoop_ok,
                'status_message': hadoop_msg
            },
            'pyspark': {
                'available': spark_ok,
                'status_message': spark_msg
            }
        }

    def process_dataset(
        self,
        job_id: str,
        dataset_id: str,
        file_path: str,
        columns: List[Any],
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> DashboardAnalytics:
        """
        Processes dataset following the strict engine priority hierarchy:
        Hadoop -> PySpark -> Fail.
        """
        start_time = time.time()
        hadoop_failure_reason: Optional[str] = None
        pyspark_failure_reason: Optional[str] = None

        # -------------------------------------------------------------
        # PRIORITY 1: APACHE HADOOP MAPREDUCE
        # -------------------------------------------------------------
        hadoop_available, hadoop_probe_msg = self.hadoop_engine.is_available()
        if hadoop_available:
            print(f"\n[ENGINE SELECTION] >>> Active Engine: Apache Hadoop MapReduce <<<", flush=True)
            logger.info(f"[{job_id}] Priority 1: Submitting workload to Hadoop MapReduce ({hadoop_probe_msg})")
            if progress_callback:
                progress_callback(10, "[Engine: Apache Hadoop MapReduce] Preparing distributed MapReduce streaming job...")
            try:
                raw_result = self.hadoop_engine.execute_analysis(
                    job_id=job_id,
                    dataset_id=dataset_id,
                    file_path=file_path,
                    columns=columns,
                    progress_callback=progress_callback
                )
                elapsed = time.time() - start_time
                analytics = normalize_to_dashboard_analytics(
                    raw_result=raw_result,
                    dataset_id=dataset_id,
                    file_path=file_path,
                    engine_name="Apache Hadoop MapReduce",
                    execution_time_sec=elapsed,
                    additional_stats={
                        'processing_engine': 'hadoop',
                        'engine_indicator': 'Apache Hadoop MapReduce',
                        'engine_badge': '🐘 Hadoop MapReduce',
                        'hadoop_probe': hadoop_probe_msg
                    }
                )
                logger.info(f"[{job_id}] Successfully completed analysis via Hadoop MapReduce in {elapsed:.2f}s")
                return analytics

            except HadoopExecutionError as he:
                hadoop_failure_reason = str(he)
                logger.warning(
                    f"[{job_id}] Hadoop MapReduce execution failed: {he}. "
                    "Initiating safe automatic fallback to PySpark..."
                )
                if progress_callback:
                    progress_callback(30, f"[Fallback] Hadoop MapReduce encountered an error. Falling back to PySpark engine...")
            except Exception as e:
                hadoop_failure_reason = f"Unexpected error during Hadoop execution: {str(e)}"
                logger.warning(f"[{job_id}] {hadoop_failure_reason}. Falling back to PySpark...")
        else:
            hadoop_failure_reason = f"Hadoop MapReduce unavailable: {hadoop_probe_msg}"
            logger.info(f"[{job_id}] Priority 1 skipped: {hadoop_failure_reason}")

        # -------------------------------------------------------------
        # PRIORITY 2: APACHE PYSPARK (FALLBACK)
        # -------------------------------------------------------------
        spark_available, spark_probe_msg = self.pyspark_engine.is_available()
        if spark_available:
            print(f"\n[ENGINE SELECTION] >>> Active Engine: Apache PySpark (DataFrame & SQL Engine) <<<", flush=True)
            logger.info(f"[{job_id}] Priority 2: Submitting workload to Apache PySpark ({spark_probe_msg})")
            if progress_callback:
                progress_callback(35, "[Engine: Apache PySpark] Executing distributed DataFrame transformations & SQL aggregations...")
            try:
                raw_result = self.pyspark_engine.execute_analysis(
                    job_id=job_id,
                    dataset_id=dataset_id,
                    file_path=file_path,
                    columns=columns,
                    progress_callback=progress_callback
                )
                elapsed = time.time() - start_time
                analytics = normalize_to_dashboard_analytics(
                    raw_result=raw_result,
                    dataset_id=dataset_id,
                    file_path=file_path,
                    engine_name="Apache PySpark (DataFrame & SQL Engine)",
                    execution_time_sec=elapsed,
                    additional_stats={
                        'processing_engine': 'pyspark',
                        'engine_indicator': 'Apache PySpark',
                        'engine_badge': '⚡ Apache PySpark',
                        'pyspark_probe': spark_probe_msg,
                        'hadoop_fallback_reason': hadoop_failure_reason
                    }
                )
                logger.info(f"[{job_id}] Successfully completed analysis via PySpark in {elapsed:.2f}s")
                return analytics

            except PySparkExecutionError as pe:
                pyspark_failure_reason = str(pe)
                logger.error(f"[{job_id}] PySpark execution failed: {pe}")
            except Exception as e:
                pyspark_failure_reason = f"Unexpected error during PySpark execution: {str(e)}"
                logger.error(f"[{job_id}] {pyspark_failure_reason}")
        else:
            pyspark_failure_reason = f"PySpark engine unavailable: {spark_probe_msg}"
            logger.warning(f"[{job_id}] Priority 2 failed: {pyspark_failure_reason}")

        # -------------------------------------------------------------
        # PRIORITY 3: NO OPERATIONAL DISTRIBUTED ENGINE AVAILABLE
        # -------------------------------------------------------------
        error_msg = (
            "Unable to process dataset: No distributed big data processing engine is operational.\n"
            f"1. Hadoop MapReduce status: {hadoop_failure_reason}\n"
            f"2. PySpark status: {pyspark_failure_reason}\n"
            "System strictly prohibits silent fallback to in-memory Pandas/NumPy for primary analytics."
        )
        logger.error(f"[{job_id}] {error_msg}")
        raise ProcessingEngineUnavailableError(error_msg)


if __name__ == "__main__":
    em = EngineManager()
    key, display = em.get_active_engine()
    status = em.get_engine_status()
    print("=" * 60)
    print("SALES ANALYZER — PROCESSING ENGINE STATUS")
    print("=" * 60)
    print(f"Active Engine:     {display}")
    print(f"Engine Key:        {key}")
    print(f"Engine Indicator:  {em.engine_indication}")
    print(f"Hadoop Available:  {status['hadoop']['available']} ({status['hadoop']['status_message']})")
    print(f"PySpark Available: {status['pyspark']['available']} ({status['pyspark']['status_message']})")
    print("=" * 60)

