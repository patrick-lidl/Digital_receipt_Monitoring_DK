from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Dict, List, Tuple, Type, Union

from pandas import DataFrame as PandasDataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.data_manager.data_manager import DataManager
from core.databricks_handler import DatabricksHandler
from core.run_state import RunState

from .checks.base import CheckResult, CheckStatus
from .checks.registry import CHECK_REGISTRY

if TYPE_CHECKING:
    from .checks.base import BaseCheck


class DataQualityValidator(DatabricksHandler):
    """
    Orchestrates data quality validation using a custom, pluggable check library.

    FIXME: usage must be updated in tests.

    This class is the primary entry point for the quality submodule. It is
    instantiated by a `Task` and provides a single `validate()` method. It is
    driven by a YAML configuration that specifies which checks to run on which
    DataFrames.
    """

    def __init__(self, config: Dict, run_state: RunState, task_name: str, data_manager: DataManager):
        """
        Initializes the DataQualityValidator.

        This ensures the shared Spark session is upgraded to be Delta-enabled,
        instantiates the DataManager, and builds the dynamic check registry by
        merging core checks with custom checks from the configuration.

        Args:
            config (Dict): The dictionary parsed from the 'data_quality_checks'
                section of a YAML configuration file.
            run_state (RunState): Run-specific metadata that will be added to every log
                entry for traceability.
            task_name (str): Name of the Task being run.
            data_manager (DataManager): Instance of DataManager.
        """
        super().__init__()
        self.config = config
        # FIXME: Hotfix, instead we should use run_state
        self.run_context = {
            "run_id": run_state.run_id,
            "run_timestamp": run_state.run_timestamp,
            "task_name": task_name,
        }
        self.is_enabled = self.config.get("enabled", False)
        self.data_manager = data_manager
        self._check_registry = self._build_dynamic_registry()

    def validate(
        self, data: Union[PandasDataFrame, SparkDataFrame], data_name: str
    ) -> Tuple[Union[PandasDataFrame, SparkDataFrame], List[CheckResult]]:
        """
        Runs the full suite of data quality checks defined in the configuration.

        This is the main public method. It orchestrates the entire validation
        process:
        1. Pre-fetches historical data needed for any drift checks.
        2. Executes all configured checks (both static and drift).
        3. Logs the results and enforces the quality gate (failing if necessary).
        4. Stages the newly calculated drift metrics for persistence.

        Args:
            data (Union[PandasDataFrame, SparkDataFrame]): The input DataFrame to validate.
            data_name (str): The logical name of the dataset, used to find its
                checks in the configuration.

        Returns:
            A tuple containing:
                - The DataFrame that was validated.
                - A list of `CheckResult` objects with the outcome of each check.

        Raises:
            Exception: If any check configured with `on_failure: fail` does not pass.
        """
        check_configs = self.config.get("checks", {}).get(data_name)
        if not self.is_enabled or not check_configs:
            return data, []

        historical_baselines = self._prefetch_historical_data(data_name, check_configs)

        results = self._run_checks(data, check_configs, historical_baselines)

        self._handle_results(results, data_name)

        self._stage_drift_metrics(results, check_configs, data_name)

        return data, results

    def set_run_context(self, run_context: Dict):
        """Allows updating the run_context for a long-lived validator instance."""
        self.run_context = run_context

    def _build_dynamic_registry(self) -> Dict[str, Type[BaseCheck]]:
        """
        Builds the final check registry.

        Builds the final check registry by merging core checks with local
        custom checks declared in the task's configuration.
        """
        final_registry = CHECK_REGISTRY.copy()
        local_custom_checks = self.config.get("custom_checks", {})
        for name, path in local_custom_checks.items():
            try:
                module_path, class_name = path.rsplit(".", 1)
                module = importlib.import_module(module_path)
                CustomCheckClass = getattr(module, class_name)
                final_registry[name] = CustomCheckClass
            except (ImportError, AttributeError, ValueError) as e:
                logging.warning(f"Could not load custom check '{name}' from path '{path}': {e}")
        return final_registry

    def _prefetch_historical_data(self, data_name: str, check_configs: List[Dict]) -> Dict[str, float]:
        """
        Loads all required historical metrics.

        Scans check configurations and loads all required historical metrics
        from the DataManager in a single query for efficiency.
        """
        drift_check_configs = [cfg for cfg in check_configs if "drift" in cfg.get("type", "")]
        if not drift_check_configs:
            return {}

        logging.info(f"Pre-fetching historical data for {len(drift_check_configs)} drift checks on '{data_name}'...")
        return self.data_manager.load_historical_metrics(data_name, drift_check_configs)

    def _run_checks(
        self,
        data: Union[PandasDataFrame, SparkDataFrame],
        check_configs: List[Dict],
        historical_baselines: Dict[str, float],
    ) -> List[CheckResult]:
        """Iterates through check configurations, instantiates, and executes them."""
        results = []
        for check_config in check_configs:
            check_type = check_config.get("type")
            if not check_type:
                logging.warning(f"Skipping check with no 'type' defined: {check_config}")
                continue
            CheckClass = self._check_registry.get(check_type)
            if not CheckClass:
                logging.warning(f"No check found in registry for type '{check_type}'. Skipping.")
                continue

            try:
                check_instance = CheckClass(check_config)
                result = check_instance.execute(data, historical_baselines)
                results.append(result)
            except ValueError as e:
                logging.warning(f"Skipping misconfigured check '{check_type}': {e}")
                continue
        return results

    def _handle_results(self, results: List[CheckResult], data_name: str):
        """Logs all results via the DataManager and raises an exception on failure."""
        if not results:
            return

        self.data_manager.log_check_results(results, self.run_context, data_name)

        critical_failures = [res for res in results if res.status == CheckStatus.FAIL]
        if critical_failures:
            failure_messages = [f"- {res.check_name}: {res.message}" for res in critical_failures]
            error_message = f"Critical data quality checks failed for '{data_name}':\n" + "\n".join(failure_messages)
            raise Exception(error_message)

        logging.info(f"All {len(results)} data quality checks passed for '{data_name}'.")

    def _stage_drift_metrics(self, results: List[CheckResult], check_configs: List[Dict], data_name: str):
        """Filters for drift metrics and passes them to the DataManager for staging."""
        metrics_to_stage = []
        for result, config in zip(results, check_configs):
            check_type = config.get("type", "")
            if "drift" in check_type and result.metric_value is not None:
                metrics_to_stage.append(
                    {
                        "run_id": self.run_context.get("run_id"),
                        "data_name": data_name,
                        "metric_id": result.check_name,
                        "metric_value": result.metric_value,
                        "run_timestamp": self.run_context.get("run_timestamp"),
                    }
                )
        if metrics_to_stage:
            logging.info(f"Staging {len(metrics_to_stage)} drift metrics...")
            self.data_manager.stage_run_metrics(metrics_to_stage)
