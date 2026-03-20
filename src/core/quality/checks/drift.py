import logging
from typing import Dict, Optional

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from .base import BaseCheck, CheckResult


class BaseDriftCheck(BaseCheck):
    """An abstract base class for all stateful, historical drift checks."""

    def __init__(self, check_config: Dict):
        """Initializes the drift check."""
        super().__init__(check_config)
        self.comparison_config = self.check_config.get("comparison", {})
        self.comparison_type = self.comparison_config.get("type")
        self.comparison_value = self.comparison_config.get("value")

    def _compare_to_baseline(
        self, metric_id: str, current_value: float, historical_baselines: Optional[Dict]
    ) -> CheckResult:
        """A common helper to perform the drift comparison logic."""
        if current_value is None:
            return self._warn(f"Metric '{metric_id}' could not be calculated.", None)

        # Baseline value is the average of all previous values (see data_manager.load_historical_metrics)
        baseline_value = historical_baselines.get(metric_id) if historical_baselines else None
        if baseline_value is None:
            logging.warning(f"No historical baseline for '{metric_id}'.")
            return self._pass(f"No historical baseline for '{metric_id}'.", current_value)

        if self.comparison_type == "tolerance_percent":
            tolerance = self.comparison_value / 100.0
            lower_bound = baseline_value * (1 - tolerance)
            upper_bound = baseline_value * (1 + tolerance)

            if not (lower_bound <= current_value <= upper_bound):
                message = (
                    f"Value {current_value:,.2f} is outside the {self.comparison_value}% "
                    f"tolerance of baseline {baseline_value:,.2f}."
                )
                return self._fail(message, current_value)
            else:
                message = (
                    f"Value {current_value:,.2f} is within the {self.comparison_value}% "
                    f"tolerance of baseline {baseline_value:,.2f}."
                )
                return self._pass(message, current_value)

        # Default pass if comparison type is unknown
        return self._pass(f"Drift check passed for '{metric_id}'.", current_value)


class RowCountDriftCheck(BaseDriftCheck):
    """Checks if the row count has drifted from its historical average."""

    def execute(self, data: SparkDataFrame, historical_baselines: Optional[Dict] = None) -> CheckResult:
        """Calculates row count and delegates comparison to the base class."""
        current_value = float(data.count())
        return self._compare_to_baseline("row_count_drift", current_value, historical_baselines)


class AverageValueDriftCheck(BaseDriftCheck):
    """Checks if a column's average value has drifted from its historical average."""

    def __init__(self, check_config: Dict):
        """Initializes the check."""
        super().__init__(check_config)
        self.column = self.check_config.get("column")
        if not self.column:
            raise ValueError("Configuration for 'avg_value_drift' requires a 'column' parameter.")

    def execute(self, data: SparkDataFrame, historical_baselines: Optional[Dict] = None) -> CheckResult:
        """Calculates column average and delegates comparison to the base class."""
        result = data.agg(F.avg(self.column)).first()
        current_value = result[0] if result else None
        metric_id = f"avg_{self.column}"
        return self._compare_to_baseline(metric_id, current_value, historical_baselines)
