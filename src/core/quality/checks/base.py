from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, Optional, Union

from pandas import DataFrame as PandasDataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame


class CheckStatus(Enum):
    """Enumeration for the status of a data quality check."""

    PASS = auto()
    WARN = auto()
    FAIL = auto()


@dataclass
class CheckResult:
    """A standard object to hold the result of a single data quality check."""

    check_name: str
    status: CheckStatus
    message: str
    metric_value: Any = None


class BaseCheck(ABC):
    """
    The abstract base class for all data quality checks.

    This class defines the "contract" that all concrete check classes must follow.
    It ensures that every check has a consistent interface for configuration and
    execution, making the system pluggable and easy to extend.
    """

    def __init__(self, check_config: Dict):
        """
        Initializes the check with its specific configuration.

        Args:
            check_config (Dict): A dictionary representing the configuration for a
                single check, parsed from the YAML file.
        """
        self.check_config = check_config
        self.check_type = check_config.get("type", "unknown_check")

        on_failure_str = check_config.get("on_failure", "fail").upper()
        self.on_failure_status = CheckStatus[on_failure_str]

        self.check_name = check_config.get("name", self.check_type)

    @abstractmethod
    def execute(
        self,
        data: Union[PandasDataFrame, SparkDataFrame],
        historical_baselines: Optional[Dict[str, float]] = None,
    ) -> CheckResult:
        """
        The core logic of the data quality check.

        This method must be implemented by all subclasses. It takes a DataFrame,
        performs its specific validation, and returns a structured CheckResult object.

        Args:
            data (Union[PandasDataFrame, SparkDataFrame]): The data to be checked.
            historical_baselines (Optional[Dict[str, float]]): A dictionary of
                pre-fetched historical metrics for data drift checks. Static checks
                will ignore this.

        Returns:
            CheckResult: A standardized object containing the outcome of the check.
        """
        pass

    def _pass(self, message: str, metric_value: Any = None) -> CheckResult:
        """Creates a PASS CheckResult."""
        return CheckResult(self.check_name, CheckStatus.PASS, message, metric_value)

    def _warn(self, message: str, metric_value: Any = None) -> CheckResult:
        """Creates a WARN CheckResult."""
        return CheckResult(self.check_name, CheckStatus.WARN, message, metric_value)

    def _fail(self, message: str, metric_value: Any = None) -> CheckResult:
        """Creates a FAIL CheckResult based on the on_failure config."""
        return CheckResult(self.check_name, self.on_failure_status, message, metric_value)
