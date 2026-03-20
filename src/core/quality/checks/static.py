from __future__ import annotations

from typing import Dict, Optional, Union

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from .base import BaseCheck, CheckResult


class NotNullCheck(BaseCheck):
    """A static check that verifies a column contains no null values."""

    def __init__(self, check_config: Dict):
        """Initializes the check."""
        super().__init__(check_config)
        self.column = self.check_config.get("column")
        if not self.column:
            raise ValueError("Configuration for 'not_null' check requires a 'column' parameter.")

    def execute(
        self, data: Union[pd.DataFrame, SparkDataFrame], historical_baselines: Optional[Dict] = None
    ) -> CheckResult:
        """Counts the nulls in the specified column and returns a PASS or FAIL result."""
        if isinstance(data, pd.DataFrame):
            null_count = int(data[self.column].isnull().sum())
        else:
            # Handle both isNull and isNaN for Spark DataFrames
            null_count = data.where(F.col(self.column).isNull() | F.isnan(self.column)).count()

        if null_count > 0:
            message = f"Found {null_count} null or NaN values in column '{self.column}'."
            return self._fail(message, null_count)
        else:
            message = f"Column '{self.column}' contains no null values."
            return self._pass(message, 0)


class UniqueCheck(BaseCheck):
    """A static check that verifies a column or set of columns are a unique key."""

    def __init__(self, check_config: Dict):
        """Initializes the check."""
        super().__init__(check_config)
        self.columns = self.check_config.get("columns")
        if not self.columns:
            raise ValueError("Configuration for 'unique' check requires a 'columns' parameter.")

        # For convenience, allow the YAML to specify a single string or a list
        if isinstance(self.columns, str):
            self.columns = [self.columns]

    def execute(
        self, data: Union[pd.DataFrame, SparkDataFrame], historical_baselines: Optional[Dict] = None
    ) -> CheckResult:
        """Counts the number of duplicate rows based on the specified columns."""
        if isinstance(data, pd.DataFrame):
            duplicate_count = int(data.duplicated(subset=self.columns).sum())
        else:
            # Group by the key columns and find groups with more than one row
            duplicate_df = data.groupBy(self.columns).count().where(F.col("count") > 1)
            duplicate_count = duplicate_df.count()

        if duplicate_count > 0:
            message = f"Found {duplicate_count} duplicate rows for key {self.columns}."
            return self._fail(message, duplicate_count)
        else:
            message = f"Key {self.columns} is unique."
            return self._pass(message, 0)


class DistinctCountCheck(BaseCheck):
    """A static check that verifies the count of distinct values in a column."""

    def __init__(self, check_config: Dict):
        """Initializes the check."""
        super().__init__(check_config)
        self.column = self.check_config.get("column")
        self.min_threshold = self.check_config.get("min_threshold")
        if not self.column or self.min_threshold is None:
            raise ValueError("DistinctCountCheck requires 'column' and 'min_threshold' parameters.")

    def execute(
        self, data: Union[pd.DataFrame, SparkDataFrame], historical_baselines: Optional[Dict] = None
    ) -> CheckResult:
        """Counts distinct values and compares against a minimum threshold."""
        if isinstance(data, pd.DataFrame):
            distinct_count = data[self.column].nunique()
        else:
            distinct_count = data.select(self.column).distinct().count()

        if distinct_count < self.min_threshold:
            message = (
                f"Distinct count of '{self.column}' is {distinct_count}, "
                f"which is below the threshold of {self.min_threshold}."
            )
            return self._fail(message, distinct_count)
        else:
            message = (
                f"Distinct count of '{self.column}' is {distinct_count}, "
                f"which is at or above the threshold of {self.min_threshold}."
            )
            return self._pass(message, distinct_count)
