from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame as SparkDataFrame, functions as F

from core.quality.checks.base import BaseCheck, CheckResult


class StoresRedFlagCheck(BaseCheck):
    """Checks that at least N stores defined in the data have at least one red flag."""

    def __init__(self, check_config: Dict):
        """Initializes the check."""
        super().__init__(check_config)
        self.column = self.check_config.get("column")
        self.min_threshold = self.check_config.get("min_threshold")
        if not self.column or self.min_threshold is None:
            raise ValueError("StoresRedFlagCheck requires 'column' and 'min_threshold' parameters.")
        if not isinstance(self.min_threshold, int) or self.min_threshold < 0:
            raise ValueError("'min_threshold' must be a non-negative integer.")

    def execute(self, data: SparkDataFrame, historical_baselines: Optional[Dict] = None) -> CheckResult:
        """Checks that at least `min_threshold` stores have at least one red flag (FLAG == 2)."""
        # Ensure the input is a Spark DataFrame
        if not isinstance(data, SparkDataFrame):
            raise TypeError(f"StoresRedFlagCheck expects a Spark DataFrame, but got {type(data)}.")

        # Ensure the configured store column exists
        if self.column not in data.columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame columns: {data.columns}")

        # 1) All stores defined in the data
        all_stores_df = data.select(self.column).distinct()
        total_store_count = all_stores_df.count()

        # 2) Stores with at least one red flag (FLAG == 2)
        stores_with_red_flags_df = data.where(F.col("FLAG") == 2).select(self.column).distinct()
        stores_with_red_flags_count = stores_with_red_flags_df.count()

        # 3) Threshold evaluation
        if stores_with_red_flags_count >= self.min_threshold:
            msg = (
                f"{stores_with_red_flags_count} stores have red flags "
                f"(threshold: {self.min_threshold}, total stores: {total_store_count})."
            )
            # Metric: number of stores that satisfy the condition
            return self._pass(msg, stores_with_red_flags_count)

        # 4) Failure path – compute deficit and provide a helpful sample of stores without red flags
        deficit = self.min_threshold - stores_with_red_flags_count

        # Which stores do not have any red flag?
        stores_wo_red_flags_df = all_stores_df.join(stores_with_red_flags_df, on=self.column, how="left_anti")
        missing_count = stores_wo_red_flags_df.count()

        # Provide a small example list (up to 20) to aid debugging
        sample_missing = [row[self.column] for row in stores_wo_red_flags_df.limit(20).collect()]
        examples_suffix = ""
        if sample_missing:
            examples_list = ", ".join(map(str, sample_missing))
            ellipsis = "…" if missing_count > len(sample_missing) else ""
            examples_suffix = f" Examples: {examples_list}{ellipsis}"

        message = (
            f"Only {stores_with_red_flags_count} stores have red flags, "
            f"but the threshold is {self.min_threshold} "
            f"(deficit: {deficit}; total stores: {total_store_count}; "
            f"stores without red flags: {missing_count})."
            f"{examples_suffix}"
        )
        # Metric: deficit to threshold (how many additional stores with red flags are required)
        return self._fail(message, deficit)


class KpiRedFlagCheck(BaseCheck):
    """Checks that every KPI defined in the data has at least one red flag."""

    def execute(self, data: SparkDataFrame, historical_baselines: Optional[Dict] = None) -> CheckResult:
        """Checks that every KPI in a Spark DataFrame has at least one red flag."""
        # Ensure the input is a Spark DataFrame
        if not isinstance(data, SparkDataFrame):
            raise TypeError(f"KpiRedFlagCheck expects a Spark DataFrame, but got {type(data)}.")

        # 1. Get all unique KPIs present in the data.
        all_kpis_df = data.select("KENNZAHL").distinct()

        # 2. Get the unique KPIs that have at least one red flag (FLAG == 2).
        kpis_with_red_flags_df = data.where(F.col("FLAG") == 2).select("KENNZAHL").distinct()

        # 3. Find the KPIs that are in the full list but NOT in the red flag list.
        #    A 'left_anti' join is the most efficient way to perform a set
        #    difference in Spark, as it avoids collecting large lists to the driver.
        kpis_wo_red_flags_df = all_kpis_df.join(kpis_with_red_flags_df, on="KENNZAHL", how="left_anti")

        # 4. Check if there are any KPIs without red flags.
        missing_kpi_count = kpis_wo_red_flags_df.count()

        if missing_kpi_count > 0:
            # Collect the names of the missing KPIs for a helpful error message.
            missing_kpis = [row.KENNZAHL for row in kpis_wo_red_flags_df.collect()]
            message = f"The following KPIs have no red flags: {', '.join(missing_kpis)}"
            return self._fail(message, missing_kpi_count)

        return self._pass("All KPIs have red flags.", 0)


class KpiOrangeFlagCheck(BaseCheck):
    """Checks that every KPI defined in the data has at least one red flag."""

    def __init__(self, check_config: Dict):
        """Initializes the check."""
        super().__init__(check_config)
        self.kpis_without_orange_flags = self.check_config.get("kpis_without_orange_flags", [])

    def execute(self, data: SparkDataFrame, historical_baselines: Optional[Dict] = None) -> CheckResult:
        """Checks that every KPI in a Spark DataFrame has at least one orange flag."""
        # Ensure the input is a Spark DataFrame
        if not isinstance(data, SparkDataFrame):
            raise TypeError(f"KpiOrangeFlagCheck expects a Spark DataFrame, but got {type(data)}.")

        # 1. Get all unique KPIs present in the data.
        all_kpis_df = data.select("KENNZAHL").distinct().filter(~F.col("KENNZAHL").isin(self.kpis_without_orange_flags))

        # 2. Get the unique KPIs that have at least one orange flag (FLAG == 2).
        kpis_with_orange_flags_df = data.where(F.col("FLAG") == 1).select("KENNZAHL").distinct()

        # 3. Find the KPIs that are in the full list but NOT in the orange flag list.
        #    A 'left_anti' join is the most efficient way to perform a set
        #    difference in Spark, as it avoids collecting large lists to the driver.
        kpis_wo_orange_flags_df = all_kpis_df.join(kpis_with_orange_flags_df, on="KENNZAHL", how="left_anti")

        # 4. Check if there are any KPIs without orange flags.
        missing_kpi_count = kpis_wo_orange_flags_df.count()

        if missing_kpi_count > 0:
            # Collect the names of the missing KPIs for a helpful error message.
            missing_kpis = [row.KENNZAHL for row in kpis_wo_orange_flags_df.collect()]
            message = f"The following KPIs have no orange flags: {', '.join(missing_kpis)}"
            return self._fail(message, missing_kpi_count)

        return self._pass("All expected KPIs have orange flags.", 0)
