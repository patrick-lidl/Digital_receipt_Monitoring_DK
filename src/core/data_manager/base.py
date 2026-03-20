from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type, Union

from pandas import DataFrame as PandasDataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import col

from core.databricks_handler import DatabricksHandler

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from core.config_manager import Config
    from core.quality.checks.base import CheckResult


class TableNotFoundError(Exception):
    """Custom exception for when a table cannot be found, regardless of method."""

    pass


class BaseDataManager(DatabricksHandler, ABC):
    """
    An abstract base class that defines the contract for all data managers.

    This ensures that every backend manager (e.g., for Unity Catalog, local
    filesystem) provides a consistent set of methods for loading, saving, and
    managing both business data and operational metadata.
    """

    def __init__(self, config: Optional["Config"]):
        """Initializes the base data manager."""
        super().__init__()
        self.config = config
        self.is_configured = config is not None

        # Extract quality tables
        if self.config:
            quality_config = self.config.get("quality", {})

            self.metrics_table_name = quality_config.get("metrics_table", "pipeline_metrics")
            self.metrics_staging_table_name = quality_config.get("metrics_staging_table", "pipeline_metrics_staging")
            self.log_table_name = quality_config.get("results_log_table", "check_results_log")

            # Log a warning if the keys were not explicitly set in the config
            for key in ["metrics_table", "metrics_staging_table", "results_log_table"]:
                if key not in quality_config:
                    logging.warning(f"Config key 'quality.{key}' not found. Using default.")

    @abstractmethod
    def setup(self) -> None:
        """
        Idempotently sets up the necessary infrastructure for the backend.

        For example, creating schemas, volumes, or local directories.
        """
        pass

    @abstractmethod
    def load_table(
        self, data_name: str, table_type: Type, schema_type: str
    ) -> Optional[Union[PandasDataFrame, SparkDataFrame]]:
        """Loads a structured table from a specific schema type (e.g., 'working', 'historical')."""
        pass

    @abstractmethod
    def save_table(
        self,
        data: Union[PandasDataFrame, SparkDataFrame],
        data_name: str,
        schema_type: Optional[str] = "working",
    ) -> None:
        """Saves a structured table to a specific schema type, overwriting if it exists."""
        pass

    @abstractmethod
    def drop_table(
        self,
        data_name: str,
        schema_type: str,
    ) -> None:
        """Remove table from a specific schema type (e.g., 'working', 'historical')."""
        pass

    @abstractmethod
    def archive_table(
        self,
        data: Union[PandasDataFrame, SparkDataFrame],
        data_name: str,
    ) -> None:
        """Archives a structured table to the 'historical' schema in append mode."""
        pass

    @abstractmethod
    def save_file(self, writer_func: Callable[[str], None], file_name: str, project_name: str) -> None:
        """Saves an unstructured file (e.g., JSON, HTML) to a managed location for a specific project."""
        pass

    # --- Methods for the Quality Module ---

    @abstractmethod
    def load_historical_metrics(self, data_name: str, metrics_to_check: List[Dict]) -> Dict[str, float]:
        """Loads and aggregates historical metrics for drift checks."""
        pass

    @abstractmethod
    def stage_run_metrics(self, metrics_to_stage: List[Dict]) -> None:
        """Saves calculated metrics to a temporary staging area during a run."""
        pass

    @abstractmethod
    def commit_staged_metrics(self, run_id: str) -> None:
        """Moves metrics from staging to the permanent store for a given run_id."""
        pass

    @abstractmethod
    def _load_check_results(self) -> SparkDataFrame:
        """Load the check results table."""
        pass

    def load_check_results(self, run_id: Optional[str] = None) -> SparkDataFrame:
        """Load the check results table, filtering by run_id, if passed."""
        df = self._load_check_results()
        # Filter using run_id, if passed
        if run_id:
            df = df.where(col("run_id") == run_id)
        return df

    @abstractmethod
    def log_check_results(self, results: List["CheckResult"], run_context: Dict, data_name: str) -> None:
        """Logs the detailed outcomes of all quality checks."""
        pass

    @abstractmethod
    def clear_quality_tables(self) -> None:
        """Removes and prepares the quality tables for a clean backfill."""
        pass

    @abstractmethod
    def _get_table_schema(self, table: str):
        """Return the schema of a table."""
        pass

    # --- Common Helper Methods ---

    def _ensure_spark_df(self, data: Union[PandasDataFrame, SparkDataFrame]) -> SparkDataFrame:
        """Ensures the input data is a Spark DataFrame."""
        if isinstance(data, SparkDataFrame):
            return data
        elif isinstance(data, PandasDataFrame):
            logging.info("Converting pandas dataframe to pyspark...")
            return self.spark.createDataFrame(data)
        raise TypeError(f"Unsupported data type: {type(data)}")

    def _align_schema_for_append(self, source_df: SparkDataFrame, target_table: str) -> SparkDataFrame:
        """
        Aligns a DataFrame's schema to a target Delta table's schema for safe appending.

        - If the target table does not exist, returns the original DataFrame.
        - If the target table exists, casts columns in the source DataFrame to match the
        data types in the target table. New columns are left as is.

        Args:
            spark: The active SparkSession.
            source_df: The source DataFrame to be aligned.
            target_table: The path to the target Delta table or table name.

        Returns:
            A new DataFrame, ready to be appended.
        """
        try:
            target_schema = self._get_table_schema(target_table)

            # Step 1: Standardize column casing
            cased_df = self._standardize_column_casing(source_df, target_schema)

            # Step 2: Cast column data types
            final_df = self._cast_column_types(cased_df, target_schema)

            return final_df

        except TableNotFoundError:
            logging.info(f"Target table '{target_table}' not found. Proceeding with initial write.")
            return source_df

    def _get_metric_id(self, metric_config: Dict) -> str:
        """Creates a unique string identifier for a metric."""
        metric_type = metric_config.get("type", "unknown")
        column = metric_config.get("column")
        return f"{metric_type}_{column}" if column else metric_type

    def _format_result_for_logging(self, result: "CheckResult", run_context: Dict, data_name: str) -> Dict:
        """Helper to format a CheckResult object into a dictionary for logging."""
        result_dict = asdict(result)

        if result_dict["metric_value"] is not None:
            try:
                result_dict["metric_value"] = float(result_dict["metric_value"])
            except (ValueError, TypeError):
                # If it's not a number (e.g., a list), keep it as a string
                result_dict["metric_value"] = str(result_dict["metric_value"])

        result_dict["status"] = result_dict["status"].name
        return {**run_context, "table_name": data_name, "check_type": "custom", **result_dict}

    @staticmethod
    def _standardize_column_casing(source_df: SparkDataFrame, target_schema: "StructType") -> SparkDataFrame:
        """Renames source DataFrame columns to match the casing of the target schema."""
        target_name_map = {field.name.lower(): field.name for field in target_schema.fields}

        df_with_renamed_cols = source_df

        for source_col_name in source_df.columns:
            lower_source_col = source_col_name.lower()
            if lower_source_col in target_name_map:
                target_col_name = target_name_map[lower_source_col]
                if source_col_name != target_col_name:
                    df_with_renamed_cols = df_with_renamed_cols.withColumnRenamed(source_col_name, target_col_name)

        return df_with_renamed_cols

    @staticmethod
    def _cast_column_types(source_df: SparkDataFrame, target_schema: "StructType") -> SparkDataFrame:
        """Casts source DataFrame columns to match the data types of the target schema."""
        target_types = {field.name: field.dataType for field in target_schema.fields}

        select_exprs = [col(c).cast(target_types[c]) if c in target_types else col(c) for c in source_df.columns]

        return source_df.select(*select_exprs)
