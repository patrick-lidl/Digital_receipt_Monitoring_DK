from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type, Union

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.utils import AnalysisException

from .base import BaseDataManager, TableNotFoundError

if TYPE_CHECKING:
    from core.config_manager import Config
    from core.quality.checks.base import CheckResult


class FilesystemManager(BaseDataManager):
    """
    Implements the data manager contract for the local filesystem.

    Handles reading and writing Parquet for working tables (in both single-file
    and directory formats), Delta for historical/quality tables, and other files
    (JSON, HTML) for project outputs.
    """

    def __init__(self, config: Optional["Config"]):
        """Initializes the FilesystemManager."""
        super().__init__(config)
        if self.is_configured and self.config is not None:
            self.data_path = Path(self.config.get("local_data_path", "data/local"))
            self.output_path = Path(self.config.get("local_output_path", "output"))

            # Define paths that mirror the UC schemas
            self.working_path = self.data_path / "cdspch"
            self.historical_path = self.data_path / "historical"
            self.quality_path = self.data_path / "quality"

    def setup(self) -> None:
        """Creates all necessary local directories defined in the configuration."""
        if not self.is_configured:
            return

        logging.info("Setting up local filesystem directories...")
        self.working_path.mkdir(parents=True, exist_ok=True)
        self.historical_path.mkdir(parents=True, exist_ok=True)
        self.quality_path.mkdir(parents=True, exist_ok=True)
        self.output_path.mkdir(parents=True, exist_ok=True)

    def load_table(
        self, data_name: str, table_type: Type, schema_type: str
    ) -> Optional[Union[pd.DataFrame, SparkDataFrame]]:
        """
        Loads a table from the local filesystem.

        Checks for both single-file (pandas) and directory (Spark) Parquet formats.
        """
        base_path = self._get_base_path(schema_type)
        path_as_file = base_path / f"{data_name}.parquet"
        path_as_dir = base_path / data_name

        load_path: Optional[Path] = None
        if path_as_file.is_file():
            load_path = path_as_file
            logging.info(f"Found local Parquet file: {load_path}")
        elif path_as_dir.is_dir():
            load_path = path_as_dir
            logging.info(f"Found local Parquet directory: {load_path}")

        if not load_path:
            return None

        try:
            logging.info(f"Loading local Parquet file: {load_path}")
            if table_type == SparkDataFrame:
                return self.spark.read.parquet(str(load_path.resolve()))
            elif table_type == pd.DataFrame:
                return pd.read_parquet(load_path)
        except Exception as e:
            logging.error(f"Failed to load Parquet data from {load_path}: {e}")

        return None

    def save_table(
        self,
        data: Union[pd.DataFrame, SparkDataFrame],
        data_name: str,
        schema_type: Optional[str] = "working",
    ) -> None:
        """
        Saves a structured table to a specific schema type, overwriting if it exists.

        Creates a single file for pandas or a directory for Spark.
        """
        base_path = self._get_base_path(schema_type)

        if isinstance(data, pd.DataFrame):
            # Pandas saves as a single file with an extension
            final_path = base_path / f"{data_name}.parquet"
            is_directory = False
        elif isinstance(data, SparkDataFrame):
            # Spark saves as a directory with no extension
            final_path = base_path / data_name
            is_directory = True
        else:
            raise TypeError(f"Unsupported data type for save_table: {type(data)}")

        logging.info(f"Saving '{data_name}' to local directory: {final_path}")

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir) / data_name
            self._write_parquet(data, str(temp_path))

            # Atomically move the result to the final destination
            if final_path.exists():
                if is_directory:
                    shutil.rmtree(final_path)
                else:
                    final_path.unlink()
            shutil.move(str(temp_path), str(final_path))

    def drop_table(
        self,
        data_name: str,
        schema_type: str,
    ) -> None:
        """
        Removes a table from a specific schema type (e.g., 'working', 'historical').

        This method handles both single-file Parquet tables (typically created by
        pandas) and directory-based tables (created by Spark).
        """
        base_path = self._get_base_path(schema_type)

        # A table could be a single file or a directory, so we check for both.
        path_as_file = base_path / f"{data_name}.parquet"
        path_as_dir = base_path / data_name

        if path_as_dir.is_dir():
            logging.info(f"Dropping table directory: {path_as_dir}")
            shutil.rmtree(path_as_dir)
        elif path_as_file.is_file():
            logging.info(f"Dropping table file: {path_as_file}")
            path_as_file.unlink()
        else:
            logging.warning(f"Could not drop table '{data_name}' in schema '{schema_type}': " "Path not found.")

    def archive_table(self, data: Union[pd.DataFrame, SparkDataFrame], data_name: str) -> None:
        """Archives a DataFrame to the 'historical' path as a Delta table in append mode."""
        archive_path = str(self.historical_path / data_name)
        logging.info(f"Archiving '{data_name}' to local historical Delta table: {archive_path}")

        spark_df = self._ensure_spark_df(data)
        df_to_write = self._align_schema_for_append(spark_df, archive_path)
        df_to_write.write.format("delta").mode("append").option("mergeSchema", "true").save(archive_path)

    def save_file(self, writer_func: Callable[[str], None], file_name: str, project_name: str) -> None:
        """Saves an unstructured file to a project-specific output directory."""
        output_dir = self.output_path / project_name
        output_dir.mkdir(exist_ok=True)
        final_path = output_dir / file_name

        logging.info(f"Saving file '{file_name}' to local output directory: {final_path}")
        writer_func(str(final_path))

    # --- Methods for the Quality Module ---

    def load_historical_metrics(self, data_name: str, metrics_to_check: List[Dict]) -> Dict[str, float]:
        """Loads historical metrics from the local quality Delta table."""
        metrics_path = self.quality_path / self.metrics_table_name
        try:
            metrics_df = self.spark.read.format("delta").load(str(metrics_path))
        except AnalysisException:
            return {}

        metric_ids = [self._get_metric_id(conf) for conf in metrics_to_check]
        baselines_df = (
            metrics_df.where((F.col("data_name") == data_name) & (F.col("metric_id").isin(metric_ids)))
            .groupBy("metric_id")
            .agg(F.avg("metric_value").alias("baseline_value"))
        )
        return {row["metric_id"]: row["baseline_value"] for row in baselines_df.collect()}

    def stage_run_metrics(self, metrics_to_stage: List[Dict]) -> None:
        """Saves metrics to a local Parquet staging directory."""
        staging_path = str(self.quality_path / self.metrics_staging_table_name)
        staging_df = self.spark.createDataFrame(metrics_to_stage)
        staging_df = self._align_schema_for_append(staging_df, staging_path)
        staging_df.write.mode("append").parquet(staging_path)

    def commit_staged_metrics(self, run_id: str) -> None:
        """Reads from local staging Parquet, filters, and persists to the final Delta table."""
        staging_path = str(self.quality_path / self.metrics_staging_table_name)
        try:
            staged_metrics_df = self.spark.read.option("mergeSchema", "true").parquet(staging_path)
            metrics_for_this_run = staged_metrics_df.filter(F.col("run_id") == run_id).collect()
            if metrics_for_this_run:
                metrics_to_persist = [row.asDict() for row in metrics_for_this_run]
                self._persist_run_metrics(metrics_to_persist)
        except Exception as e:
            logging.warning(f"Could not commit metrics from staging area '{staging_path}': {e}")

    def _load_check_results(self) -> SparkDataFrame:
        """Load the check results table."""
        log_path = str(self.quality_path / self.log_table_name)
        return self.spark.read.format("delta").load(log_path)

    def log_check_results(self, results: List["CheckResult"], run_context: Dict, data_name: str) -> None:
        """Logs the outcomes of all quality checks to the local quality Delta table."""
        log_path = str(self.quality_path / self.log_table_name)
        log_entries = [self._format_result_for_logging(res, run_context, data_name) for res in results]
        results_df = self.spark.createDataFrame(log_entries)
        results_df = self._align_schema_for_append(results_df, log_path)
        results_df.write.format("delta").mode("append").option("mergeSchema", "true").save(log_path)

    def clear_quality_tables(self) -> None:
        """Deletes the local quality directories to ensure a clean slate."""
        for name in [
            self.metrics_table_name,
            self.log_table_name,
            self.metrics_staging_table_name,
        ]:
            path = self.quality_path / name
            if path.exists():
                shutil.rmtree(path)
                logging.info(f"Removed old local quality directory: {path}")

    # --- Private Helper Methods ---

    def _get_base_path(self, schema_type: Optional[str]) -> Path:
        if schema_type == "working":
            base_path = self.working_path
        elif schema_type == "historical":
            base_path = self.historical_path
        elif schema_type == "quality":
            base_path = self.quality_path
        else:
            raise ValueError(f"Unsupported schema_type: {schema_type}")
        return base_path

    def _get_table_schema(self, table: str):
        """Return the schema of a table."""
        try:
            return self.spark.read.format("delta").load(table).schema
        except AnalysisException as e:
            if "[DELTA_TABLE_NOT_FOUND]" in str(e) or "[PATH_NOT_FOUND]" in str(e):
                raise TableNotFoundError(f"Table at path '{table}' not found.") from e
            raise  # Re-raise other AnalysisExceptions

    def _write_parquet(self, data: Union[pd.DataFrame, SparkDataFrame], path: str):
        """Helper to write either a pandas or Spark DF to Parquet."""
        if isinstance(data, SparkDataFrame):
            data.write.mode("overwrite").parquet(path)
        elif isinstance(data, pd.DataFrame):
            data.to_parquet(path, index=False)

    def _persist_run_metrics(self, current_metrics: List[Dict]) -> None:
        """Appends metrics from a successful run to the local quality Delta table."""
        metrics_path = str(self.quality_path / self.metrics_table_name)
        metrics_df = self.spark.createDataFrame(current_metrics)
        metrics_df = self._align_schema_for_append(metrics_df, metrics_path)
        metrics_df.write.format("delta").mode("append").option("mergeSchema", "true").save(metrics_path)
