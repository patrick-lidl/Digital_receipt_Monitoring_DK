from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type, Union

from jinja2 import Template
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.utils import AnalysisException

from .base import BaseDataManager, TableNotFoundError

if TYPE_CHECKING:
    from core.config_manager import Config
    from core.quality.checks.base import CheckResult


class UnityCatalogManager(BaseDataManager):
    """
    Implements the data manager contract for Databricks Unity Catalog.

    This class provides a high-level interface for interacting with UC tables
    and volumes, including setup, I/O for business data, and I/O for the
    data quality module's operational metadata.
    """

    def __init__(self, config: Optional["Config"]):
        """
        Initializes the UnityCatalogManager.

        Loads configuration and computes the names for the catalog, schemas,
        and volumes based on the deployment stage.

        Args:
            config (Optional["Config"]): The 'unity_catalog' section of the
                storage.yml configuration.
        """
        super().__init__(config)
        if self.config:
            self.catalog = Template(self.config["catalog"]).render(
                deploymentStageShort=os.environ.get("DEPLOYMENT_STAGE_SHORT") or "e"
            )
            self.schemas = self.config.get("schemas", {})
            self.quality_schema = self.schemas.get("quality")
            self.volumes = self.config.get("volumes", {})

    def setup(self) -> None:
        """
        Ensures that the configured catalog, schemas, and volumes exist in UC.

        This method is idempotent and can be called safely on every run to
        prepare the necessary infrastructure in the Databricks environment.
        """
        if not self.is_configured or not self.on_databricks:
            return

        logging.info("Setting up Unity Catalog objects...")  # FIXME: why is this called twice in FlagCashiers?
        for schema_name in self.schemas.values():
            self._create_schema(schema_name)

        for volume_name in self.volumes.values():
            historical_schema = self.schemas.get("historical", "default")
            self._create_volume(volume_name, historical_schema)

    def load_table(
        self, data_name: str, table_type: Type, schema_type: str
    ) -> Optional[Union[PandasDataFrame, SparkDataFrame]]:
        """
        Loads a table from the specified schema in Unity Catalog.

        Args:
            data_name (str): The name of the table to load.
            table_type (Type): The desired type of the loaded table (pandas or Spark).
            schema_type (str): The logical name of the schema ('working' or 'historical').

        Returns:
            The loaded DataFrame, or None if the table is not found.
        """
        schema = self.schemas.get(schema_type)
        if not self.is_configured or not schema:
            return None

        full_table_name = f"`{self.catalog}`.`{schema}`.`{data_name}`"
        try:
            logging.info(f"Loading table from Unity Catalog: {full_table_name}")
            output = self.spark.table(full_table_name)
            return output.toPandas() if table_type == PandasDataFrame else output
        except AnalysisException:
            logging.info(f"Table '{full_table_name}' not found in Unity Catalog.")
            return None

    def save_table(
        self,
        data: Union[PandasDataFrame, SparkDataFrame],
        data_name: str,
        schema_type: Optional[str] = "working",
    ) -> None:
        """Saves a structured table to a specific schema type, overwriting if it exists."""
        selected_schema = self.schemas.get(schema_type)
        if not self.is_configured or not selected_schema:
            raise ValueError(f"Schema ('{selected_schema}') is not configured in storage.yml.")

        full_table_name = f"`{self.catalog}`.`{selected_schema}`.`{data_name}`"
        spark_df = self._ensure_spark_df(data)
        logging.info(f"Overwriting table: {full_table_name}")
        spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    def drop_table(
        self,
        data_name: str,
        schema_type: str,
    ) -> None:
        """
        Removes a table from a specific schema in Unity Catalog.

        This method uses a 'DROP TABLE IF EXISTS' command to ensure it runs
        without errors even if the table is already gone.
        """
        schema = self.schemas.get(schema_type)
        if not self.is_configured or not schema:
            logging.warning(f"Cannot drop table '{data_name}'. Schema type '{schema_type}' is not configured.")
            return

        full_table_name = f"`{self.catalog}`.`{schema}`.`{data_name}`"

        try:
            logging.info(f"Dropping Unity Catalog table: {full_table_name}")
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception as e:
            # Log error but don't fail the entire process
            logging.error(f"Failed to drop table {full_table_name}: {e}")

    def archive_table(self, data: Union[PandasDataFrame, SparkDataFrame], data_name: str) -> None:
        """Archives a DataFrame to the 'historical' schema in append mode."""
        historical_schema = self.schemas.get("historical")
        if not self.is_configured or not historical_schema:
            raise ValueError("Historical schema ('historical') is not configured in storage.yml.")

        full_table_name = f"`{self.catalog}`.`{historical_schema}`.`{data_name}`"
        spark_df = self._ensure_spark_df(data)
        logging.info(f"Appending to historical table: {full_table_name}")
        spark_df = self._align_schema_for_append(spark_df, full_table_name)
        spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)

    def save_file(self, writer_func: Callable[[str], None], file_name: str, project_name: str) -> None:
        """Saves a file to a project-specific Volume in the historical schema."""
        volume_name = self.volumes.get(project_name)
        historical_schema = self.schemas.get("historical")
        if not self.is_configured or not volume_name or not historical_schema:
            raise ValueError(f"Volume for project '{project_name}' is not configured in storage.yml.")

        volume_path = Path("/Volumes") / self.catalog / historical_schema / volume_name / file_name
        dbfs_volume_path = f"dbfs:{volume_path}"

        with tempfile.NamedTemporaryFile(suffix=volume_path.suffix, delete=False) as tmp:
            local_path = tmp.name
            writer_func(local_path)
            tmp.flush()

        try:
            self.dbutils.fs.cp(f"file:{local_path}", dbfs_volume_path, recurse=True)
            logging.info(f"File successfully written to volume: {volume_path}")
        finally:
            os.remove(local_path)

    # --- Methods for the Quality Module ---

    def load_historical_metrics(self, data_name: str, metrics_to_check: List[Dict]) -> Dict[str, float]:
        """Loads historical metrics from the quality schema."""
        full_table_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.metrics_table_name}`"

        try:
            metrics_df = self.spark.table(full_table_name)
        except AnalysisException:
            logging.warning(f"Metrics table '{full_table_name}' not found. No historical data to compare.")
            return {}

        metric_ids = [self._get_metric_id(conf) for conf in metrics_to_check]
        baselines_df = (
            metrics_df.where((F.col("data_name") == data_name) & (F.col("metric_id").isin(metric_ids)))
            .groupBy("metric_id")
            .agg(F.avg("metric_value").alias("baseline_value"))
        )
        baselines = baselines_df.collect()
        return {row["metric_id"]: row["baseline_value"] for row in baselines}

    def stage_run_metrics(self, metrics_to_stage: List[Dict]) -> None:
        """Saves calculated metrics to a transient Unity Catalog staging table."""
        full_table_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.metrics_staging_table_name}`"
        logging.info(f"Staging {len(metrics_to_stage)} metrics to UC table: {full_table_name}")

        staging_df = self.spark.createDataFrame(metrics_to_stage)
        # Append the new metrics for this run to the staging table
        staging_df = self._align_schema_for_append(staging_df, full_table_name)
        staging_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)

    def commit_staged_metrics(self, run_id: str) -> None:
        """
        Commits the stage metrics to the persistent table.

        Reads metrics for a run_id from the staging table, appends them to the
        permanent store, and then removes them from the staging table.
        """
        full_staging_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.metrics_staging_table_name}`"
        full_permanent_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.metrics_table_name}`"

        try:
            staged_df = self.spark.table(full_staging_name)
        except AnalysisException:
            logging.warning(f"Metrics staging table '{full_staging_name}' not found. No metrics to commit.")
            return

        # Isolate the metrics for the current run and those that remain
        metrics_to_commit = staged_df.where(f"run_id = '{run_id}'")
        remaining_metrics = staged_df.where(f"run_id != '{run_id}'")

        # Cache the dataframe to avoid re-computation
        metrics_to_commit.cache()

        if metrics_to_commit.count() > 0:
            logging.info(f"Committing {metrics_to_commit.count()} metrics from staging to {full_permanent_name}")

            metrics_to_persist = [row.asDict() for row in metrics_to_commit.collect()]
            self._persist_run_metrics(metrics_to_persist)

            # Clean up by overwriting the staging table with the remaining metrics
            logging.info(f"Cleaning up committed metrics for run_id '{run_id}' from staging table.")
            remaining_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                full_staging_name
            )
        else:
            logging.info(f"No metrics found in staging for run_id '{run_id}'.")

        metrics_to_commit.unpersist()

    def _load_check_results(self) -> SparkDataFrame:
        """Load the check results table."""
        full_table_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.log_table_name}`"
        return self.spark.table(full_table_name)

    def log_check_results(self, results: List["CheckResult"], run_context: Dict, data_name: str) -> None:
        """Logs the outcomes of all quality checks to the quality schema."""
        full_table_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.log_table_name}`"

        log_entries = [self._format_result_for_logging(res, run_context, data_name) for res in results]
        results_df = self.spark.createDataFrame(log_entries)
        results_df = self._align_schema_for_append(results_df, full_table_name)
        results_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)
        logging.info(f"Logged {results_df.count()} check results to '{full_table_name}'.")

    def clear_quality_tables(self) -> None:
        """
        Drops the quality-related tables from Unity Catalog to ensure a clean slate.

        This method is idempotent and will not fail if the tables do not exist.
        """
        for table_name in [
            self.metrics_table_name,
            self.log_table_name,
            self.metrics_staging_table_name,
        ]:
            # Construct the full three-level table name
            full_table_name = f"`{self.catalog}`.`{self.quality_schema}`.`{table_name}`"
            logging.info(f"Dropping UC table to clear it: {full_table_name}")
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            except Exception as e:
                # Log error but don't fail the whole process
                logging.error(f"Failed to drop table {full_table_name}: {e}")

    # --- Private Helper Methods ---

    def _get_table_schema(self, table: str):
        """Return the schema of a table."""
        try:
            return self.spark.table(table).schema
        except AnalysisException as e:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(e):
                raise TableNotFoundError(f"Table or view '{table}' not found in catalog.") from e
            raise  # Re-raise other AnalysisExceptions

    def _create_schema(self, schema: str):
        """Creates a Unity Catalog schema if it does not exist."""
        full_schema_name = f"`{self.catalog}`.`{schema}`"
        logging.debug(f"Ensuring schema exists: {full_schema_name}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}")

    def _create_volume(self, volume: str, schema: str):
        """Creates a Unity Catalog volume if it does not exist."""
        full_volume_name = f"`{self.catalog}`.`{schema}`.`{volume}`"
        logging.debug(f"Ensuring volume exists: {full_volume_name}")
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_volume_name}")

    def _persist_run_metrics(self, current_metrics: List[Dict]) -> None:
        """Appends metrics to the permanent historical metrics store in UC."""
        if not current_metrics:
            return

        full_table_name = f"`{self.catalog}`.`{self.quality_schema}`.`{self.metrics_table_name}`"
        logging.info(f"Persisting {len(current_metrics)} metrics to UC table: {full_table_name}")

        metrics_df = self.spark.createDataFrame(current_metrics)
        metrics_df = self._align_schema_for_append(metrics_df, full_table_name)
        metrics_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)
