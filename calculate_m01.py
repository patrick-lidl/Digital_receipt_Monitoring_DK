import logging
from typing import Dict, List

from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.task import Task


class JobPostProcessing(Task):
    """
    A final task that runs after a successful job.

    Persists metrics, configs, and archive final output tables.
    """

    task_returns: List[str] = []
    # This ensures the task runs last. The deployer will wire up the dependencies.
    forced_dependencies: List[str] = [
        "FlagCashiers",
        "ExportReceiptMonitoringJson",
        "CreateWeeklyReport",
    ]

    def run_content(self) -> Dict:
        """Orchestrates all post-run activities by calling the DataManager."""
        logging.info("--- Running Job Post-Processing ---")

        if self.config.get("persist_job_run"):
            run_id = str(self._run_state.run_id)  # FIXME: this should be consistent across the whole repo

            # 1. Commit Staged Metrics
            self._persist_metrics(run_id)

            # 2. Archive Final Output Tables
            self._archive_output_tables(run_id)

            # 3. Archive Job Configuration
            self._archive_job_configuration()

        logging.info("--- Job Post-Processing Complete ---")
        return {}

    def _persist_metrics(self, run_id: str) -> None:
        logging.info(f"Committing metrics for run_id: {run_id}")
        self.data_manager.commit_staged_metrics(run_id)

    def _archive_output_tables(self, run_id: str) -> None:
        tables_to_archive = self.config.get("tables_to_archive", [])
        logging.info(f"Archiving final tables: {tables_to_archive}")
        for table_name in tables_to_archive:
            try:
                # Load from the 'working' area and archive to the 'historical' area
                df_to_archive = self.data_manager.load_table(table_name, SparkDataFrame, schema_type="working")
                self.data_manager.archive_table(
                    df_to_archive,
                    table_name,
                    run_id,
                    self._run_state.run_timestamp,  # FIXME: this is task-specific...
                )
            except Exception as e:
                logging.error(f"Failed to archive table '{table_name}': {e}")

    def _archive_job_configuration(self):
        job_name = "digital_receipt_monitoring"  # FIXME
        timestamp = self._run_state.run_timestamp.split("T")[0]
        config_file_name = f"{timestamp}_{job_name}_config.json"
        logging.info(f"Archiving job configuration to {config_file_name}")
        self.data_manager.save_json(
            output=self.config,
            file_name=config_file_name,
            project_name=job_name,
        )
