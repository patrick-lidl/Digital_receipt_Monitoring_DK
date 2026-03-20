from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type, Union

from ch_utils.data.general import add_static_columns
from ch_utils.io.io_utils import save_json_to_path
from core.config_manager import Config
from core.databricks_handler import DatabricksHandler

from .base import TableNotFoundError
from .filesystem_manager import FilesystemManager
from .unity_catalog_manager import UnityCatalogManager

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame
    from pyspark.sql.dataframe import DataFrame as SparkDataFrame

    from core.quality.checks.base import CheckResult


class DataManager(DatabricksHandler):
    """
    Orchestrates all data I/O, acting as a facade over backend managers.

    This class is the single entry point for data operations. It holds instances
    of backend managers for Unity Catalog, the local filesystem, and Snowflake,
    and delegates calls to the appropriate manager based on the environment.
    """

    def __init__(self, config: "Config"):
        """Initializes the DataManager without creating backend managers."""
        super().__init__()
        self._data_mgmt_configs = config
        self._uc_config: Optional["Config"] = None
        self._fs_config: Optional["Config"] = None

    @property
    def uc_manager(self) -> UnityCatalogManager:
        """Lazily initializes and returns the UnityCatalogManager instance."""
        if not hasattr(self, "_uc_manager"):
            logging.debug("Lazily initializing UnityCatalogManager...")
            if self._uc_config is None:
                # Prepare config on first access
                self._uc_config = self._data_mgmt_configs.get("unity_catalog") or Config()
                self._uc_config["quality"] = self._data_mgmt_configs.get("quality") or Config()
            self._uc_manager = UnityCatalogManager(self._uc_config)
            self._uc_manager.setup()
        return self._uc_manager

    @property
    def fs_manager(self) -> FilesystemManager:
        """Lazily initializes and returns the FilesystemManager instance."""
        if not hasattr(self, "_fs_manager"):
            logging.debug("Lazily initializing FilesystemManager...")
            if self._fs_config is None:
                # Prepare config on first access
                self._fs_config = self._data_mgmt_configs.get("filesystem") or Config()
                self._fs_config["quality"] = self._data_mgmt_configs.get("quality") or Config()
            self._fs_manager = FilesystemManager(self._fs_config)
            self._fs_manager.setup()
        return self._fs_manager

    @property
    def active_manager(self) -> Union[UnityCatalogManager, FilesystemManager]:
        """Lazily determines and returns the primary active manager."""
        if self.on_databricks:
            return self.uc_manager
        return self.fs_manager

    # --- Public Methods ---

    def load_table(
        self, data_name: str, table_type: Type, schema_type: str = "working"
    ) -> Union["PandasDataFrame", "SparkDataFrame"]:
        """Loads a table with a 'UC first, then filesystem' strategy."""
        if self.on_databricks:
            data = self.uc_manager.load_table(data_name, table_type, schema_type)
            if data is not None:
                logging.info(f"Loaded '{data_name}' from Unity Catalog.")
                return data

        data = self.fs_manager.load_table(data_name, table_type, schema_type)
        if data is not None:
            logging.info(f"Loaded '{data_name}' from local filesystem.")
            return data

        raise TableNotFoundError(f"Could not find data '{data_name}' in any configured location.")

    def save_table(
        self, data: Union["PandasDataFrame", "SparkDataFrame"], data_name: str, schema_type: Optional[str] = "working"
    ) -> None:
        """Saves a structured table to a specific schema type, overwriting if it exists."""
        self.active_manager.save_table(data, data_name, schema_type)

    def drop_table(
        self,
        data_name: str,
        schema_type: str,
    ) -> None:
        """Remove table from a specific schema type (e.g., 'working', 'historical')."""
        self.active_manager.drop_table(data_name, schema_type)

    def archive_table(
        self,
        data: Union["PandasDataFrame", "SparkDataFrame"],
        data_name: str,
        run_id: str,
        run_timestamp: str,
    ) -> None:
        """
        Archives a table to the 'historical' area.

        Archives a table to the 'historical' area after adding run context columns,
        then delegates to the correct manager.
        """
        logging.info(f"Adding run context and archiving '{data_name}'...")

        # FIXME: what if columns `run_id` and `run_timestamp` already exist?
        # Define the columns and their static values in a dictionary
        columns_to_add = {
            "run_id": int(run_id),
            "run_timestamp": str(run_timestamp),
        }

        # Use the helper to augment the DataFrame
        data_augm = add_static_columns(data, columns_to_add)

        # Delegate the save operation to the correct backend manager
        self.active_manager.archive_table(data_augm, data_name)

    def save_file(self, writer_func: Callable[[str], None], file_name: str, project_name: str = "default") -> None:
        """
        Saves an unstructured file (e.g., JSON, HTML), delegating to the correct manager.

        Args:
            writer_func (Callable[[str], None]): A function that takes a file path
                and writes the desired content to it.
            file_name (str): The name of the file to be saved.
            project_name (str): The logical project name, used to determine the
                correct output volume or directory.
        """
        self.active_manager.save_file(writer_func, file_name, project_name)

    def save_json(
        self,
        output: Dict,
        file_name: str,
        project_name: str = "default",
        print_pretty: bool = True,
    ) -> None:
        """A convenience wrapper to save a dictionary as a JSON file."""

        def writer(path: str) -> None:
            save_json_to_path(output, path, print_pretty=print_pretty)

        self.save_file(writer_func=writer, file_name=file_name, project_name=project_name)

    # --- Methods for the Quality Module ---

    def load_historical_metrics(self, data_name: str, metrics_to_check: List[Dict]) -> Dict[str, float]:
        """Loads historical metrics, delegating to the correct manager."""
        return self.active_manager.load_historical_metrics(data_name, metrics_to_check)

    def stage_run_metrics(self, metrics_to_stage: List[Dict]) -> None:
        """Delegates staging of metrics to the correct manager."""
        self.active_manager.stage_run_metrics(metrics_to_stage)

    def commit_staged_metrics(self, run_id: str) -> None:
        """Delegates committing of staged metrics to the correct manager."""
        self.active_manager.commit_staged_metrics(run_id)

    def load_check_results(self, run_id: Optional[str] = None) -> SparkDataFrame:
        """Load the check results table, filtering by run_id, if passed."""
        return self.active_manager.load_check_results(run_id)

    def log_check_results(self, results: List["CheckResult"], run_context: Dict, data_name: str) -> None:
        """Logs check results, delegating to the correct manager."""
        self.active_manager.log_check_results(results, run_context, data_name)

    def clear_quality_tables(self) -> None:
        """Delegates the clearing of quality tables to the correct manager."""
        self.active_manager.clear_quality_tables()
