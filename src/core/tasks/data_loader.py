import logging
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

import pandas as pd
from pandas.core.api import DataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from ch_utils.common.string_utils import snake_to_camel
from core.task import Task


class DataSourcePaths:
    """
    Discovers the file paths for a given table name from various sources.

    This class decouples the DataLoader from the physical layout of the file
    system by searching for corresponding .sql, .csv, or .xlsx files.
    """

    def __init__(self, table_name: str):
        """Initializes the DataSourcePaths instance."""
        self.table_name = table_name
        self.snowflake_sql_path: Optional[Path] = self._find_snowflake_source()
        self.local_file_path: Optional[Path] = self._find_local_source()

    def _find_snowflake_source(self) -> Optional[Path]:
        """
        Searches for a .sql file in the SQL_PATH directory.

        FIXME: only look in shared and the project path, preferring the project path
        """
        sql_path = Path(os.environ.get("SQL_PATH", "sql"))
        return self._recursive_search(sql_path, ".sql")

    def _find_local_source(self) -> Optional[Path]:
        """Searches for a .csv or .xlsx file in the DATA_PATH directory."""
        data_path = Path(os.environ.get("DATA_PATH", "data"))
        return self._recursive_search(data_path, [".csv", ".xlsx"])

    def _recursive_search(self, base_path: Path, valid_extensions: Union[str, List[str]]) -> Optional[Path]:
        """Recursively searches for a file matching the table name, raising an error if multiple files are found."""
        if isinstance(valid_extensions, str):
            valid_extensions = [valid_extensions]

        matching_files = [
            file
            for file in base_path.rglob(f"{self.table_name}.*")
            if file.is_file() and file.suffix in valid_extensions and "archive" not in str(file.resolve()).lower()
        ]

        if not matching_files:
            return None
        elif len(matching_files) == 1:
            return matching_files[0]
        else:
            raise ValueError(f"Multiple matching files found for '{self.table_name}' in {base_path}: {matching_files}")


class DataLoader(Task):
    """
    A specialized Task that loads data from an external source.

    This Task has no data inputs and produces a single table as its output.
    It follows a configurable, prioritized strategy to find and load the data.
    By default, the priority is:
    0. datastore: Looks for data store source specified in the job config.
    1. local_file: Looks for a local .csv/.xlsx file.
    2. snowflake: Checks for a .sql file to run against Snowflake (if on Databricks).
    3. unity_catalog: Falls back to loading directly from Unity Catalog.

    This priority can be overridden by providing a 'data_loader_priority' list in the config.
    """

    table_name: str
    default_priority: List[str] = ["datastore", "local_file", "snowflake", "unity_catalog"]

    def __init__(self, *args, **kwargs):
        """Initializes the DataLoader instance."""
        super().__init__(*args, **kwargs)
        self._sources = DataSourcePaths(self.table_name)

        # Set loading priority based on config
        self._loaders = self._get_loaders_ordered()

    def _get_loaders_ordered(self) -> Dict[str, Callable]:
        # 1. Define all available loader mapping (unordered)
        available_loaders = {
            "datastore": self._try_load_datastore,
            "local_file": self._try_load_local_file,
            "snowflake": self._try_load_snowflake,
            "unity_catalog": self._try_load_unity_catalog,
        }

        # 2. Fetch custom priority from config
        priority_config = self.config.get("data_loader_priority", self.default_priority)

        # 3. Resolve the priority list based on the YAML structure
        if isinstance(priority_config, list):
            # YAML is a flat list
            priority_list = priority_config

        elif isinstance(priority_config, dict):
            # YAML is a dictionary (check for table-specific, fallback to default)
            if self.table_name in priority_config:
                priority_list = priority_config[self.table_name]
            else:
                priority_list = priority_config.get("default", self.default_priority)

        else:
            logging.warning("Invalid 'data_loader_priority' format in config. Falling back to default.")
            priority_list = self.default_priority

        # 4. Build the final ordered dictionary
        new_order = {}
        for source in priority_list:
            if source not in available_loaders:
                logging.warning(f"Unknown data loader source in config: '{source}'. Skipping.")
                continue

            # Python 3.7+ guarantees dictionaries maintain insertion order
            new_order[source] = available_loaders[source]

        logging.info(f"Using data loading priority for '{self.table_name}': {list(new_order.keys())}")

        return new_order

    def run_content(self) -> Dict[str, Union[DataFrame, SparkDataFrame]]:
        """Executes the data loading logic based on the prioritized strategy."""
        for source, func in self._loaders.items():
            # Execute the mapped loader method
            output = func()

            # If the method successfully loaded data, return it and break the loop
            if output is not None:
                logging.info(f"Successfully loaded '{self.table_name}' via '{source}'.")
                return {self.table_name: output}

        # If the loop exhausts the list without returning, fail loudly
        raise RuntimeError(
            f"Could not load data for '{self.table_name}'. "
            f"Exhausted the priority list: {list(self._loaders.keys())}"
        )

    # --- Individual Strategy Methods ---

    def _try_load_datastore(self) -> Optional[SparkDataFrame]:
        data_override = self.config.get("load_external_data", {})
        if self.on_databricks and (data_path := data_override.get(self.table_name)):
            logging.info(f"Manual override found for {self.table_name}: {data_path}")
            return self._load_from_datastore(str(data_path))
        return None

    def _try_load_local_file(self) -> Optional[Union[DataFrame, SparkDataFrame]]:
        if self._sources.local_file_path:
            file_path = self._sources.local_file_path
            logging.info(f"Loading from local file override: {file_path}")
            if file_path.suffix == ".csv":
                return pd.read_csv(file_path)
            elif file_path.suffix == ".xlsx":
                return pd.read_excel(file_path)
            else:
                raise NotImplementedError(f"Unsupported local file type: {file_path.suffix}")
        return None

    def _try_load_snowflake(self) -> Optional[SparkDataFrame]:
        if self._sources.snowflake_sql_path:
            logging.info(f"Loading from Snowflake using template: {self._sources.snowflake_sql_path}")
            params = self.config.get("snowflake_params", {})
            return self.snowflake_manager.load_from_rendered_sql_template(
                template_path=self._sources.snowflake_sql_path,
                params=params,
                table_type=SparkDataFrame,
            )
        return None

    def _try_load_unity_catalog(self) -> Optional[SparkDataFrame]:
        logging.info(f"Loading from Unity Catalog table: {self.table_name}")
        return self.data_manager.active_manager.load_table(
            self.table_name,
            table_type=SparkDataFrame,
            schema_type="working",
        )

    # --- Helper Methods ---

    def _load_from_datastore(self, path_str: str) -> SparkDataFrame:
        # Validate that the path is actually an external path
        valid_prefixes = ("/mnt/", "abfss://")
        if not path_str.startswith(valid_prefixes):
            raise ValueError(
                f"Invalid external data path: '{path_str}'. Expected path to start with one of {valid_prefixes}."
            )

        logging.info(f"Loading from datastore: {path_str}")
        try:
            self.data_manager.active_manager.update_spark_config_for_datastore()
            output = self.data_manager.active_manager.spark.read.format("delta").load(path_str)
            return output
        except Exception as e:
            # Fail loudly with a highly descriptive error and preserve the original traceback
            error_msg = (
                f"Failed to load external Delta data at '{path_str}'. "
                f"Please verify that the path exists, it is a valid Delta format, "
                f"and that your Azure credentials have read access."
            )
            logging.error(f"{error_msg} Original error: {str(e)}")
            raise RuntimeError(error_msg) from e

    # --- Class Methods ---

    @classmethod
    def create_data_loader_task(cls, table_name: str):
        """A factory method to dynamically configure the DataLoader class."""
        cls.table_name = table_name
        cls.task_name = "Load" + snake_to_camel(table_name, is_class=True)
        cls.task_type = "data_loader"
        cls.task_params = {}
        cls.task_returns = [table_name]
