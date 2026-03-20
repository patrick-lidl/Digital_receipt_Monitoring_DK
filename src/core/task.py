import inspect
import logging
import logging.config
import re
import time
from abc import ABC, ABCMeta, abstractmethod
from typing import Dict, List, Optional, Type, Union

from pandas.core.api import DataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.config_manager import Config, ConfigManager
from core.data_manager import DataManager
from core.databricks_handler import DatabricksHandler
from core.quality.validator import DataQualityValidator
from core.run_state import RunState
from core.snowflake_manager import SnowflakeManager


class TaskMeta(ABCMeta):
    """
    A metaclass that automatically assigns the class name to a `task_name` attribute.

    This allows the dependency graph to be constructed using mock tasks,
    as the task name is available at the class level without needing to
    instantiate the object.
    """

    task_name: str

    def __new__(cls, name, bases, dct):
        """Create a new class and assign its name to the `task_name` attribute."""
        new_class = super().__new__(cls, name, bases, dct)
        new_class.task_name = name
        return new_class


class Task(DatabricksHandler, ABC, metaclass=TaskMeta):
    """
    An abstract base class for executing a single step in an ETL or data processing job.

    This class provides a standardized framework for handling configuration,
    managing data I/O (loading inputs and saving outputs), and wrapping the core
    business logic. Subclasses must implement the `run_content` method, which
    contains the specific data transformations. The framework automatically
    handles Spark/Pandas DataFrame conversions and saving data to configured
    storage locations like Unity Catalog, Snowflake, or a local filesystem.

    Attributes:
        task_name (str): The name of the task, automatically set to the class name.
        task_type (str): The type of the task, e.g., "standard". Used by the Job orchestrator.
        task_params (Optional[Dict[str, Type]]): A dictionary mapping input data names
            to their expected types (e.g., SparkDataFrame, DataFrame). If not set,
            it's inferred from the `run_content` signature.
        task_returns (List[str]): A list of output data names produced by the task.
            Inferred from the `run_content` return statement if not set.
        forced_dependencies (List[str]): A list of task names that must run before this task,
            even if not an explicit data dependency.
        extra_configs_to_load (List[str]): A list of additional configuration file names to load.
    """

    # --- Class attributes for Job orchestration ---
    task_name: str
    task_type: str = "standard"
    task_params: Optional[Dict[str, Type]] = None
    task_returns: List[str] = []
    forced_dependencies: List[str] = []

    # --- Class attributes for internal configuration ---
    # Used by ConfigManager
    extra_configs_to_load: List[str] = []

    def __init__(
        self,
        config: Optional[Config] = None,
        data_manager: Optional[DataManager] = None,
        snowflake_manager: Optional[SnowflakeManager] = None,
    ):
        """Initializes the Task instance."""
        super().__init__()

        # Store for lazy loading
        self._config = config
        self._data_manager = data_manager
        self._snowflake_manager = snowflake_manager

        # It will be injected when run() is called.
        self._run_state: Optional[RunState] = None

        # Internal helper for local fallback
        self._config_manager: Optional[ConfigManager] = None

        if self.task_params is None:
            self.task_params = self._get_task_inputs()

        self._run_timestamp: Optional[str] = None
        self.quality_validator: Optional[DataQualityValidator] = None

    @property
    def config(self) -> Config:
        """Returns the config of the task."""
        if self._config is None:
            logging.info("Loading the default task config.")
            self._config = self._local_config_manager.build_task_config(self.__class__)
        return self._config

    @property
    def data_manager(self) -> DataManager:
        """
        Returns the DataManager instance.

        Lazy-loads a default instance if one was not injected.
        """
        if self._data_manager is None:
            # Default behavior: Create a standard DataManager
            self._data_manager = DataManager()
        return self._data_manager

    @property
    def snowflake_manager(self) -> SnowflakeManager:
        """
        Returns the SnowflakeManager instance.

        Lazy-loads using the global snowflake.yml if one was not injected.
        """
        if self._snowflake_manager is None:
            # Default behavior: Load config and create client
            logging.info("Lazy-loading SnowflakeManager...")
            # We use the ConfigManager to get the repo-level infra config
            sf_config = self._local_config_manager.get_snowflake_config()
            self._snowflake_manager = SnowflakeManager(sf_config)
        return self._snowflake_manager

    @property
    def _local_config_manager(self) -> ConfigManager:
        """Returns a ConfigManager for local/fallback usage."""
        if self._config_manager is None:
            self._config_manager = ConfigManager()
        return self._config_manager

    @abstractmethod
    def run_content(self, *args, **kwargs) -> Dict:
        """
        Executes the main business logic of the task.

        This method must be implemented by all subclasses. It receives input data
        as keyword arguments and should return a dictionary where keys are the names
        of the output datasets and values are the corresponding DataFrames.
        """

    @classmethod
    def _get_task_inputs(cls) -> Dict[str, Type]:
        """
        Infers task input parameters from the `run_content` method signature.

        Returns:
            Dict[str, Type]: A dictionary mapping parameter names to their type annotations.
        """
        return {
            param: value.annotation
            for param, value in inspect.signature(cls.run_content).parameters.items()
            if param != "self"
        }

    @classmethod
    def _get_task_outputs(cls) -> List[str]:
        """
        Infers task output names by inspecting the `run_content` source code.

        This method parses the return statement of `run_content` to find the
        keys of the returned dictionary.

        Returns:
            List[str]: A list of strings representing the output table names.
        """
        out_source = inspect.getsource(cls.run_content)

        if match := re.search(r"return\s*{([^}]*)}", out_source):
            return re.findall(r'"(\w+)"\s*:', match.group(1))
        else:
            return []

    @classmethod
    def load_class_attributes(cls):
        """
        Loads or infers class attributes required for building the dependency graph.

        This method populates `task_params` and `task_returns` by inspecting
        the `run_content` method, allowing the job orchestrator to understand
        task dependencies without instantiating the task.
        """
        cls.task_params = cls._get_task_inputs()
        cls.task_returns = cls._get_task_outputs()

    def run(
        self,
        input_data: Optional[dict[str, Union[DataFrame, SparkDataFrame]]] = None,
        setup_logging: bool = True,
        run_state: Optional[RunState] = None,
    ) -> dict[str, Union[DataFrame, SparkDataFrame]]:
        """
        Executes the full task lifecycle.

        This wrapper method orchestrates the task execution by setting up logging,
        loading necessary input data, calling `run_content` to perform the core logic,
        and saving the resulting output data.

        Args:
            input_data: A dictionary of pre-loaded DataFrames to be used as input.
                If a required input is not provided, it will be loaded from the configured source.
            setup_logging: If True, configures logging based on the 'logging' config file.
            run_state: State, including the run_id and run_timestamp.

        Returns:
            A dictionary containing both the input and output DataFrames of the task.
        """
        if setup_logging:
            logging_config = self._local_config_manager.get_logging_config()
            logging.config.dictConfig(logging_config)

        if run_state:
            self._run_state = run_state
        elif self._run_state is None:
            # Fallback for local dev: Create a default run_state automatically
            self._run_state = RunState(job_name=self.task_name)

        self.quality_validator = DataQualityValidator(
            self.config.get("data_quality_checks", {}),
            self._run_state,
            self.task_name,
            self.data_manager,
        )

        logging.info(f"Calling task {self.__class__.__name__}")

        input_data = self.load_input_data(input_data or {})

        start_time = time.time()

        assert self.task_params is not None, "Task parameters were not initialized"
        output_data = self.run_content(
            **{
                name: data.copy() if isinstance(data, DataFrame) else data
                for name, data in input_data.items()
                if name in self.task_params.keys()
            }
        )

        logging.info(f"Run content took {(time.time() - start_time)} seconds")

        self.save_data(output_data)

        logging.info(f"Called task {self.__class__.__name__}")

        return input_data | output_data

    def load_input_data(
        self,
        input_data: Dict[str, Union[DataFrame, SparkDataFrame]],
    ) -> Dict[str, Union[DataFrame, SparkDataFrame]]:
        """
        Loads and prepares all required input data for the task.

        It iterates through the expected `task_params`. If a dataset is not already
        provided in `input_data`, it's loaded using the DataManager. It also handles
        automatic conversion between Pandas and Spark DataFrames to match the
        type hints in `task_params`.

        Args:
            input_data: A dictionary of already-loaded data, which may be empty.

        Returns:
            A new dictionary containing all required input datasets, correctly typed.

        Raises:
            TypeError: If an input DataFrame has an unexpected type that
                       cannot be converted.
        """
        logging.info(f"Loading all input data for task {self.__class__.__name__}")

        assert self.task_params is not None, "Task parameters were not initialized"

        # Create a new dictionary to hold the results.
        # This avoids modifying the input_data dictionary (a side effect).
        prepared_data: Dict[str, Union[DataFrame, SparkDataFrame]] = {}

        for table_name, expected_type in self.task_params.items():
            # --- Step 1: Get the data (from cache or load it) ---
            if table_name in input_data:
                current_data = input_data[table_name]
            else:
                logging.debug(f"'{table_name}' not found in job cache. Loading from DataManager.")
                current_data = self.data_manager.load_table(table_name, expected_type, schema_type="working")

            # --- Step 2: Check, convert, and store the data ---

            # Case 1: Type is already correct (e.g., Spark-to-Spark or Pandas-to-Pandas)
            # We use isinstance() for robustness (handles classic vs. canonical Spark DF)
            if isinstance(current_data, expected_type):
                prepared_data[table_name] = current_data

            # Case 2: Data is Spark, but we expected Pandas
            elif isinstance(current_data, SparkDataFrame) and (expected_type == DataFrame):
                logging.info(f"Converting '{table_name}' from Spark to Pandas.")
                prepared_data[table_name] = current_data.toPandas()

            # Case 3: Data is Pandas, but we expected Spark
            elif isinstance(current_data, DataFrame) and (expected_type == SparkDataFrame):
                logging.info(f"Converting '{table_name}' from Pandas to Spark.")
                prepared_data[table_name] = self.data_manager.spark.createDataFrame(current_data)

            # Case 4: Data is of a completely unexpected type
            else:
                raise TypeError(
                    f"Type mismatch for input '{table_name}' for task '{self.__class__.__name__}'. "
                    f"Expected type {expected_type}, but got {type(current_data)}."
                )

        logging.info(f"Loaded all data for task {self.__class__.__name__}")

        return prepared_data

    def save_data(
        self,
        output_data: dict[str, Union[DataFrame, SparkDataFrame]],
    ) -> None:
        """
        Validates and saves the task's output DataFrames.

        This method saves every output to the default 'working' area. It also
        checks the task's configuration for a list of outputs that should
        additionally be saved to Snowflake.
        """
        assert self.quality_validator is not None, "Validator must be initialized in the run() method."

        # Get the simple list of tables to save to Snowflake from the config
        tables_to_save_on_snowflake: List = self.config.get("tables_to_save_on_snowflake") or []

        for data_name, data in output_data.items():
            # The quality check is the gate before any save operations
            validated_data, _ = self.quality_validator.validate(data, data_name)

            # 1. Default Action: Always save to the working area.
            logging.info(f"Saving '{data_name}' to the working area...")
            self.data_manager.save_table(validated_data, data_name)

            # 2. Optional Action: Save to Snowflake if configured.
            if data_name in tables_to_save_on_snowflake:
                logging.info(f"Saving '{data_name}' to Snowflake...")
                if self.on_databricks:
                    # Use the specialized manager directly for this special case
                    self.snowflake_manager.save_to_snowflake(validated_data, data_name)
                else:
                    logging.info(f"Skipping Snowflake save for '{data_name}' on local run.")
