import importlib
import logging.config
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union, cast

from ch_utils.common.string_utils import camel_to_snake
from core.config_manager import Config, ConfigManager
from core.run_state import RunState

if TYPE_CHECKING:
    from pandas.core.api import DataFrame
    from pyspark.sql.dataframe import DataFrame as SparkDataFrame

    from core.task import Task
    from core.tasks.data_loader import DataLoader


class Job:
    """
    Orchestrates the execution of a collection of tasks as a dependency graph.

    This class is responsible for loading task classes from configuration,
    computing the execution order, and running the tasks in sequence. It
    manages data in memory, passing the outputs of one task as inputs to
    subsequent tasks. It can also automatically inject `DataLoader` tasks for
    any inputs that are not produced by another task within the job.

    This base class is intended for runtime execution and will fail if task
    dependencies are not installed.

    Attributes:
        current_data (Dict): A dictionary holding the in-memory data tables
            (as DataFrames) that are passed between tasks.
        tasks (Optional[List[Type[Task]]]): The list of task classes included
            in the job, sorted after the first call to `get_ordered_tasks`.
        dependencies (Optional[Dict[str, List[str]]]): A dictionary mapping each
            task name to a list of its prerequisite task names.
    """

    def __init__(self, job_config: Optional[str] = None):
        """
        Initializes a new Job instance.

        Args:
            job_config: The name of a job-specific config file (e.g.,
                'my_job.yml') that can override task-level configurations.
        """
        self.config_manager = ConfigManager(job_config)
        self._tasks_config: Config = self.config_manager.get_task_registry()

        self.current_data: Dict[str, Union["DataFrame", "SparkDataFrame"]] = {}
        self._tasks_are_ordered: bool = False
        self.tasks: Optional[List[Type["Task"]]] = None
        self.dependencies: Optional[Dict[str, List[str]]] = None

    def run(
        self,
        setup_logging: bool = True,
        config_override: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Executes all tasks in the job in the correct topological order.

        This method sets up logging, retrieves the ordered list of tasks,
        and then iterates through them, instantiating and running each one
        while passing data between them.

        Args:
            setup_logging: If True, configures logging based on the
                'logging' config file.
            config_override: A dictionary of config values to
                override task-level configs.
        """
        # Defer import, so that deployer does not need dependencies
        from core.data_manager import DataManager
        from core.snowflake_manager import SnowflakeManager

        if setup_logging:
            logging_config = self.config_manager.get_logging_config()
            logging.config.dictConfig(logging_config)

        # Create the Shared State
        current_state = RunState()

        # Instantiate the Managers
        storage_config = self.config_manager.get_storage_config()
        job_data_manager = DataManager(storage_config)

        sf_config = self.config_manager.get_snowflake_config()
        job_snowflake_manager = SnowflakeManager(sf_config)

        for task_class in self.get_ordered_tasks():
            # Build Task Config
            task_config = self.config_manager.build_task_config(task_class)

            if config_override:
                task_config.update(config_override)

            # Create the task instance and run
            current_task = task_class(
                config=task_config, data_manager=job_data_manager, snowflake_manager=job_snowflake_manager
            )

            data = current_task.run(input_data=self.current_data, setup_logging=False, run_state=current_state)
            self.current_data.update(data)

    def load_tasks(self, task_list: List[str], fill_data_loaders: bool = True) -> List[Type["Task"]]:
        """
        Loads task classes from a list of names and adds required data loaders.

        This populates `self.tasks` with the specified task classes and any
        `DataLoader` tasks needed to satisfy the job's data dependencies.

        Args:
            task_list: A list of task class names to include in the job.
            fill_data_loaders: If True, automatically adds `DataLoader` tasks
                for inputs not produced by other tasks in the job.

        Returns:
            The final list of task classes loaded into the job instance.
        """
        tasks = [self._get_task_by_name(task_name) for task_name in task_list]
        logging.info("Loading Tasks from list...")
        for task in tasks:
            logging.info(f"Loaded the Task: '{task.task_name}'")
        if fill_data_loaders:
            logging.info("Loading DataLoader Tasks...")
            data_inputs = self._get_missing_inputs(tasks)
            data_loader_tasks = [self._get_data_loader_task_by_name(table_name) for table_name in data_inputs]
            tasks.extend(data_loader_tasks)
            for task in data_loader_tasks:
                logging.info(f"Loaded the DataLoader Task: '{task.task_name}'")
        self.tasks = tasks
        self._tasks_are_ordered = False
        return self.tasks

    def get_dependencies(self) -> Dict[str, List[str]]:
        """
        Calculates and returns the job's dependency graph.

        The result is cached after the first computation.

        Returns:
            A dictionary where each key is a task name and the value is a list
            of its prerequisite task names.
        """
        if self.dependencies is None:
            self.dependencies = self._compute_dependencies()
        return self.dependencies

    def get_ordered_tasks(self) -> List[Type["Task"]]:
        """
        Returns a topologically sorted list of task classes for execution.

        The sorted list is cached after the first computation, and the
        `self.tasks` attribute is re-ordered in place.

        Returns:
            A list of task classes in a valid execution order.
        """
        if self.tasks is None:
            raise RuntimeError("Tasks must be loaded via load_tasks() before they can be ordered.")
        if len(self.tasks) <= 1:
            return self.tasks
        if not self._tasks_are_ordered:
            self.dependencies = self.get_dependencies()
            task_names_ordered = self._order_tasks()
            task_map = {cls.task_name: cls for cls in self.tasks}
            self.tasks = [task_map[task_name] for task_name in task_names_ordered]
            self._tasks_are_ordered = True
        return self.tasks

    @staticmethod
    def _get_missing_inputs(tasks: List[Type["Task"]]) -> List[str]:
        """
        Finds all task inputs that are not produced by another task in the job.

        Args:
            tasks: A list of task classes in the job.

        Returns:
            A list of unique data table names that need to be loaded from an
            external source.
        """
        task_outputs = {output_param for current_task in tasks for output_param in current_task.task_returns}
        task_inputs = {input_param for current_task in tasks for input_param in current_task.task_params or []}
        return sorted(list(task_inputs - task_outputs))

    @staticmethod
    def _get_output_to_task_map(original_dict: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Inverts a task-to-outputs mapping to create an output-to-task map.

        This utility creates a quick lookup from an output data name to the
        task that produces it, ensuring that each output is produced by
        only one task.

        Args:
            original_dict: A dictionary mapping task names to their list of outputs.

        Returns:
            A dictionary mapping output names to their parent task name.

        Raises:
            ValueError: If an output name is produced by more than one task.
        """
        new_dict = {}
        for task_name, output_values in original_dict.items():
            for output_name in output_values:
                if output_name in new_dict:
                    raise ValueError(f"Duplicate output '{output_name}' is produced by multiple tasks.")
                new_dict[output_name] = task_name
        return new_dict

    def _get_data_loader_task_by_name(self, table_name: str) -> Type["Task"]:
        """
        Dynamically creates a DataLoader task class for a given table name.

        This method generates a new class that inherits from the base `DataLoader`
        and is configured to load the specified table. This allows data loading
        to be treated as a standard task in the dependency graph.

        Args:
            table_name: The name of the data table to load.

        Returns:
            A new Task class configured to act as a data loader for the table.
        """
        module = importlib.import_module("core.tasks.data_loader")
        DataLoader_cls: Type["DataLoader"] = getattr(module, "DataLoader")

        # Dynamically create the new class using the type() constructor.
        NewDataLoaderTask = type("NewDataLoaderTask", (DataLoader_cls,), {})

        # Cast to the more specific DataLoader type, which owns the method.
        cls = cast(Type["DataLoader"], NewDataLoaderTask)

        cls.create_data_loader_task(table_name)
        return cls

    def _get_task_by_name(self, class_name: str) -> Type["Task"]:
        """
        Loads a task class dynamically by its name using the 'tasks' config.

        Args:
            class_name: The name of the task class to load.

        Returns:
            The loaded task class.

        Raises:
            ModuleNotFoundError: If the task's module cannot be found.
            AttributeError: If the class is not found within the module.
        """
        module_path = self._tasks_config[class_name]["path"] + "." + camel_to_snake(class_name)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        cls.load_class_attributes()
        return cls

    def _order_tasks(self) -> List[str]:
        """
        Performs a topological sort on the tasks to find the execution order.

        This method uses Kahn's algorithm to sort the dependency graph. It starts
        with tasks that have no dependencies and iteratively processes tasks
        whose dependencies have been met.

        Returns:
            A list of task names in a valid execution order.

        Raises:
            ValueError: If a task depends on another task that is not part of the
                current job, or if a circular dependency is detected.
        """
        if self.dependencies is None:
            raise RuntimeError("Dependencies must be computed before tasks can be ordered.")

        graph = defaultdict(list)
        in_degree = {task_name: 0 for task_name in self.dependencies}

        for task_name, task_deps in self.dependencies.items():
            for dep in task_deps:
                if dep not in self.dependencies:
                    raise ValueError(
                        f"Task '{task_name}' has an unmet dependency. "
                        f"The required task '{dep}' is not included in this job's task list."
                    )
                graph[dep].append(task_name)
                in_degree[task_name] += 1

        queue = deque([task_name for task_name, degree in in_degree.items() if degree == 0])
        ordered_tasks = []

        while queue:
            node = queue.popleft()
            ordered_tasks.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered_tasks) != len(self.dependencies):
            raise ValueError("Circular dependency detected among tasks.")

        return ordered_tasks

    def _compute_dependencies(self) -> Dict[str, List[str]]:
        """
        Computes the complete dependency graph for all tasks loaded in the job.

        Dependencies are inferred in two ways:
        - **Data Dependencies**: If Task B uses a data table produced by Task A,
          Task B depends on Task A.
        - **Forced Dependencies**: Explicit dependencies defined in the
          `forced_dependencies` class attribute of a task.

        Returns:
            A dictionary representing the adjacency list of the dependency graph.
        """
        assert self.tasks is not None, "Tasks must be loaded to find dependencies."
        output_to_task_map = self._get_output_to_task_map(
            {current_task.task_name: current_task.task_returns for current_task in self.tasks}
        )

        dependencies: Dict[str, List[str]] = defaultdict(list)
        for current_task in self.tasks:
            task_name = current_task.task_name

            # 1. Add data dependencies
            data_deps = {
                output_to_task_map[input_param]
                for input_param in current_task.task_params or []
                if input_param in output_to_task_map
            }
            dependencies[task_name].extend(list(data_deps))

            # 2. Add forced dependencies
            if hasattr(current_task, "forced_dependencies") and current_task.forced_dependencies:
                dependencies[task_name].extend(current_task.forced_dependencies)

            # Ensure the task exists in the dict even if it has no dependencies
            if task_name not in dependencies:
                dependencies[task_name] = []

        # Return a standard dict with unique, sorted dependency lists
        return {task_name: sorted(list(set(deps))) for task_name, deps in dependencies.items()}
