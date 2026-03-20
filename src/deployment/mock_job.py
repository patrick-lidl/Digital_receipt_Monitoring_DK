import logging
from typing import TYPE_CHECKING, Type, Union

from ch_utils.common.string_utils import camel_to_snake
from core.job import Job
from deployment.mock_task import MockTask, mock_data_loader_task, mock_task

if TYPE_CHECKING:
    # This import is only for static type checking and will not run at runtime,
    # avoiding an unnecessary imports.
    from core.task import Task


class MockJob(Job):
    """
    A subclass of Job used for dependency analysis in a lightweight environment.

    This class overrides the task-loading methods to return mock tasks if the
    actual task dependencies are not installed. This allows the dependency

    graph to be built for deployment without needing a full runtime environment.
    """

    def _get_task_by_name(self, class_name: str) -> Union[Type["Task"], Type[MockTask]]:  # type: ignore[override]
        """
        Loads a task class by name, falling back to a mock if dependencies are missing.

        NOTE: This intentionally violates the Liskov Substitution Principle
        by returning a wider type than the parent Job class. This is necessary
        for dependency analysis in a lightweight CI environment.
        """
        try:
            return super()._get_task_by_name(class_name)
        except ModuleNotFoundError as e:
            module_name = self._tasks_config[class_name]["path"] + "." + camel_to_snake(class_name)

            logging.warning(f"Module {module_name} could not be loaded: {e}. Using a mock task.")

            # Read the analysis function from config, defaulting to 'run_content'
            task_config = self._tasks_config.get(class_name, {})
            analysis_function = task_config.get("analysis_function", "run_content")

            # If dependencies are not installed, mock the task.
            return mock_task(module_name, analysis_function)

    def _get_data_loader_task_by_name(self, table_name: str) -> Union[Type["Task"], Type[MockTask]]:  # type: ignore[override]
        """
        Creates a DataLoader task, falling back to a mock if core dependencies are missing.

        NOTE: This intentionally violates the Liskov Substitution Principle
        by returning a wider type than the parent Job class. This is necessary
        for dependency analysis in a lightweight CI environment.
        """
        try:
            return super()._get_data_loader_task_by_name(table_name)
        except ModuleNotFoundError:
            logging.warning(f"Could not load DataLoader. Using a mock for table '{table_name}'.")
            return mock_data_loader_task(table_name)
