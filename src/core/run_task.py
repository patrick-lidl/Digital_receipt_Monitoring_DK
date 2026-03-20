"""Run each task as an individual Job in Databricks."""

import importlib
import logging
import os
import sys
import warnings
from typing import Type

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=UserWarning)

sys.path.insert(0, os.environ["BASE_PATH"])
sys.path.insert(0, os.environ["BASE_PATH"] + "/src")

# ruff: noqa: E402
from ch_utils.common.string_utils import camel_to_snake
from core.config_manager import ConfigManager
from core.data_manager import DataManager
from core.run_state import RunState
from core.snowflake_manager import SnowflakeManager
from core.task import Task
from core.tasks.data_loader import DataLoader

# --- 1. SETUP CONTEXT ---
# Retrieve Widget parameters
task_name = dbutils.widgets.get("task_name")  # type: ignore # noqa: F821
task_type = dbutils.widgets.get("task_type")  # type: ignore # noqa: F821
run_id = dbutils.widgets.get("run_id")  # type: ignore # noqa: F821

# Defining a job config is optional
try:
    job_config_name = dbutils.widgets.get("config_file")  # type: ignore # noqa: F821
except Exception as e:
    # Check if the error is specifically about the widget not being defined
    if "InputWidgetNotDefined" in str(e):
        job_config_name = None
    else:
        raise e

# Handle empty string widget (Databricks sometimes passes "" for empty)
if not job_config_name:
    job_config_name = None

# Initialize the Central Config Manager
config_manager = ConfigManager(job_config_name)

# --- 2. SETUP LOGGING ---
# We configure logging once at the script level
logging.config.dictConfig(config_manager.get_logging_config())


def _get_task_class(class_name: str) -> Type[Task]:
    """Resolves the Task Class definition (Type) from the registry."""
    tasks_registry = config_manager.get_task_registry()

    module_path = tasks_registry[class_name]["path"] + "." + camel_to_snake(class_name)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    cls.load_class_attributes()
    return cls


def run_orchestrator() -> None:
    """Runs a single-task job."""
    # --- 3. SETUP INFRASTRUCTURE ---
    # Load infra configs and instantiate Managers once
    sf_config = config_manager.get_snowflake_config()
    storage_config = config_manager.get_storage_config()

    # Note: DataManager now requires the config passed in __init__
    dm = DataManager(storage_config)
    sm = SnowflakeManager(sf_config)

    # --- 4. SETUP STATE ---
    # Create the RunState using the ID provided by the external scheduler
    run_state = RunState(run_id=run_id)

    # --- 5. RESOLVE TASK CLASS ---
    task_cls: Type[Task]

    if task_type == "standard":
        task_cls = _get_task_class(task_name)
    elif task_type == "data_loader":
        # Special handling for generic DataLoader
        task_cls = DataLoader
        table_name = dbutils.widgets.get("table_name")  # type: ignore # noqa: F821
        task_cls.create_data_loader_task(table_name)
    else:
        raise ValueError(f"Unknown task type: {task_type}")

    # --- 6. BUILD CONFIG & INJECT ---
    # Build the task-specific config (merging defaults + job overrides)
    task_config = config_manager.build_task_config(task_cls)

    # Instantiate the Task with all dependencies injected
    task_instance = task_cls(config=task_config, data_manager=dm, snowflake_manager=sm)

    # --- 7. EXECUTE ---
    # Run with the explicit state and logging already configured
    task_instance.run(
        setup_logging=False,  # We configured it above
        run_state=run_state,
    )


if __name__ == "__main__":
    run_orchestrator()
