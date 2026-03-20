import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from ch_utils.foundations.config_manager import Config, load_configs


class ConfigManager:
    """
    The central authority for loading, merging, and serving configuration objects.

    This manager abstracts away the file system and merge logic, providing
    Jobs and Tasks with fully resolved configuration objects.

    ### Assumptions on Repository Structure
    This class assumes the following directory structure relative to the project root/config:
    - `task_registry.yml`: A registry mapping task names to python modules.
    - `storage.yml`: Infrastructure settings (Unity Catalog, Filesystem).
    - `platform.yml`: Platform settings (Databricks users, secrets).
    - `snowflake.yml`: Snowflake connection settings.
    - `job_registry.yml`: Registry of Databricks Jobs for deployment.
    - `clusters.yml`: Registry of Databricks Clusters.
    - `logging.yml`: Configuration for the logging module.
    - `tasks/<TaskClassName>.yml`: Default parameters for a specific Task class.
    - `jobs/<JobName>.yml`: Job-specific overrides (optional).

    ### Assumptions on Merge Logic
    Configurations are merged using a "Winner Takes All" strategy for overlapping keys.
    The priority order (lowest to highest) is:
    1. Parent Class Configs (following the MRO, excluding `object` and `Task`)
    2. Task Class Config (matching `TaskClassName`)
    3. Extra Configs (defined in `Task.extra_configs_to_load`)
    4. Job Config (defined by `job_config_name`)
    5. Runtime Overrides (passed dynamically to `build_task_config`)
    """

    def __init__(self, job_config_name: Optional[str] = None):
        """
        Initialize the ConfigManager.

        Args:
            job_config_name: The filename (stem) of the job configuration to apply
                             as a context layer. E.g., "daily_report".
        """
        self.job_config_name = job_config_name

        # Internal cache to prevent re-reading static global files from disk multiple times
        self._global_cache: Dict[str, Config] = {}

        # Validate that the core environment exists
        self._validate_critical_files()

    def _validate_critical_files(self) -> None:
        """
        Checks for the existence of critical configuration files on initialization.

        Logs warnings if core registries are missing.
        """
        base_path = os.environ.get("BASE_PATH", ".")
        config_path = Path(base_path) / "config"

        # List of files that SHOULD exist for the framework to function correctly
        critical_files = [
            "task_registry.yml",
            "storage.yml",
            "platform.yml",
            "logging.yml",
            "clusters.yml",
            "job_registry.yml",
        ]

        logging.debug(f"Validating config environment at: {config_path.absolute()}")

        for filename in critical_files:
            file_path = config_path / filename
            if not file_path.exists():
                logging.warning(f"Critical config file missing: '{filename}'. " "Some functionality may fail.")
            else:
                logging.debug(f"Found config file: {filename}")

    # --- Global Config Getters ---

    def get_clusters_config(self) -> Config:
        """Loads clusters.yml and merges 'base_cluster' into each cluster definition."""
        return self._load_and_distribute_children("clusters", base_key="base_cluster")

    def get_storage_config(self) -> Config:
        """
        Returns storage backend settings (Unity Catalog, Filesystem).

        Source: `storage.yml`
        """
        return self._get_cached_global("storage")

    def get_platform_config(self) -> Config:
        """
        Returns platform infrastructure settings (users, secrets, APIs).

        Source: `platform.yml`
        """
        return self._get_cached_global("platform")

    def get_snowflake_config(self) -> Config:
        """
        Returns Snowflake connection details and params.

        Source: `snowflake.yml`
        """
        return self._get_cached_global("snowflake")

    def get_job_registry(self) -> Config:
        """
        Loads job_registry.yml and merges 'base_config' into each job definition.

        Source: `job_registry.yml`
        """
        return self._load_and_distribute_children(
            "job_registry", base_key="base_config", cache_key="job_registry_registry"
        )

    def get_logging_config(self) -> Config:
        """
        Loads and returns the global logging configuration.

        Source: `logging.yml`
        """
        return self._get_cached_global("logging")

    def get_task_registry(self) -> Config:
        """
        Loads and returns the global task registry.

        Source: `task_registry.yml`
        """
        return self._get_cached_global("task_registry")

    def get_job_config(self) -> Config:
        """
        Returns the raw configuration for the current job context.

        Source: `jobs/{self.job_config_name}.yml`
        """
        if not self.job_config_name:
            return Config({})

        # Clean the name
        clean_job_name = self.job_config_name.replace("jobs/", "")

        logging.debug(f"Loading Job context: {clean_job_name}")
        # We rely on load_configs to resolve the absolute path to "jobs/"
        return load_configs([f"jobs/{clean_job_name}"])

    # --- Task Configuration Builder ---

    def build_task_config(self, task_class: Type[Any], extra_overrides: Optional[Dict[str, Any]] = None) -> Config:
        """Constructs the final, fully merged configuration for a specific Task class."""
        logging.debug(f"Building configuration for Task: {task_class.__name__}")

        files_to_load: List[str] = []

        # 1. Inheritance Chain
        bases = [b.__name__ for b in task_class.__bases__ if b.__name__ not in ("object", "Task", "DatabricksHandler")]
        files_to_load.extend([f"tasks/{b}" for b in bases])

        # 2. The Task itself
        files_to_load.append(f"tasks/{task_class.__name__}")

        # 3. Explicit Extras
        if hasattr(task_class, "extra_configs_to_load"):
            extras = task_class.extra_configs_to_load
            if extras:
                logging.debug(f"Task '{task_class.__name__}' requested extras: {extras}")
                # Prepend 'tasks/' to match the old behavior
                files_to_load.extend([f"tasks/{e}" for e in extras])

        # 4. The Job Context
        if self.job_config_name:
            # Clean the name to ensure we don't double-nest
            clean_job_name = self.job_config_name.replace("jobs/", "")
            files_to_load.append(f"jobs/{clean_job_name}")

        # Execute the merge
        final_config = load_configs(files_to_load)

        # 5. Runtime Overrides
        if extra_overrides:
            logging.debug(f"Applying {len(extra_overrides)} runtime overrides.")
            final_config.update(extra_overrides)

        # 6. Post-Processing (Base Config Distribution)
        if "base_config" in final_config:
            self._flatten_base_config(final_config)

        return final_config

    # --- Internal Helpers ---

    def _get_cached_global(self, name: str) -> Config:
        """Helper to load and cache global files."""
        if name not in self._global_cache:
            logging.debug(f"Loading global config: {name}")
            self._global_cache[name] = load_configs(name)
        return self._global_cache[name]

    def _flatten_base_config(self, config: Config, key: str = "base_config") -> None:
        """
        Promotes keys from config['base_config'] to the root of config.

        Only sets the value if the key does not already exist in the root.
        """
        base = config.pop(key, {})
        if not base:
            return

        logging.debug(f"Flattening '{key}' into root configuration.")
        for k, v in base.items():
            if k not in config:
                config[k] = v

    def _load_and_distribute_children(self, filename: str, base_key: str, cache_key: Optional[str] = None) -> Config:
        """
        Load config files with base.

        Loads a file (registry), finds a base template key, and merges that
        template into all other top-level keys (the children).
        """
        key = cache_key or filename
        if key not in self._global_cache:
            logging.info(f"Loading and distributing registry: {filename}")

            # Load raw
            config = load_configs(filename)

            # Pop the base section
            base_config = config.pop(base_key, {})

            # Distribute base into every child
            for child_key, child_values in config.items():
                # We use a simple loop to set defaults if they don't exist
                for k, v in base_config.items():
                    if k not in child_values:
                        child_values[k] = v

            self._global_cache[key] = config

        return self._global_cache[key]
