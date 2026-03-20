import copy
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Type, Union

from jinja2 import Template

from core.config_manager import Config, ConfigManager  # FIXME: why does this work during azure deployment?
from deployment.api import DatabricksApi
from deployment.mock_job import MockJob
from deployment.mock_task import MockTask

# Avoid uncessary importing during deployment
if TYPE_CHECKING:
    from core.task import Task


class DatabricksDeployer:
    """Deploys project clusters and jobs defined in configs."""

    def __init__(self, deployment_stage_short: Literal["e", "q", "p"] = "e") -> None:
        """Load configs and open the api."""
        self.deployment_stage_short = deployment_stage_short
        self.api_manager: DatabricksApi = DatabricksApi()

        # Load configs
        config_manager = ConfigManager()
        cluster_configs: Config = config_manager.get_clusters_config()
        platform_config: Config = config_manager.get_platform_config()
        self.job_registry: Config = config_manager.get_job_registry()

        # Prepare final cluster configs based on stage
        self.cluster_configs = self._prepare_cluster_configs(cluster_configs, platform_config, deployment_stage_short)

    def _prepare_cluster_configs(
        self,
        cluster_configs: Config,
        platform_config: Config,
        deployment_stage_short: Literal["e", "q", "p"],
    ) -> Config:
        """
        Creates a stage-specific copy of all cluster configurations.

        This method iterates through each cluster defined in the config, modifying
        a deep copy to inject stage-specific values like the user name and init
        script paths where applicable.

        Returns:
            A new Config object with the stage-specific settings applied to all clusters.
        """
        prepared_configs = copy.deepcopy(cluster_configs)

        # Iterate over all clusters defined in the configuration (e.g., 'automation_cluster', etc.)
        for cluster_name, cluster_config in prepared_configs.items():
            # 1. Safely add the deployment stage as a Spark environment variable
            # This is useful for all clusters deployed via this process.
            if "spark_env_vars" not in cluster_config:
                cluster_config["spark_env_vars"] = {}  # Create dict if it doesn't exist
            cluster_config["spark_env_vars"]["DEPLOYMENT_STAGE_SHORT"] = deployment_stage_short

            # 2. Conditionally set the user name ONLY for single-user clusters
            if cluster_config.get("data_security_mode") == "SINGLE_USER":
                user = platform_config.users.get(deployment_stage_short)
                if user:
                    cluster_config["single_user_name"] = user
                else:
                    logging.warning(
                        f"No user found for stage '{deployment_stage_short}' " f"for cluster '{cluster_name}'."
                    )

            # 3. Safely render the init script path if it's a template
            if "init_scripts" in cluster_config:
                for script in cluster_config["init_scripts"]:
                    # Check for the expected structure before trying to render
                    if isinstance(script, dict) and "volumes" in script and "destination" in script["volumes"]:
                        template_path = script["volumes"]["destination"]
                        # Only render if it looks like a Jinja2 template
                        if isinstance(template_path, str) and "{{" in template_path:
                            script["volumes"]["destination"] = Template(template_path).render(
                                deploymentStageShort=deployment_stage_short
                            )

        return prepared_configs

    def deploy_all_clusters(self) -> None:
        """Deploy all clusters defined in the config."""
        for _, cluster_config in self.cluster_configs.items():
            self.api_manager.create_or_update_cluster(cluster_config)

    def deploy_all_jobs(self) -> None:
        """Deploy all jobs defined in the config."""
        for job_name, job_settings in self.job_registry.items():
            job_settings["cluster_id"] = self.api_manager.clusters_on_databricks.get(job_settings["cluster_name"])
            self._deploy_job(job_name, job_settings)

    def _deploy_job(self, job_name: str, job_settings: Config) -> None:
        keys_to_ignore = ["cluster_id", "cluster_name", "run_path", "tasks"]
        # Only include scheduling and email notifications on PROD
        if self.deployment_stage_short != "p":
            keys_to_ignore += ["schedule", "email_notifications"]
        # Convert yml to correct json for deployment
        job_config: Dict[str, Any] = {
            k: v.to_dict() if isinstance(v, Config) else v for k, v in job_settings.items() if k not in keys_to_ignore
        }
        job_config["name"] = job_name

        # Load job to compute dependencies
        job = MockJob()
        tasks = job.load_tasks(job_settings.tasks)
        dependencies = job.get_dependencies()
        job_config["tasks"] = self._convert_task_configuration(job_settings, tasks, dependencies)  # pyright: ignore[reportArgumentType]

        # NOTE: a job named 'MyJob' automatically uses the config file 'jobs/MyJob.yml' if it exists.
        config_name = f"jobs/{job_config.get("config_file", job_name)}"
        config_path = Path(f"config/{config_name}.yml")

        if config_path.exists():
            logging.info(f"Found config '{config_path}' for job '{job_name}'.")
            job_config["parameters"] = [
                {"name": "config_file", "default": config_name},
            ]
        else:
            logging.warning(f"No config found '{config_path}' for job '{job_name}'.")

        # Deploy using API
        self.api_manager.create_or_update_job(job_config)

    def _convert_task_configuration(
        self,
        job_settings: Config,
        tasks: List[Union[Type["Task"], Type["MockTask"]]],
        dependencies: Dict[str, List[str]],
    ) -> List[Dict[str, Any]]:
        task_configs: List[Dict[str, Any]] = []

        # Get the notebook path from the job's config, with a sensible default.
        notebook_path = job_settings.get("run_path", "/Workspace/Shared/cdspch/run_task.py")

        for task in tasks:
            task_dependencies = dependencies.get(task.task_name)
            task_dict: Dict[str, Any] = self._get_task_configuration_by_task(task, task_dependencies, notebook_path)
            task_dict["existing_cluster_id"] = job_settings["cluster_id"]
            task_configs.append(task_dict)
        return task_configs

    @staticmethod
    def _get_task_configuration_by_task(
        task: Union[Type["Task"], Type["MockTask"]],
        dependencies: Optional[List[str]],
        notebook_run_path: str,
    ) -> Dict[str, Any]:
        task_dict: Dict[str, Any] = {"task_key": task.task_name}
        # Add task dependencies
        if dependencies:
            task_dict["depends_on"] = [{"task_key": dependency} for dependency in dependencies]
        # NOTE: Notebook tasks must be used to use classical compute
        task_dict["notebook_task"] = {
            "notebook_path": notebook_run_path,
            "base_parameters": {
                "task_name": task.task_name,
                "task_type": task.task_type,
                "run_id": "{{job.run_id}}",
            },
        }
        if hasattr(task, "table_name"):  # Data loading task
            task_dict["notebook_task"]["base_parameters"].update({"table_name": task.table_name})
        return task_dict
