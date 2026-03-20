import logging
import logging.config
import os
import time
from typing import Any, Dict, List
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout

from core.config_manager import Config, ConfigManager


class DatabricksApi:
    """Class to manage Databricks API."""

    def __init__(
        self,
        dotenv_path: str = ".env",
        setup_logging: bool = True,
    ) -> None:
        """
        Class to manage Databricks API.

        Information on the databricks API can be found here: https://docs.databricks.com/api/

        In order to use this class you need to have a .env file with the following variables:
            * DATABRICKS_HOST - the host of the databricks workspace
                Example: https://adb-7391844640783235.15.azuredatabricks.net
            * DATABRICKS_TOKEN - the token to use for authentication
                Generate under User Settings > User > Developer > Access tokens
                Needs to be regenerated every 90 days

        Args:
            dotenv_path: where to look for .env file, defaults to .env in current directory
            setup_logging (bool): whether to use logging

        """
        load_dotenv(dotenv_path, override=True)

        if setup_logging:
            logging_config = ConfigManager().get_logging_config()
            logging.config.dictConfig(logging_config)

        parsed_url = urlparse(os.environ["DATABRICKS_HOST"])
        self.databricks_instance = f"{parsed_url.scheme}://{parsed_url.netloc}"
        self.databricks_token = os.environ["DATABRICKS_TOKEN"]
        self.clusters_on_databricks: Dict[str, str] = self._query_clusters()
        self.jobs_on_databricks: Dict[str, str] = self._query_jobs()

    def create_or_update_cluster(self, cluster_config: Config) -> str:
        """Ensure cluster with given settings exists on databricks."""
        cluster_name = cluster_config["cluster_name"]
        if cluster_id := self.clusters_on_databricks.get(cluster_name):
            logging.info(f"Updating cluster {cluster_name}")
            if self._update_cluster(cluster_config, cluster_id):
                logging.info(f"Updating configuration of {cluster_name} successful")
            else:
                logging.critical(f"Updating configuration of {cluster_name} NOT successful")
        else:
            logging.info(f"Creating cluster {cluster_name}")
            cluster_id = self._create_cluster(cluster_config)
            self.clusters_on_databricks[cluster_name] = cluster_id

        # NOTE: delete means to stop the cluster not actual deleting it
        # NOTE: we need to sleep 2 seconds because otherwise it won't have time to create the cluster before
        # we terminate it
        time.sleep(2)
        requests.post(
            timeout=30,
            url=f"{self.databricks_instance}/api/2.1/clusters/delete",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            json={"cluster_id": cluster_id},
        )
        if not self._add_manage_permissions(cluster_id):
            logging.critical(f"Cluster permissions for {cluster_name} could not be granted")
        logging.info(f"Finished creating or updating cluster {cluster_name}")
        return cluster_id

    def create_or_update_job(self, job_config: Dict[str, Any]) -> str:
        """Ensure job with given settings exists on databricks."""
        job_name = job_config["name"]
        if job_config is None:
            raise ValueError("Job configuration is empty. Were settings loaded?")
        if job_id := self.jobs_on_databricks.get(job_name):
            logging.info(f"Updating job {job_name}")
            if self._update_job(job_id, job_config):
                logging.info(f"Updating configuration of {job_name} successful")
            else:
                logging.critical(f"Updating configuration of {job_name} NOT successful")
        else:
            logging.info(f"Creating job {job_name}")
            job_id = self._create_job(job_config)
            self.jobs_on_databricks[job_name] = job_id

        return job_id

    def _query_databricks(self, obj: str) -> List[Dict[str, Any]]:
        """
        Query databricks in a loop.

        NOTE: Looping is necessary starting API verion 2.1
        """
        output_list = []
        next_page_token = None
        while True:
            params: Dict[str, str] = {}
            if next_page_token:
                params["page_token"] = next_page_token
            response = requests.get(
                timeout=30,
                url=f"{self.databricks_instance}/api/2.1/{obj}/list",
                headers={"Authorization": f"Bearer {self.databricks_token}"},
                params=params,
            )
            response_data = response.json()
            output_list.extend(response_data[obj])

            next_page_token = response_data.get("next_page_token")
            if next_page_token is None or len(next_page_token) == 0:
                break
        return output_list

    def _query_clusters(self) -> Dict[str, str]:
        """
        List all clusters currently on databricks.

        NOTE: cluster names do not have to be unique
        """
        try:
            cluster_list = self._query_databricks("clusters")
        except ReadTimeout:
            logging.critical("Could not query clusters from databricks")
            cluster_list = []
        # Check for duplicate cluster names
        cluster_map = {}
        for cluster in cluster_list:
            name = cluster["cluster_name"]
            if name in cluster_map:
                logging.warning(
                    f"Duplicate cluster name '{name}' found. "
                    f"Using cluster_id {cluster['cluster_id']} and ignoring previous ones."
                )
            cluster_map[name] = cluster["cluster_id"]
        return cluster_map

    def _query_jobs(self) -> Dict[str, str]:
        """List all current workflows."""
        try:
            job_list = self._query_databricks("jobs")
        except ReadTimeout:
            logging.critical("Could not query jobs from databricks")
            job_list = []
        return {job["settings"]["name"]: job["job_id"] for job in job_list}

    def _create_cluster(self, cluster_config: Dict) -> str:
        """
        Create clusters according to configs using ClusterAPI.

        More info: https://docs.databricks.com/api/workspace/clusters/create
        """
        response = requests.post(
            timeout=30,
            url=f"{self.databricks_instance}/api/2.1/clusters/create",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            json=cluster_config,
        )
        logging.info(f"Response: {response.json()}")
        return response.json()["cluster_id"]

    def _update_cluster(self, cluster_config: Dict, cluster_id: str) -> bool:
        """
        Return whether update was successful.

        More info: https://docs.databricks.com/api/workspace/clusters/edit
        """
        cluster_config["cluster_id"] = cluster_id
        response = requests.post(
            timeout=30,
            url=f"{self.databricks_instance}/api/2.1/clusters/edit",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            json=cluster_config,
        )
        try:
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            try:
                # Decode the byte string from the error response for clean logging
                error_details = e.response.content.decode("utf-8")
            except UnicodeDecodeError:
                # Fallback if the response content isn't valid text
                error_details = repr(e.response.content)

            logging.warning(f"Failed to update cluster. API responded with: {error_details}")
            return False

    def _add_manage_permissions(self, cluster_id: str) -> bool:
        """
        Give all admins manage permissions.

        More Info: https://docs.databricks.com/api/workspace/jobs/updatepermissions
        """
        response = requests.patch(
            timeout=30,
            url=f"{self.databricks_instance}/api/2.0/permissions/clusters/{cluster_id}",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            json={"access_control_list": [{"group_name": "admins", "permission_level": "CAN_MANAGE"}]},
        )
        return hasattr(response, "status_code") and (response.status_code == 200)

    def _create_job(self, job_config: Dict[str, Any]) -> str:
        """
        Create a single workflow according to job settings and tasks.

        More info: https://docs.databricks.com/api/workspace/clusters/create
        """
        response = requests.post(
            timeout=30,
            url=f"{self.databricks_instance}/api/2.2/jobs/create",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            json=job_config,
        )
        logging.info(f"Response: {response.json()}")
        return response.json()["job_id"]

    def _update_job(self, job_id: str, new_settings: Dict) -> bool:
        """
        Overwrite all settings for the given job.

        More info: https://docs.databricks.com/api/workspace/jobs/reset
        """
        response = requests.post(
            timeout=30,
            url=f"{self.databricks_instance}/api/2.2/jobs/reset",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            json={
                "job_id": job_id,
                "new_settings": new_settings,
            },
        )
        if hasattr(response, "status_code") and (response.status_code == 200):
            return True
        elif hasattr(response, "status_code"):
            logging.warning(response.content)
            return False
        else:
            raise Exception
