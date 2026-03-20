"""Script for adding Databricks clusters and jobs."""

import argparse

from deployment import DatabricksDeployer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the stage argument.")
    parser.add_argument(
        "--stage",
        choices=["e", "q", "p"],
        default="e",
        help="Stage of the process: 'e' for development (default), 'q' for QAS, 'p' for production",
    )
    args = parser.parse_args()

    deployer = DatabricksDeployer(deployment_stage_short=args.stage)
    deployer.deploy_all_clusters()
    deployer.deploy_all_jobs()

    # for job_name, job_settings in deployer.jobs_config.items():
    #     if job_name != "kolli_sort_dev":
    #         continue
    #     job_settings["cluster_id"] = deployer.api_manager.clusters_on_databricks.get(job_settings["cluster_name"])
    #     deployer._deploy_job(job_name, job_settings)
