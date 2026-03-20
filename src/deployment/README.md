# Deployment Package 🚀

> This package contains the complete toolchain for analyzing and deploying Databricks jobs and clusters from local YAML configurations.

This package is designed to run in a lightweight CI/CD environment where heavy data science libraries (`pandas`, `pyspark`, etc.) are not installed. It enables the deployment pipeline to build and understand the job dependency graph without needing to execute any of the application's core logic.

## Core Concept: Dependency Analysis without Execution

The primary challenge this package solves is determining a job's task dependencies without a full runtime environment. Standard imports would fail if a library like `pandas` is missing.

To solve this, the package uses a sophisticated fallback mechanism:

1.  It first **attempts to import** a task's module. If successful, it gets perfect dependency metadata.
2.  If the import fails with a `ModuleNotFoundError`, it falls back to reading the task's raw Python source code. It then parses the file's **Abstract Syntax Tree (AST)** to find the inputs (`task_params`), outputs (`task_returns`), and `forced_dependencies` without ever executing the code.

This AST-based analysis allows the CI/CD pipeline to construct a highly accurate dependency graph and deploy complex, multi-task jobs safely.

-----

## Key Components

  * `DatabricksDeployer`: The main high-level orchestrator. It reads configuration files and manages the entire deployment process.
  * `MockJob`: The dependency analysis engine. It takes a list of tasks for a job and uses the AST analysis to build a complete dependency graph.
  * `DatabricksApi`: A low-level client that handles all direct communication with the Databricks REST API for creating and updating jobs and clusters.
  * `TaskAnalyzer`: The `ast`-based parser that inspects the source code of a task to extract its metadata.

-----

## Workflow

The deployment process follows these steps:

1.  The main deployment script instantiates `DatabricksDeployer` for a specific stage (e.g., development, production).
2.  The deployer reads the job and cluster definitions from the YAML configuration files.
3.  For each job defined in the config, it instantiates `MockJob`.
4.  `MockJob` loads the required tasks. For each one, it tries to import it; if that fails, it uses `TaskAnalyzer` to parse the source code and create a `MockTask`.
5.  With a complete list of real and mocked tasks, `MockJob` computes the final dependency graph.
6.  `DatabricksDeployer` translates this graph into the JSON format required by the Databricks Jobs API.
7.  Finally, `DatabricksDeployer` uses `DatabricksApi` to send the request to Databricks, creating or updating the job.

-----

## Configuration

This package is driven by three main configuration files:

  * `configs/tasks.yml`: Defines the location of all task modules. Can include an optional `analysis_function` for non-standard tasks.
  * `configs/jobs.yml`: Defines the jobs, the tasks they contain, and their target clusters.
  * `configs/clusters.yml`: Defines the specifications for all available Databricks clusters.

-----

## Usage

The package is typically used in a deployment script, like the one for the Azure pipeline:

```python
import argparse
from deployment import DatabricksDeployer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the stage argument.")
    parser.add_argument(
        "--stage",
        choices=["e", "q", "p"],
        default="e",
        help="Stage: 'e' for development, 'q' for QAS, 'p' for production",
    )
    args = parser.parse_args()

    # Instantiate the deployer for the target stage
    deployer = DatabricksDeployer(deployment_stage_short=args.stage)

    # Deploy all clusters and jobs defined in the configs
    deployer.deploy_all_clusters()
    deployer.deploy_all_jobs()
```
