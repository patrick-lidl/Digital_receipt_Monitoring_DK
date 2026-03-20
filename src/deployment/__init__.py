"""
The Deployment Package.

This package contains all the operational code required to analyze, mock,
and deploy data processing jobs to Databricks. It is designed to be used in a
lightweight CI/CD environment where the full dependencies of the main
application (like pandas or pyspark) may not be available.

Its primary purpose is to read the application's source code, build an
accurate dependency graph of all tasks, and translate that graph into the
correct format for the Databricks Jobs API.

Key Components
--------------
- `DatabricksDeployer`: The main class for managing the deployment
  of Databricks jobs and clusters based on local YAML configurations.
- `MockJob`: A subclass of `core.Job` that can build a dependency graph
  by reading task source code directly, falling back to mocks when
  heavy dependencies are not installed.
"""

__all__ = ["DatabricksDeployer", "MockJob"]

from .deployer import DatabricksDeployer
from .mock_job import MockJob
