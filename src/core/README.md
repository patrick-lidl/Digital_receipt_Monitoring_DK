# Core Framework

The `core` package is the foundational engine for all data pipelines in this repository. It establishes a standardized, reusable framework for building, orchestrating, and executing data applications, ensuring they are easy to develop, test, and maintain.

-----

## 1\. Core Philosophy: Enabling Local-First Development

The framework's primary goal is to **enable a local-first workflow** by enforcing a clear **separation of concerns**. This is the key principle that allows you to write code in your local IDE (like VS Code) and have it run unchanged in Databricks.

This means you can leverage powerful tools like the debugger, linters, and unit tests on your local machine, running entire pipelines offline. The framework handles the environment-specific details automatically when your code is deployed.

-----

## 2\. Key Concepts

### Job

The **`Job`** is the primary orchestrator and execution engine. It's responsible for building and executing a plan based on a list of tasks.

A `Job`'s workflow is defined in two different ways depending on the context:

1.  **Programmatically (for Local Development):** You instantiate and run a `Job` from a Python script. This gives you full control to run a specific list of tasks.

    ```python
    from core.job import Job

    # Initialize the job with a specific configuration context
    job = Job(job_config="digital_receipt_monitoring")

    # Define exactly which tasks you want to run
    task_list = ["FlagCashiers", "ExportReceiptMonitoringJson"]

    # Load the tasks into an execution plan
    job.load_tasks(task_list, fill_data_loaders=True)

    # Run the plan
    job.run()
    ```

2.  **Via YAML (for Deployment):** When deploying to Databricks, the `deployment` package uses the **`jobs.yml`** file to define the entire job, including its full list of tasks.

### Task

The **`Task`** is the fundamental unit of work. It is a Python class that encapsulates a single, specific piece of business logic (e.g., cleaning data, training a model).

The `Task` class is designed around **Dependency Injection**:
* **Runtime:** When run by a `Job`, the task receives its Configuration, State, and Data Managers explicitly.
* **Local Development:** When instantiated standalone (`t = MyTask()`), it "lazy loads" its own dependencies, falling back to local defaults automatically.

### RunState

The **`RunState`** is a lightweight object that encapsulates the execution context of a pipeline. It separates "static configuration" (YAML) from "runtime state" (IDs, timestamps). It ensures that every task in a job shares the exact same `run_id` and start time.

### DatabricksHandler

The **`DatabricksHandler`** provides a consistent, singleton-like interface for managing the Spark session and environment-specific Databricks utilities (like `dbutils`). It seamlessly bridges the gap between executing natively on Databricks and running your pipelines locally.

When running locally, it intercepts calls for a Spark session and provisions a custom local instance configured with all necessary third-party connectors (Delta Lake and Snowflake) so your data pipelines behave identically to the production environment.

**Local Setup Requirements:**
To run data ingestion and storage tasks locally, you must download the underlying Java JAR files and declare their paths in your `.env` file.

1. **Download the JARs:**
Run the following commands (e.g., in WSL or your Linux terminal) to fetch the necessary files into a local `jars` directory:
```bash
mkdir -p ~/jars/ && cd ~/jars/

# Download Snowflake Connectors
curl -O https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.13/3.1.7/spark-snowflake_2.13-3.1.7.jar
curl -O https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.5/snowflake-jdbc-3.14.5.jar

# (Ensure your Delta Lake JARs are also in this directory)
```


2. **Update your `.env` file:**
Add the absolute paths to these downloaded files to your `.env` file:
```env
DELTA_JAR_PATH="/home/<your-user>/jars/delta-spark_2.13-4.0.0.jar"
DELTA_STORAGE_JAR_PATH="/home/<your-user>/jars/delta-storage-4.0.0.jar"
SNOWFLAKE_JAR_PATH="/home/<your-user>/jars/spark-snowflake_2.13-3.1.7.jar"
SNOWFLAKE_JDBC_JAR_PATH="/home/<your-user>/jars/snowflake-jdbc-3.14.5.jar"
```

### Data I/O Managers

The framework provides two types of data managers for handling I/O:

* **`DataManager` (General Purpose):** This is the primary, environment-aware I/O handler for your main storage backends (Unity Catalog and the local filesystem). It abstracts away the storage details, allowing a `Task` to simply `load_table()` or `save_table()`. It is provided to every `Task` instance.
* **`SnowflakeManager` (Specialized):** A specialized client for querying the Snowflake data warehouse. Tasks that need to ingest data from Snowflake (like the `DataLoader`) use this manager directly.

-----

## 3\. How Data Flows: The `Job` and the `DataLoader`

When you ask a `Job` to execute a list of `Task`s, data flows between them according to a clear set of rules.

1.  **The `Job` Analyzes Dependencies**
    When you call `job.load_tasks(...)`, the `Job` inspects the inputs and outputs of every `Task`. If it determines that a `Task` requires an input (e.g., `raw_customers`) that is not produced by another `Task` in the list, it automatically injects a special **`DataLoader`** task into the beginning of the execution plan.

2.  **The `DataLoader` Fetches Initial Data**
    The `DataLoader` is a standard `Task` that uses a **configurable, prioritized strategy** to fetch data from external sources. It dynamically tries different loading methods in a specific order until one succeeds.

    By default, it follows this fallback sequence:
    * **1. Datastore (`datastore`):** Checks for a data store path specified in the job configuration.
    * **2. Local File (`local_file`):** Looks for a local `.csv` or `.xlsx` file (highly useful for local testing).
    * **3. Snowflake Template (`snowflake`):** Instantiates a `SnowflakeManager` and searches for a `.sql` template to execute against Snowflake.
    * **4. Unity Catalog (`unity_catalog`):** Falls back to using the default `DataManager` to load the table directly from Unity Catalog.

    **Customizing the Priority**
    You can easily override this order in your job's YAML configuration using the `data_loader_priority` key. You can define a single global priority list, or specify different strategies on a per-table basis:

    ```yaml
    # Example: Custom priorities per table with a global fallback
    data_loader_priority:
      default:
        - snowflake
        - local_file
        - unity_catalog
      raw_customers:
        - datastore
        - unity_catalog
    ```

3.  **Subsequent Tasks Access the Data**
    When your own `Task` runs later in the plan, it asks its `DataManager` to `load_table('raw_customers', schema_type='working')`. The `DataManager` now finds this data in the in-memory store (put there by the `DataLoader`) and provides it directly.

This automatic injection of the `DataLoader` task is the core mechanism that enables seamless data ingestion for your pipelines.
-----

## 4\. A Developer's First Pipeline

Here is the essential workflow for creating and testing a new piece of logic.

### Step 1: Create a New Task

Create a Python file and define a class that inherits from `Task`. The `run_content` method contains your logic. Its parameters are the names of the input DataFrames you need, and it returns a dictionary where keys are the names of the output DataFrames.

```python
# In: src/my_project/tasks/standardize_country_codes.py
from pandas import DataFrame
from core.task import Task

class StandardizeCountryCodes(Task):

    def run_content(self, raw_customers: DataFrame) -> dict[str, DataFrame]:
        """Takes raw customer data and standardizes country codes."""
        customers_standardized = raw_customers.copy()
        customers_standardized['country_code'] = customers_standardized['country_code'].str.upper()

        return {"customers_standardized": customers_standardized}
```

### Step 2: Run Your Task Locally

Create a separate run script (e.g., `run.py`) to execute your new task. This allows you to test it in isolation.

```python
# In: run.py
from core.job import Job

# Create a job instance
job = Job(job_config="my_job_config")

# Tell the job to run only your new task
job.load_tasks(
    ["StandardizeCountryCodes"],
    fill_data_loaders=True # Set to True to auto-load 'raw_customers'
)

# Run it!
job.run()
```

When you execute this script, the `Job` will analyze the `StandardizeCountryCodes` task and see its dependency on `raw_customers`. Because `fill_data_loaders` is `True`, the `Job` will automatically add a `DataLoader` task to the execution plan.

-----

## 5\. Ensuring Data Quality

The framework includes a built-in, extensible data quality module to act as an automated quality gate, preventing bad data from being saved.

### How It Works

The quality check is an automatic step in the `Task` lifecycle.

1.  **Configure in YAML:** You enable and define checks in a `data_quality_checks` section within your task's configuration file.
2.  **Automatic Validation:** When your task's `save_data` method is called, it first passes the output DataFrame to the built-in `DataQualityValidator`.
3.  **The Quality Gate:** The validator runs all configured checks. If a check marked as `on_failure: fail` fails, it raises an exception, **stopping the pipeline before the bad data is saved**.

### Configuration Example

You can configure both static and dynamic data drift checks. The system is pluggable, using a **check registry** to find and execute the check classes.

```yaml
# In: config/tasks/FlagCashiers.yaml
data_quality_checks:
  # Global settings for the quality module
  enabled: true
  metrics_table_fqn: "my_catalog.quality.pipeline_metrics"
  results_log_table_fqn: "my_catalog.quality.check_results_log"

  # Define all checks for each output table
  checks:
    flagged_cashier_kpis:
      # A static check to ensure a column is never null
      - type: not_null
        column: "BEDIENER"
        on_failure: fail

      # A dynamic check to monitor for significant changes in row count
      - type: row_count_drift
        on_failure: warn
        comparison:
          type: tolerance_percent
          value: 30
```

All results are automatically logged to the `results_log_table_fqn` for auditing and reporting.

-----

## 6\. Configuration & Architecture ⚙️

The framework is driven by a hierarchy of YAML configuration files managed by the **`ConfigManager`**.

### Configuration Hierarchy

When a `Task` runs, the `ConfigManager` constructs its configuration by merging files in the following order (Winner Takes All):

1.  **Inheritance Defaults:** Configurations for parent classes (e.g., `tasks/StandardTask.yml`).
2.  **Task Defaults:** The configuration for the task itself (e.g., `tasks/MyTask.yml`).
3.  **Extra Configs:** Explicitly requested shared configs (defined in `extra_configs_to_load`).
4.  **Job Context:** The active job configuration (e.g., `jobs/end_of_year_report.yml`). This layer overrides everything else.

### Framework Configuration (`config/`)

The root `config/` directory contains global files that configure the framework's core behavior.

| File | Purpose |
| :--- | :--- |
| **`task_registry.yml`** | **The Phonebook.** Maps simple task names (e.g., `FlagCashiers`) to their Python module paths. Used by the `Job` to dynamically import classes. |
| **`job_registry.yml`** | **Deployment Definitions.** Defines the Databricks Workflows (Jobs) that will be deployed via CI/CD. Includes schedules, cluster assignments, and task lists. |
| **`storage.yml`** | **Storage Backends.** Configures Unity Catalog schemas/volumes and their local filesystem equivalents for offline development. |
| **`platform.yml`** | **Environment Settings.** Defines Databricks-specific settings like Service Principals, Secrets, Key Vault scopes, and API endpoints for each environment (Dev/QAS/Prod). |
| **`snowflake.yml`** | **Data Warehouse.** Connection parameters, warehouse sizes, and Jinja templates for SQL generation targeting Snowflake. |
| **`clusters.yml`** | **Compute Definitions.** Defines the Databricks clusters used by the deployment tool. Supports inheritance via a `base_cluster` template. |
| **`logging.yml`** | **Observability.** Standard Python logging configuration (handlers, formatters, log levels). |

-----

## 7\. Environment Variables

The framework's behavior can be controlled by the following environment variables:

  * **`BASE_PATH`**: The absolute path to the project's root directory. This is used to locate the `configs/` and `src/` directories. Defaults to the current working directory.
  * **`DATA_PATH`**: The path to the directory used by the `DataManager` for the local data cache (where Parquet files are saved and loaded from). Defaults to `./data`.
