# Configuration Architecture

This directory acts as the central nervous system for the data framework. It allows the Python code to remain stateless and environment-agnostic while the configuration files handle infrastructure, parameterization, and deployment details.

All configuration loading is handled by the **`ConfigManager`** class, which abstracts away file reading, caching, and the complex merging logic described below.

---

## 1. Global Registries & Infrastructure

These files define the "Physical Layer" of the platform—where code lives, where data lives, and how compute is provisioned. They are loaded once per run.

| File | Purpose |
| :--- | :--- |
| **`task_registry.yml`** | **The Phonebook.** Maps simple task names (e.g., `FlagCashiers`) to their Python module paths. Used by the `Job` to dynamically import classes. |
| **`job_registry.yml`** | **Deployment Definitions.** Defines the Databricks Workflows (Jobs) that will be deployed via CI/CD. Includes schedules, cluster assignments, and task lists. |
| **`storage.yml`** | **Storage Backends.** Configures Unity Catalog schemas/volumes and their local filesystem equivalents for offline development. |
| **`platform.yml`** | **Environment Settings.** Defines Databricks-specific settings like Service Principals, Secrets, Key Vault scopes, and API endpoints for each environment (Dev/QAS/Prod). |
| **`snowflake.yml`** | **Data Warehouse.** Connection parameters, warehouse sizes, and Jinja templates for SQL generation targeting Snowflake. |
| **`clusters.yml`** | **Compute Definitions.** Defines the Databricks clusters used by the deployment tool. Supports inheritance via a `base_cluster` template. |
| **`logging.yml`** | **Observability.** Standard Python logging configuration (handlers, formatters, log levels). |

---

## 2. Parameter Configuration (`tasks/` & `jobs/`)

This framework uses a powerful **Hierarchical Configuration System** to determine the parameters for any given task execution. This allows you to define sensible defaults once and override them only when necessary.

### The Merge Strategy: "Winner Takes All"

When the `ConfigManager` builds the configuration for a task (e.g., `MyTask`), it loads multiple files and merges them in a strict priority order. **Later layers override earlier layers.**



1.  **Inheritance Defaults (Lowest Priority):**
    If `MyTask` inherits from `StandardTask`, the manager first loads `config/tasks/StandardTask.yml`. This allows you to set broad defaults (e.g., "All validation tasks have a timeout of 60s").

2.  **Task Defaults:**
    The manager loads `config/tasks/MyTask.yml`. This contains parameters specific to the logic of that class.

3.  **Job Context (Highest Priority):**
    If the task is running as part of a specific job (e.g., `EndOfWeekReport`), the manager loads `config/jobs/end_of_week_report.yml`. This layer can override *any* parameter from the previous layers for this specific run.

### Example

**Task Default (`config/tasks/FlagCashiers.yml`):**
```yaml
threshold: 10
country: "CH"
````

**Job Context (`config/jobs/german_analysis.yml`):**

```yaml
# Override country for this job only
country: "DE"
```

**Final Result injected into Task:**

```json
{
  "threshold": 10,
  "country": "DE"
}
```

-----

## 3\. Special Patterns

### The `base_config` Pattern (Inner Flattening)

Inside Task or Job configuration files, you may see a key named `base_config`.

  * **Purpose:** Allows you to group default values neatly.
  * **Behavior:** The `ConfigManager` flattens these keys into the root dictionary *unless* the key was already defined by a higher-priority layer.

### The `base_cluster` Pattern (Sibling Distribution)

In registry files like `clusters.yml` and `job_registry.yml`, you will see a `base_cluster` or `base_config` key.

  * **Purpose:** Acts as a template for other entries in the same file.
  * **Behavior:** The manager copies these settings into every other entry in the file. For example, `base_cluster` defines the Spark Runtime version, and specific clusters (like `automation_cluster`) only define their size.

-----

## 4\. Environment Variables

The configuration system relies on two key environment variables to locate files:

  * **`BASE_PATH`**: The absolute path to the project root. Used to locate the `config/` directory. Defaults to the current working directory.
  * **`DATA_PATH`**: The path used by `storage.yml` for local data emulation. Defaults to `./data`.
