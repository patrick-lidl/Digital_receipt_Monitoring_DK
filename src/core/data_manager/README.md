# Data Manager Submodule 💾

> This submodule is the data abstraction layer for the `core` framework. It provides a single, unified interface for all data I/O operations.

## Overall Design

This package follows the **Facade** design pattern. The main `DataManager` class acts as a simplified entry point that coordinates the more complex, low-level interactions handled by the backend managers: the **`UnityCatalogManager`** and the **`FilesystemManager`**.

This design decouples the application logic in a `Task` from the specifics of where data is stored, allowing for a clean separation of concerns and making the system easier to test and maintain.

The **`SnowflakeManager`** is treated as a separate, specialized client that is used directly by specific tasks (like the `DataLoader`) for complex query operations, rather than being part of the general-purpose facade.

-----

## Component Roles

This submodule is composed of several classes, each with a distinct responsibility:

| Class | File | Role |
| :--- | :--- | :--- |
| **`DataManager`** | `data_manager.py` | The **Facade** and single entry point for all `Task`s. It's a pure dispatcher that delegates calls to the appropriate backend manager. |
| **`BaseDataManager`**| `base.py` | The **Abstract Base Class** that defines the "contract"—a consistent set of methods that all backend managers must implement. |
| **`UnityCatalogManager`**| `unity_catalog_manager.py` | The backend manager for all interactions with the **Databricks Unity Catalog** (tables, volumes, schemas). |
| **`FilesystemManager`**| `filesystem_manager.py` | The backend manager for all interactions with the **local filesystem** (Parquet files, Delta tables, local directories). |
| **`DatabricksHandler`**| `databricks_handler.py` | A shared utility base class that provides access to the Spark session and environment context. |

-----

## Core Operations

The `DataManager` provides a clear, high-level API for all standard I/O operations.

  * **`load_table(data_name, table_type, schema_type)`**: Loads a structured table. The `schema_type` argument explicitly defines which area to load from (e.g., `'working'` or `'historical'`).
  * **`save_table(data, data_name)`**: Saves a DataFrame to the main **`working`** area, overwriting any existing data. This is for intermediate outputs.
  * **`archive_table(data, data_name)`**: Archives a DataFrame to the **`historical`** area in append mode. This is for creating a permanent record of final outputs.
  * **`save_file(writer_func, file_name, project_name)`**: Saves an unstructured file (like an HTML report or JSON config) to a project-specific, managed location (a local directory or a UC Volume).

### Quality Module Integration

The `DataManager` also serves as the I/O layer for the `quality` submodule, providing these specialized methods:

  * **`load_historical_metrics(...)`**: Fetches historical data for data drift checks.
  * FIXME (deprecated): **`persist_run_metrics(...)`**: Saves the calculated metrics from a successful run.
  * **`log_check_results(...)`**: Writes the outcome of every quality check to a persistent audit log.

-----

## Configuration

The behavior of this entire module is controlled by `configs/data_manager.yml`. This file is the single source of truth for all I/O connections and locations.

FIXME: This has to split into the files `configs/storage.yml` and `configs/snowflake.yml`.


```yaml
# In: config/data_manager.yml

unity_catalog:
  # The catalog is templated to switch between dev/qas/prod
  catalog: "uapc_{{deploymentStageShort}}_prj_cdspch_catalog"

  # Defines all the schemas the framework will manage
  schemas:
    working: "cdspch"
    historical: "historical"
    quality: "quality"

  # Defines all project-specific output volumes, which are created in the 'historical' schema
  volumes:
    digital_receipt_monitoring: "digital_receipt_monitoring_output"

# Configuration for the local filesystem backend
filesystem:
  # The root directory for local data that mimics UC tables
  local_data_path: "data/unity_catalog" # Changed as per your request

  # The root directory for local outputs (HTML, JSON) that mimic UC volumes
  local_output_path: "output"

# The Snowflake configuration remains separate for its specialized use case
snowflake:
  # ... (snowflake settings) ...
```
