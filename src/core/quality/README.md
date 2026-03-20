# Quality Submodule ✅

> This submodule provides an automated, extensible data quality framework for the `core` package. It acts as a quality gate, ensuring that data produced by a `Task` meets predefined standards before it is saved.

## Overall Design

The framework is designed to be **pluggable** and **configuration-driven**. The central `DataQualityValidator` orchestrates the process, but the actual validation logic is contained in small, independent **check classes**. A **check registry** allows developers to easily add new, custom checks to the library without modifying the core validation logic.

The system is fully integrated with the `Task` lifecycle and is automatically triggered by the `save_data` method.

### Component Roles

| Class/File | Location | Role |
| :--- | :--- | :--- |
| **`DataQualityValidator`** | `validator.py` | The **Orchestrator**. It reads the YAML config, uses the registry to find and run checks, and processes the results. |
| **`BaseCheck`** | `checks/base.py` | The **Abstract "Contract"**. A blueprint that all check classes must follow, ensuring a consistent interface. |
| **`CheckResult`** | `checks/base.py` | The **Standardized Output**. A dataclass that holds the outcome of a check (status, message, metric value). |
| **`CHECK_REGISTRY`** | `checks/registry.py` | The **"Plugin Manager"**. A dictionary that maps the `type` name from the YAML config to a specific check class. |
| **Concrete Checks** | `checks/*.py` | The **Implementations**. Individual classes (e.g., `NotNullCheck`, `RowCountDriftCheck`) that contain the actual validation logic. |

-----

## How to Use

Using the quality module involves a single step: configuring it in your task's YAML file. The `Task` framework handles the execution automatically.

### Configure the Checks in YAML

In your task's configuration file (e.g., `config/tasks/MyTask.yaml`), add a `data_quality_checks` section.

```yaml
data_quality_checks:
  # A master switch to enable or disable all quality checks for this task.
  enabled: true

  # (Optional) Declare custom, project-specific checks.
  # The key is the name used in 'type', and the value is the full Python import path.
  custom_checks:
    kpi_red_flag: "digital_receipt_monitoring.checks.KpiRedFlagCheck"

  # Define the list of checks to run for each output DataFrame.
  checks:
    my_output_table:
      # 1. A built-in static check
      - type: not_null
        name: "User ID must be present" # Optional descriptive name
        column: "user_id"
        on_failure: fail # 'fail' will stop the pipeline; 'warn' will only log.

      # 2. A built-in drift check
      - type: row_count_drift
        on_failure: warn
        comparison:
          type: tolerance_percent
          value: 20

      # 3. A custom check defined above
      - type: kpi_red_flag
        on_failure: fail
```

The `DataQualityValidator` is automatically called by the `Task.save_data` method. If any check with `on_failure: fail` fails, an exception is raised, and the pipeline stops. All results are logged by the `DataManager`.

-----

## How to Add a New Check

The framework is designed to be easily extended. To add a new, reusable check:

**1. Create the Check Class:**
Create a new class in `core/quality/checks/static.py` (for stateless checks) or `drift.py` (for stateful checks) that inherits from `BaseCheck` and implements the `execute` method.

```python
# In static.py
class IsPositiveCheck(BaseCheck):
    def __init__(self, check_config: Dict):
        super().__init__(check_config)
        self.column = self.check_config.get("column")

    def execute(self, data: SparkDataFrame, **kwargs) -> CheckResult:
        negative_count = data.where(F.col(self.column) < 0).count()
        if negative_count > 0:
            return self._fail(f"Found {negative_count} negative values.", negative_count)
        return self._pass("All values are positive.", 0)
```

**2. Register the Check:**
Add your new class to the `CHECK_REGISTRY` dictionary.

```python
# In registry.py
CHECK_REGISTRY = {
    # ... other checks ...
    "is_positive": IsPositiveCheck,
}
```

You can now use `type: is_positive` in any of your YAML configuration files.
