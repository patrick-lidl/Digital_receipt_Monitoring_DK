# CH Utils Package 🛠️

> This package is the foundational utility library for the entire monorepo. It provides a collection of shared, reusable functions and classes that are used across all projects and core libraries.

## Overview & Philosophy

The `ch_utils` package is designed to be a **universal toolkit**. Its primary goal is to provide robust, well-documented, and consistent solutions to common problems (like configuration, secret management, and data manipulation) to keep our main application code clean and DRY (Don't Repeat Yourself).

A key architectural principle of this library is that it must remain **lightweight**. Core packages like `deployment` depend on it, and they cannot be burdened with heavy, optional dependencies like `pandas` or `pyspark`. This is achieved through a careful submodule structure and a "lazy loading" pattern for imports.

## Package Structure

The library is organized into submodules based on functionality:

| Submodule | Description |
| :--- | :--- |
| `foundations/` | Low-level, critical utilities for environment detection, configuration, and secret management. These have minimal dependencies. |
| `data/` | Helper functions specifically for `pandas` and `pyspark` DataFrames. |
| `time/` | A collection of utilities for date and time manipulation. |
| `io/` | Functions for handling file input and output operations, like creating unique filenames. |
| `connectors/` | Self-contained clients for interacting with external services like MyAPI or Snowflake. |
| `domain/` | Reusable classes that encapsulate business-specific logic, like the `ItemHierarchy`. |
| `reporting/` | Base classes and helpers for generating standardized HTML reports. |
| `common/` | Other general-purpose utilities for strings, timeouts, etc. |

## Important: How to Add Functions with Heavy Dependencies (Lazy Loading)

To keep this library lightweight, any function that requires a heavy external dependency (like `pandas`, `pyspark`, `jinja2`, etc.) **must** use a lazy import pattern. This ensures the library can be imported without forcing the installation of these heavy packages.

The pattern has two parts:

1.  **Type Hinting**: Place the import for type annotations inside an `if TYPE_CHECKING:` block. This makes the types available to `mypy` and your IDE without triggering a runtime import.
2.  **Runtime Import**: Place the import *inside* the function that uses the library. This guarantees the library is only loaded if and when that specific function is actually called.

### Example: Adding a New PySpark Helper

Here is the complete, correct pattern for adding a new function to `data/pyspark_helpers.py`:

```python
# in src/ch_utils/data/pyspark_helpers.py

from typing import TYPE_CHECKING

# 1. Import for type hints only
if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F

def my_new_spark_function(df: "DataFrame") -> "DataFrame":
    """
    This is a docstring for the new function.
    """
    # 2. Import for runtime logic inside the function
    from pyspark.sql import functions as F

    return df.withColumn("new_col", F.lit(1))
```

By following this pattern, you ensure that `ch_utils` remains a truly universal and lightweight foundation for all other packages in the monorepo.
