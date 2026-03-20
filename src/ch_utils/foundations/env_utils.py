"""Handle basic differences between databricks and local."""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def on_databricks() -> bool:
    """
    Returns True if the code is running in a Databricks environment.

    Checks for the existence of the '/databricks' directory or a
    Databricks-specific environment variable.
    """
    return Path("/databricks").is_dir() or "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark() -> "SparkSession":
    """Gets or creates a SparkSession."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def get_dbutils(spark: "SparkSession") -> Optional[Any]:
    """
    Returns the DBUtils object.

    - In a Databricks notebook environment, it uses the dbruntime library.
    - For Databricks Connect, it uses pyspark.dbutils.
    - Returns None if not on Databricks.

    Args:
        spark: The active SparkSession.

    Returns:
        The DBUtils object or None.
    """
    if on_databricks():
        try:
            # Standard way to get dbutils in a Databricks notebook
            from dbruntime.dbutils import get_dbutils

            return get_dbutils(spark)
        except ImportError:
            # Fallback for Databricks Connect or older runtimes
            from pyspark.dbutils import DBUtils

            return DBUtils(spark)
    return None
