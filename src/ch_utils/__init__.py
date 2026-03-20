"""
A collection of shared, reusable utility functions for Lidl Switzerland.

This package provides a wide range of helpers organized into submodules:
- foundations: Low-level utilities for environment, config, and secrets.
- data: Helpers for pandas and PySpark DataFrames.
- time: Date and time manipulation functions.
- io: Utilities for file input/output.
- connectors: Clients for external APIs and databases.
- domain: Business-specific helper classes.
- reporting: Base classes for generating reports.
- common: Other general-purpose utilities.
"""

__all__ = [
    # foundations
    "Config",
    "load_configs",
    "load_config_with_base",
    "KeyVaultManager",
    "SqlRenderer",
    "on_databricks",
    "get_spark",
    "get_dbutils",
    # data
    "reindex_spark_df",
    "col_as_list",
    # time
    "get_year_cw",
    "get_year_cw_list",
    "days_in_range",
    "weeks_in_range",
    "months_in_range",
    "get_previous_month",
    "get_previous_week_monday",
    "get_previous_week_sunday",
    "nth_day_this_month",
    "first_day_this_month",
    "first_day_previous_month",
    "last_day_previous_month",
    # io
    "get_unique_filename",
    "save_json_to_path",
    # connectors
    "MyApiClient",
    "load_snowflake_data_using_query",
    # domain
    "ItemHierarchy",
    # reporting
    "ReportGeneratorBase",
    # common
    "camel_to_snake",
    "snake_to_camel",
    "run_with_timeout",
    "TimeoutException",
    "get_client_id_from_code",
    "list_wo_param",
    "try_round",
    "is_subset",
    "convert_to_percentages",
]

# --- Import from submodules to expose them at the top level ---

# foundations
# common
from .common.string_utils import camel_to_snake, snake_to_camel
from .common.timeout_handler import TimeoutException, run_with_timeout
from .common.utils import convert_to_percentages, get_client_id_from_code, is_subset, list_wo_param, try_round

# connectors
from .connectors.load_snowflake_data import load_snowflake_data_using_query
from .connectors.my_api_client import MyApiClient

# data
from .data.pyspark_helpers import col_as_list, reindex_spark_df

# domain
from .domain.item_hierarchy import ItemHierarchy
from .foundations.config_manager import Config, load_config_with_base, load_configs
from .foundations.env_utils import get_dbutils, get_spark, on_databricks
from .foundations.key_vault_manager import KeyVaultManager
from .foundations.sql_renderer import SqlRenderer

# io
from .io.io_utils import get_unique_filename, save_json_to_path

# reporting
from .reporting.report_generator_base import ReportGeneratorBase

# time
from .time.date_utils import (
    days_in_range,
    first_day_previous_month,
    first_day_this_month,
    get_previous_month,
    get_previous_week_monday,
    get_previous_week_sunday,
    get_year_cw,
    get_year_cw_list,
    last_day_previous_month,
    months_in_range,
    nth_day_this_month,
    weeks_in_range,
)
