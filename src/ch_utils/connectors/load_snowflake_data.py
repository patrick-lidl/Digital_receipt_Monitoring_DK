"""
Functions for loading data from Snowflake database.

> NOTE: The configuration is hard-coded here to allow packages to work independantly.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Literal

# Use TYPE_CHECKING for static analysis without runtime cost
if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from ch_utils.foundations.config_manager import Config
from ch_utils.foundations.env_utils import get_spark
from ch_utils.foundations.key_vault_manager import KeyVaultManager

__all__ = [
    "get_snowflake_options",
    "load_snowflake_data_using_query",
]

URL = "sit_ino.west-europe.privatelink.snowflakecomputing.com"
SECRET_SCOPE = "uapc-prj-kv-secret-scope"


def _get_snowflake_options_uapc(
    country_code: str, project_id: str, warehouse_size: Literal["L", "S"] = "S"
) -> Dict[str, str]:
    """Get Snowflake connection options for the UAPC database."""
    # Use the KeyVaultManager to securely fetch secrets
    keyvault_config = Config(
        {
            "secret_scope": SECRET_SCOPE,
            "secrets": {
                "uapc_snowflake_user": {"key": "SnowflakeTechUserName", "env_var": "SNOWFLAKE_USER"},
                "uapc_snowflake_password": {"key": "SnowflakeTechUserSecret", "env_var": "SNOWFLAKE_PASSWORD"},
            },
        }
    )
    keyvault = KeyVaultManager(keyvault_config)
    my_user = keyvault.get_secret("uapc_snowflake_user")
    my_pass = keyvault.get_secret("uapc_snowflake_password")

    warehouse_addon = "" if warehouse_size == "L" else "_S"

    return {
        "sfUrl": URL,
        "sfUser": my_user,
        "sfPassword": my_pass,
        "sfRole": f"R_UAPC_{project_id}_{country_code}_DBX".upper(),
        "sfWarehouse": f"WH_UAPC_{project_id}_{country_code}{warehouse_addon}".upper(),
        "sfDatabase": "",
    }


def _get_snowflake_options_ssbi(country_code: str, project_id: str) -> Dict[str, str]:
    """Get Snowflake connection options for the SSBI database."""
    # Use the KeyVaultManager to securely fetch secrets
    keyvault_config = Config(
        {
            "secret_scope": SECRET_SCOPE,
            "secrets": {
                "ssbi_snowflake_user": {"key": "SsbiCoSnowflakeTechUserName", "env_var": "SSBI_SNOWFLAKE_USER"},
                "ssbi_snowflake_password": {
                    "key": "SsbiCoSnowflakeTechUserSecret",
                    "env_var": "SSBI_SNOWFLAKE_PASSWORD",
                },
            },
        }
    )
    keyvault = KeyVaultManager(keyvault_config)
    my_ssbiuser = keyvault.get_secret("ssbi_snowflake_user")
    my_ssbipass = keyvault.get_secret("ssbi_snowflake_password")

    return {
        "sfUrl": URL,
        "sfUser": my_ssbiuser,
        "sfPassword": my_ssbipass,
        "sfRole": f"R_SSBI_LIDL_CO_INT_{project_id}_{country_code}_DBX".upper(),
        "sfWarehouse": f"WH_SSBI_LIDL_CO_INT_{project_id}_{country_code}_XS".upper(),
        "sfDatabase": "",
    }


def get_snowflake_options(
    country_code: str, project_id: str, source: Literal["uapc", "ssbi"] = "uapc"
) -> Dict[str, str]:
    """
    Get Snowflake connection options for PySpark.

    Args:
        country_code: ISO country code (e.g., 'CH').
        project_id: UAPC project ID.
        source: Snowflake database to use ('uapc' or 'ssbi').

    Returns:
        A dictionary of Snowflake connection options.
    """
    if source == "uapc":
        return _get_snowflake_options_uapc(country_code, project_id)
    elif source == "ssbi":
        return _get_snowflake_options_ssbi(country_code, project_id)
    else:
        raise ValueError(f"Invalid source: {source}. Must be either 'uapc' or 'ssbi'")


def _cast_columns(data: "DataFrame", column_names: List[str], column_type: str) -> "DataFrame":
    """Casts selected columns of a DataFrame to a specified data type."""
    # Lazy import for performance
    from pyspark.sql import functions as F

    for col_name in column_names:
        try:
            # Ensure column names are lowercase for consistency
            lower_col_name = col_name.lower()
            data = data.withColumn(lower_col_name, F.col(lower_col_name).cast(column_type))
        except Exception as exc:
            raise TypeError(f"Unsupported data type '{column_type}' for column '{col_name}'") from exc
    return data


def _cast_dataframe(data: "DataFrame", config: Dict[str, List[str]]) -> "DataFrame":
    """Casts DataFrame columns according to a given column config."""
    for key, value in config.items():
        data_type = key.lower().split("_")[0]  # Simplified logic to get type from key
        data = _cast_columns(data, value, data_type)
    return data


def _cast_snowflake_data_columns(data: "DataFrame", **columns_types: Any) -> "DataFrame":
    """Iterates the dataframe and casts the columns according to the given instructions."""
    # Cast all column names to lowercase
    data = data.toDF(*[c.lower() for c in data.columns])
    # Type cast columns
    data = _cast_dataframe(data, columns_types)
    return data


def load_snowflake_data_using_query(
    sql_query: str,
    country_code: str = "CH",
    project_id: str = "CDSPCH",
    source: Literal["uapc", "ssbi"] = "uapc",
    cast_columns: bool = False,
    **columns_types: Any,
) -> "DataFrame":
    """
    Execute SQL query to load data from Snowflake and cast columns according to specified type definitions.

    Args:
        sql_query (str): SQL Query.
        country_code (str): ISO country code, e.g. Stiftung=INT, Germany=DE, Switzerland=CH etc.
        project_id (str): UAPC project ID (e.g. mefes).
        source (Literal["uapc", "ssbi"]): Snowflake database to use.
        cast_columns (bool): Whether to cast columns.
        columns_types (Dict, optional): Type definitions for columns.

    Returns:
        DataFrame: Loaded and transformed dataframe.

    Example:
        >>> load_snowflake_data_using_query(
                sql_query="SELECT * FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_T_CLIENT",
                country_code='INT',
                project_id="nfdf",
                int_columns=["client_id", "client_online_fg", "curr_sid"],
                boolean_columns=["deleted_fg"],
            )
        DataFrame[client_id: int, client_cd: string, client_name: string, client_online_fg: int, country_cd: string,
        curr_sid: int, curr_cd: string, deleted_fg: boolean]

    """
    spark = get_spark()
    options = get_snowflake_options(country_code, project_id, source)
    data = spark.read.format("snowflake").options(**options).option("query", sql_query).load()
    if cast_columns:
        data = _cast_snowflake_data_columns(data, **columns_types)
    return data
