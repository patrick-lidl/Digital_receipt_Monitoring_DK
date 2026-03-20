import logging
from pathlib import Path
from typing import Any, Dict, Literal, Type, Union

from pandas.core.api import DataFrame as PandasDataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from ch_utils.foundations.key_vault_manager import KeyVaultManager
from ch_utils.foundations.sql_renderer import SqlRenderer
from core.config_manager import Config, load_configs
from core.databricks_handler import DatabricksHandler


class SnowflakeManager(DatabricksHandler):
    """
    Manages all data loading from and saving to Snowflake.

    This class handles connecting to Snowflake, executing queries, and writing
    DataFrames back to tables. It is configured via the `snowflake` section
    in `configs/snowflake.yml`.
    """

    def __init__(self, config: "Config"):
        """Initializes the SnowflakeManager with connection configurations."""
        super().__init__()
        self._config = config
        self._keyvault = KeyVaultManager()
        self._sql_renderer = SqlRenderer()

    def load_from_rendered_sql_template(
        self, template_path: Path, params: Dict[str, Any], table_type: Type = SparkDataFrame
    ) -> Union[PandasDataFrame, SparkDataFrame]:
        """Renders a Jinja SQL template with given parameters and executes it."""
        logging.debug(f"SQL parameters passed as arguments: {params}")
        final_params = {
            **self._config.get("snowflake_params", {}).get("common", {}),
            **self._config.get("snowflake_params", {}).get(template_path.stem, {}),
            **params,
        }
        logging.debug(f"Final SQL parameters: {final_params}")
        query = self._sql_renderer.render_query(template_path, final_params)
        logging.debug(f"Final SQL query:\n{query}")
        return self.load_from_snowflake_query(query, table_name=template_path.stem, table_type=table_type)

    def load_from_snowflake_query(
        self, query: str, table_name: str, table_type: Type = SparkDataFrame
    ) -> Union[PandasDataFrame, SparkDataFrame]:
        """Executes a SQL query string against Snowflake and returns the result."""
        logging.info(f"Loading data for '{table_name}' from Snowflake.")
        options = self._get_connection_options_for_table(table_name)

        logging.info(f"Option keys: {', '.join([str(key) for key in options.keys() if key is not None])}")

        spark_df = self.spark.read.format("snowflake").options(**options).option("query", query).load()

        if self._config.get("force_column_names_to_lower", False):
            spark_df = spark_df.toDF(*[c.lower() for c in spark_df.columns])

        if table_type == PandasDataFrame:
            return spark_df.toPandas()
        return spark_df

    def save_to_snowflake(self, data: Union[PandasDataFrame, SparkDataFrame], table_name: str) -> None:
        """Saves a Pandas or Spark DataFrame to a Snowflake table."""
        if isinstance(data, PandasDataFrame):
            spark_df = self.spark.createDataFrame(data)
            self._save_spark_table(spark_df, table_name)
        elif isinstance(data, SparkDataFrame):
            self._save_spark_table(data, table_name)
        else:
            raise TypeError(f"Unsupported data type for saving to Snowflake: {type(data)}")

    # ------------- Private Helper Methods -------------

    def _get_connection_options_for_table(self, table_name: str) -> Dict[str, str]:
        """Builds a dictionary of connection options for a given table."""
        db_map = self._config.get("database_by_table", {})
        default_db = self._config.get("default_database")
        connection_name = db_map.get(table_name, default_db)

        return self._get_connection_options(connection_name)

    def _get_connection_options(self, connection_name: str) -> Dict[str, str]:
        """Builds a dictionary of connection options for a given connection."""
        if not connection_name:
            raise ValueError(f"No Snowflake connection defined for '{connection_name}'.")

        base_settings = self._config.get("base_connection", {})
        specific_settings = self._config.get("connection_settings", {}).get(connection_name, {})

        if not specific_settings:
            raise ValueError(f"Connection settings for '{connection_name}' not found in snowflake.yml.")

        # Merge the base and specific settings. Specific keys will override base keys.
        merged_settings = base_settings | specific_settings

        secrets = {
            key: self._keyvault.get_secret(logical_name)
            for key, logical_name in merged_settings.get("secrets", {}).items()
        }

        # Build the final parameters from the merged settings
        base_params = {
            "url": merged_settings.get("url"),
            "account": merged_settings.get("account"),
            "warehouse": merged_settings.get("warehouse"),
            "role": merged_settings.get("role"),
            "database": merged_settings.get("database"),
            "schema": merged_settings.get("schema"),
            **secrets,
        }

        # The Spark connector uses 'sf' + capitalized key for options
        return {"sf" + key.capitalize(): value for key, value in base_params.items() if value is not None}

    def _save_spark_table(self, df: SparkDataFrame, table_name: str) -> None:
        """Saves a Spark DataFrame to Snowflake."""
        options = self._get_connection_options(table_name)
        options["dbtable"] = table_name
        if options.get("sfSchema"):
            logging.info(f"Saving Spark DataFrame to Snowflake table: {table_name}")
            df.write.format("snowflake").options(**options).mode("overwrite").save()
            logging.info(f"Successfully saved to {table_name}")
        else:
            logging.warning(f"The table {table_name} cannot be written to {options['sfDatabase']}")


def load_from_snowflake(
    sql_query: str, source: Literal["uapc", "ssbi"] = "uapc", config_path: str = "snowflake"
) -> PandasDataFrame:
    """Executes a SQL query string against Snowflake and returns the result."""
    sf_config = load_configs(config_path)
    sm = SnowflakeManager(sf_config)

    if source == "uapc":
        options = sm._get_connection_options("UAPC_CDSPCH_CH")
    elif source == "ssbi":
        options = sm._get_connection_options("SSBI_TEC_CH")
    else:
        raise ValueError(f"Invalid source: {source}")

    spark_df = sm.spark.read.format("snowflake").options(**options).option("query", sql_query).load()

    if sm._config.get("force_column_names_to_lower", False):
        spark_df = spark_df.toDF(*[c.lower() for c in spark_df.columns])

    return spark_df
