import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from ch_utils.foundations.config_manager import load_configs
from ch_utils.foundations.env_utils import get_dbutils, on_databricks
from ch_utils.foundations.key_vault_manager import KeyVaultManager


class DatabricksHandler:
    """
    Provides a consistent interface for a SINGLE, SHARED Spark session and other Databricks objects.

    This handler uses class-level variables to ensure that all instances share the
    same Spark session, preventing context isolation issues.
    """

    # Use class-level variables for a shared, singleton-like state
    _spark_session: Optional[SparkSession] = None

    def __init__(self):
        """Initializes the handler and loads environment variables from the `.env` file."""
        load_dotenv()

        self.databricks_config = load_configs("platform")
        self.keyvault = KeyVaultManager()

    @property
    def on_databricks(self) -> bool:
        """
        Checks if the code is running within a Databricks environment.

        Returns:
            bool: True if running on Databricks, False otherwise.
        """
        return on_databricks()

    @property
    def spark(self) -> SparkSession:
        """
        Retrieves or creates the single, shared SparkSession.

        On first access locally, it creates a Spark session configured for Delta Lake
        and Snowflake. On Databricks, it retrieves the existing active session.

        Returns:
            SparkSession: The active, shared SparkSession.
        """
        if DatabricksHandler._spark_session is None:
            logging.info("Creating shared Spark session.")
            if self.on_databricks:
                DatabricksHandler._spark_session = SparkSession.builder.getOrCreate()
            else:
                DatabricksHandler._spark_session = self._setup_local_spark_session()

            # Set the log level to ERROR to hide WARN messages from the console
            DatabricksHandler._spark_session.sparkContext.setLogLevel("ERROR")

        return DatabricksHandler._spark_session

    @classmethod
    def _setup_local_spark_session(cls) -> SparkSession:
        """
        Ensures the shared SparkSession is configured for Delta Lake and Snowflake.

        Note on Efficiency:
            Spark evaluates lazily. Providing JAR paths to `spark.jars` simply adds
            them to the JVM classpath. No actual network connections or significant
            memory allocations occur until a specific connector (like format("snowflake"))
            is explicitly used in the code.
        """
        logging.info("Upgrading shared Spark session with local JARs (Delta + Snowflake).")
        if cls._spark_session:
            cls._spark_session.stop()

        # Fetch environment variables for manual JAR paths
        delta_jar = os.getenv("DELTA_JAR_PATH")
        delta_storage_jar = os.getenv("DELTA_STORAGE_JAR_PATH")
        snowflake_jar = os.getenv("SNOWFLAKE_JAR_PATH")
        snowflake_jdbc_jar = os.getenv("SNOWFLAKE_JDBC_JAR_PATH")

        # Validate Delta JARs (assuming these are strict requirements for your app)
        if not delta_jar or not delta_storage_jar:
            logging.warning(
                "Cannot create local session: DELTA_JAR_PATH and "
                "DELTA_STORAGE_JAR_PATH environment variables must be set."
            )

        # Validate Snowflake JARs
        if not snowflake_jar or not snowflake_jdbc_jar:
            logging.warning("Snowflake JAR paths are missing. Snowflake ingestion will fail locally.")

        # Filter out None values and combine into a comma-separated string
        jar_list = [delta_jar, delta_storage_jar, snowflake_jar, snowflake_jdbc_jar]
        combined_jar_paths = ",".join(path for path in jar_list if path)

        return (
            SparkSession.builder.appName("LocalDataPipelineApp")
            .master("local[*]")
            .config("spark.jars", combined_jar_paths)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

    def update_spark_config_for_datastore(self) -> None:
        """Configures the Spark session for Azure ADLS OAuth authentication."""
        # Load configuration
        azure_client_id = self.keyvault.get_secret("azure_client_id")
        azure_secret = self.keyvault.get_secret("azure_secret")
        storage_account = self.databricks_config.get("azure_storage_account")
        tenant_id = self.databricks_config.get("azure_tenant_id")

        if not storage_account or not tenant_id:
            raise ValueError("You must set the configuration in `platform.yml`.")

        self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        self.spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        self.spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", azure_client_id
        )
        self.spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", azure_secret
        )
        self.spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        )

    @property
    def dbutils(self) -> Optional[Any]:
        """
        Retrieves the dbutils object if running on Databricks.

        Returns:
            Optional[Any]: The dbutils instance, or None if running locally.
        """
        if self.on_databricks:
            return get_dbutils(self.spark)
        return None

    @property
    def current_user(self) -> str:
        """
        Retrieves the current user's email as registered in Databricks.

        Returns:
            str: The email address of the current user.

        Raises:
            RuntimeError: If called outside of a Databricks environment.
        """
        if self.on_databricks:
            return self.spark.sql("SELECT current_user() AS user").collect()[0]["user"]
        raise RuntimeError("The current user can only be determined when running on Databricks.")
