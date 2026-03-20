import os
from typing import Any, Optional

from ch_utils.foundations.config_manager import Config, load_configs
from ch_utils.foundations.env_utils import get_dbutils, get_spark, on_databricks


class KeyVaultManager:
    """A centralized manager for retrieving secrets from Databricks Key Vault or local .env files."""

    def __init__(self, databricks_config: Optional[Config] = None):
        """Initializes the manager by loading the secret manifest."""
        if not databricks_config:
            databricks_config = load_configs("platform")

        # Load config parameters
        self._secret_scope: str = databricks_config.get("secret_scope", "")
        self._secrets_map: Config = databricks_config.get("secrets", Config())

        # Store env status and dbutils upon initialization
        self._on_databricks = on_databricks()
        self._dbutils: Any = get_dbutils(get_spark()) if self._on_databricks else None

        # Load .env file once for local development
        if not self._on_databricks:
            from dotenv import load_dotenv

            load_dotenv(os.environ.get("DOT_ENV_PATH"))

    def get_secret(self, logical_name: str) -> str:
        """
        Retrieves a secret using its logical name defined in platform.yml.

        Args:
            logical_name: The simple, logical name for the secret (e.g., "my_api_key").

        Returns:
            The secret's value as a string.

        Raises:
            KeyError: If the logical_name is not found in the configuration.
            ValueError: If trying to access a Databricks secret without a scope.
        """
        if logical_name not in self._secrets_map:
            raise KeyError(f"Secret '{logical_name}' not defined in platform.yml")

        secret_config = self._secrets_map[logical_name]

        if self._on_databricks:
            if not self._secret_scope:
                raise ValueError("A global 'secret_scope' must be defined in platform.yml for Databricks execution.")
            return self._dbutils.secrets.get(scope=self._secret_scope, key=secret_config["key"])
        else:
            return os.environ[secret_config["env_var"]]
