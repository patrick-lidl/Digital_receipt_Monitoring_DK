import logging
import os
import time
from typing import Any, Dict, Optional

from ch_utils.foundations.config_manager import load_configs
from ch_utils.foundations.key_vault_manager import KeyVaultManager


class MyApiClient:
    """
    A client to interact with a MyAPI endpoint.

    This class handles authentication, environment-specific URLs, token caching,
    session management, and robust error handling for a specific API.
    """

    def __init__(self, api_name: str):
        """
        Initializes the MyApiClient.

        It fetches credentials using the KeyVaultManager and loads the correct
        API URLs from the platform.yml config based on the
        DEPLOYMENT_STAGE_SHORT environment variable ('e' for dev, 'p' for prod).

        Args:
            api_name (str): The name of the API to connect to, as defined
                            in the `my_api_urls` section of platform.yml
                            (e.g., "lchdatagate").
        """
        import requests

        # 1. Fetch credentials securely
        keyvault = KeyVaultManager()
        self.client_id = keyvault.get_secret("my_api_key")
        self.client_secret = keyvault.get_secret("my_api_secret")

        if not self.client_id or not self.client_secret:
            raise ValueError("Client ID and Client Secret could not be retrieved.")

        # 2. Load URLs using the new helper method
        self.api_url, self.token_url = self._load_urls_from_config(api_name)

        stage = os.environ.get("DEPLOYMENT_STAGE_SHORT", "e").lower()
        logging.info(f"MyApiClient initialized for API '{api_name}' in stage '{stage}'. Using URL: {self.api_url}")

        self._session = requests.Session()
        self._bearer_token: Optional[str] = None
        self._token_expiry_time: float = 0

    def _load_urls_from_config(self, api_name: str) -> tuple[str, str]:
        """Loads and returns the API and token URLs from config for the current stage."""
        config = load_configs("platform")

        # Determine environment stage
        stage = os.environ.get("DEPLOYMENT_STAGE_SHORT", "e").lower()

        # Get API-specific URLs
        api_urls = config.my_api_urls.get(api_name)
        if not api_urls or stage not in api_urls:
            raise KeyError(f"URL for API '{api_name}' and stage '{stage}' not found in platform.yml")

        # Get token URL
        token_urls = config.my_api_urls.get("token")
        if not token_urls or stage not in token_urls:
            raise KeyError(f"Token URL for stage '{stage}' not found in platform.yml")

        return api_urls[stage], token_urls[stage]

    def _get_bearer_token(self) -> str:
        """
        Retrieves and caches the bearer token.

        A new token is fetched only if the current one is expired or does not exist.
        """
        from requests.exceptions import RequestException

        # Check if the token is still valid (with a 60-second buffer)
        if self._bearer_token and time.time() < self._token_expiry_time - 60:
            return self._bearer_token

        logging.info(f"Fetching a new bearer token from {self.token_url}.")
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        try:
            response = self._session.post(self.token_url, headers=headers, data=data, timeout=30)
            response.raise_for_status()  # Raises an HTTPError for bad responses

            token_data = response.json()
            access_token = token_data["access_token"]
            expires_in = int(token_data.get("expires_in", 3600))

            self._bearer_token = f"Bearer {access_token}"
            self._token_expiry_time = time.time() + expires_in

            logging.info("Successfully fetched and cached a new bearer token.")
            return self._bearer_token

        except RequestException as e:
            logging.error(f"Failed to retrieve bearer token: {e}")
            raise
        except KeyError as e:
            logging.error(f"Unexpected response format from token endpoint. Missing key: {e}")
            raise

    def post_json(self, payload: Dict[str, Any]) -> bool:
        """
        Sends a JSON payload to the configured API endpoint via a POST request.

        Args:
            payload: The JSON payload to send.

        Returns:
            True if the request was successful (status code 200), False otherwise.
        """
        from requests.exceptions import RequestException

        try:
            token = self._get_bearer_token()
            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
            }

            response = self._session.post(
                url=self.api_url,
                headers=headers,
                json=payload,
                timeout=180,
            )

            call_info = f"API Request to {self.api_url} returned status {response.status_code}"
            response_content = f"Response Content: {response.content.decode('utf-8', errors='ignore')}"

            if success := (response.status_code >= 200 and response.status_code < 300):
                logging.info(call_info)
                logging.info(response_content)
            else:
                logging.warning(call_info)
                logging.warning(response_content)

            return success

        except RequestException as e:
            logging.error(f"An error occurred while sending data to the API: {e}")
            return False
        except (KeyError, ValueError) as e:
            logging.error(f"Failed to send data due to an authentication error: {e}")
            return False
