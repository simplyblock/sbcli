import logging
from uuid import UUID

import requests
from requests.exceptions import HTTPError, RequestException
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from ._base import KMS
from ._exceptions import KMSException

logger = logging.getLogger(__name__)


class HCPClient(KMS):
    def __init__(
        self,
        address: str,
        token: str,
        cluster_id: UUID,
        timeout: int = 300,
        retry: int = 5,
    ):
        self.url = f"http://{address}/v1/"
        self.timeout = timeout
        self.session = requests.session()
        self.cluster_id = cluster_id
        self.session.verify = False
        self.session.headers["Content-Type"] = "application/json"
        self.session.headers["Authorization"] = f"Bearer {token}"
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def __enter__(self):
        self.session.__enter__()
        return self

    def __exit__(self, *args):
        return self.session.__exit__(*args)

    def _request(self, method, path, payload=None):
        try:
            logger.debug("Requesting path: %s, params: %s", self.url + path, payload)
            response = self.session.request(
                method,
                self.url + path,
                json=payload if method != "GET" else None,
                params=payload if method == "GET" else None,
                timeout=self.timeout,
            )
            logger.debug(
                "Response: status_code: %s, content: %s",
                response.status_code,
                response.content,
            )
            response.raise_for_status()
            return response.json() if response.content else None
        except HTTPError as e:
            raise KMSException(
                "Request failed, response indicates error: {response.json()['errors']}"
            ) from e
        except RequestException as e:
            raise KMSException("Request failed") from e

    def _create_data_encryption_key(self, kek_name: str) -> str:
        return self._request("POST", f"transit/datakey/wrapped/{kek_name}")["data"][
            "ciphertext"
        ]

    def _encrypt(self, kek_name: str, plaintext: str) -> str:
        return self._request(
            "POST", f"transit/encrypt/{kek_name}", {"plaintext": plaintext}
        )["data"]["ciphertext"]

    def _decrypt(self, kek_name: str, ciphertext: str) -> str:
        return self._request(
            "POST", f"transit/decrypt/{kek_name}", {"ciphertext": ciphertext}
        )["data"]["plaintext"]

    def create_data_encryption_keys(self, kek_name: str, name: str) -> None:
        self._request(
            "POST",
            f"{self.cluster_id}/{name}",
            {"keys": [self._create_data_encryption_key(kek_name), self._create_data_encryption_key(kek_name)]},
        )

    def import_data_encryption_keys(self, kek_name: str, name: str, keys: tuple[str, str]) -> None:
        self._request(
            "POST",
            f"{self.cluster_id}/{name}",
            {"keys": [self._encrypt(kek_name, keys[0]), self._encrypt(kek_name, keys[1])]},
        )

    def get_data_encryption_keys(self, kek_name: str, name: str) -> tuple[str, str]:
        encrypted_key1, encrypted_key2 = self._request("GET", f"{self.cluster_id}/{name}")["data"]["keys"]
        return (self._decrypt(kek_name, encrypted_key1), self._decrypt(kek_name, encrypted_key2))

    def delete_data_encryption_keys(self, name: str) -> None:
        self._request("DELETE", f"{self.cluster_id}/{name}")

    def create_key_encryption_key(self, name: str) -> None:
        params = {"type": "aes256-gcm96", "exportable": False}
        self._request("POST", f"transit/keys/{name}", params)
        self._request("POST", f"transit/keys/{name}/config", {"deletion_allowed": True})

    def delete_key_encryption_key(self, name: str) -> None:
        self._request("DELETE", f"transit/keys/{name}")
