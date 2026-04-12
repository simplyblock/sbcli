import requests
import logging

from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException
from urllib3 import Retry

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


class KMSClientException(Exception):
    def __init__(self, message):
        self.message = message


class KMSClient:
    def __init__(self, cluster, timeout=300, retry=5):
        db_controller = DBController()
        if cluster.mode == "docker":
            mnode = db_controller.get_mgmt_nodes(cluster.get_id())[0]
            if not mnode:
                raise KMSClientException("Cluster has no mgmt nodes")
            self.ip_address = f"{mnode.mgmt_ip}:8200"
        else:
            self.ip_address = "simplyblock-kms:8200"
        self.url = f"http://{self.ip_address}/v1/"
        self.timeout = timeout
        self.session = requests.session()
        self.cluster_id = cluster.get_id()
        self.session.verify = False
        self.session.headers["Content-Type"] = "application/json"
        self.session.headers["Authorization"] = f"Bearer {cluster.kms_root_token}"
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
            raise KMSClientException(
                "Request failed, response indicates error: {response.json()['errors']}"
            ) from e
        except RequestException as e:
            raise KMSClientException("Request failed") from e

    def get_keys(self, key_name) -> dict:
        return self._request("GET", f"{self.cluster_id}/{key_name}")

    def save_keys(self, key: str, key1: str, key2: str) -> None:
        self._request(
            "POST",
            f"{self.cluster_id}/{key}",
            {"key1": key1, "key2": key2},
        )

    def encrypt(self, key: str, plaintext: str) -> str:
        return self._request("POST", f"transit/encrypt/{key}", {"plaintext": plaintext})

    def decrypt(self, key: str, ciphertext: str) -> str:
        return self._request(
            "POST", f"transit/decrypt/{key}", {"ciphertext": ciphertext}
        )

    def create_pool_key(self, pool: Pool) -> None:
        params = {"type": "aes256-gcm96", "exportable": False}
        self._request("POST", f"transit/keys/{pool.get_id()}", params)

    def update_pool_key(self, pool: Pool) -> None:
        self._request(
            "POST", f"transit/keys/{pool.get_id()}/config", {"deletion_allowed": True}
        )

    def delete_pool_key(self, pool: Pool) -> None:
        self._request("DELETE", f"transit/keys/{pool.get_id()}")

    def delete_key(self, key: str) -> None:
        self._request("DELETE", f"{self.cluster_id}/{key}")
