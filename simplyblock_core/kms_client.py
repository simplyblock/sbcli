import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

from simplyblock_core.db_controller import DBController

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


class KMSClientException(Exception):
    def __init__(self, message):
        self.message = message


class KMSClient:
    def __init__(self, cluster_id, timeout=300, retry=5):
        db_controller = DBController()
        cluster = db_controller.get_cluster_by_id(cluster_id)
        if cluster.mode == "docker":
            mnode = db_controller.get_mgmt_nodes(cluster_id)[0]
            if not mnode:
                raise KMSClientException("Cluster has no mgmt nodes")
            self.ip_address = f"{mnode.mgmt_ip}:8200"
        else:
            self.ip_address = "simplyblock-kms:8200"
        self.url = "http://%s/" % self.ip_address
        self.timeout = timeout
        self.session = requests.session()
        self.cluster_id = cluster_id
        self.session.verify = False
        self.session.headers["Content-Type"] = "application/json"
        self.session.headers["Authorization"] = f"Bearer {cluster.kms_root_token}"
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def _request(self, method, path, payload=None):
        try:
            logger.debug("Requesting path: %s, params: %s", self.url + path, payload)
            data = None
            params = None
            if payload:
                if method == "GET":
                    params = payload
                else:
                    data = json.dumps(payload)

            response = self.session.request(
                method, self.url + path, data=data, timeout=self.timeout, params=params
            )
        except Exception as e:
            raise KMSClientException(str(e))

        logger.debug(
            "Response: status_code: %s, content: %s",
            response.status_code,
            response.content,
        )
        ret_code = response.status_code

        result = None
        error = None
        if ret_code == 204:
            return True, None

        if ret_code == 200:
            try:
                decoded_data = response.json()
            except Exception:
                return response.content, None

            result = decoded_data.get("data")
            error = decoded_data.get("errors")
            if result is not None or error is not None:
                return result, error
            else:
                return data, None

        if ret_code in [500, 400]:
            raise KMSClientException("Invalid http status: %s" % ret_code)

        if ret_code == 422:
            raise KMSClientException(f"Request validation failed: '{response.text}'")

        logger.error("Unknown http status: %s", ret_code)
        return None, None

    def get_keys(self, key_name):
        return self._request("GET", f"v1/{self.cluster_id}/{key_name}")

    def save_keys(self, key_name, key1, key2):
        params = {
            "key1": key1,
            "key2": key2,
        }
        return self._request("POST", f"v1/{self.cluster_id}/{key_name}", params)

    def encrypt(self, key_name, plaintext):
        params = {"plaintext": plaintext}
        return self._request("POST", f"v1/transit/encrypt/{key_name}", params)

    def decrypt(self, key_name, ciphertext):
        params = {"ciphertext": ciphertext}
        return self._request("POST", f"v1/transit/decrypt/{key_name}", params)

    def create_pool_key(self, pool_uuid):
        params = {"type": "aes256-gcm96", "exportable": False}
        return self._request("POST", f"v1/transit/keys/{pool_uuid}", params)

    def update_pool_key(self, pool_uuid):
        params = {"deletion_allowed": True}
        return self._request("POST", f"v1/transit/keys/{pool_uuid}/config", params)

    def delete_pool_key(self, pool_uuid):
        return self._request("DELETE", f"v1/transit/keys/{pool_uuid}")

    def delete_key(self, key_name):
        return self._request("DELETE", f"v1/{self.cluster_id}/{key_name}")
