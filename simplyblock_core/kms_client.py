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
        mnode = db_controller.get_mgmt_nodes(cluster_id)[0]
        if not mnode:
            raise KMSClientException("Cluster has no mgmt nodes")
        cluster = db_controller.get_cluster_by_id(cluster_id)
        self.ip_address = f"{mnode.mgmt_ip}:8200"
        self.url = 'http://%s/' % self.ip_address
        self.timeout = timeout
        self.session = requests.session()
        self.cluster_id = cluster_id
        self.session.verify = False
        self.session.headers['Content-Type'] = "application/json"
        self.session.headers['X-Vault-Token'] = cluster.kms_root_token
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def _request(self, method, path, payload=None):
        try:
            logger.error("Requesting path: %s, params: %s", path, payload)
            data = None
            params = None
            if payload:
                if method == "GET" :
                    params = payload
                else:
                    data = json.dumps(payload)

            response = self.session.request(method, self.url+path, data=data,
                                            timeout=self.timeout, params=params)
        except Exception as e:
            raise KMSClientException(str(e))

        logger.error("Response: status_code: %s, content: %s",
                     response.status_code, response.content)
        ret_code = response.status_code

        result = None
        error = None
        if ret_code == 200:
            try:
                decoded_data = response.json()
            except Exception:
                return response.content, None

            result = decoded_data.get('data')
            error = decoded_data.get('errors')
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


    def get_key(self, key_name):
        return self._request("GET", f"v1/{self.cluster_id}/{key_name}")
