import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger()


class FirewallClientException(Exception):
    def __init__(self, message):
        self.message = message


class FirewallClient:

    def __init__(self, ip_address, timeout=300, retry=5):
        self.ip_address = ip_address
        self.url = 'http://%s/' % self.ip_address
        self.timeout = timeout
        self.session = requests.session()
        self.session.verify = False
        self.session.headers['Content-Type'] = "application/json"
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def _request(self, method, path, payload=None):
        try:
            logger.debug("Requesting path: %s, params: %s", path, payload)
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
            raise e

        logger.debug("Response: status_code: %s, content: %s",
                     response.status_code, response.content)
        ret_code = response.status_code

        result = None
        error = None
        if ret_code == 200:
            try:
                decoded_data = response.json()
            except Exception:
                return response.content, None

            result = decoded_data.get('results')
            error = decoded_data.get('error')
            if result is not None or error is not None:
                return result, error
            else:
                return data, None

        if ret_code in [500, 400]:
            raise FirewallClientException("Invalid http status: %s" % ret_code)

        if ret_code == 422:
            raise FirewallClientException(f"Request validation failed: '{response.text}'")

        logger.error("Unknown http status: %s", ret_code)
        return None, None

    def firewall_set_port(self, port_id, port_type="tcp", action="block", rpc_port=None):
        params = {
            "port_id": port_id,
            "port_type": port_type,
            "action": action,
            "rpc_port": rpc_port,
        }
        return self._request("POST", "firewall", params)

    def get_firewall(self, rpc_port=None):
        params = {"rpc_port": rpc_port}
        return self._request("GET", "firewall", params)
