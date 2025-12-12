import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

from simplyblock_core import constants

logger = logging.getLogger()


class FirewallClientException(Exception):
    def __init__(self, message):
        self.message = message


class FirewallClient:

    def __init__(self, node, timeout=300, retry=5):
        self.node = node
        self.ip_address = f"{node.mgmt_ip}:{node.firewall_port}"
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
            raise FirewallClientException(str(e))

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

    def firewall_set_port(self, port_id, port_type="tcp", action="block", rpc_port=None, is_reject=False):
        params = {
            "port_id": port_id,
            "port_type": "tcp",
            "action": action,
            "rpc_port": rpc_port,
            "is_reject": is_reject,
        }
        response = None
        try:
            response = self._request("POST", "firewall", params)
        except Exception as e:
            logger.warning(e)
            logger.info("Using other firewall path: firewall_set_port")
            mgmt_ip = self.ip_address.split(":")[0]
            self.url = f"http://{mgmt_ip}:5000/"
            response = self._request("POST", "snode/firewall_set_port", params)

        if response and self.node.active_rdma:
            if action == "block":
                response = self.node.rpc_client().nvmf_port_block_rdma(port_id)
            else:
                response = self.node.rpc_client().nvmf_port_unblock_rdma(port_id)
        return response

    def get_firewall(self, rpc_port=None):
        params = {"rpc_port": rpc_port}
        try:
            return self._request("GET", "firewall", params)
        except Exception as e:
            logger.warning(e)
            logger.info("Using other firewall path: get_firewall")
            mgmt_ip = self.ip_address.split(":")[0]
            self.url = f"http://{mgmt_ip}:5000/"
            return self._request("GET", "snode/get_firewall", params)
