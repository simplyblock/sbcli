import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger()


class CNodeClientException(Exception):
    def __init__(self, message):
        self.message = message


class CNodeClient:

    def __init__(self, ip_address):
        self.ip_address = ip_address
        self.url = 'http://%s/' % self.ip_address
        self.timeout = 120
        self.session = requests.session()
        self.session.verify = False
        self.session.timeout = self.timeout
        self.session.headers['Content-Type'] = "application/json"
        retries = Retry(total=30, backoff_factor=1, connect=30, read=30)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def _request(self, method, path, params=None):
        try:
            logger.debug("Requesting path: %s, params: %s", path, params)
            dt = None
            if params:
                dt = json.dumps(params)
            response = self.session.request(method, self.url+path, data=dt, timeout=self.timeout)
        except Exception as e:
            raise e

        logger.debug("Response: status_code: %s, content: %s",
                     response.status_code, response.content)
        ret_code = response.status_code

        result = None
        error = None
        if ret_code == 200:
            try:
                data = response.json()
            except Exception:
                return response.content, None

            if 'results' in data:
                result = data['results']
            if 'error' in data:
                error = data['error']
            if result is not None or error is not None:
                return result, error
            else:
                return data, None

        if ret_code in [500, 400]:
            raise CNodeClientException("Invalid http status: %s" % ret_code)
        logger.error("Unknown http status: %s", ret_code)
        return None, None

    def is_live(self):
        return self._request("GET", "")

    def info(self):
        return self._request("GET", "cnode/info")

    def spdk_process_start(
            self, spdk_cpu_mask, spdk_mem, spdk_image, server_ip, rpc_port, rpc_username, rpc_password, namespace=None):
        params = {
            "spdk_cpu_mask": spdk_cpu_mask,
            "spdk_mem": spdk_mem,
            "spdk_image": spdk_image,
            "server_ip": server_ip,
            "rpc_port": rpc_port,
            "rpc_username": rpc_username,
            "rpc_password": rpc_password,
        }
        if namespace:
            params["namespace"] = namespace
        return self._request("POST", "cnode/spdk_process_start", params)

    def join_db(self, db_connection):
        params = {"db_connection": db_connection}
        return self._request("POST", "cnode/join_db", params)

    def spdk_process_kill(self):
        return self._request("GET", "cnode/spdk_process_kill")

    def spdk_process_is_up(self):
        return self._request("GET", "cnode/spdk_process_is_up")

    def connect_nvme(self, ip, port, nqn):
        params = {
            "ip": ip,
            "port": port,
            "nqn": nqn}
        return self._request("POST", "cnode/nvme_connect", params)

    def disconnect_device(self, dev_path):
        params = {"dev_path": dev_path}
        return self._request("POST", "cnode/disconnect_device", params)

    def disconnect_nqn(self, nqn):
        params = {"nqn": nqn}
        return self._request("POST", "cnode/disconnect_nqn", params)

    def disconnect_all(self):
        return self._request("POST", "cnode/disconnect_all")

    def make_gpt_partitions(self, nbd_device, jm_percent):
        params = {
            "nbd_device": nbd_device,
            "jm_percent": jm_percent,
        }
        return self._request("POST", "cnode/make_gpt_partitions", params)

    def delete_dev_gpt_partitions(self, device_pci):
        params = {"device_pci": device_pci}
        return self._request("POST", "cnode/delete_dev_gpt_partitions", params)

    def iscsi_connect(self, ip, port, nqn):
        params = {
            "ip": ip,
            "port": port,
            "iqn": nqn}
        return self._request("POST", "cnode/iscsi_connect", params)

    def get_iscsi_dev_path(self, nqn):
        params = {
            "iqn": nqn}
        return self._request("POST", "cnode/get_iscsi_dev_path", params)

    def disconnect_iscsi(self, nqn):
        params = {
            "iqn": nqn}
        return self._request("POST", "cnode/disconnect_iscsi", params)
