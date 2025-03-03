import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger()


class SNodeClientException(Exception):
    def __init__(self, message):
        self.message = message


class SNodeClient:

    def __init__(self, ip_address, timeout=300, retry=5):
        self.ip_address = ip_address
        self.url = 'http://%s/snode/' % self.ip_address
        self.timeout = timeout
        self.session = requests.session()
        self.session.verify = False
        self.session.timeout = self.timeout
        self.session.headers['Content-Type'] = "application/json"
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.timeout = self.timeout

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
            raise SNodeClientException("Invalid http status: %s" % ret_code)
        logger.error("Unknown http status: %s", ret_code)
        return None, None

    def is_live(self):
        return self._request("GET", "")

    def info(self):
        return self._request("GET", "info")

    def spdk_process_start(self, spdk_cpu_mask, spdk_mem, spdk_image=None, spdk_debug=None, cluster_ip=None,
                           fdb_connection=None, namespace=None, server_ip=None, rpc_port=None,
                           rpc_username=None, rpc_password=None, multi_threading_enabled=False, timeout=0, ssd_pcie=None,
                           total_mem=None):
        params = {
            "cluster_ip": cluster_ip,
            "server_ip": server_ip,
            "rpc_port": rpc_port,
            "rpc_username": rpc_username,
            "rpc_password": rpc_password}

        if spdk_cpu_mask:
            params['spdk_cpu_mask'] = spdk_cpu_mask
        if spdk_mem:
            params['spdk_mem'] = spdk_mem
        if spdk_image:
            params['spdk_image'] = spdk_image
        if spdk_debug:
            params['spdk_debug'] = spdk_debug
        if fdb_connection:
            params['fdb_connection'] = fdb_connection
        if namespace:
            params["namespace"] = namespace
        if multi_threading_enabled:
            params["multi_threading_enabled"] = multi_threading_enabled
        if timeout:
            params["timeout"] = timeout
        if ssd_pcie:
            params["ssd_pcie"] = ssd_pcie
        if total_mem:
            params["total_mem"] = total_mem
        return self._request("POST", "spdk_process_start", params)

    def join_swarm(self, cluster_ip, join_token, db_connection, cluster_id):
        params = {
            "cluster_ip": cluster_ip,
            "cluster_id": cluster_id,
            "join_token": join_token,
            "db_connection": db_connection}
        return self._request("POST", "join_swarm", params)

    def spdk_process_kill(self, rpc_port):
        params = {"rpc_port": rpc_port}
        return self._request("GET", "spdk_process_kill", params)

    def leave_swarm(self):
        return self._request("GET", "leave_swarm")

    def make_gpt_partitions(self, nbd_device, jm_percent, num_partitions, partition_percent):
        params = {
            "nbd_device": nbd_device,
            "jm_percent": int(jm_percent),
            "num_partitions": int(num_partitions),
            "partition_percent": int(partition_percent),
        }
        return self._request("POST", "make_gpt_partitions", params)

    def delete_dev_gpt_partitions(self, device_pci):
        params = {"device_pci": device_pci}
        return self._request("POST", "delete_dev_gpt_partitions", params)

    def bind_device_to_spdk(self, device_pci):
        params = {"device_pci": device_pci}
        return self._request("POST", "bind_device_to_spdk", params)

    def spdk_process_is_up(self, rpc_port):
        params = {"rpc_port": rpc_port}
        return self._request("GET", "spdk_process_is_up", params)

    def get_file_content(self, file_name):
        return self._request("GET", f"get_file_content/{file_name}")


    def firewall_set_port(self, port_id, port_type="tcp", action="block", rpc_port=None):
        params = {
            "port_id": port_id,
            "port_type": port_type,
            "action": action,
            "rpc_port": rpc_port,
        }
        return self._request("POST", "firewall_set_port", params)

    def get_firewall(self,rpc_port=None):
        params = {"rpc_port": rpc_port}
        return self._request("GET", "get_firewall", params)
