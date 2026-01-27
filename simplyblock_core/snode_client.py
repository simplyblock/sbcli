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
            raise SNodeClientException(str(e))

        logger.debug("Response: status_code: %s, content: %s",
                     response.status_code, response.content)
        ret_code = response.status_code

        result = None
        error = None
        if ret_code == 200:
            try:
                decoded_data = response.json()
            except Exception as e:
                logger.error("Failed to decode JSON response: %s", e)
                return response.content, None

            result = decoded_data.get('results')
            error = decoded_data.get('error')
            if result is not None or error is not None:
                return result, error
            else:
                return decoded_data, None

        if ret_code in [500, 400]:
            raise SNodeClientException("Invalid http status: %s" % ret_code)

        if ret_code == 422:
            raise SNodeClientException(f"Request validation failed: '{response.text}'")

        raise SNodeClientException(f"Unknown http status: {ret_code}")

    def is_live(self):
        try:
            return self._request("GET", "check")
        except SNodeClientException:
            logger.warning("Failed to call snode/check, trying snode/info")
            return self.info()

    def info(self):
        return self._request("GET", "info")

    def spdk_process_start(self, l_cores, spdk_mem, spdk_image=None, spdk_debug=None, cluster_ip=None,
                           fdb_connection=None, namespace=None, server_ip=None, rpc_port=None,
                           rpc_username=None, rpc_password=None, multi_threading_enabled=False, timeout=0, ssd_pcie=None,
                           total_mem=None, system_mem=None, cluster_mode=None, socket=0, firewall_port=0, cluster_id=None):
        params = {
            "cluster_ip": cluster_ip,
            "server_ip": server_ip,
            "rpc_port": rpc_port,
            "rpc_username": rpc_username,
            "rpc_password": rpc_password}

        if l_cores:
            params['l_cores'] = l_cores
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
        if system_mem:
            params["system_mem"] = system_mem
        if cluster_mode:
            params["cluster_mode"] = cluster_mode
        if firewall_port:
            params["firewall_port"] = firewall_port
        params["socket"] = socket
        if cluster_id:
            params["cluster_id"] = cluster_id
        return self._request("POST", "spdk_process_start", params)

    def join_swarm(self, cluster_ip, join_token, db_connection, cluster_id):
        return True, None
        # params = {
        #     "cluster_ip": cluster_ip,
        #     "cluster_id": cluster_id,
        #     "join_token": join_token,
        #     "db_connection": db_connection}
        # return self._request("POST", "join_swarm", params)

    def spdk_process_kill(self, rpc_port, cluster_id=None):
        return self._request("GET", "spdk_process_kill", {"rpc_port": rpc_port, "cluster_id": cluster_id})

    def leave_swarm(self):
        return True
        # return self._request("GET", "leave_swarm")

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

    def bind_device_to_nvme(self, device_pci):
        params = {"device_pci": device_pci}
        return self._request("POST", "bind_device_to_nvme", params)

    def bind_device_to_spdk(self, device_pci):
        params = {"device_pci": device_pci}
        return self._request("POST", "bind_device_to_spdk", params)

    def spdk_process_is_up(self, rpc_port, cluster_id):
        params = {"rpc_port": rpc_port, "cluster_id": cluster_id}
        return self._request("GET", "spdk_process_is_up", params)

    def get_file_content(self, file_name):
        return self._request("GET", f"get_file_content/{file_name}")

    def spdk_proxy_restart(self,rpc_port=None):
        params = {"rpc_port": rpc_port}
        return self._request("GET", "spdk_proxy_restart", params)

    def set_hugepages(self):
        return self._request("POST", "set_hugepages")

    def ifc_is_roce(self, nic):
        params = {"nic": nic}
        return self._request("GET", "ifc_is_roce", params)

    def ifc_is_tcp(self, nic):
        params = {"nic": nic}
        return self._request("GET", "ifc_is_tcp", params)