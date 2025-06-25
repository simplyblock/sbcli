
import json
import requests
from http import HTTPStatus
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


class SbcliUtils:
    """Contains all API calls
    """

    def __init__(self, cluster_secret, cluster_api_url, cluster_id):
        self.cluster_id = cluster_id
        self.cluster_secret = cluster_secret
        self.cluster_api_url = cluster_api_url
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{cluster_id} {cluster_secret}"
        }
        self.logger = setup_logger(__name__)

    def get_request(self, api_url, headers=None, expected_error_code=None):
        """Performs get request on the given API URL

        Args:
            api_url (str): Endpoint to request
            headers (dict, optional): Headers needed. Defaults to None.

        Returns:
            dict: response returned
        """
        print(self.cluster_api_url)
        print(api_url)
        request_url = self.cluster_api_url + api_url
        print(request_url)
        headers = headers if headers else self.headers
        print(headers)
        self.logger.info(f"Calling GET for {api_url} with headers: {headers}")
        retry = 10
        data = None
        while retry > 0:
            try:
                resp = requests.get(request_url, headers=headers)
                if resp.status_code == HTTPStatus.OK:
                    data = resp.json()
                    return data
                else:
                    self.logger.error(f"request failed. status_code: {resp.status_code}, text: {resp.text}")
                    resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                if expected_error_code:
                    if e.response.status_code in expected_error_code:
                        self.logger.info(f"Expected error: {e}")
                        break
                else:
                    retry -= 1
                    if retry == 0:
                        self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                        raise e
                    self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                    sleep_n_sec(1)
            except Exception as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                    raise e
                self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                sleep_n_sec(1)

    def post_request(self, api_url, headers=None, body=None, retry=10, expected_error_code=None):
        """Performs post request on the given API URL

        Args:
            api_url (str): Endpoint to request
            headers (dict, optional): Headers needed. Defaults to None.
            body (dict, optional): Body to send in request. Defaults to None.
            retry (int, optional): number of retries if request failed. Default is 10.

        Returns:
            dict: response returned
        """
        request_url = self.cluster_api_url + api_url
        headers = headers if headers else self.headers
        self.logger.info(f"Calling POST for {api_url} with headers: {headers}, body: {body}")
        while retry > 0:
            try:
                resp = requests.post(request_url, headers=headers,
                                     json=body, timeout=100)
                if resp.status_code == HTTPStatus.OK:
                    data = resp.json()
                    return data
                else:
                    self.logger.error(f"request failed. status_code: {resp.status_code}, text: {resp.text}")
                    resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                if expected_error_code:
                    if e.response.status_code in expected_error_code:
                        self.logger.info(f"Expected error: {e}")
                else:
                    retry -= 1
                    if retry == 0:
                        self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                        raise e
                    self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                    sleep_n_sec(1)
            except Exception as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                    raise e
                self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                sleep_n_sec(3)

    def delete_request(self, api_url, headers=None, expected_error_code=None):
        """Performs delete request on the given API URL

        Args:
            api_url (str): Endpoint to request
            headers (dict, optional): Headers needed. Defaults to None.

        Returns:
            dict: response returned
        """
        request_url = self.cluster_api_url + api_url
        headers = headers if headers else self.headers
        self.logger.info(f"Calling DELETE for {api_url} with headers: {headers}")
        retry = 10
        while retry > 0:
            try:
                resp = requests.delete(request_url, headers=headers)
                if resp.status_code == HTTPStatus.OK:
                    data = resp.json()
                    return data
                else:
                    self.logger.error(f"request failed. status_code: {resp.status_code}, text: {resp.text}")
                    resp.raise_for_status()
                
            except requests.exceptions.HTTPError as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                if expected_error_code:
                    if e.response.status_code in expected_error_code:
                        self.logger.info(f"Expected error: {e}")
                else:
                    retry -= 1
                    if retry == 0:
                        self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                        raise e
                    self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                    sleep_n_sec(1)
            except Exception as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                    raise e
                self.logger.info(f"Retrying API {api_url}. Attempt: {5 - retry + 1}")
                sleep_n_sec(3)

    def put_request(self, api_url, headers=None, body=None, expected_error_code=None):
        """Performs put request on the given API URL

        Args:
            api_url (str): Endpoint to request
            headers (dict, optional): Headers needed. Defaults to None.
            body (dict, optional): Body to send in request. Defaults to None.

        Returns:
            dict: response returned
        """
        request_url = self.cluster_api_url + api_url
        headers = headers if headers else self.headers
        self.logger.info(f"Calling POST for {api_url} with headers: {headers}, body: {body}")
        retry = 5
        while retry > 0:
            try:
                resp = requests.put(request_url, headers=headers,
                                     json=body, timeout=100)
                if resp.status_code == HTTPStatus.OK:
                    data = resp.json()
                    return data
                else:
                    self.logger.error(f"request failed. status_code: {resp.status_code}, text: {resp.text}")
                    resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                if expected_error_code:
                    if e.response.status_code in expected_error_code:
                        self.logger.info(f"Expected error: {e}")
                else:
                    retry -= 1
                    if retry == 0:
                        self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                        raise e
                    self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                    sleep_n_sec(1)
            except Exception as e:
                self.logger.debug(f"API call {api_url} failed with error:{e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attempt exhausted. API {api_url} failed with: {e}.")
                    raise e
                self.logger.info(f"Retrying API {api_url}. Attempt: {10 - retry + 1}")
                sleep_n_sec(3)

    def add_storage_node(self, cluster_id, node_ip, ifname, max_lvol, max_prov, max_snap,
                         number_of_distribs, number_of_devices, partitions, jm_percent,
                         disable_ha_jm, enable_test_device, iobuf_small_pool_count,
                         iobuf_large_pool_count, spdk_debug, spdk_image, spdk_cpu_mask):
        """Adds the storage node with given name
        """

        body = {
            "cluster_id": cluster_id,
            "node_ip": node_ip,
            "ifname": ifname,
            "max_lvol": max_lvol,
            "max_prov": max_prov,
            "max_snap": max_snap,
            "number_of_distribs": number_of_distribs,
            "number_of_devices": number_of_devices,
            "partitions": partitions,
            "jm_percent": jm_percent,
            "disable_ha_jm": disable_ha_jm,
            "enable_test_device": enable_test_device,
            "iobuf_small_pool_count": iobuf_small_pool_count,
            "iobuf_large_pool_count": iobuf_large_pool_count,
            "spdk_debug": spdk_debug,
            "spdk_image": spdk_image,
            "spdk_cpu_mask": spdk_cpu_mask
        }

        self.post_request(api_url="/storagenode/add", body=body)

    
    def get_node_without_lvols(self) -> str:
        """
        returns a single nodeID which doesn't have any lvol attached
        """
        # a node which doesn't have any lvols attached
        node_uuid = ""
        data = self.get_request(api_url="/storagenode")
        for result in data['results']:
            if result['lvols'] == 0 and result['is_secondary_node'] is False:
                node_uuid = result['uuid']
                break
        return node_uuid
    
    def get_all_node_without_lvols(self) -> str:
        """
        returns all nodeID which doesn't have any lvol attached
        """
        # a node which doesn't have any lvols attached
        node_uuids = []
        data = self.get_request(api_url="/storagenode")
        for result in data['results']:
            if result['lvols'] == 0 and result['is_secondary_node'] is False:
                node_uuids.append(result['uuid'])
        return node_uuids

    def shutdown_node(self, node_uuid: str, expected_error_code=None, force=False):
        """
        given a node_UUID, shutdowns the node
        """
        # TODO: parse and display error accordingly: {'results': True, 'status': True}
        self.logger.info(f"Shutting down node with uuid: {node_uuid}")
        force_str = "?force=true" if force else ""
        self.get_request(api_url=f"/storagenode/shutdown/{node_uuid}{force_str}", expected_error_code=expected_error_code)


    def suspend_node(self, node_uuid: str, expected_error_code=None):
        """
        given a node_UUID, suspends the node
        """
        # TODO: parse and display error accordingly: {'results': True, 'status': True}
        self.get_request(api_url=f"/storagenode/suspend/{node_uuid}", expected_error_code=expected_error_code)


    def resume_node(self, node_uuid: str):
        """
        given a node_UUID, resumes the node
        """
        # TODO: parse and display error accordingly: {'results': True, 'status': True}
        self.get_request(api_url=f"/storagenode/resume/{node_uuid}")

    def restart_node(self, node_uuid: str, expected_error_code=None, force=False):
        """
        given a node_UUID, restarts the node
        """
        # TODO: parse and display error accordingly: {'results': True, 'status': True}
        body = {
            "uuid": node_uuid
        }

        if force:
            body["force"] = True

        self.put_request(api_url="/storagenode/restart/", body=body, expected_error_code=expected_error_code)

    def get_all_nodes_ip(self):
        """Return all nodes part of cluster
        """
        management_nodes = []
        storage_nodes = []

        print("get_all_nodes_ip")
        data = self.get_management_nodes()

        for nodes in data["results"]:
            management_nodes.append(nodes["mgmt_ip"])

        data = self.get_storage_nodes()

        for nodes in data["results"]:
            storage_nodes.append(nodes["mgmt_ip"])

        return management_nodes, storage_nodes

    def get_management_nodes(self):
        """Return management nodes part of cluster
        """
        print("get_management_nodes")
        data = self.get_request(api_url="/mgmtnode/")
        return data

    def get_storage_nodes(self):
        """Return storage nodes part of cluster
        """
        data = self.get_request(api_url="/storagenode/")
        return data

    def list_storage_pools(self):
        """Return storage pools
        """
        pool_data = dict()
        data = self.get_request(api_url="/pool")
        for pool_info in data["results"]:
            pool_data[pool_info["pool_name"]] = pool_info["id"]

        return pool_data

    def get_pool_by_id(self, pool_id):
        """Return storage pool with given id
        """
        data = self.get_request(api_url=f"/pool/{pool_id}")

        return data

    def add_storage_pool(self, pool_name, cluster_id=None, max_rw_iops=0, max_rw_mbytes=0, max_r_mbytes=0, max_w_mbytes=0):
        """Adds the storage with given name
        """
        pools = self.list_storage_pools()
        for name in list(pools.keys()):
            if name == pool_name:
                print(f"Pool {pool_name} already exists. Exiting")
                return

        body = {
            "name": pool_name,
            "max_rw_iops": str(max_rw_iops),
            "max_rw_mbytes": str(max_rw_mbytes),
            "max_r_mbytes": str(max_r_mbytes),
            "max_w_mbytes": str(max_w_mbytes),
            "cluster_id": self.cluster_id
        }
        if cluster_id:
            body["cluster_id"] = cluster_id

        self.post_request(api_url="/pool", body=body)
        # TODO: Add assertions

    def get_storage_pool_id(self, pool_name):
        """Return storage pool by name
        """
        pools = self.list_storage_pools()
        pool_id = None
        for name in list(pools.keys()):
            if name == pool_name:
                pool_id = pools[name]
        return pool_id

    def delete_storage_pool(self, pool_name):
        """Delete storage pool with given name
        """
        pool_id = self.get_storage_pool_id(pool_name=pool_name)
        if not pool_id:
            self.logger.info("Pool does not exist. Exiting")
            return

        pool_data = self.get_pool_by_id(pool_id=pool_id)

        header = self.headers.copy()
        header["secret"] = pool_data["results"][0]["secret"]

        self.delete_request(api_url=f"/pool/{pool_id}",
                                   headers=header)

    def delete_all_storage_pools(self):
        """Deletes all the storage pools
        """
        pools = self.list_storage_pools()
        for name in list(pools.keys()):
            self.logger.info(f"Deleting pool: {name}")
            self.delete_storage_pool(pool_name=name)

    def list_lvols(self):
        """Return all lvols
        """
        lvol_data = dict()
        data = self.get_request(api_url="/lvol")
        # self.logger.info(f"LVOL List: {data}")
        for lvol_info in data["results"]:
            lvol_data[lvol_info["lvol_name"]] = lvol_info["id"]
            self.logger.info(f"Lvol hostname: {lvol_info['hostname']}")
        self.logger.info(f"LVOL List: {lvol_data}")
        return lvol_data

    def get_lvol_by_id(self, lvol_id):
        """Return all lvol with given id
        """
        data = self.get_request(api_url=f"/lvol/{lvol_id}")
        return data

    def add_lvol(self, lvol_name, pool_name, size="256M", distr_ndcs=0, distr_npcs=0,
                 distr_bs=4096, distr_chunk_bs=4096, max_rw_iops=0, max_rw_mbytes=0,
                 max_r_mbytes=0, max_w_mbytes=0, host_id=None, retry=10,
                 crypto=False, key1=None, key2=None):
        """Adds lvol with given params
        """
        if crypto:
            if not key1 or not key2:
                raise Exception("Need two keys for crypto lvols")
        lvols = self.list_lvols()
        for name in list(lvols.keys()):
            if name == lvol_name:
                self.logger.info(f"LVOL {lvol_name} already exists. Exiting")
                return

        body = {
            "name": lvol_name,
            "size": size,
            "pool": pool_name,
            "max_rw_iops": str(max_rw_iops),
            "max_rw_mbytes": str(max_rw_mbytes),
            "max_r_mbytes": str(max_r_mbytes),
            "max_w_mbytes": str(max_w_mbytes),
        }
        if distr_ndcs != 0 and distr_npcs != 0:
            body["distr_ndcs"] = str(distr_ndcs)
            body["distr_npcs"] = str(distr_ndcs)
            body["bs"] = str(distr_ndcs)
            body["chunk_bs"] = str(distr_ndcs)
        if host_id:
            body["host_id"] = host_id
        if crypto:
            body["crypto"] = True
            body["crypto_key1"] = key1
            body["crypto_key2"] = key2
        
        self.post_request(api_url="/lvol", body=body, retry=retry)

    def delete_lvol(self, lvol_name, skip_error=False):
        """Deletes lvol with given name
        """
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)

        if not lvol_id:
            self.logger.info("Lvol does not exist. Exiting")
            return

        data = self.delete_request(api_url=f"/lvol/{lvol_id}")
        self.logger.info(f"Delete lvol resp: {data}")

        lvols = self.list_lvols()
        attempt = 0
        while True:
            if lvol_name not in list(lvols.keys()):
                self.logger.info(f"Lvol {lvol_name} deleted successfully!!")
                break
            if attempt % 12 == 0:
                cur_state = self.get_lvol_details(lvol_id=lvol_id)[0]["status"]
                if cur_state == "online":
                    self.logger.info(f"Lvol {lvol_name} in online state. Retrying Delete!")
                    data = self.delete_request(api_url=f"/lvol/{lvol_id}")
                    self.logger.info(f"Delete lvol resp: {data}")
            if attempt > 120:
                if skip_error:
                    return
                raise Exception(f"Lvol {lvol_name} is not getting deleted!!")
            
            attempt += 1
            self.logger.info(f"Lvol {lvol_name} is in_deletion. Checking again!")
            sleep_n_sec(5)
            lvols = self.list_lvols()

    def delete_all_lvols(self):
        """Deletes all lvols
        """
        lvols = self.list_lvols()
        for name in list(lvols.keys()):
            self.logger.info(f"Deleting lvol: {name}")
            self.delete_lvol(lvol_name=name)

    def get_lvol_id(self, lvol_name):
        """Return lvol by lvol name
        """
        lvols = self.list_lvols()
        return lvols.get(lvol_name, None)

    def get_lvol_connect_str(self, lvol_name):
        """Return list of formatted connect strings for the lvol"""
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            self.logger.info(f"Lvol {lvol_name} does not exist. Exiting")
            return

        data = self.get_request(api_url=f"/lvol/connect/{lvol_id}")
        self.logger.info(f"Connect lvol resp: {data}")

        connect_lines = []

        for entry in data.get("results", []):
            connect_line = (
                "sudo nvme connect "
                f"--reconnect-delay={entry['reconnect-delay']} "
                f"--ctrl-loss-tmo=-1 "
                f"--nr-io-queues={entry['nr-io-queues']} "
                f"--keep-alive-tmo={entry['keep-alive-tmo']} "
                f"--transport={entry['transport']} "
                f"--traddr={entry['ip']} "
                f"--trsvcid={entry['port']} "
                f"--nqn={entry['nqn']}"
            )
            connect_lines.append(connect_line)

        return connect_lines

    def get_cluster_status(self, cluster_id=None):
        """Return cluster status for given cluster id
        """
        cluster_id = self.cluster_id if not cluster_id else cluster_id
        cluster_details = self.get_request(api_url=f"/cluster/status/{cluster_id}")
        self.logger.info(f"Cluster Status: {cluster_details}")
        return cluster_details["results"]

    def get_storage_node_details(self, storage_node_id):
        """Get Storage Node details for given node id
        """
        node_details = self.get_request(api_url=f"/storagenode/{storage_node_id}")
        self.logger.debug(f"Node Details: {node_details}")
        return node_details["results"]

    def get_device_details(self, storage_node_id):
        """Get Device details for given node id
        """
        device_details = self.get_request(api_url=f"/device/list/{storage_node_id}")
        self.logger.info(f"Device Details: {device_details}")
        return device_details["results"]

    def get_lvol_details(self, lvol_id):
        """Get lvol details for given lvol id
        """
        lvol_details = self.get_request(api_url=f"/lvol/{lvol_id}")
        self.logger.info(f"Lvol Details: {lvol_details}")
        return lvol_details["results"]

    def get_cluster_logs(self, cluster_id=None):
        """Get Cluster logs for given cluster id
        """
        cluster_id = self.cluster_id if not cluster_id else cluster_id
        cluster_logs = self.get_request(api_url=f"/cluster/get-logs/{cluster_id}?limit=0")
        self.logger.info(f"Cluster Logs: {cluster_logs}")
        return cluster_logs["results"]
    
    def get_cluster_tasks(self, cluster_id=None):
        """Get Cluster tasks for given cluster id
        """
        cluster_id = self.cluster_id if not cluster_id else cluster_id
        cluster_tasks = self.get_request(api_url=f"/cluster/get-tasks/{cluster_id}?limit=0")
        self.logger.debug(f"Cluster Tasks: {cluster_tasks}")
        return cluster_tasks["results"]

    def get_cluster_details(self, cluster_id=None):
        """Get Cluster details

        Args:
            cluster_id (str, optional): Get cluster details. Defaults to object cluster id.
        """
        cluster_id = self.cluster_id if not cluster_id else cluster_id
        cluster_list = self.get_request(api_url="/cluster/")
        self.logger.debug(f"Cluster List: {cluster_list}")

        cluster_list = cluster_list["results"]

        cluster_detail = None

        for cluster in cluster_list:
            if cluster["id"] == cluster_id:
                cluster_detail = cluster
        
        if cluster_detail:
            return cluster_detail
        raise Exception(f"No Cluster with id: {cluster_id} found!!")
    
    def wait_for_cluster_status(self, cluster_id=None, status="active", timeout=60):
        actual_status = None
        while timeout > 0:
            cluster_details = self.get_cluster_details(cluster_id=cluster_id)
            actual_status = cluster_details["status"]
            status = status if isinstance(status, list) else [status]
            if actual_status in status:
                return cluster_details
            self.logger.info(f"Expected Status: {status} / Actual Status: {actual_status}")
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(f"Timed out waiting for cluster status, {cluster_id},"
                           f"Expected status: {status}, Actual status: {actual_status}")
    
    def wait_for_storage_node_status(self, node_id, status, timeout=60):
        actual_status = None
        while timeout > 0:
            node_details = self.get_storage_node_details(storage_node_id=node_id)
            actual_status = node_details[0]["status"]
            status = status if isinstance(status, list) else [status]
            if actual_status in status:
                return node_details[0]
            self.logger.info(f"Expected Status: {status} / Actual Status: {actual_status}")
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(f"Timed out waiting for node status, {node_id},"
                           f"Expected status: {status}, Actual status: {actual_status}")
    
    def all_expected_status(self, value_dict, expected_status):
        value_match = []
        for key, value in value_dict.items():
            self.logger.info(f"Entity: {key}, Expected: {expected_status}, Actual: {value}")
            if value in expected_status:
                value_match.append(True)
            else:
                value_match.append(False)
        self.logger.info(f"Value: {value_match}")
        return all(value_match)
    
    def wait_for_device_status(self, node_id, status, timeout=60):
        device_ids = {}
        device_details = self.get_device_details(storage_node_id=node_id)
        total_devices = len(device_details)
        while timeout > 0:
            self.logger.info("Retrying Device Status check")
            device_details = self.get_device_details(storage_node_id=node_id)
            for device in device_details:
                device_ids[device['id']] = device['status']
                status = status if isinstance(status, list) else [status]
                self.logger.info(f"Device statuses: {device_ids}")
                if device['status'] in status:
                    if len(device_ids) == total_devices and self.all_expected_status(device_ids, status):
                        return device_details
                self.logger.info(f"Device ID: {device['id']} Expected Status: {status} / Actual Status: {device['status']}")
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(f"Timed out waiting for device status, Node id: {node_id}, Device id: {list(device_ids.keys())}"
                            f"Expected status: {status}, Actual status: {list(device_ids.values())}")
    
    def wait_for_health_status(self, node_id, status, timeout=60, device_id=None):
        actual_status = None
        if not device_id:
            node_details = self.get_storage_node_details(storage_node_id=node_id)
            while timeout > 0:
                node_details = self.get_storage_node_details(storage_node_id=node_id)
                actual_status = node_details[0]["health_check"]
                status = status if isinstance(status, list) else [status]
                if actual_status in status:
                    return node_details[0]
                self.logger.info(f"Expected Status: {status} / Actual Status: {actual_status}")
                sleep_n_sec(1)
                timeout -= 1
            if False in status and node_details[0]["status"] != "offline":
                assert actual_status is True, "Health Status not True for node not in offline state"
                return node_details[0]
            raise TimeoutError(f"Timed out waiting for node health status, {node_id},"
                               f"Expected status: {status}, Actual status: {actual_status}")
        else:
            device_details = self.get_device_details(storage_node_id=node_id)
            while timeout > 0:
                device_details = self.get_device_details(storage_node_id=node_id)
                for device in device_details:
                    if device_id == device['id']:
                        actual_status = device["health_check"]
                        status = status if isinstance(status, list) else [status]
                        if actual_status in status:
                            return device
                        self.logger.info(f"Expected Status: {status} / Actual Status: {actual_status}")
                    else:
                        continue
                sleep_n_sec(1)
                timeout -= 1
            raise TimeoutError(f"Timed out waiting for device status, Node id: {node_id}, Device id: {device_id}"
                                f"Expected status: {status}, Actual status: {actual_status}")
        
    

    def list_migration_tasks(self, cluster_id):
        """List all migration tasks for a given cluster."""
        return self.get_request(f"/cluster/list-tasks/{cluster_id}?limit=0")
    
    def get_io_stats(self, cluster_id, time_duration=None):
        """
        Fetch I/O statistics for the given cluster at the specified time duration.
        Args:
            cluster_id (str): Cluster ID
            time_duration (str): Time duration (e.g., '1hr30m', '40m')

        Returns:
            dict: Parsed I/O stats
        """
        if time_duration:
            api_url = f"/cluster/iostats/{cluster_id}/history/{time_duration}"
            self.logger.info(f"Fetching I/O stats for cluster {cluster_id} with time duration {time_duration}.")
            response = self.get_request(api_url)
        else:
            api_url = f"/cluster/iostats/{cluster_id}"
            self.logger.info(f"Fetching I/O stats for cluster {cluster_id}.")
            response = self.get_request(api_url)
        return response.get("results", {}).get("stats", [])
    
    def resize_lvol(self, lvol_id, new_size):
        """Resizes lvol to given size

        Args:
            lvol_id (str): LVOL id for which we need to modify size
            new_size (str): New size of lvol. Eg: 20G
        """
        body = {
            "size": new_size
        }
        self.put_request(api_url=f"/lvol/resize/{lvol_id}", 
                         body=body)
        
    def is_secondary_node(self, node_id):
        sec_nodes = []
        storage_nodes = self.get_storage_nodes()
        for result in storage_nodes['results']:
            if result['is_secondary_node'] is True:
                sec_nodes.append(result["uuid"])
        return node_id in sec_nodes
    
    def add_snapshot(self, lvol_id, snapshot_name, retry=3):
        """Adds snapshot with given params
        """
        
        body = {
            "lvol_id": lvol_id,
            "snapshot_name": snapshot_name,
        }
        
        self.post_request(api_url="/snapshot", body=body, retry=retry)

    def add_clone(self, snapshot_id, clone_name, retry=3):
        """Adds clone with given params
        """
        
        body = {
            "snapshot_id": snapshot_id,
            "clone_name": clone_name,
        }
        
        self.post_request(api_url="/snapshot/clone", body=body, retry=retry)

    def list_snapshot(self):
        """Return all snapshots
        """
        snap_data = dict()
        data = self.get_request(api_url="/snapshot")
        for snap_info in data["results"]:
            snap_data[snap_info["snap_name"]] = snap_info["id"]
        self.logger.info(f"Snap List: {snap_data}")
        return snap_data

    def get_snapshot_id(self, snap_name):
        """Get snapshot id
        """
        snap_list = self.list_snapshot()
        return snap_list.get(snap_name, None)


    def delete_snapshot(self, snap_name):
        """Deletes lvol with given name
        """
        snap_id = self.get_snapshot_id(snap_name=snap_name)

        if not snap_id:
            self.logger.info("Snap does not exist. Exiting")
            return

        data = self.delete_request(api_url=f"/snapshot/{snap_id}")
        self.logger.info(f"Delete snap resp: {data}")
    
    def get_cluster_capacity(self):
        """Get cluster capacity
        """
        data = self.get_request(api_url=f"/cluster/capacity/{self.cluster_id}")
        return data["results"]
    
    def activate_cluster(self, cluster_id):
        """Activate the given cluster

        Args:
            cluster_id (str): Activates the given cluster
        """
        self.put_request(api_url=f"/cluster/activate/{cluster_id}")
