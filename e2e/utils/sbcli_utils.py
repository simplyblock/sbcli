import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def delete_request(self, api_url, headers=None, expected_error_code=None, treat_404_as_success=False):
        """Performs delete request on the given API URL

        Args:
            api_url (str): Endpoint to request
            headers (dict, optional): Headers needed. Defaults to None.
            treat_404_as_success (bool): If True, treat HTTP 404 as idempotent
                success (resource already deleted). Defaults to False.

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
                elif resp.status_code == HTTPStatus.NOT_FOUND and treat_404_as_success:
                    self.logger.info(f"DELETE {api_url} returned 404 — resource already deleted, treating as success")
                    return {"status": True, "results": [], "error": None}
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
        self.logger.debug(f"LVOL List: {lvol_data}")
        return lvol_data

    def get_clones_of_snapshot(self, snap_id):
        """Return list of clone lvols for a given snapshot ID.
        Queries GET /lvol and filters by cloned_from_snap.
        Returns list of {"lvol_name": ..., "id": ...} dicts.
        """
        data = self.get_request(api_url="/lvol")
        clones = []
        for lvol_info in data.get("results", []):
            if lvol_info.get("cloned_from_snap") == snap_id:
                clones.append({
                    "lvol_name": lvol_info.get("lvol_name"),
                    "id": lvol_info.get("id"),
                })
        return clones

    def get_lvol_by_id(self, lvol_id):
        """Return all lvol with given id
        """
        data = self.get_request(api_url=f"/lvol/{lvol_id}")
        return data

    def add_lvol(self, lvol_name, pool_name, size="256M", distr_ndcs=0, distr_npcs=0,
                 distr_bs=4096, distr_chunk_bs=4096, max_rw_iops=0, max_rw_mbytes=0,
                 max_r_mbytes=0, max_w_mbytes=0, host_id=None, retry=10,
                 crypto=False, fabric="tcp", cluster_id=None,
                 max_namespace_per_subsys=None, namespace=None):
        """Adds lvol with given params
        """
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
            "fabric": fabric
        }
        if distr_ndcs != 0 and distr_npcs != 0:
            body["ndcs"] = int(distr_ndcs)
            body["npcs"] = int(distr_npcs)
            body["bs"] = str(distr_bs)
            body["chunk_bs"] = str(distr_chunk_bs)
        if host_id:
            body["host_id"] = host_id
        if crypto:
            body["crypto"] = True
        
        if max_namespace_per_subsys is not None:
            body["max_namespace_per_subsys"] = int(max_namespace_per_subsys)

        if namespace:
            # flag for auto-grouping into existing parent subsystem
            body["namespaced"] = True
        
        self.post_request(api_url="/lvol", body=body, retry=retry)

    def delete_lvol(self, lvol_name, max_attempt=120, skip_error=False, deadline=None):
        """Deletes lvol with given name.

        Args:
            deadline: Optional absolute time.time() deadline. Overrides the
                default 15-minute per-lvol timeout when set by callers like
                delete_all_lvols that enforce a shared global timeout.
        """
        try:
            lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        except:
            if skip_error:
                self.logger.info(f"Lvol {lvol_name} not not found!! Continuing without Delete!!")
                return True
            raise Exception(f"No such Lvol {lvol_name} found!!")

        if not lvol_id:
            if skip_error:
                self.logger.info(f"Lvol {lvol_name} does not exist. Exiting!!")
                return True
            raise Exception(f"Lvol {lvol_name} does not exist")
        self.logger.info(f"ledoo {lvol_name}, {lvol_id}")

        try:
            data = self.delete_request(api_url=f"/lvol/{lvol_id}", treat_404_as_success=True)
        except Exception as e:
            if skip_error:
                self.logger.warning(f"delete_lvol DELETE request for {lvol_name} ({lvol_id}) failed: {e}. Continuing with skip_error=True")
                return False
            raise
        self.logger.info(f"Delete lvol resp: {data}")

        lvols = self.list_lvols()
        attempt = 0
        if deadline is None:
            deadline = time.time() + 15 * 60  # hard 15-minute wallclock timeout
        while True:
            if lvol_name not in list(lvols.keys()):
                self.logger.info(f"Lvol {lvol_name} deleted successfully!!")
                return True
            if time.time() > deadline:
                msg = f"Lvol {lvol_name} not deleted before deadline"
                if skip_error:
                    self.logger.warning(msg)
                    return False
                raise Exception(msg)
            if attempt % 12 == 0:
                try:
                    cur_state = self.get_lvol_details(lvol_id=lvol_id)[0]["status"]
                except Exception as _:
                    self.logger.info(f"Lvol {lvol_name} is not in the lvol list as error. Checking again!")
                    lvols = self.list_lvols()
                    attempt += 1
                    sleep_n_sec(5)
                    continue
                if cur_state in ("online", "in_deletion"):
                    self.logger.info(f"Lvol {lvol_name} in {cur_state} state. Retrying Delete!")
                    try:
                        data = self.delete_request(api_url=f"/lvol/{lvol_id}", treat_404_as_success=True)
                        self.logger.info(f"Delete lvol resp: {data}")
                    except Exception as e:
                        if skip_error:
                            self.logger.warning(
                                f"delete_lvol retry DELETE for {lvol_name} ({lvol_id}) failed: {e}. "
                                f"Continuing with skip_error=True"
                            )
                        else:
                            raise
            if attempt > max_attempt:
                if skip_error:
                    return False
                raise Exception(f"Lvol {lvol_name} is not getting deleted!!")

            attempt += 1
            self.logger.info(f"Lvol {lvol_name} is in_deletion. Checking again!")
            sleep_n_sec(5)
            lvols = self.list_lvols()

    def delete_all_clones(self, max_workers=10, timeout=1800, stall_timeout=1800):
        """Delete all clone lvols (lvols with cloned_from_snap set).

        Must be called BEFORE delete_all_snapshots, because SPDK refuses
        to delete a snapshot that still has clones.

        Uses fire-then-bulk-wait: issues DELETE for all clones in parallel,
        then polls list_lvols() in a single loop until all are gone.
        Gives up if no progress is made for *stall_timeout* seconds.
        """
        data = None
        for attempt in range(3):
            try:
                data = self.get_request(api_url="/lvol")
                break
            except Exception as e:
                self.logger.warning(
                    f"delete_all_clones: /lvol list attempt {attempt+1} failed: {e}"
                )
                time.sleep(5)
        if data is None:
            self.logger.warning("delete_all_clones: could not list lvols after 3 attempts, skipping")
            return
        clone_names = [
            lvol_info.get("lvol_name")
            for lvol_info in data.get("results", [])
            if lvol_info.get("cloned_from_snap")
        ]
        if not clone_names:
            return
        self.logger.info(f"Deleting {len(clone_names)} clones (max_workers={max_workers})")

        # Phase A: fire DELETE for every clone without waiting
        clone_ids = {
            lvol_info.get("lvol_name"): lvol_info.get("id")
            for lvol_info in data.get("results", [])
            if lvol_info.get("cloned_from_snap")
        }
        self._fire_delete_lvols(clone_ids, max_workers)

        # Phase B: bulk-wait
        self._wait_lvols_gone(
            clone_names, label="clone_wait",
            timeout=timeout, stall_timeout=stall_timeout,
        )

    def delete_all_lvols(self, max_workers=10, timeout=1800, stall_timeout=1800):
        """Deletes all lvols using fire-then-bulk-wait.

        Issues DELETE for every lvol in parallel, then polls list_lvols()
        in a single loop until all are gone or *timeout* is reached.
        Gives up early if no lvols are deleted for *stall_timeout* seconds
        (indicates all nodes hosting these lvols are down).
        """
        lvols = self.list_lvols()
        if not lvols:
            return
        names = list(lvols.keys())
        self.logger.info(f"Deleting {len(names)} lvols (max_workers={max_workers})")

        # Phase A: fire DELETE for every lvol without waiting
        self._fire_delete_lvols(lvols, max_workers)

        # Phase B: bulk-wait
        self._wait_lvols_gone(
            names, label="lvol_wait",
            timeout=timeout, stall_timeout=stall_timeout,
        )

    def _fire_delete_lvols(self, name_to_id: dict, max_workers: int = 10):
        """Issue DELETE for each lvol without waiting for completion."""
        def _fire(name):
            lvol_id = name_to_id.get(name)
            if not lvol_id:
                return
            try:
                self.delete_request(
                    api_url=f"/lvol/{lvol_id}", treat_404_as_success=True,
                )
            except Exception as e:
                self.logger.warning(f"[fire_delete] {name}: {e}")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = {pool.submit(_fire, n): n for n in name_to_id}
            for f in as_completed(futs):
                f.result()

    def _wait_lvols_gone(
        self, names: list, label: str = "lvol_wait",
        timeout: int = 1800, stall_timeout: int = 180,
        re_delete_interval: int = 120,
    ):
        """Wait for lvols to disappear from the API.

        Polls list_lvols() every 30s (single call, not per-lvol).
        Re-issues DELETE for stuck items every *re_delete_interval* seconds.
        Gives up early if no progress for *stall_timeout* seconds.
        """
        deadline = time.time() + timeout
        remaining = set(names)
        last_progress = time.time()
        last_re_delete = time.time()

        self.logger.info(
            f"[{label}] Waiting for {len(remaining)} lvols to be deleted "
            f"(timeout={timeout}s, stall_timeout={stall_timeout}s)"
        )

        while remaining and time.time() < deadline:
            try:
                current = self.list_lvols()
            except Exception as exc:
                self.logger.warning(f"[{label}] list_lvols failed: {exc}")
                sleep_n_sec(10)
                continue

            still_present = remaining & set(current.keys())
            just_deleted = remaining - still_present
            if just_deleted:
                self.logger.info(
                    f"[{label}] {len(just_deleted)} more deleted, "
                    f"{len(still_present)} remaining"
                )
                last_progress = time.time()
            remaining = still_present

            if not remaining:
                break

            # Stall detection: if no lvols were deleted for stall_timeout,
            # the backend nodes are likely down — stop waiting.
            if time.time() - last_progress > stall_timeout:
                self.logger.warning(
                    f"[{label}] No progress for {stall_timeout}s with "
                    f"{len(remaining)} lvols remaining — giving up "
                    f"(nodes likely down)"
                )
                break

            # Re-issue DELETE for stuck items
            now = time.time()
            if now - last_re_delete >= re_delete_interval:
                self.logger.info(
                    f"[{label}] Re-issuing DELETE for "
                    f"{len(remaining)} stuck lvols"
                )
                for name in list(remaining)[:200]:
                    lvol_id = current.get(name)
                    if lvol_id:
                        try:
                            self.delete_request(
                                api_url=f"/lvol/{lvol_id}",
                                treat_404_as_success=True,
                            )
                        except Exception:
                            pass
                last_re_delete = now

            sleep_n_sec(30)

        if remaining:
            self.logger.warning(
                f"[{label}] Finished with {len(remaining)}/{len(names)} "
                f"lvols still present"
            )

    def get_lvol_id(self, lvol_name):
        """Return lvol by lvol name
        """
        lvols = self.list_lvols()
        return lvols.get(lvol_name, None)
    
    def lvol_exists(self, lvol_name):
        """Return if lvol exists or not
        """
        lvols = self.list_lvols()
        lvol_exists = lvols.get(lvol_name, None)
        if lvol_exists:
            return True
        else:
            return False

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
            if "connect" in entry:
                connect_lines.append(entry["connect"])
            else:
                # Fallback: support both hyphenated and underscored keys
                def _g(key):
                    return entry.get(key) or entry.get(key.replace("-", "_"))

                connect_line = (
                    "sudo nvme connect "
                    f"--reconnect-delay={_g('reconnect-delay')} "
                    f"--ctrl-loss-tmo=-1 "
                    f"--nr-io-queues={_g('nr-io-queues')} "
                    f"--keep-alive-tmo={_g('keep-alive-tmo')} "
                    f"--transport={entry.get('transport', 'tcp')} "
                    f"--traddr={entry.get('ip')} "
                    f"--trsvcid={entry.get('port')} "
                    f"--nqn={entry.get('nqn')}"
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

    def remove_device(self, device_id):
        """Remove a storage device (transitions ONLINE → REMOVED).

        Args:
            device_id (str): The storage device UUID.

        Returns:
            dict: API response results.
        """
        data = self.get_request(api_url=f"/device/remove/{device_id}")
        self.logger.info(f"Remove device response: {data}")
        return data

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
    
    def wait_for_device_status(self, node_id, status, timeout=60, device_id=None):
        """Wait for device(s) to reach the expected status.

        Args:
            node_id: Storage node UUID.
            status: Expected status string or list of status strings.
            timeout: Max seconds to wait.
            device_id: If provided, only check this specific device.
                       If None, check ALL devices on the node (legacy behaviour).
        """
        status = status if isinstance(status, list) else [status]
        device_ids = {}
        device_details = self.get_device_details(storage_node_id=node_id)
        total_devices = len(device_details)
        while timeout > 0:
            self.logger.info("Retrying Device Status check")
            device_details = self.get_device_details(storage_node_id=node_id)

            if device_id:
                # Single-device mode: only check the specified device
                for device in device_details:
                    if device['id'] == device_id:
                        actual = device['status']
                        self.logger.info(f"Device ID: {device_id} Expected Status: {status} / Actual Status: {actual}")
                        if actual in status:
                            return device_details
                        break
                else:
                    self.logger.warning(f"Device {device_id} not found on node {node_id}")
            else:
                # All-devices mode (legacy): require every device to match
                device_ids = {}
                for device in device_details:
                    device_ids[device['id']] = device['status']
                self.logger.info(f"Device statuses: {device_ids}")
                if len(device_ids) == total_devices and self.all_expected_status(device_ids, status):
                    return device_details
                for did, dstatus in device_ids.items():
                    self.logger.info(f"Device ID: {did} Expected Status: {status} / Actual Status: {dstatus}")

            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(f"Timed out waiting for device status, Node id: {node_id}, Device id: {device_id or list(device_ids.keys())}, "
                            f"Expected status: {status}, Actual status: {list(device_ids.values()) if not device_id else 'see above'}")
    
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
        return self.get_request(f"/cluster/get-tasks/{cluster_id}?limit=0")

    def wait_migration_tasks_complete(self, timeout=3600):
        """Wait until all failed_device_migration tasks finish.

        Polls ``list_migration_tasks`` every 10 seconds until no active
        failure-migration tasks remain or *timeout* seconds elapse.

        Args:
            timeout (int): Maximum seconds to wait (default 3600).

        Returns:
            float: Elapsed seconds until migration completed.

        Raises:
            TimeoutError: If active tasks remain after *timeout*.
        """
        import time as _time
        start = _time.time()
        active = []
        while _time.time() - start < timeout:
            try:
                tasks = self.list_migration_tasks(self.cluster_id)
            except Exception as exc:
                self.logger.warning(f"list_migration_tasks API failed: {exc}")
                sleep_n_sec(10)
                continue
            active = [
                t for t in tasks.get("results", [])
                if t.get("function_name") == "failed_device_migration"
                and t.get("status") not in ("done", "cancelled", "error")
            ]
            if not active:
                elapsed = _time.time() - start
                self.logger.info(
                    f"All failure-migration tasks complete in {elapsed:.1f}s"
                )
                return elapsed
            self.logger.info(
                f"Waiting for {len(active)} migration task(s) to finish …"
            )
            sleep_n_sec(10)
        raise TimeoutError(
            f"Migration not complete after {timeout}s, "
            f"{len(active)} task(s) remain"
        )

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
    
    # def add_snapshot(self, lvol_id, snapshot_name, retry=3):
    #     """Adds snapshot with given params
    #     """
        
    #     body = {
    #         "lvol_id": lvol_id,
    #         "snapshot_name": snapshot_name,
    #     }
        
    #     self.post_request(api_url="/snapshot", body=body, retry=retry)

    # def add_clone(self, snapshot_id, clone_name, retry=3):
    #     """Adds clone with given params
    #     """
        
    #     body = {
    #         "snapshot_id": snapshot_id,
    #         "clone_name": clone_name,
    #     }
        
    #     self.post_request(api_url="/snapshot/clone", body=body, retry=retry)

    # def list_snapshot(self):
    #     """Return all snapshots
    #     """
    #     snap_data = dict()
    #     data = self.get_request(api_url="/snapshot")
    #     for snap_info in data["results"]:
    #         snap_data[snap_info["snap_name"]] = snap_info["id"]
    #     self.logger.info(f"Snap List: {snap_data}")
    #     return snap_data

    # def get_snapshot_id(self, snap_name):
    #     """Get snapshot id
    #     """
    #     snap_list = self.list_snapshot()
    #     return snap_list.get(snap_name, None)


    # def delete_snapshot(self, snap_name):
    #     """Deletes lvol with given name
    #     """
    #     snap_id = self.get_snapshot_id(snap_name=snap_name)

    #     if not snap_id:
    #         self.logger.info("Snap does not exist. Exiting")
    #         return

    #     data = self.delete_request(api_url=f"/snapshot/{snap_id}")
    #     self.logger.info(f"Delete snap resp: {data}")
    
    def get_cluster_capacity(self):
        """Get cluster capacity
        """
        data = self.get_request(api_url=f"/cluster/capacity/{self.cluster_id}")
        return data["results"]

    def get_node_capacity(self, node_id, history=None):
        """Get per-node capacity statistics.

        Args:
            node_id (str): Storage node UUID.
            history (str, optional): History window, e.g. ``"1d12h"``.

        Returns:
            dict: Capacity record(s) with keys like *size_total*,
            *size_used*, *size_util*, etc.
        """
        url = f"/storagenode/capacity/{node_id}"
        if history:
            url += f"/history/{history}"
        data = self.get_request(api_url=url)
        self.logger.info(f"Node capacity for {node_id}: {data}")
        return data["results"]

    def get_device_capacity(self, device_id):
        """Get per-device capacity statistics.

        Args:
            device_id (str): Device UUID.

        Returns:
            list: Capacity records with keys *size_total*, *size_used*,
            *size_free*, *size_util*.
        """
        url = f"/device/capacity/{device_id}"
        data = self.get_request(api_url=url)
        return data.get("results", [])

    def activate_cluster(self, cluster_id):
        """Activate the given cluster

        Args:
            cluster_id (str): Activates the given cluster
        """
        self.put_request(api_url=f"/cluster/activate/{cluster_id}")

        # ---------------------------------------------------------
    # Snapshot + Clone APIs
    # ---------------------------------------------------------
    def add_snapshot(self, lvol_id: str, snapshot_name: str, retry: int = 10):
        """
        Create snapshot from LVOL (API).
        Endpoint: POST /snapshot
        Body: { "lvol_id": "...", "snapshot_name": "..." }
        """
        body = {
            "lvol_id": lvol_id,
            "snapshot_name": snapshot_name
        }
        return self.post_request(api_url="/snapshot", body=body, retry=retry)

    def add_clone(self, snapshot_id: str, clone_name: str, retry: int = 10):
        """
        Create clone from snapshot (API).
        Endpoint: POST /snapshot/clone
        Body: { "snapshot_id": "...", "clone_name": "..." }
        """
        body = {
            "snapshot_id": snapshot_id,
            "clone_name": clone_name
        }
        return self.post_request(api_url="/snapshot/clone", body=body, retry=retry)

    def list_snapshots(self):
        """
        List snapshots (API).
        Endpoint: GET /snapshot
        Returns dict: { snap_name: snap_id }
        """
        data = self.get_request(api_url="/snapshot")
        snap_data = {}

        for item in data.get("results", []):
            # Different builds sometimes expose snap name fields differently.
            # Handle both to avoid breakages.
            name = (
                item.get("snap_name")
                or item.get("snapshot_name")
                or item.get("name")
            )
            sid = item.get("id") or item.get("uuid")
            if name and sid:
                snap_data[name] = sid

        self.logger.debug(f"Snapshot List: {snap_data}")
        return snap_data

    def get_snapshot_id(self, snap_name: str):
        """
        Get snapshot id by name using list_snapshots().
        """
        return self.list_snapshots().get(snap_name)

    def delete_snapshot(self, snap_name: str = None, snap_id: str = None,
                        max_attempt: int = 60, skip_error: bool = False,
                        wait: bool = True):
        """
        Delete snapshot by name or id (API).
        Endpoint: DELETE /snapshot/{snap_id}

        If *wait* is True (default), polls until the snapshot disappears.
        If *wait* is False, issues the DELETE and returns immediately
        (fire-and-forget mode for bulk deletion).
        """
        if not snap_id:
            if not snap_name:
                raise ValueError("delete_snapshot requires snap_name or snap_id")
            snap_id = self.get_snapshot_id(snap_name=snap_name)

        if not snap_id:
            if skip_error:
                self.logger.info(f"Snapshot not found (skip_error=True). snap_name={snap_name}")
                return
            raise Exception(f"Snapshot not found. snap_name={snap_name}")

        resp = self.delete_request(api_url=f"/snapshot/{snap_id}", treat_404_as_success=True)
        self.logger.info(f"Delete snapshot resp: {resp}")

        if not wait:
            return

        # wait for removal
        attempt = 0
        while attempt < max_attempt:
            cur = self.list_snapshots()
            # if deleting by name, use name check; else id check
            if snap_name:
                if snap_name not in cur:
                    return
            else:
                if snap_id not in cur.values():
                    return

            attempt += 1
            sleep_n_sec(5)

        if skip_error:
            return
        raise Exception(f"Snapshot did not get deleted in time. snap_name={snap_name}, snap_id={snap_id}")

    def delete_all_snapshots(self, max_workers=10):
        """
        Convenience cleanup via API — fire-and-forget parallel deletion
        followed by a single bulk-wait loop.
        """
        snaps = self.list_snapshots()
        snap_names = list(snaps.keys())
        if not snap_names:
            return
        self.logger.info(f"Deleting {len(snap_names)} snapshots (max_workers={max_workers})")

        # Phase A: fire DELETE for every snapshot without waiting
        def _fire(name):
            try:
                sid = snaps.get(name)
                self.delete_snapshot(snap_id=sid, skip_error=True, wait=False)
            except Exception as e:
                self.logger.info(f"Snapshot delete failed (continuing): {name}, err={e}")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = {pool.submit(_fire, n): n for n in snap_names}
            for f in as_completed(futs):
                f.result()

        # Phase B: bulk-wait until all snapshots disappear
        self.wait_snapshots_deleted(snap_names, timeout=1800)

    def wait_snapshots_deleted(
        self, names: list, timeout: int = 1800, re_delete_interval: int = 120,
    ):
        """Wait for snapshots to disappear from the API.

        Polls list_snapshots() every 30s (single call, not per-snapshot)
        and re-issues DELETE for stuck items periodically.
        """
        deadline = time.time() + timeout
        remaining = set(names)
        last_re_delete = time.time()

        self.logger.info(
            f"[snap_wait] Waiting for {len(remaining)} snapshots "
            f"to be deleted (timeout={timeout}s)"
        )

        while remaining and time.time() < deadline:
            try:
                current = self.list_snapshots()
            except Exception as exc:
                self.logger.warning(f"[snap_wait] list_snapshots failed: {exc}")
                sleep_n_sec(10)
                continue

            still_present = remaining & set(current.keys())
            just_deleted = remaining - still_present
            if just_deleted:
                self.logger.info(
                    f"[snap_wait] {len(just_deleted)} more deleted, "
                    f"{len(still_present)} remaining"
                )
            remaining = still_present

            if not remaining:
                break

            # Re-issue DELETE for stuck items
            now = time.time()
            if now - last_re_delete >= re_delete_interval:
                self.logger.info(
                    f"[snap_wait] Re-issuing DELETE for "
                    f"{len(remaining)} stuck snapshots"
                )
                for name in list(remaining)[:200]:
                    sid = current.get(name)
                    if sid:
                        try:
                            self.delete_request(
                                api_url=f"/snapshot/{sid}",
                                treat_404_as_success=True,
                            )
                        except Exception:
                            pass
                last_re_delete = time.time()

            sleep_n_sec(30)

        if remaining:
            self.logger.warning(
                f"[snap_wait] Timed out with {len(remaining)} "
                f"snapshots still present"
            )

    # ── Pool-level host management (DHCHAP) ─────────────────────────────────

    def add_host_to_pool(self, pool_id, host_nqn):
        """Register a client NQN at pool level.

        POST /pool/<pool_id>/host  body: {"host_nqn": "<nqn>"}
        """
        body = {"host_nqn": host_nqn}
        self.logger.info(f"[add_host_to_pool] pool={pool_id} nqn={host_nqn}")
        return self.post_request(api_url=f"/pool/{pool_id}/host", body=body)

    def remove_host_from_pool(self, pool_id, host_nqn):
        """Remove a client NQN from pool-level host list.

        DELETE /pool/<pool_id>/host  body: {"host_nqn": "<nqn>"}
        """
        self.logger.info(f"[remove_host_from_pool] pool={pool_id} nqn={host_nqn}")
        return self.delete_request(api_url=f"/pool/{pool_id}/host/{host_nqn}")

