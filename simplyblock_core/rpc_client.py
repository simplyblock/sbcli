
import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger()


def print_dict(d):
    print(json.dumps(d, indent=2))


def print_json(s):
    print(json.dumps(s, indent=2).strip('"'))


class RPCException(Exception):
    def __init__(self, message):
        self.message = message


class RPCClient:

    # ref: https://spdk.io/doc/jsonrpc.html
    DEFAULT_ALLOWED_METHODS = ["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]

    def __init__(self, ip_address, port, username, password, timeout=60, retry=3):
        self.ip_address = ip_address
        self.port = port
        self.url = 'http://%s:%s/' % (self.ip_address, self.port)
        self.username = username
        self.password = password
        self.timeout = timeout
        self.session = requests.session()
        self.session.auth = (self.username, self.password)
        self.session.verify = False
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry,
                        allowed_methods=self.DEFAULT_ALLOWED_METHODS)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.timeout = self.timeout

    def _request(self, method, params=None):
        ret, _ = self._request2(method, params)
        return ret

    def _request2(self, method, params=None):
        payload = {'id': 1, 'method': method}
        if params:
            payload['params'] = params
        try:
            logger.debug("Requesting method: %s, params: %s", method, params)
            response = self.session.post(self.url, data=json.dumps(payload), timeout=self.timeout)
        except Exception as e:
            logger.error(e)
            return False, str(e)

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

            if 'result' in data:
                result = data['result']
            if 'error' in data:
                error = data['error']
            if result is not None or error is not None:
                return result, error
            else:
                return data, None

        if ret_code in [500, 400]:
            raise RPCException("Invalid http status: %s" % ret_code)
        logger.error("Unknown http status: %s", ret_code)
        return None, None

    def get_version(self):
        return self._request("spdk_get_version")

    def subsystem_list(self, nqn_name=None):
        data = self._request("nvmf_get_subsystems")
        if data and nqn_name:
            for d in data:
                if d['nqn'] == nqn_name:
                    return [d]
            return []
        else:
            return data

    def subsystem_delete(self, nqn):
        return self._request("nvmf_delete_subsystem", params={'nqn': nqn})

    def subsystem_create(self, nqn, serial_number, model_number):
        params = {
            "nqn": nqn,
            "serial_number": serial_number,
            "allow_any_host": True,
            "ana_reporting": True,
            "model_number": model_number}
        return self._request("nvmf_create_subsystem", params)

    def subsystem_add_host(self, nqn, host):
        params = {"nqn": nqn, "host": host}
        return self._request("nvmf_subsystem_add_host", params)

    def transport_list(self, trtype=None):
        params = None
        if trtype:
            params = {"trtype": trtype}
        return self._request("nvmf_get_transports", params)

    def transport_create(self, trtype, qpair_count=256):
        """
            [{'trtype': 'TCP', 'max_queue_depth': 128,
               'max_io_qpairs_per_ctrlr': 127, 'in_capsule_data_size': 4096,
               'max_io_size': 131072, 'io_unit_size': 131072, 'max_aq_depth': 128,
               'num_shared_buffers': 511, 'buf_cache_size': 4294967295,
               'dif_insert_or_strip': False, 'zcopy': False, 'c2h_success': True,
               'sock_priority': 0, 'abort_timeout_sec': 1}]
            The output above is the default values of nvmf_get_transports
            Failing of creating more than 127 lvols is because of max_io_qpairs_per_ctrlr
            Currently, we set it to 256 and the max now is 246 lvols, because of bdev_io_pool_size
            TODO, investigate what is the best configuration for the parameters above and bdev_io_pool_size
        """
        params = {
            "trtype": trtype,
            "max_io_qpairs_per_ctrlr": qpair_count,
            "max_queue_depth": 512,
            "abort_timeout_sec": 5,
            "ack_timeout": 512,
            "zcopy": True,
            "in_capsule_data_size": 4096,
            "max_io_size": 131072,
            "io_unit_size": 131072,
            "max_aq_depth": 128,
            "num_shared_buffers": 8192,
            "buf_cache_size": 32,
            "dif_insert_or_strip": False,
            "c2h_success": True,
            "sock_priority": 0

        }
        return self._request("nvmf_create_transport", params)

    def transport_create_caching(self, trtype):
        params = {
            "trtype": trtype,
        }
        return self._request("nvmf_create_transport", params)

    def listeners_list(self, nqn):
        params = {"nqn": nqn}
        return self._request("nvmf_subsystem_get_listeners", params)

    def listeners_create(self, nqn, trtype, traddr, trsvcid):
        """"
            nqn: Subsystem NQN.
            trtype: Transport type ("RDMA").
            traddr: Transport address.
            trsvcid: Transport service ID (required for RDMA or TCP).
        """
        params = {
            "nqn": nqn,
            "listen_address": {
                "trtype": trtype,
                "adrfam": "IPv4",
                "traddr": traddr,
                "trsvcid": trsvcid
            }
        }
        return self._request("nvmf_subsystem_add_listener", params)

    def bdev_nvme_controller_list(self, name=None):
        params = None
        if name:
            params = {"name": name}
        return self._request("bdev_nvme_get_controllers", params)

    def bdev_nvme_controller_attach(self, name, pci_addr):
        params = {"name": name, "trtype": "pcie", "traddr": pci_addr}
        return self._request2("bdev_nvme_attach_controller", params)

    def alloc_bdev_controller_attach(self, name, pci_addr):
        params = {"traddr": pci_addr, "ns_id": 1, "label": name}
        return self._request2("ultra21_alloc_ns_mount", params)

    def bdev_nvme_detach_controller(self, name):
        params = {"name": name}
        return self._request2("bdev_nvme_detach_controller", params)

    def ultra21_alloc_ns_init(self, pci_addr):
        params = {
            "traddr": pci_addr,
            "ns_id": 1,
            "label": "SYSVMS84-x86",
            "desc": "A volume to keep OpenVMS/VAX/Alpha/IA64/x86 operation system data",
            "pagesz": 16384
        }
        return self._request2("ultra21_alloc_ns_init", params)

    def nvmf_subsystem_add_ns(self, nqn, dev_name, uuid=None, nguid=None):
        params = {
            "nqn": nqn,
            "namespace": {
                "bdev_name": dev_name
            }
        }

        if uuid:
            params['namespace']['uuid'] = uuid

        if nguid:
            params['namespace']['nguid'] = nguid

        return self._request("nvmf_subsystem_add_ns", params)

    def nvmf_subsystem_remove_ns(self, nqn, nsid):
        params = {
            "nqn": nqn,
            "nsid": nsid}
        return self._request("nvmf_subsystem_remove_ns", params)

    def nvmf_subsystem_listener_set_ana_state(self, nqn, ip, port, is_optimized=True):
        params = {
            "nqn": nqn,
            "listen_address": {
                "trtype": "tcp",
                "adrfam": "ipv4",
                "traddr": ip,
                "trsvcid": str(port)
            },
        }
        if is_optimized:
            params['ana_state'] = "optimized"
        else:
            params['ana_state'] = "non_optimized"

        return self._request("nvmf_subsystem_listener_set_ana_state", params)

    def get_device_stats(self, uuid):
        params = {"name": uuid}
        return self._request("bdev_get_iostat", params)

    def reset_device(self, device_name):
        params = {"name": device_name}
        return self._request("bdev_nvme_reset_controller", params)

    def create_lvstore(self, name, bdev_name, cluster_sz, clear_method, num_md_pages_per_cluster_ratio):
        params = {
            "bdev_name": bdev_name,
            "lvs_name": name,
            "cluster_sz": cluster_sz,
            "clear_method": clear_method,
            "num_md_pages_per_cluster_ratio": num_md_pages_per_cluster_ratio,
        }
        return self._request("bdev_lvol_create_lvstore", params)

    def create_lvol(self, name, size_in_mib, lvs_name):
        params = {
            "lvol_name": name,
            "size_in_mib": size_in_mib,
            "lvs_name": lvs_name,
            "thin_provision": True,
            "clear_method": "unmap",
        }
        return self._request("bdev_lvol_create", params)

    def delete_lvol(self, name):
        params = {"name": name}
        return self._request("bdev_lvol_delete", params)

    def get_bdevs(self, name=None):
        params = None
        if name:
            params = {"name": name}
        return self._request("bdev_get_bdevs", params)

    def resize_lvol(self, lvol_bdev, blockcnt):
        params = {
            "lvol_bdev": lvol_bdev,
            "blockcnt": blockcnt
        }
        return self._request("ultra21_lvol_set", params)

    def resize_clone(self, clone_bdev, blockcnt):
        params = {
            "clone_bdev": clone_bdev,
            "blockcnt": blockcnt
        }
        return self._request("ultra21_lvol_set", params)

    def lvol_read_only(self, name):
        params = {"name": name}
        return self._request("bdev_lvol_set_read_only", params)

    def lvol_create_snapshot(self, lvol_id, snapshot_name):
        params = {
            "lvol_name": lvol_id,
            "snapshot_name": snapshot_name}
        return self._request("bdev_lvol_snapshot", params)

    def lvol_clone(self, snapshot_name, clone_name):
        params = {
            "snapshot_name": snapshot_name,
            "clone_name": clone_name}
        return self._request("bdev_lvol_clone", params)

    def lvol_compress_create(self, base_bdev_name, pm_path):
        params = {
            "base_bdev_name": base_bdev_name,
            "pm_path": pm_path
        }
        return self._request("bdev_compress_create", params)

    def lvol_crypto_create(self, name, base_name, key_name):
        params = {
            "base_bdev_name": base_name,
            "name": name,
            "key_name": key_name,
        }
        return self._request("bdev_crypto_create", params)

    def lvol_crypto_key_create(self, name, key, key2):
        # todo: mask the keys so that they don't show up in logs
        params = {
            "cipher": "AES_XTS",
            "key": key,
            "key2": key2,
            "name": name
        }
        return self._request("accel_crypto_key_create", params)

    def lvol_crypto_delete(self, name):
        params = {"name": name}
        return self._request("bdev_crypto_delete", params)

    def lvol_compress_delete(self, name):
        params = {"name": name}
        return self._request("bdev_compress_delete", params)

    def ultra21_bdev_pass_create(self, base_bdev, vuid, pt_name):
        params = {
            "base_bdev": base_bdev,
            "vuid": vuid,
            "pt_bdev": pt_name
        }
        return self._request2("ultra21_bdev_pass_create", params)

    def ultra21_bdev_pass_delete(self, name):
        params = {"name": name}
        return self._request2("ultra21_bdev_pass_delete", params)

    def bdev_alceml_create(self, alceml_name, nvme_name, uuid, pba_init_mode=3,
                           alceml_cpu_mask="", alceml_worker_cpu_mask=""):
        params = {
            "name": alceml_name,
            "cntr_path": nvme_name,
            "num_blocks": 0,
            "block_size": 0,
            "num_blocks_reported": 0,
            "md_size": 0,
            "use_ram": False,
            "pba_init_mode": pba_init_mode,
            "pba_page_size": 2097152,
            "uuid": uuid,
            # "use_scheduling": True,
            "use_optimized": True,
            "pba_nbalign": 4096,
            "use_map_whole_page_on_1st_write": True
        }
        if alceml_cpu_mask:
            params["bdb_lcpu_mask"] = int(alceml_cpu_mask, 16)
        if alceml_worker_cpu_mask:
            params["bdb_lcpu_mask_alt_workers"] = int(alceml_worker_cpu_mask,16)
        return self._request("bdev_alceml_create", params)

    def bdev_distrib_create(self, name, vuid, ndcs, npcs, num_blocks, block_size, jm_names,
                            chunk_size, ha_comm_addrs=None, ha_inode_self=None, pba_page_size=2097152,
                            distrib_cpu_mask="", ha_is_non_leader=True, jm_vuid=0):
        """"
            // Optional (not specified = no HA)
            // Comma-separated communication addresses, for each node, e.g. "192.168.10.1:45001,192.168.10.1:32768".
            // Number of addresses in the list is exactly the number of nodes in HA group,
            //  this must be common among all DISTRIB instances in the group.
          "ha_comm_addrs": "192.168.10.1:45001,192.168.10.1:32768"

            // Optional, default = 0
            //  This node (device) number, in the group, defined by ha_comm_addrs.
          "ha_inode_self": 1
        """
        try:
            ret = self.get_bdevs(name)
            if ret:
                return ret
        except:
            pass
        params = {
            "name": name,
            "jm_names": ",".join(jm_names),
            "vuid": vuid,
            "ndcs": ndcs,
            "npcs": npcs,
            "num_blocks": num_blocks,
            "block_size": block_size,
            "chunk_size": chunk_size,
            "pba_page_size": pba_page_size,
        }
        if jm_vuid > 0:
            params["jm_vuid"] = jm_vuid
            params["ha_is_non_leader"] = ha_is_non_leader

        if ha_comm_addrs:
            params['ha_comm_addrs'] = ha_comm_addrs
            params['ha_inode_self'] = ha_inode_self
        if distrib_cpu_mask:
            params["bdb_lcpu_mask"] = int(distrib_cpu_mask, 16)

        return self._request("bdev_distrib_create", params)

    def bdev_lvol_delete_lvstore(self, name):
        params = {"lvs_name": name}
        return self._request2("bdev_lvol_delete_lvstore", params)

    def bdev_distrib_delete(self, name):
        params = {"name": name}
        return self._request2("bdev_distrib_delete", params)

    def bdev_alceml_delete(self, name):
        params = {"name": name}
        return self._request2("bdev_alceml_delete", params)

    def get_lvol_stats(self, uuid):
        params = {"name": uuid}
        return self._request("bdev_get_iostat", params)

    def bdev_raid_create(self, name, bdevs_list, raid_level="0", strip_size_kb=4):
        try:
            ret = self.get_bdevs(name)
            if ret:
                return ret
        except:
            pass
        params = {
            "name": name,
            "raid_level": raid_level,
            "strip_size_kb": strip_size_kb,
            "base_bdevs": bdevs_list
        }
        if raid_level == "1":
            params["strip_size_kb"] = 0
        return self._request("bdev_raid_create", params)

    def bdev_raid_delete(self, name):
        params = {
            "name": name
        }
        return self._request("bdev_raid_delete", params)

    def bdev_set_qos_limit(self, name, rw_ios_per_sec, rw_mbytes_per_sec, r_mbytes_per_sec, w_mbytes_per_sec):
        params = {
            "name": name
        }
        if rw_ios_per_sec is not None and rw_ios_per_sec >= 0:
            params['rw_ios_per_sec'] = rw_ios_per_sec
        if rw_mbytes_per_sec is not None and rw_mbytes_per_sec >= 0:
            params['rw_mbytes_per_sec'] = rw_mbytes_per_sec
        if r_mbytes_per_sec is not None and r_mbytes_per_sec >= 0:
            params['r_mbytes_per_sec'] = r_mbytes_per_sec
        if w_mbytes_per_sec is not None and w_mbytes_per_sec >= 0:
            params['w_mbytes_per_sec'] = w_mbytes_per_sec
        return self._request("bdev_set_qos_limit", params)

    def distr_send_cluster_map(self, params):
        return self._request("distr_send_cluster_map", params)

    def distr_get_cluster_map(self, name):
        params = {"name": name}
        return self._request("distr_dump_cluster_map", params)

    def distr_add_nodes(self, params):
        return self._request("distr_add_nodes", params)

    def distr_status_events_update(self, params):
        # ultra/DISTR_v2/src_code_app_spdk/specs/message_format_rpcs__distrib__v5.txt#L396C1-L396C27
        return self._request("distr_status_events_update", params)

    def bdev_nvme_attach_controller_tcp(self, name, nqn, ip, port):
        params = {
            "name": name,
            "trtype": "tcp",
            "traddr": ip,
            "adrfam": "ipv4",
            "trsvcid": str(port),
            "subnqn": nqn,
            "fabrics_connect_timeout_us": 100000,
            # "fast_io_fail_timeout_sec": 1,
            "num_io_queues": 16384,
            # "ctrlr_loss_timeout_sec": 1,
            "multipath":"disable",
            # "reconnect_delay_sec":1
        }
        return self._request("bdev_nvme_attach_controller", params)

    def bdev_nvme_attach_controller_tcp_caching(self, name, nqn, ip, port):
        params = {
            "name": name,
            "trtype": "tcp",
            "traddr": ip,
            "adrfam": "ipv4",
            "trsvcid": str(port),
            "subnqn": nqn
        }
        return self._request("bdev_nvme_attach_controller", params)

    def bdev_split(self, base_bdev, split_count):
        params = {
            "base_bdev": base_bdev,
            "split_count": split_count
        }
        return self._request("bdev_split_create", params)

    def bdev_PT_NoExcl_create(self, name, base_bdev_name):
        params = {
            "name": name,
            "base_bdev_name": base_bdev_name
        }
        return self._request("bdev_ptnonexcl_create", params)

    def bdev_PT_NoExcl_delete(self, name):
        params = {
            "name": name
        }
        return self._request("bdev_ptnonexcl_delete", params)

    def bdev_passtest_create(self, name, base_name):
        params = {
            "base_name": base_name,
            "pt_name": name
        }
        return self._request("bdev_passtest_create", params)

    def bdev_passtest_mode(self, name, mode):
        params = {
            "pt_name": name,
            "mode": mode
        }
        return self._request("bdev_passtest_mode", params)

    def bdev_passtest_delete(self, name):
        params = {
            "pt_name": name
        }
        return self._request("bdev_passtest_delete", params)

    def bdev_nvme_set_options(self):
        params = {
            # "action_on_timeout": "abort",
            "bdev_retry_count": 0,
            "transport_retry_count": 0,
            "ctrlr_loss_timeout_sec": 1,
            "fast_io_fail_timeout_sec": 0,
            "reconnect_delay_sec": 1,
            "keep_alive_timeout_ms": 10000,
            "transport_ack_timeout": 9,
            "timeout_us": 0
        }
        return self._request("bdev_nvme_set_options", params)

    def bdev_set_options(self, bdev_io_pool_size, bdev_io_cache_size, iobuf_small_cache_size, iobuf_large_cache_size):
        params = {"bdev_auto_examine": False}
        if bdev_io_pool_size > 0:
            params['bdev_io_pool_size'] = bdev_io_pool_size
        if bdev_io_cache_size > 0:
            params['bdev_io_cache_size'] = bdev_io_cache_size
        if iobuf_small_cache_size > 0:
            params['iobuf_small_cache_size'] = iobuf_small_cache_size
        if iobuf_small_cache_size > 0:
            params['iobuf_large_cache_size'] = iobuf_large_cache_size
        if params:
            return self._request("bdev_set_options", params)
        else:
            return False

    def iobuf_set_options(self, small_pool_count, large_pool_count, small_bufsize, large_bufsize):
        params = {}
        if small_pool_count > 0:
            params['small_pool_count'] = small_pool_count
        if large_pool_count > 0:
            params['large_pool_count'] = large_pool_count
        if small_bufsize > 0:
            params['small_bufsize'] = small_bufsize
        if large_bufsize > 0:
            params['large_bufsize'] = large_bufsize
        if params:
            return self._request("iobuf_set_options", params)
        else:
            return False

    def distr_status_events_get(self):
        return self._request("distr_status_events_get")

    def distr_status_events_discard_then_get(self, nev_discard, nev_read):
        params = {
            "nev_discard": nev_discard,
            "nev_read": nev_read,
        }
        return self._request("distr_status_events_discard_then_get", params)

    def alceml_get_capacity(self, name):
        params = {"name": name}
        return self._request("alceml_get_pages_usage", params)

    def bdev_ocf_create(self, name, mode, cache_name, core_name):
        params = {
            "name": name,
            "mode": mode,
            "cache_bdev_name": cache_name,
            "core_bdev_name": core_name}
        return self._request("bdev_ocf_create", params)

    def bdev_ocf_delete(self, name):
        params = {"name": name}
        return self._request("bdev_ocf_delete", params)

    def bdev_malloc_create(self, name, block_size, num_blocks):
        params = {
            "name": name,
            "block_size": block_size,
            "num_blocks": num_blocks,
        }
        return self._request("bdev_malloc_create", params)

    def ultra21_lvol_bmap_init(self, bdev_name, num_blocks, block_len, page_len, max_num_blocks):
        params = {
            "base_bdev": bdev_name,
            "blockcnt": num_blocks,
            "blocklen": block_len,
            "pagelen": page_len,
            "maxblockcnt": max_num_blocks
        }
        return self._request("ultra21_lvol_bmap_init", params)

    def ultra21_lvol_mount_snapshot(self, snapshot_name, lvol_bdev, base_bdev):
        params = {
            "modus": "SNAPSHOT",
            "lvol_bdev": lvol_bdev,
            "base_bdev": base_bdev,
            "snapshot_bdev": snapshot_name
        }
        return self._request("ultra21_lvol_mount", params)

    def ultra21_lvol_mount_lvol(self, lvol_name, base_bdev):
        params = {
            "modus": "BASE",
            "lvol_bdev": lvol_name,
            "base_bdev": base_bdev
        }
        return self._request("ultra21_lvol_mount", params)

    def ultra21_lvol_dismount(self, lvol_name):
        params = {
            "lvol_bdev": lvol_name
        }
        return self._request("ultra21_lvol_dismount", params)

    def bdev_jm_create(self, name, name_storage1, block_size=4096, jm_cpu_mask=""):
        params = {
            "name": name,
            "name_storage1": name_storage1,
            "block_size": block_size
        }
        if jm_cpu_mask:
            params["bdb_lcpu_mask"] = int(jm_cpu_mask, 16)
        return self._request("bdev_jm_create", params)

    def bdev_jm_delete(self, name, safe_removal=False):
        params = {"name": name}
        if safe_removal is True:
            params["safe_removal"] = True
        return self._request("bdev_jm_delete", params)

    def ultra21_util_get_malloc_stats(self):
        params = {"socket_id": 0}
        return self._request("ultra21_util_get_malloc_stats", params)

    def ultra21_lvol_mount_clone(self, clone_name, snap_bdev, base_bdev, blockcnt):
        params = {
            "modus": "CLONE",
            "clone_bdev": clone_name,
            "base_bdev": base_bdev,
            "lvol_bdev": snap_bdev,
            "blockcnt": blockcnt,
        }
        return self._request("ultra21_lvol_mount", params)

    def alceml_unmap_vuid(self, name, vuid):
        params = {"name": name, "vuid": vuid}
        return self._request("alceml_unmap_vuid", params)

    def jm_delete(self):
        params = {"name": 0, "vuid": 0}
        return self._request("jm_delete", params)

    def framework_start_init(self):
        return self._request("framework_start_init")

    def bdev_examine(self, name):
        params = {"name": name}
        return self._request("bdev_examine", params)

    def bdev_wait_for_examine(self):
        return self._request("bdev_wait_for_examine")

    def nbd_start_disk(self, bdev_name, nbd_device="/dev/nbd0"):
        params = {
            "bdev_name": bdev_name,
            "nbd_device": nbd_device,
        }
        return self._request("nbd_start_disk", params)

    def nbd_stop_disk(self, nbd_device):
        params = {
            "nbd_device": nbd_device
        }
        return self._request("nbd_stop_disk", params)


    def bdev_jm_unmap_vuid(self, name, vuid):
        params = {"name": name, "vuid": vuid}
        return self._request("bdev_jm_unmap_vuid", params)

    def sock_impl_set_options(self):
        method = "sock_impl_set_options"
        params = {"impl_name": "posix", "enable_quickack": True,
                  "enable_zerocopy_send_server": True,
                  "enable_zerocopy_send_client": True}
        return self._request(method, params)

    def nvmf_set_config(self, poll_groups_mask):
        params = {"poll_groups_mask": poll_groups_mask}
        return self._request("nvmf_set_config", params)

    def jc_set_hint_lcpu_mask(self, jc_singleton_mask):
        params = {"hint_lcpu_mask": int(jc_singleton_mask, 16)}
        return self._request("jc_set_hint_lcpu_mask", params)


    def thread_get_stats(self):
        return self._request("thread_get_stats")

    def thread_set_cpumask(self, app_thread_process_id, app_thread_mask):
        params = {"id": app_thread_process_id, "cpumask": app_thread_mask}
        return self._request("thread_set_cpumask", params)

    def distr_migration_to_primary_start(self, storage_ID, name):
        params = {
            "name": name,
            "storage_ID": storage_ID,
        }
        return self._request("distr_migration_to_primary_start", params)

    def distr_migration_status(self, name):
        params = {"name": name}
        return self._request("distr_migration_status", params)

    def distr_migration_failure_start(self, name, storage_ID):
        params = {
            "name": name,
            "storage_ID": storage_ID
        }
        return self._request("distr_migration_failure_start", params)

    def distr_migration_expansion_start(self, name):
        params = {
            "name": name,
        }
        return self._request("distr_migration_expansion_start", params)

    def bdev_raid_add_base_bdev(self, raid_bdev, base_bdev):
        params = {
            "raid_bdev": raid_bdev,
            "base_bdev": base_bdev,
        }
        return self._request("bdev_raid_add_base_bdev", params)

    def bdev_raid_remove_base_bdev(self, raid_bdev, base_bdev):
        params = {
            "raid_bdev": raid_bdev,
            "base_bdev": base_bdev,
        }
        return self._request("bdev_raid_remove_base_bdev", params)

    def bdev_lvol_get_lvstores(self, name):
        params = {"lvs_name": name}
        _, err = self._request2("bdev_lvol_get_lvstores", params)
        if err:
            return False
        return True

    def bdev_lvol_resize(self, name, size_in_mib):
        params = {
            "name": name,
            "size_in_mib": size_in_mib
        }
        return self._request("bdev_lvol_resize", params)

    def bdev_lvol_inflate(self, name):
        params = {"name": name}
        return self._request("bdev_lvol_inflate", params)

    def bdev_distrib_toggle_cluster_full(self, name, cluster_full=False):
        params = {
            "name": name,
            "cluster_full": cluster_full,
        }
        return self._request("bdev_distrib_toggle_cluster_full", params)

    def log_set_print_level(self, level):
        params = {
            "level": level
        }
        return self._request("log_set_print_level", params)

    def bdev_lvs_dump(self, lvs_name, file):
        params = {
            "lvs_name": lvs_name,
            "file": file,
        }
        return self._request("bdev_lvs_dump", params)

    def jc_explicit_synchronization(self, jm_vuid):
        params = {
            "jm_vuid": jm_vuid
        }
        return self._request("jc_explicit_synchronization", params)

    def iscsi_create_portal_group(self, tag, host, port):
        params = {
            "tag": tag,
            "portals" : [{
                "host": host,
                "port": port}]
        }
        return self._request("iscsi_create_portal_group", params)

    def iscsi_create_initiator_group(self, tag, initiators, netmasks):
        params = {
            "tag": tag,
            "initiators" : initiators,
            "netmasks" : netmasks,
        }
        return self._request("iscsi_create_initiator_group", params)

    def iscsi_create_target_node(self, name, ig_tag, pg_tag, bdev_name):
        params = {
            "name": name,
            "pg_ig_maps" : [{
                "ig_tag": ig_tag,
                "pg_tag": pg_tag }],
            "luns" : [{
                "lun_id": 0,
                "bdev_name": bdev_name}],

            "disable_chap": True,
            "queue_depth": 24
        }
        return self._request("iscsi_create_target_node", params)
