# coding=utf- 8
import datetime
import json
import math
import platform
import socket

import psutil
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional

import threading

import time
import uuid

import docker
from docker.types import LogConfig

from simplyblock_core import constants, scripts, distr_controller, cluster_ops
from simplyblock_core import utils
from simplyblock_core.constants import LINUX_DRV_MASS_STORAGE_NVME_TYPE_ID, LINUX_DRV_MASS_STORAGE_ID
from simplyblock_core.controllers import lvol_controller, storage_events, snapshot_controller, device_events, \
    device_controller, tasks_controller, health_controller, tcp_ports_events, qos_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.fw_api_client import FirewallClient
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice, RemoteDevice, RemoteJMDevice
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.prom_client import PromClient
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.snode_client import SNodeClient, SNodeClientException
from simplyblock_web import node_utils
from simplyblock_core.utils import addNvmeDevices
from simplyblock_core.utils import pull_docker_image_with_retry
import os


logger = utils.get_logger(__name__)


class LVSRestartRequiredError(Exception):
    """Raised when an LVS fails to recover via ``bdev_examine`` during
    activation-mode recreate. The node's SPDK holds partial state that
    the activation path cannot safely reconcile: the caller should
    reject the (re)activation and tell the operator to restart that
    specific node before trying again.
    """

    def __init__(self, node_id, lvs_name, detail=""):
        self.node_id = node_id
        self.lvs_name = lvs_name
        self.detail = detail
        msg = (f"LVS {lvs_name} did not recover on examine on node "
               f"{node_id}")
        if detail:
            msg += f": {detail}"
        msg += ". Restart this node before continuing."
        super().__init__(msg)


def _rpc_subsystem_exists(rpc_client, nqn):
    """True iff a subsystem with the given NQN exists in SPDK."""
    try:
        return bool(rpc_client.subsystem_list(nqn_name=nqn))
    except Exception:
        return False


def _rpc_subsystem_has_ns(rpc_client, nqn, nsid=None, bdev_name=None):
    """True iff the subsystem has a namespace matching nsid and/or bdev_name."""
    try:
        subs = rpc_client.subsystem_list(nqn_name=nqn)
        if not subs:
            return False
        for ns in subs[0].get('namespaces', []) or []:
            if nsid is not None and ns.get('nsid') != nsid:
                continue
            if bdev_name is not None and ns.get('bdev_name') != bdev_name:
                continue
            return True
        return False
    except Exception:
        return False


def _rpc_subsystem_has_listener(rpc_client, nqn, trtype, traddr, trsvcid):
    """True iff the subsystem already has a matching listener."""
    try:
        subs = rpc_client.subsystem_list(nqn_name=nqn)
        if not subs:
            return False
        for la in subs[0].get('listen_addresses', []) or []:
            if (la.get('trtype', '').upper() == trtype.upper()
                    and la.get('traddr') == traddr
                    and str(la.get('trsvcid')) == str(trsvcid)):
                return True
        return False
    except Exception:
        return False


def _rpc_bdev_exists(rpc_client, name):
    """True iff a bdev with the given name is visible to SPDK."""
    try:
        ret = rpc_client.get_bdevs(name)
        return bool(ret)
    except Exception:
        return False


def _rpc_lvstore_exists(rpc_client, lvs_name):
    """True iff bdev_lvol_get_lvstores(lvs_name) returns a live lvstore."""
    try:
        ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
        return bool(ret)
    except Exception:
        return False


def _reapply_allowed_hosts(lvol, snode, rpc_client):
    """Re-register allowed hosts (with DHCHAP keys) on a subsystem after recreation."""
    from simplyblock_core.controllers.lvol_controller import _register_dhchap_keys_on_node, _get_dhchap_group
    db_ctrl = DBController()
    cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
    dhchap_group = _get_dhchap_group(cluster)
    for host_entry in lvol.allowed_hosts:
        logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
        has_keys = any(host_entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
        if has_keys:
            key_names = _register_dhchap_keys_on_node(snode, host_entry["nqn"], host_entry, rpc_client)
            rpc_client.subsystem_add_host(
                lvol.nqn, host_entry["nqn"],
                psk=key_names.get("psk"),
                dhchap_key=key_names.get("dhchap_key"),
                dhchap_ctrlr_key=key_names.get("dhchap_ctrlr_key"),
                dhchap_group=dhchap_group,
            )
        else:
            rpc_client.subsystem_add_host(lvol.nqn, host_entry["nqn"])


def _set_lvol_ana_on_node(lvol, node, ana_state):
    """Set ANA state for a single lvol's listeners on a given node."""
    rpc_client = node.rpc_client(timeout=10, retry=2)
    listener_port = node.get_lvol_subsys_port(lvol.lvs_name)
    for iface in node.data_nics:
        if iface.ip4_address and (lvol.fabric == iface.trtype.lower() or (lvol.fabric == "tcp" and node.active_tcp)):
            trtype = iface.trtype if lvol.fabric == iface.trtype.lower() else "TCP"
            ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                lvol.nqn, iface.ip4_address, listener_port, trtype=trtype, ana=ana_state)
            if not ret:
                logger.warning("Failed to set ANA state %s for %s on %s", ana_state, lvol.nqn, node.get_id())
            else:
                logger.info("ANA: %s on %s (%s) → %s", lvol.nqn, node.get_id(), iface.ip4_address, ana_state)


def _failover_primary_ana(primary_node):
    """Primary failed: promote first_sec→optimized.

    The second_sec stays at non_optimized (its permanent state).
    """
    db_ctrl = DBController()
    lvol_list = [lv for lv in db_ctrl.get_lvols_by_node_id(primary_node.get_id())
                 if lv.status in [LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE]]

    first_sec = None
    if primary_node.secondary_node_id:
        first_sec = db_ctrl.get_storage_node_by_id(primary_node.secondary_node_id)

    for lvol in lvol_list:
        if first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            _set_lvol_ana_on_node(lvol, first_sec, "optimized")


def _failback_primary_ana(primary_node):
    """Primary restarting: demote first_sec→non_optimized.

    The second_sec is already non_optimized and never changes.
    """
    db_ctrl = DBController()
    lvol_list = [lv for lv in db_ctrl.get_lvols_by_node_id(primary_node.get_id())
                 if lv.status in [LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE]]

    first_sec = None
    if primary_node.secondary_node_id:
        first_sec = db_ctrl.get_storage_node_by_id(primary_node.secondary_node_id)

    for lvol in lvol_list:
        if first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            _set_lvol_ana_on_node(lvol, first_sec, "non_optimized")


def trigger_ana_failover_for_node(offline_node):
    """Trigger ANA failover when a node goes offline.

    Only action needed: if the offline node is a primary, promote its
    first_sec to optimized.  The second_sec is always non_optimized and
    never needs ANA state changes.
    """
    node_id = offline_node.get_id()

    if offline_node.secondary_node_id:
        logger.info("ANA failover: node %s is primary, promoting first_sec", node_id)
        try:
            _failover_primary_ana(offline_node)
        except Exception as e:
            logger.error("ANA failover for primary role of %s failed: %s", node_id, e)


def trigger_ana_failback_for_node(restarting_node):
    """Trigger ANA failback when a primary comes back online.

    Demote first_sec from optimized back to non_optimized.
    The second_sec is always non_optimized and never changes.
    """
    node_id = restarting_node.get_id()

    if restarting_node.secondary_node_id:
        first_sec = DBController().get_storage_node_by_id(restarting_node.secondary_node_id)
        if first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            logger.info("ANA failback: primary %s restarting, demoting first_sec", node_id)
            try:
                _failback_primary_ana(restarting_node)
            except Exception as e:
                logger.error("ANA failback for primary %s failed: %s", node_id, e)


#: Hard cap on the bdev_nvme_attach_controller RPC timeout (seconds).
#: A reachable peer replies in microseconds; anything longer is an unreachable
#: path and we prefer a fast failure so per-peer iteration stays bounded and
#: the overall connect_device budget stays ~2s across two data NICs.
_ATTACH_CONTROLLER_MAX_TIMEOUT_SEC = 1


def connect_device(name: str, device: NVMeDevice, node: StorageNode, bdev_names: List[str], reattach: bool,
                   attach_timeout: Optional[float] = None):
    """Connect snode to device

    This only performs the actual operation between both involved SPDK instances,
    no book-keeping is done here.

    The bdev_nvme_attach_controller RPC is always bounded by
    ``_ATTACH_CONTROLLER_MAX_TIMEOUT_SEC`` (1 s) with no retries. Callers may
    pass ``attach_timeout`` to shorten further (kept as-is if lower); values
    above the cap are clamped, since a reachable SPDK peer answers in µs and
    anything longer is an unreachable path we want to fail fast on.

    More sensibly this would be a member function of either StorageNode or NVMeDevice.
    """

    logger.info(f'Connecting to {name}')
    for bdev in bdev_names:
        if bdev.startswith(name):
            logger.debug(f"Already connected, bdev found in bdev_get_bdevs: {bdev}")
            return bdev

    rpc_client = node.rpc_client()
    if attach_timeout is None or attach_timeout > _ATTACH_CONTROLLER_MAX_TIMEOUT_SEC:
        attach_timeout = _ATTACH_CONTROLLER_MAX_TIMEOUT_SEC
    attach_rpc_client = node.rpc_client(timeout=attach_timeout, retry=0)
    # check connection status
    if device.is_connection_in_progress_to_node(node.get_id()):
        logger.warning("This device is being connected to from other node, sleep for 5 seconds")
        time.sleep(5)

    device.lock_device_connection(node.get_id())

    ret = rpc_client.bdev_nvme_controller_list(name)
    if ret:
        counter = 0
        while (counter < 5):
            waiting = False
            for controller in ret[0]["ctrlrs"]:
                controller_state = controller["state"]
                logger.info(f"Controller found: {name}, status: {controller_state}")
                if controller_state== "failed":
                    # we can remove the controller only for certain, if its failed. other states are intermediate and require retry.
                    rpc_client.bdev_nvme_detach_controller(name)
                    time.sleep(2)
                    break
                elif controller_state == "resetting" or controller_state == "deleting" or controller_state == "reconnect_is_delayed":
                    if counter < 5:
                        time.sleep(2)
                        waiting = True
                        break
                    else:  # this should never happen. It means controller is "hanging" in an intermediate state for more than 10 seconds. usually if some io is hanging.
                        raise RuntimeError(f"Controller: {name}, status is {controller_state}")
            if not waiting:
                counter = 5
            else:
                counter += 1

        # if reattach:
        #    rpc_client.bdev_nvme_detach_controller(name)
        #    time.sleep(1)

    # only if the controller is really gone we try to reattach it
    if not rpc_client.bdev_nvme_controller_list(name):
        bdev_name = None

        db_ctrl = DBController()
        node = db_ctrl.get_storage_node_by_id(device.node_id)
        if node.active_rdma:
            tr_type = "RDMA"
        else:
            if node.active_tcp:
                tr_type = "TCP"
            else:
                msg = "target node to connect has no active fabric."
                logger.error(msg)
                raise RuntimeError(msg)

        # nvmf_multipath is a bool on the device record; translate it into
        # the SPDK string mode here. ``True`` must mean active-active
        # (``"multipath"``), not failover — passing the bool through to
        # rpc_client.bdev_nvme_attach_controller would coerce True ->
        # ``"failover"`` (active-passive) and remote alceml/jm controllers
        # would carry all IO on a single path. That's what produced the
        # remote_alceml_*/remote_jm_* in failover mode (vs hublvols
        # correctly in multipath) and made a single-NIC drop fatal: every
        # outstanding JC write was on the dropped path, all aborted at
        # once, all retried as duplicates.
        attach_mode = "multipath" if device.nvmf_multipath else False
        for ip in device.nvmf_ip.split(","):
            try:
                ret = attach_rpc_client.bdev_nvme_attach_controller(
                    name, device.nvmf_nqn, ip, device.nvmf_port, tr_type,
                    multipath=attach_mode)
                if not bdev_name and ret and isinstance(ret, list):
                    bdev_name = ret[0]
            except Exception as e:
                logger.warning(f"Failed to attach controller {name} via {ip}: {e}")

            if device.nvmf_multipath and bdev_name:
                rpc_client.bdev_nvme_set_multipath_policy(bdev_name, "active_active")

        if not bdev_name:
            msg = f"Bdev name not returned from controller attach for {name}"
            logger.error(msg)
            raise RuntimeError(msg)
        bdev_found = False
        for i in range(5):
            ret = rpc_client.get_bdevs(bdev_name)
            if ret:
                bdev_found = True
                break
            else:
                time.sleep(1)

        device.release_device_connection()

        if not bdev_found:
            logger.error("Bdev not found after 5 attempts")
            raise RuntimeError(f"Failed to connect to device: {device.get_id()}")

        return bdev_name
    return None


def repair_multipath_controller(name: str, device, node: StorageNode):
    """Check a multipath NVMe controller and re-attach any missing paths.

    For a multipath device the controller should have one primary trid plus
    one alternate_trid per additional data NIC.  If the controller exists but
    has fewer paths than expected (e.g. a NIC went down and came back but the
    path was not re-established), re-attach the missing IPs.

    Returns True if all paths are healthy (or were repaired), False if repair
    was not possible.
    """
    if not device.nvmf_multipath:
        return True

    expected_ips = set(ip.strip() for ip in device.nvmf_ip.split(",") if ip.strip())
    if len(expected_ips) < 2:
        return True  # not actually multipath

    rpc_client = node.rpc_client()
    ret = rpc_client.bdev_nvme_controller_list(name)
    if not ret:
        return True  # controller gone, connect_device will handle full reconnect

    db_ctrl = DBController()
    target_node = db_ctrl.get_storage_node_by_id(device.node_id)
    if target_node.active_rdma:
        tr_type = "RDMA"
    elif target_node.active_tcp:
        tr_type = "TCP"
    else:
        return False

    for ctrl_entry in ret:
        ctrlrs = ctrl_entry.get("ctrlrs", [])
        for ct in ctrlrs:
            state = ct.get("state", "")
            if state != "enabled":
                logger.warning("Controller %s path state=%s, skipping repair", name, state)
                continue

            # Collect all IPs currently attached (primary + alternates)
            attached_ips = set()
            attached_ips.add(ct["trid"]["traddr"])
            for alt in ct.get("alternate_trids", []):
                attached_ips.add(alt["traddr"])

            missing_ips = expected_ips - attached_ips
            if not missing_ips:
                return True  # all paths present

            logger.info("Controller %s has %d/%d paths, re-attaching: %s",
                        name, len(attached_ips), len(expected_ips), missing_ips)
            for ip in missing_ips:
                try:
                    rpc_client.bdev_nvme_attach_controller(
                        name, device.nvmf_nqn, ip, device.nvmf_port,
                        tr_type, multipath="multipath")
                    logger.info("Re-attached path %s on controller %s", ip, name)
                except Exception as e:
                    logger.error("Failed to re-attach path %s on controller %s: %s", ip, name, e)
                    return False

    return True


def get_next_cluster_device_order(db_controller, cluster_id):
    max_order = 0
    found = False
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        for dev in node.nvme_devices:
            found = True
            max_order = max(max_order, dev.cluster_device_order)
    if found:
        return max_order + 1
    return 0


def get_next_physical_device_order(snode):
    db_controller = DBController()
    used_labels = []
    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.physical_label > 0:
            if node.mgmt_ip == snode.mgmt_ip:
                return node.physical_label
            else:
                used_labels.append(node.physical_label)

    next_label = 1
    while next_label in used_labels:
        next_label += 1
    return next_label


def _search_for_partitions(rpc_client, nvme_device):
    partitioned_devices = []
    for bdev in rpc_client.get_bdevs():
        name = bdev['name']
        if name.startswith(f"{nvme_device.nvme_bdev}p"):
            new_dev = NVMeDevice(nvme_device.to_dict())
            new_dev.uuid = str(uuid.uuid4())
            new_dev.device_name = name
            new_dev.nvme_bdev = name
            new_dev.is_partition = True
            new_dev.size = bdev['block_size'] * bdev['num_blocks']
            partitioned_devices.append(new_dev)
    return partitioned_devices


def _create_jm_stack_on_raid(rpc_client, jm_nvme_bdevs, snode, after_restart):
    if snode.jm_device and snode.jm_device.raid_bdev:
        raid_bdev = snode.jm_device.raid_bdev
        if raid_bdev.startswith("raid_jm_"):
            raid_level = "1"
            ret = rpc_client.bdev_raid_create(raid_bdev, jm_nvme_bdevs, raid_level)
            if not ret:
                logger.error(f"Failed to create raid_jm_{snode.get_id()}")
                return False
    else:
        if len(jm_nvme_bdevs) > 1:
            raid_bdev = f"raid_jm_{snode.get_id()}"
            raid_level = "1"
            ret = rpc_client.bdev_raid_create(raid_bdev, jm_nvme_bdevs, raid_level)
            if not ret:
                logger.error(f"Failed to create raid_jm_{snode.get_id()}")
                return False
        else:
            raid_bdev = jm_nvme_bdevs[0]

    alceml_id = snode.get_id()
    alceml_name = f"alceml_jm_{snode.get_id()}"
    nvme_bdev = raid_bdev

    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    ret = snode.create_alceml(
        alceml_name, nvme_bdev, alceml_id,
        pba_init_mode=1 if after_restart else 3,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap
    )

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    pt_name = ""
    subsystem_nqn = ""
    ip_list = []
    if snode.enable_ha_jm:
        # add pass through
        pt_name = f"{jm_bdev}_PT"
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            return False

        subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        logger.info("creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
        logger.info(f"add {pt_name} to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, alceml_id)
        if not ret:
            logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
            return False

        for iface in snode.data_nics:
            logger.info(f"adding {iface.trtype} listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
            ip_list.append(iface.ip4_address)

    if len(ip_list) > 1:
        IP = ",".join(ip_list)
        multipath = True
    else:
        IP = next((iface.ip4_address for iface in snode.data_nics if iface.ip4_address), "")
        multipath = False

    ret = rpc_client.get_bdevs(raid_bdev)

    return JMDevice({
        'uuid': alceml_id,
        'device_name': jm_bdev,
        'size': ret[0]["block_size"] * ret[0]["num_blocks"],
        'status': JMDevice.STATUS_ONLINE,
        'jm_nvme_bdev_list': jm_nvme_bdevs,
        'raid_bdev': raid_bdev,
        'alceml_bdev': alceml_name,
        'alceml_name': alceml_name,
        'jm_bdev': jm_bdev,
        'pt_bdev': pt_name,
        'nvmf_nqn': subsystem_nqn,
        'nvmf_ip': IP,
        'nvmf_port': snode.nvmf_port,
        'nvmf_multipath': multipath,
        'node_id': snode.get_id(),
    })


def _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart):
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)
    db_controller = DBController()
    nvme_bdev = nvme.nvme_bdev
    test_name = ""
    if snode.enable_test_device:
        test_name = f"{nvme.nvme_bdev}_test"
        ret = rpc_client.bdev_passtest_create(test_name, nvme.nvme_bdev)
        if not ret:
            logger.error(f"Failed to create passtest bdev {test_name}")
            return False
        nvme_bdev = test_name

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    ret = snode.create_alceml(
        alceml_name, nvme_bdev, alceml_id,
        pba_init_mode=1 if after_restart else 3,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap
    )

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    pt_name = ""
    subsystem_nqn = ""
    ip_list = []
    if snode.enable_ha_jm:
        # add pass through
        pt_name = f"{jm_bdev}_PT"
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            return False

        subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        logger.info("creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
        logger.info(f"add {pt_name} to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, alceml_id)
        if not ret:
            logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
            return False

        for iface in snode.data_nics:
            if iface.ip4_address:
                logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
                ip_list.append(iface.ip4_address)

    if len(ip_list) > 1:
        IP = ",".join(ip_list)
        multipath = True
    else:
        IP = next((iface.ip4_address for iface in snode.data_nics if iface.ip4_address), "")
        multipath = False

    return JMDevice({
        'uuid': alceml_id,
        'device_name': jm_bdev,
        'size': nvme.size,
        'status': JMDevice.STATUS_ONLINE,
        'alceml_bdev': alceml_name,
        'alceml_name': alceml_name,
        'nvme_bdev': nvme.nvme_bdev,
        "serial_number": nvme.serial_number,
        "device_data_dict": nvme.to_dict(),
        'jm_bdev': jm_bdev,
        'testing_bdev': test_name,
        'pt_bdev': pt_name,
        'nvmf_nqn': subsystem_nqn,
        'nvmf_ip': IP,
        'nvmf_port': snode.nvmf_port,
        'nvmf_multipath': multipath,
        'node_id': snode.get_id(),
    })


def _create_storage_device_stack(rpc_client, nvme, snode, after_restart):
    db_controller = DBController()
    nvme_bdev = nvme.nvme_bdev
    if snode.enable_test_device:
        test_name = f"{nvme.nvme_bdev}_test"
        ret = rpc_client.bdev_passtest_create(test_name, nvme_bdev)
        if not ret:
            logger.error(f"Failed to create passtest bdev {test_name}")
            return None
        nvme_bdev = test_name
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)

    ret = snode.create_alceml(
        alceml_name, nvme_bdev, alceml_id,
        pba_init_mode=1 if (after_restart and nvme.status != NVMeDevice.STATUS_NEW) else 3,
        write_protection=cluster.distr_ndcs > 1,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap,
    )

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return None
    alceml_bdev = alceml_name

    # add pass through
    pt_name = f"{alceml_name}_PT"
    ret = rpc_client.bdev_PT_NoExcl_create(pt_name, alceml_bdev)
    if not ret:
        logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
        return None

    subsystem_nqn = snode.subsystem + ":dev:" + alceml_id
    logger.info("creating subsystem %s", subsystem_nqn)
    ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', alceml_id)
    ip_list = []
    for iface in snode.data_nics:
        if iface.ip4_address:
            logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
            ip_list.append(iface.ip4_address)

    logger.info(f"add {pt_name} to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, alceml_id)
    if not ret:
        logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
        return None

    if len(ip_list) > 1:
        IP = ",".join(ip_list)
        multipath = True
    else:
        IP = ip_list[0]
        multipath = False

    nvme.alceml_bdev = alceml_bdev
    nvme.pt_bdev = pt_name
    nvme.alceml_name = alceml_name
    nvme.nvmf_nqn = subsystem_nqn
    nvme.nvmf_ip = IP
    nvme.nvmf_port = snode.nvmf_port
    nvme.io_error = False
    nvme.nvmf_multipath = multipath
    # if nvme.status != NVMeDevice.STATUS_NEW:
    #     nvme.status = NVMeDevice.STATUS_ONLINE
    return nvme


def _create_device_partitions(rpc_client, nvme, snode, num_partitions_per_dev, jm_percent, partition_size, nbd_index):
    nbd_device = rpc_client.nbd_start_disk(nvme.nvme_bdev, f"/dev/nbd{nbd_index}")
    time.sleep(3)
    if not nbd_device:
        logger.error("Failed to start nbd dev")
        return False
    snode_api = snode.client()
    partition_percent = 0
    if partition_size:
        partition_percent = int(partition_size * 100 / nvme.size)

    result, error = snode_api.make_gpt_partitions(nbd_device, jm_percent, num_partitions_per_dev, partition_percent)
    if error:
        logger.error("Failed to make partitions")
        logger.error(error)
        return False
    time.sleep(3)
    rpc_client.nbd_stop_disk(nbd_device)
    for i in range(10):
        if not rpc_client.nbd_get_disks(nbd_device):
            break
        time.sleep(1)
    rpc_client.bdev_nvme_detach_controller(nvme.nvme_controller)
    for i in range(10):
        if not rpc_client.bdev_nvme_controller_list(nvme.nvme_controller):
            break
        time.sleep(1)
    try:
        rpc_client.bdev_nvme_controller_attach(nvme.nvme_controller, nvme.pcie_address)
    except RPCException as e:
        logger.error('Failed to create device partitions: ' + str(e))
        return False
    time.sleep(1)
    rpc_client.bdev_examine(nvme.nvme_bdev)
    time.sleep(1)
    return True


def _prepare_cluster_devices_partitions(snode, devices):
    db_controller = DBController()
    new_devices = []
    devices_to_partition = []
    thread_list = []
    for index, nvme in enumerate(devices):
        if nvme.status == "not_found":
            continue
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            new_devices.append(nvme)
            continue
        if nvme.is_partition:
            t = threading.Thread(target=_create_storage_device_stack, args=(snode.rpc_client(), nvme, snode, False,))
            thread_list.append(t)
            new_devices.append(nvme)
            t.start()
        else:
            devices_to_partition.append(nvme)
            partitioned_devices = _search_for_partitions(snode.rpc_client(), nvme)
            if len(partitioned_devices) != (1 + snode.num_partitions_per_dev):
                logger.info(f"Creating partitions for {nvme.nvme_bdev}")
                t = threading.Thread(
                    target=_create_device_partitions,
                    args=(snode.rpc_client(), nvme, snode, snode.num_partitions_per_dev,
                          snode.jm_percent, snode.partition_size, index + 1,))
                thread_list.append(t)
                t.start()

    for thread in thread_list:
        thread.join()

    thread_list = []
    for nvme in devices_to_partition:
        partitioned_devices = _search_for_partitions(snode.rpc_client(), nvme)
        if len(partitioned_devices) == (1 + snode.num_partitions_per_dev):
            logger.info("Device partitions created")
            # remove 1st partition for jm
            partitioned_devices.pop(0)
            for dev in partitioned_devices:
                t = threading.Thread(target=_create_storage_device_stack,
                                     args=(snode.rpc_client(), dev, snode, False,))
                thread_list.append(t)
                new_devices.append(dev)
                t.start()
        else:
            logger.error("Failed to create partitions")
            return False

    for thread in thread_list:
        thread.join()

    # assign device order
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    for nvme in new_devices:
        if nvme.status == NVMeDevice.STATUS_ONLINE:
            if nvme.cluster_device_order < 0:
                nvme.cluster_device_order = dev_order
                dev_order += 1
        device_events.device_create(nvme)

    # create jm device
    jm_devices = []
    bdevs_names = [d['name'] for d in snode.rpc_client().get_bdevs()]
    for nvme in new_devices:
        if nvme.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
            dev_part = f"{nvme.nvme_bdev[:-2]}p1"
            if dev_part in bdevs_names:
                if dev_part not in jm_devices:
                    jm_devices.append(dev_part)

    if jm_devices:
        jm_device = _create_jm_stack_on_raid(snode.rpc_client(), jm_devices, snode, after_restart=False)
        if not jm_device:
            logger.error("Failed to create JM device")
            return False

        snode.jm_device = jm_device

    snode.nvme_devices = new_devices
    return True


def _prepare_cluster_devices_jm_on_dev(snode, devices):
    db_controller = DBController()
    if not devices:
        return True

    # Set device cluster order
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    rpc_client = snode.rpc_client()
    new_devices = []
    for index, nvme in enumerate(devices):
        if nvme.status == "not_found":
            continue

        if nvme.status == NVMeDevice.STATUS_JM:
            jm_device = _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart=False)
            if not jm_device:
                logger.error("Failed to create JM device")
                return False
            snode.jm_device = jm_device
            continue

        new_devices.append(nvme)
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_READONLY]:
            logger.debug(f"Device is not online : {nvme.get_id()}, status: {nvme.status}")
        else:
            ret = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=False)
            if not ret:
                logger.error("failed to create dev stack")
                return False
            if nvme.status == NVMeDevice.STATUS_ONLINE:
                if nvme.cluster_device_order < 0:
                    nvme.cluster_device_order = dev_order
                    dev_order += 1
                device_events.device_create(nvme)

    snode.nvme_devices = new_devices
    return True


def _prepare_cluster_devices_on_restart(snode, clear_data=False):
    db_controller = DBController()

    new_devices = []

    rpc_client = snode.rpc_client(timeout=5 * 60)

    thread_list = []
    for index, nvme in enumerate(snode.nvme_devices):
        if nvme.status == NVMeDevice.STATUS_JM:
            continue

        new_devices.append(nvme)

        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                               NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            continue

        t = threading.Thread(
            target=_create_storage_device_stack,
            args=(rpc_client, nvme, snode, not clear_data,))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    snode.nvme_devices = new_devices
    snode.write_to_db()

    # prepare JM device
    jm_device = snode.jm_device
    if jm_device is None:
        return True

    if not jm_device or not jm_device.uuid:
        return True

    jm_device.status = JMDevice.STATUS_UNAVAILABLE

    if jm_device.jm_nvme_bdev_list:
        if len(jm_device.jm_nvme_bdev_list) == 1:
            ret = rpc_client.get_bdevs(jm_device.jm_nvme_bdev_list[0])
            if not ret:
                logger.error(f"BDev not found: {jm_device.jm_nvme_bdev_list[0]}")
                jm_device.status = JMDevice.STATUS_REMOVED
                return True
            ret = _create_jm_stack_on_raid(rpc_client, jm_device.jm_nvme_bdev_list, snode, after_restart=not clear_data)
            if not ret:
                logger.error("Failed to create JM device")
                return False
            snode.jm_device = ret
            snode.write_to_db()
            return True

        jm_bdevs_found = []
        for bdev_name in jm_device.jm_nvme_bdev_list:
            ret = rpc_client.get_bdevs(bdev_name)
            if ret:
                logger.info(f"JM bdev found: {bdev_name}")
                jm_bdevs_found.append(bdev_name)
            else:
                logger.error(f"JM bdev not found: {bdev_name}")

        if len(jm_bdevs_found) > 1:
            ret = _create_jm_stack_on_raid(rpc_client, jm_bdevs_found, snode, after_restart=not clear_data)
            if not ret:
                logger.error("Failed to create JM device")
                return False
            snode.jm_device = ret
            snode.write_to_db()
        else:
            logger.error("Only one jm nvme bdev found, setting jm device to removed")
            jm_device.status = JMDevice.STATUS_REMOVED
            return True

    else:
        nvme_bdev = jm_device.nvme_bdev
        if snode.enable_test_device:
            ret = rpc_client.bdev_passtest_create(jm_device.testing_bdev, jm_device.nvme_bdev)
            if not ret:
                logger.error(f"Failed to create passtest bdev {jm_device.testing_bdev}")
                return False
            nvme_bdev = jm_device.testing_bdev

        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        ret = snode.create_alceml(
            jm_device.alceml_bdev, nvme_bdev, jm_device.get_id(),
            pba_init_mode=3 if clear_data else 1,
            pba_page_size=cluster.page_size_in_blocks,
            full_page_unmap=cluster.full_page_unmap
        )

        if not ret:
            logger.error(f"Failed to create alceml bdev: {jm_device.alceml_bdev}")
            return False

        jm_bdev = f"jm_{snode.get_id()}"
        ret = rpc_client.bdev_jm_create(jm_bdev, jm_device.alceml_bdev, jm_cpu_mask=snode.jm_cpu_mask)
        if not ret:
            logger.error(f"Failed to create {jm_bdev}")
            return False

        if snode.enable_ha_jm:
            # add pass through
            pt_name = f"{jm_bdev}_PT"
            ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
            if not ret:
                logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
                return False

            cluster = db_controller.get_cluster_by_id(snode.cluster_id)
            subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
            logger.info("creating subsystem %s", subsystem_nqn)
            ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
            logger.info(f"add {pt_name} to subsystem")
            ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, snode.get_id())
            if not ret:
                logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
                return False

            for iface in snode.data_nics:
                if iface.ip4_address:
                    logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                    ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
        jm_device.status = JMDevice.STATUS_ONLINE
        snode.jm_device = jm_device
        snode.write_to_db()

    return True


def _connect_to_remote_devs(
        this_node: StorageNode, /,
        reattach: bool = True, force_connect_restarting_nodes: bool = False
):
    db_controller = DBController()

    rpc_client = this_node.rpc_client(timeout=5, retry=1)

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]
    else:
        node_bdev_names = []

    remote_devices = []
    existing_remote_devices = {dev.get_id(): dev for dev in this_node.remote_devices}

    allowed_node_statuses = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]
    allowed_dev_statuses = [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]

    if force_connect_restarting_nodes:
        allowed_node_statuses.append(StorageNode.STATUS_RESTARTING)
        allowed_dev_statuses.append(NVMeDevice.STATUS_UNAVAILABLE)

    devices_to_connect = []
    connect_threads = []
    nodes = db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id)
    # connect to remote devs
    for node_index, node in enumerate(nodes):
        if node.get_id() == this_node.get_id() or node.status not in allowed_node_statuses:
            continue
        logger.info(f"Connecting to node {node.get_id()}")
        for index, dev in enumerate(node.nvme_devices):

            if dev.status not in allowed_dev_statuses:
                logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                continue

            if not dev.alceml_bdev:
                raise ValueError(f"device alceml bdev not found!, {dev.get_id()}")
            devices_to_connect.append(dev)
            t = threading.Thread(
                target=connect_device,
                args=(f"remote_{dev.alceml_bdev}", dev, this_node, node_bdev_names, reattach,))
            connect_threads.append(t)
            t.start()

    for t in connect_threads:
        t.join()

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]

    def _find_remote_bdev(dev):
        expected_prefix = f"remote_{dev.alceml_bdev}"
        for bdev in node_bdev_names:
            if bdev.startswith(expected_prefix):
                return bdev
        return ""

    remote_device_ids = set()
    for dev in devices_to_connect:
        remote_bdev = RemoteDevice()
        remote_bdev.uuid = dev.uuid
        remote_bdev.alceml_name = dev.alceml_name
        remote_bdev.node_id = dev.node_id
        remote_bdev.size = dev.size
        remote_bdev.status = NVMeDevice.STATUS_ONLINE
        remote_bdev.nvmf_multipath = dev.nvmf_multipath
        remote_bdev.remote_bdev = _find_remote_bdev(dev)
        for _ in range(10):
            if remote_bdev.remote_bdev:
                break
            time.sleep(0.5)
            node_bdevs = rpc_client.get_bdevs()
            if node_bdevs:
                node_bdev_names = [b['name'] for b in node_bdevs]
            remote_bdev.remote_bdev = _find_remote_bdev(dev)
        if not remote_bdev.remote_bdev and dev.get_id() in existing_remote_devices:
            existing_remote_device = existing_remote_devices[dev.get_id()]
            if existing_remote_device.remote_bdev and rpc_client.get_bdevs(existing_remote_device.remote_bdev):
                remote_bdev.remote_bdev = existing_remote_device.remote_bdev
        if not remote_bdev.remote_bdev:
            logger.error(f"Failed to connect to remote device {dev.alceml_name}")
            continue
        remote_devices.append(remote_bdev)
        remote_device_ids.add(dev.get_id())

    # Some callers overwrite node.remote_devices with this return value. Make
    # the return value authoritative for existing SPDK state, not only for the
    # connect attempts above.
    for node in nodes:
        if node.get_id() == this_node.get_id() or node.status not in allowed_node_statuses:
            continue
        for dev in node.nvme_devices:
            if dev.get_id() in remote_device_ids:
                continue
            if dev.status not in allowed_dev_statuses:
                continue
            expected_bdev = f"remote_{dev.alceml_bdev}n1"
            if expected_bdev not in node_bdev_names:
                continue
            remote_bdev = RemoteDevice()
            remote_bdev.uuid = dev.uuid
            remote_bdev.alceml_name = dev.alceml_name
            remote_bdev.node_id = dev.node_id
            remote_bdev.size = dev.size
            remote_bdev.status = NVMeDevice.STATUS_ONLINE
            remote_bdev.nvmf_multipath = dev.nvmf_multipath
            remote_bdev.remote_bdev = expected_bdev
            remote_devices.append(remote_bdev)
            remote_device_ids.add(dev.get_id())

    return remote_devices


def sync_remote_devices_from_spdk(this_node: StorageNode, node_bdev_names=None):
    """Persist remote data bdevs that already exist in SPDK for this node."""
    db_controller = DBController()
    if node_bdev_names is None:
        rpc_client = this_node.rpc_client(timeout=5, retry=1)
        node_bdevs = rpc_client.get_bdevs()
        node_bdev_names = [b["name"] for b in node_bdevs] if node_bdevs else []
    elif isinstance(node_bdev_names, dict):
        node_bdev_names = list(node_bdev_names.keys())

    node_bdev_names = set(node_bdev_names)
    fresh_node = db_controller.get_storage_node_by_id(this_node.get_id())
    remote_by_id = {dev.get_id(): dev for dev in fresh_node.remote_devices}
    changed = False

    for peer in db_controller.get_storage_nodes_by_cluster_id(fresh_node.cluster_id):
        if peer.get_id() == fresh_node.get_id():
            continue
        if peer.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_RESTARTING]:
            continue
        for dev in peer.nvme_devices:
            if dev.status not in [
                NVMeDevice.STATUS_ONLINE,
                NVMeDevice.STATUS_READONLY,
                NVMeDevice.STATUS_CANNOT_ALLOCATE,
            ]:
                continue
            expected_bdev = f"remote_{dev.alceml_bdev}n1"
            if expected_bdev not in node_bdev_names:
                continue
            remote_dev = remote_by_id.get(dev.get_id())
            if remote_dev:
                if remote_dev.remote_bdev != expected_bdev or remote_dev.status != NVMeDevice.STATUS_ONLINE:
                    remote_dev.remote_bdev = expected_bdev
                    remote_dev.status = NVMeDevice.STATUS_ONLINE
                    changed = True
            else:
                remote_dev = RemoteDevice()
                remote_dev.uuid = dev.uuid
                remote_dev.alceml_name = dev.alceml_name
                remote_dev.node_id = dev.node_id
                remote_dev.size = dev.size
                remote_dev.status = NVMeDevice.STATUS_ONLINE
                remote_dev.nvmf_multipath = dev.nvmf_multipath
                remote_dev.remote_bdev = expected_bdev
                fresh_node.remote_devices.append(remote_dev)
                remote_by_id[dev.get_id()] = remote_dev
                changed = True

    if changed:
        fresh_node.write_to_db(db_controller.kv_store)
    return changed


def _peer_reachable_via_jm_quorum(target_node_id, this_node, peer_probe_timeout=1):
    """Check whether ``target_node`` is reachable on the data plane by asking
    other online peers about their JM quorum state.

    Each peer's ``jc_get_jm_status(jm_vuid)`` returns a dict that includes
    ``remote_jm_<peer>n1: bool``. If any online peer (other than this_node and
    target) reports the target's remote_jm as True, the target is reachable
    from at least one vantage point and we attempt the attach. If we can probe
    one or more peers and none of them report the target reachable, treat it
    as data-plane unreachable and skip the attach. If we can't probe any
    peer, default to True (don't block on missing information).
    """
    db_controller = DBController()
    remote_key = f"remote_jm_{target_node_id}n1"
    probed = False
    for peer in db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id):
        if peer.get_id() in (target_node_id, this_node.get_id()):
            continue
        if peer.status != StorageNode.STATUS_ONLINE:
            continue
        if not peer.jm_vuid:
            continue
        try:
            ret = peer.rpc_client(timeout=peer_probe_timeout, retry=0).jc_get_jm_status(peer.jm_vuid)
        except Exception as e:
            logger.debug("JM-quorum probe on %s failed: %s", peer.get_id(), e)
            continue
        if not isinstance(ret, dict):
            continue
        probed = True
        if ret.get(remote_key) is True:
            return True
    return not probed


def _connect_to_remote_jm_devs(this_node, jm_ids=None):
    db_controller = DBController()

    rpc_client = this_node.rpc_client(timeout=5, retry=2)

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]
    else:
        node_bdev_names = []
    remote_devices = []
    if jm_ids:
        for jm_id in jm_ids:
            jm_dev = db_controller.get_jm_device_by_id(jm_id)
            if jm_dev:
                remote_devices.append(jm_dev)

    if this_node.jm_ids:
        for jm_id in this_node.jm_ids:
            jm_dev = db_controller.get_jm_device_by_id(jm_id)
            if jm_dev and jm_dev not in remote_devices:
                remote_devices.append(jm_dev)

    for sec_attr in ['lvstore_stack_secondary', 'lvstore_stack_tertiary']:
        sec_primary_id = getattr(this_node, sec_attr, None)
        if sec_primary_id:
            org_node = db_controller.get_storage_node_by_id(sec_primary_id)
            if org_node.jm_device and org_node.jm_device not in remote_devices:
                remote_devices.append(org_node.jm_device)
            for jm_id in org_node.jm_ids:
                jm_dev = db_controller.get_jm_device_by_id(jm_id)
                if jm_dev and jm_dev not in remote_devices:
                    remote_devices.append(jm_dev)

    logger.debug(f"remote_devices: {remote_devices}")
    allowed_node_statuses = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_RESTARTING]
    allowed_dev_statuses = [NVMeDevice.STATUS_ONLINE]

    new_devs = []
    existing_remote_jm_devices = {dev.get_id(): dev for dev in this_node.remote_jm_devices}
    for jm_dev in remote_devices:
        if not jm_dev.jm_bdev:
            continue

        org_dev = None
        org_dev_node = None
        for node in db_controller.get_storage_nodes():
            if node.jm_device and node.jm_device.get_id() == jm_dev.get_id():
                org_dev = node.jm_device
                org_dev_node = node
                break

        if not org_dev or org_dev in new_devs or org_dev_node and org_dev_node.get_id() == this_node.get_id():
            continue

        if org_dev_node is not None and org_dev_node.status not in allowed_node_statuses:
            logger.warning(f"Skipping node:{org_dev_node.get_id()} with status: {org_dev_node.status}")
            continue

        if org_dev is not None and org_dev.status not in allowed_dev_statuses:
            logger.warning(f"Skipping device:{org_dev.get_id()} with status: {org_dev.status}")
            continue

        # Quorum reachability check intentionally not gated here:
        # during cluster_activate the peers' JC quorums are still being
        # bootstrapped, so _peer_reachable_via_jm_quorum cannot answer
        # correctly for a not-yet-built group and would skip every intended
        # member of the new jm_vuid. Runtime re-attach paths (rejoin,
        # restart-task) carry their own reachability gating.

        remote_device = RemoteJMDevice()
        remote_device.uuid = org_dev.uuid
        remote_device.alceml_name = org_dev.alceml_name
        remote_device.node_id = org_dev.node_id
        remote_device.size = org_dev.size
        remote_device.jm_bdev = org_dev.jm_bdev
        remote_device.status = NVMeDevice.STATUS_ONLINE
        remote_device.nvmf_multipath = org_dev.nvmf_multipath
        expected_bdev = f"remote_{org_dev.jm_bdev}n1"
        try:
            remote_device.remote_bdev = connect_device(
                f"remote_{org_dev.jm_bdev}", org_dev, this_node,
                bdev_names=node_bdev_names, reattach=True,
                attach_timeout=1,
            )
        except RuntimeError:
            logger.error(f'Failed to connect to {org_dev.get_id()}')
        for _ in range(10):
            if remote_device.remote_bdev and rpc_client.get_bdevs(remote_device.remote_bdev):
                break
            if rpc_client.get_bdevs(expected_bdev):
                remote_device.remote_bdev = expected_bdev
                break
            time.sleep(0.5)
        if not remote_device.remote_bdev and org_dev.get_id() in existing_remote_jm_devices:
            existing_remote_device = existing_remote_jm_devices[org_dev.get_id()]
            if existing_remote_device.remote_bdev and rpc_client.get_bdevs(existing_remote_device.remote_bdev):
                remote_device.remote_bdev = existing_remote_device.remote_bdev
        if not remote_device.remote_bdev:
            logger.error(f"Failed to connect to remote JM device {org_dev.alceml_name}")
            continue
        new_devs.append(remote_device)

    return new_devs


def _refresh_cluster_maps_after_node_recovery(snode: StorageNode):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(snode.get_id())

    # Push a full cluster map after reconnect/restart recovery so peers do not
    # remain on stale per-device availability derived from transient reconnect state.
    distr_controller.send_cluster_map_to_node(snode)

    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.get_id() == snode.get_id():
            continue
        if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
            distr_controller.send_cluster_map_to_node(node)


def ifc_is_tcp(nic):
    addrs = psutil.net_if_addrs().get(nic, [])
    for addr in addrs:
        if addr.family == socket.AF_INET:
            return True
    return False


def ifc_is_roce(nic):
    rdma_path = "/sys/class/infiniband/"
    if not os.path.exists(rdma_path):
        return False

    for rdma_dev in os.listdir(rdma_path):
        net_path = os.path.join(rdma_path, rdma_dev, "device/net")
        if os.path.exists(net_path):
            for iface in os.listdir(net_path):
                if iface == nic:
                    return True
    return False


def get_required_ha_jm_count(cluster) -> int:
    return 4 if cluster.max_fault_tolerance >= 2 else 3


def resolve_ha_jm_count(cluster, ha_jm_count) -> int:
    required_ha_jm_count = get_required_ha_jm_count(cluster)

    if ha_jm_count is None:
        return required_ha_jm_count

    if ha_jm_count < required_ha_jm_count:
        raise ValueError(
            f"ha_jm_count={ha_jm_count} is too low for max_fault_tolerance="
            f"{cluster.max_fault_tolerance}; minimum required is {required_ha_jm_count}"
        )

    return ha_jm_count


def add_node(cluster_id, node_addr, iface_name, data_nics_list,
             max_snap, spdk_image=None, spdk_debug=False,
             small_bufsize=0, large_bufsize=0,
             num_partitions_per_dev=0, jm_percent=0, enable_test_device=False,
             namespace=None, enable_ha_jm=False, cr_name=None, cr_namespace=None, cr_plural=None,
             id_device_by_nqn=False, partition_size="", ha_jm_count=None, format_4k=False, spdk_proxy_image=None,
             expansion=False):
    snode_api = SNodeClient(node_addr)
    node_info, _ = snode_api.info()
    if node_info.get("nodes_config") and node_info["nodes_config"].get("nodes"):
        nodes = node_info["nodes_config"]["nodes"]
    else:
        logger.error("Please run sbcli sn configure before adding the storage node, "
                     "If you run it and the config has been manually changed please "
                     "run 'sbcli sn configure-upgrade'")
        return False
    snode_api.set_hugepages()
    for node_config in nodes:
        logger.debug(node_config)
        db_controller = DBController()
        kv_store = db_controller.kv_store

        try:
            cluster = db_controller.get_cluster_by_id(cluster_id)
        except KeyError:
            logger.error("Cluster not found: %s", cluster_id)
            return False

        ha_jm_count = resolve_ha_jm_count(cluster, ha_jm_count)

        logger.info(f"Adding Storage node: {node_addr}")

        if not node_info:
            logger.error("SNode API is not reachable")
            return False
        logger.info(f"Node found: {node_info['hostname']}")
        # if "cluster_id" in node_info and node_info['cluster_id']:
        #     if node_info['cluster_id'] != cluster_id:
        #         logger.error(f"This node is part of another cluster: {node_info['cluster_id']}")
        #         return False
        ip_iface = utils.get_mgmt_ip(node_info, iface_name)
        mgmt_ip = ip_iface[0] if ip_iface else None

        cloud_instance = node_info['cloud_instance']
        if not cloud_instance:
            # Create a static cloud instance from node info
            cloud_instance = {"id": node_info['system_id'], "type": "None", "cloud": "None",
                              "ip": mgmt_ip,
                              "public_ip": mgmt_ip}
        """"
         "cloud_instance": {
              "id": "565979732541",
              "type": "m6id.large",
              "cloud": "google",
              "ip": "10.10.10.10",
              "public_ip": "20.20.20.20",
        }
        """""
        logger.debug(json.dumps(cloud_instance, indent=2))
        logger.info(f"Instance id: {cloud_instance['id']}")
        logger.info(f"Instance cloud: {cloud_instance['cloud']}")
        logger.info(f"Instance type: {cloud_instance['type']}")
        logger.info(f"Instance privateIp: {cloud_instance['ip']}")
        logger.info(f"Instance public_ip: {cloud_instance['public_ip']}")

        alceml_cpu_index = 0
        alceml_worker_cpu_index = 0
        distrib_cpu_index = 0
        jc_singleton_mask = ""

        req_cpu_count = len(node_config.get("isolated"))

        if req_cpu_count >= 64:
            logger.error(
                f"ERROR: The provided cpu mask {req_cpu_count} has values greater than 63, which is not allowed")
            return False

        # Calculate pool count
        max_prov = 0
        if node_config.get("max_size"):
            max_prov = int(utils.parse_size(node_config.get("max_size")))
        if max_prov < 0:
            logger.error(f"Incorrect max-prov value {max_prov}")
            return False

        minimum_hp_memory = node_config.get("huge_page_memory")

        minimum_hp_memory = max(minimum_hp_memory, max_prov)

        # check for memory
        if "memory_details" in node_info and node_info['memory_details']:
            memory_details = node_info['memory_details']
            logger.info("Node Memory info")
            logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
            logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
            logger.info(f"huge_total: {utils.humanbytes(memory_details['huge_total'])}")
            logger.info(f"huge_free: {utils.humanbytes(memory_details['huge_free'])}")
            logger.info(f"Set huge pages memory is : {utils.humanbytes(minimum_hp_memory)}")
        else:
            logger.error("Cannot get memory info from the instance.. Exiting")
            return False

        # Calculate minimum sys memory
        minimum_sys_memory = node_config.get("sys_memory")
        max_lvol = node_config.get("max_lvol")
        ssd_pcie = node_config.get("ssd_pcis")

        if ssd_pcie:
            for ssd in ssd_pcie:
                for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
                    if node.api_endpoint == node_addr:
                        if ssd in node.ssd_pcie:
                            if node.status == StorageNode.STATUS_IN_CREATION:
                                logger.warning(
                                    f"Node {node.get_id()} is in_creation status with SSD {ssd}, "
                                    f"removing and deleting it")
                                remove_storage_node(node.get_id(), force_remove=True)
                                delete_storage_node(node.get_id(), force=True)
                                break
                            logger.error(f"SSD is being used by other node, ssd: {ssd}, node: {node.get_id()}")
                            return False

        fdb_connection = cluster.db_connection

        if cluster.mode == "docker":
            logger.info("Joining docker swarm...")
            cluster_docker = utils.get_docker_client(cluster_id)
            cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
            results, err = snode_api.join_swarm(
                cluster_ip=cluster_ip,
                join_token=cluster_docker.swarm.attrs['JoinTokens']['Worker'],
                db_connection=cluster.db_connection,
                cluster_id=cluster_id)

            if not results:
                logger.error(f"Failed to Join docker swarm: {err}")
                return False
        else:
            cluster_ip = utils.get_k8s_node_ip()

        rpc_user, rpc_pass = utils.generate_rpc_user_and_pass()
        mgmt_info = utils.get_mgmt_ip(node_info, iface_name)
        if not mgmt_info:
            logger.error(f"No management interface with IP found in provided interfaces: {iface_name}")
            return False

        mgmt_ip, mgmt_iface = mgmt_info
        firewall_port = utils.get_next_fw_port(cluster_id, mgmt_ip=mgmt_ip)
        rpc_port = utils.get_next_rpc_port(cluster_id)
        logger.info(f"mgmt interface is {mgmt_iface}")

        if not spdk_image:
            spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE

        if cluster.mode == "docker":
            log_config_type = utils.get_storage_node_api_log_type(mgmt_ip, '/SNodeAPI')
            if log_config_type and log_config_type != LogConfig.types.GELF:
                logger.info("SNodeAPI container found but not configured with gelf logger")
                start_storage_node_api_container(mgmt_ip, cluster_ip)
        node_socket = node_config.get("socket")

        total_mem = minimum_hp_memory
        for n in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
            if n.api_endpoint == node_addr and n.socket == node_socket:
                total_mem += (n.spdk_mem + 500000000)

        logger.info("Deploying SPDK")
        results = None
        l_cores = node_config.get("l-cores")
        spdk_cpu_mask = node_config.get("cpu_mask")
        for ssd in ssd_pcie:
            if format_4k:
                snode_api.format_device_with_4k(ssd)
                snode_api.bind_device_to_spdk(ssd)
            snode_api.bind_device_to_spdk(ssd)

        if not spdk_proxy_image:
            spdk_proxy_image = cluster.container_image_prefix + constants.SIMPLY_BLOCK_DOCKER_IMAGE
        try:
            results, err = snode_api.spdk_process_start(
                l_cores, minimum_hp_memory, spdk_image, spdk_debug, cluster_ip, fdb_connection,
                namespace, mgmt_ip, rpc_port, rpc_user, rpc_pass,
                multi_threading_enabled=constants.SPDK_PROXY_MULTI_THREADING_ENABLED,
                timeout=constants.SPDK_PROXY_TIMEOUT,
                ssd_pcie=ssd_pcie, total_mem=total_mem, system_mem=minimum_sys_memory, cluster_mode=cluster.mode,
                socket=node_socket, firewall_port=firewall_port, cluster_id=cluster_id, spdk_proxy_image=spdk_proxy_image)
            time.sleep(5)

        except Exception as e:
            logger.error(e)
            return False

        if not results:
            logger.error(f"Failed to start spdk: {err}")
            return False
        number_of_alceml_devices = node_config.get("number_of_alcemls")
        # Increase number of alcemls by one for the JM
        number_of_alceml_devices += 1
        small_pool_count = node_config.get("small_pool_count")
        large_pool_count = node_config.get("large_pool_count")

        cores, _ = snode_api.read_allowed_list()

        if len(cores) == req_cpu_count:
            new_distribution, _ = snode_api.recalculate_cores_distribution(cores, number_of_alceml_devices)
            poller_cpu_cores = new_distribution.get("poller_cpu_cores")
            alceml_cpu_cores = new_distribution.get("alceml_cpu_cores")
            distrib_cpu_cores = new_distribution.get("distrib_cpu_cores")
            alceml_worker_cpu_cores = new_distribution.get("alceml_worker_cpu_cores")
            jc_singleton_core = new_distribution.get("jc_singleton_core")
            app_thread_core = new_distribution.get("app_thread_core")
            jm_cpu_core = new_distribution.get("jm_cpu_core")
            lvol_poller_core = new_distribution.get("lvol_poller_core")
            lvol_poller_mask = utils.generate_mask(lvol_poller_core)
        else:
            poller_cpu_cores = node_config.get("distribution").get("poller_cpu_cores")
            alceml_cpu_cores = node_config.get("distribution").get("alceml_cpu_cores")
            distrib_cpu_cores = node_config.get("distribution").get("distrib_cpu_cores")
            alceml_worker_cpu_cores = node_config.get("distribution").get("alceml_worker_cpu_cores")
            jc_singleton_core = node_config.get("distribution").get("jc_singleton_core")
            app_thread_core = node_config.get("distribution").get("app_thread_core")
            jm_cpu_core = node_config.get("distribution").get("jm_cpu_core")
            lvol_poller_core =  node_config.get("distribution").get("lvol_poller_core")
            lvol_poller_mask = utils.generate_mask(lvol_poller_core)

        number_of_distribs = node_config.get("number_of_distribs")

        pollers_mask = utils.generate_mask(poller_cpu_cores)
        app_thread_mask = utils.generate_mask(app_thread_core)

        if jc_singleton_core:
            jc_singleton_mask = utils.decimal_to_hex_power_of_2(jc_singleton_core[0])
        jm_cpu_mask = utils.generate_mask(jm_cpu_core)


        data_nics = []

        active_tcp = False
        active_rdma = False
        fabric_tcp = cluster.fabric_tcp
        fabric_rdma = cluster.fabric_rdma
        names = data_nics_list or [mgmt_iface]
        logger.info(f"fabric_tcp is {fabric_tcp}")
        logger.info(f"fabric_rdma is {fabric_rdma}")
        logger.debug(f"Data nics ports are: {names}")
        for nic in names:
            device = node_info['network_interface'][nic]
            base_ifc_cfg = {
                'uuid': str(uuid.uuid4()),
                'if_name': nic,
                'ip4_address': device['ip'],
                'status': device['status'],
                'net_type': device['net_type'], }
            if fabric_rdma and snode_api.ifc_is_roce(nic):
                cfg = base_ifc_cfg.copy()
                cfg['trtype'] = "RDMA"
                data_nics.append(IFace(cfg))
                active_rdma = True
                if fabric_tcp and snode_api.ifc_is_tcp(nic):
                    active_tcp = True
            elif fabric_tcp and snode_api.ifc_is_tcp(nic):
                cfg = base_ifc_cfg.copy()
                cfg['trtype'] = "TCP"
                data_nics.append(IFace(cfg))
                active_tcp = True

        if not active_tcp and not active_rdma:
            logger.error("No usable storage network interface found.")
            return False

        hostname = node_info['hostname'] + f"_{rpc_port}"
        BASE_NQN = cluster.nqn.split(":")[0]
        subsystem_nqn = f"{BASE_NQN}:{hostname}"
        # creating storage node object
        snode = StorageNode()
        snode.uuid = str(uuid.uuid4())
        snode.status = StorageNode.STATUS_IN_CREATION
        snode.baseboard_sn = node_info['system_id']
        snode.system_uuid = node_info['system_id']
        snode.create_dt = str(datetime.datetime.now())

        snode.cloud_instance_id = cloud_instance['id']
        snode.cloud_instance_type = cloud_instance['type']
        snode.cloud_instance_public_ip = cloud_instance['public_ip']
        snode.cloud_name = cloud_instance['cloud'] or ""

        snode.namespace = namespace
        snode.cr_name = cr_name
        snode.cr_namespace = cr_namespace
        snode.cr_plural = cr_plural
        snode.ssd_pcie = ssd_pcie
        snode.hostname = hostname
        snode.host_nqn = subsystem_nqn
        snode.subsystem = subsystem_nqn
        snode.data_nics = data_nics
        snode.mgmt_ip = mgmt_ip
        snode.primary_ip = mgmt_ip
        snode.rpc_port = rpc_port
        snode.rpc_username = rpc_user
        snode.rpc_password = rpc_pass
        snode.cluster_id = cluster_id
        snode.api_endpoint = node_addr
        snode.host_secret = utils.generate_string(20)
        snode.ctrl_secret = utils.generate_string(20)
        snode.number_of_distribs = number_of_distribs
        snode.number_of_alceml_devices = number_of_alceml_devices
        snode.enable_ha_jm = enable_ha_jm
        snode.ha_jm_count = ha_jm_count
        snode.minimum_sys_memory = minimum_sys_memory
        snode.active_tcp = active_tcp
        snode.active_rdma = active_rdma
        snode.spdk_proxy_image = spdk_proxy_image

        if 'cpu_count' in node_info:
            snode.cpu = node_info['cpu_count']
        if 'cpu_hz' in node_info:
            snode.cpu_hz = node_info['cpu_hz']
        if 'memory' in node_info:
            snode.memory = node_info['memory']
        if 'hugepages' in node_info:
            snode.hugepages = node_info['hugepages']

        snode.l_cores = l_cores or ""
        snode.spdk_cpu_mask = spdk_cpu_mask or ""
        snode.spdk_mem = minimum_hp_memory
        snode.max_lvol = max_lvol
        snode.max_snap = max_snap
        snode.max_prov = max_prov
        snode.spdk_image = spdk_image or ""
        snode.spdk_debug = spdk_debug or False
        snode.write_to_db(kv_store)
        snode.app_thread_mask = app_thread_mask or ""
        snode.pollers_mask = pollers_mask or ""
        snode.lvol_poller_mask = lvol_poller_mask or ""
        snode.jm_cpu_mask = jm_cpu_mask
        snode.alceml_cpu_index = alceml_cpu_index
        snode.alceml_worker_cpu_index = alceml_worker_cpu_index
        snode.distrib_cpu_index = distrib_cpu_index
        snode.alceml_cpu_cores = alceml_cpu_cores
        snode.alceml_worker_cpu_cores = alceml_worker_cpu_cores
        snode.distrib_cpu_cores = distrib_cpu_cores
        snode.jc_singleton_mask = jc_singleton_mask or ""
        snode.nvmf_port = utils.get_next_dev_port(cluster_id)
        snode.poller_cpu_cores = poller_cpu_cores or []
        snode.socket = node_socket
        snode.iobuf_small_pool_count = small_pool_count or 0
        snode.iobuf_large_pool_count = large_pool_count or 0
        snode.iobuf_small_bufsize = small_bufsize or 0
        snode.iobuf_large_bufsize = large_bufsize or 0
        snode.enable_test_device = enable_test_device
        snode.firewall_port = firewall_port

        if cluster.is_single_node:
            snode.physical_label = 0
        else:
            snode.physical_label = get_next_physical_device_order(snode)

        snode.num_partitions_per_dev = num_partitions_per_dev
        snode.jm_percent = jm_percent
        snode.id_device_by_nqn = id_device_by_nqn

        if partition_size:
            snode.partition_size = utils.parse_size(partition_size)

        rpc_client = snode.rpc_client(timeout=3 * 60, retry=10)

        # 1- set iobuf options
        if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
                snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
            ret = rpc_client.iobuf_set_options(
                snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
                snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
            if not ret:
                logger.error("Failed to set iobuf options")
                return False
        rpc_client.bdev_set_options(0, 0, 0, 0)
        rpc_client.accel_set_options()

        snode.write_to_db(kv_store)

        ret = rpc_client.nvmf_set_max_subsystems(constants.NVMF_MAX_SUBSYSTEMS)
        if not ret:
            logger.warning(f"Failed to set nvmf max subsystems {constants.NVMF_MAX_SUBSYSTEMS}")

        # 2- set socket implementation options
        bind_to_device = None
        if snode.data_nics and len(snode.data_nics) == 1:
            bind_to_device = snode.data_nics[0].if_name
        ret = rpc_client.sock_impl_set_options(bind_to_device)
        if not ret:
            logger.error("Failed to set optimized socket options")
            return False

        # 3- set nvme config
        if snode.pollers_mask:
            ret = rpc_client.nvmf_set_config(
                snode.pollers_mask,
                dhchap_digests=constants.DHCHAP_DIGESTS,
                dhchap_dhgroups=[constants.DHCHAP_DHGROUP],
            )
            if not ret:
                logger.error("Failed to set pollers mask")
                return False

        # 4- start spdk framework
        ret = rpc_client.framework_start_init()
        if not ret:
            logger.error("Failed to start framework")
            return False

        rpc_client.log_set_print_level("DEBUG")

        if snode.lvol_poller_mask:
            ret = rpc_client.bdev_lvol_create_poller_group(snode.lvol_poller_mask)
            if not ret:
                logger.error("Failed to set pollers mask")
                return False

        # 5- set app_thread cpu mask
        if snode.app_thread_mask:
            ret = rpc_client.thread_get_stats()
            app_thread_process_id = 0
            if ret.get("threads"):
                for entry in ret["threads"]:
                    if entry['name'] == 'app_thread':
                        app_thread_process_id = entry['id']
                        break

            ret = rpc_client.thread_set_cpumask(app_thread_process_id, snode.app_thread_mask)
            if not ret:
                logger.error("Failed to set app thread mask")
                return False

        # 6- set nvme bdev options
        # bdev_nvme_set_options is a pure local SPDK config call; bound it at
        # 5 s so a stuck proxy can't consume the 3 min startup RPC budget.
        set_opts_rpc = snode.rpc_client(timeout=5, retry=0)
        ret = set_opts_rpc.bdev_nvme_set_options()
        if not ret:
            logger.error("Failed to set nvme options")
            return False

        qpair = cluster.qpair_count

        if not cluster.fabric_tcp and not cluster.fabric_rdma:
            logger.error("no active fabric")
            return False

        if cluster.fabric_tcp:
            ret = rpc_client.transport_create("TCP", qpair, 512 * (req_cpu_count + 1))
            if not ret:
                logger.error(f"Failed to create transport TCP with qpair: {qpair}")
                return False
        if cluster.fabric_rdma:
            ret = rpc_client.transport_create("RDMA", qpair, 512 * (req_cpu_count + 1))
            if not ret:
                logger.error(f"Failed to create transport RDMA with qpair: {qpair}")
                return False

        # 7- set jc singleton mask
        if snode.jc_singleton_mask:
            ret = rpc_client.jc_set_hint_lcpu_mask(snode.jc_singleton_mask)
            if not ret:
                logger.error("Failed to set jc singleton mask")
                return False

        # get new node info after starting spdk
        # node_info, _ = snode_api.info()

        # if not snode.ssd_pcie:
        #     snode = db_controller.get_storage_node_by_id(snode.get_id())
        #     snode.ssd_pcie = node_info['spdk_pcie_list']
        #     snode.write_to_db()
        # discover devices
        if not snode.ssd_pcie:
            node_info, _ = snode_api.info()
            ssds = node_info['spdk_pcie_list']
        else:
            ssds = snode.ssd_pcie

        nvme_devs = addNvmeDevices(rpc_client, snode, ssds)
        if nvme_devs:

            for nvme in nvme_devs:
                nvme.status = NVMeDevice.STATUS_ONLINE

            # prepare devices
            if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:

                jm_device = nvme_devs[0]
                for index, nvme in enumerate(nvme_devs):
                    if nvme.size < jm_device.size:
                        jm_device = nvme
                jm_device.status = NVMeDevice.STATUS_JM

                ret = _prepare_cluster_devices_jm_on_dev(snode, nvme_devs)
            else:
                ret = _prepare_cluster_devices_partitions(snode, nvme_devs)
            if not ret:
                logger.error("Failed to prepare cluster devices")
                return False

        # set qos values if enabled
        if cluster.is_qos_set():
            logger.info("Setting Alcemls QOS weights")
            ret = rpc_client.alceml_set_qos_weights(qos_controller.get_qos_weights_list(cluster_id))
            if not ret:
                logger.error("Failed to set Alcemls QOS")
                return False

        logger.info("Connecting to remote devices")
        remote_devices = _connect_to_remote_devs(snode)
        snode.remote_devices = remote_devices

        if snode.enable_ha_jm:
            logger.info("Connecting to remote JMs")
            snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)

        snode.write_to_db(kv_store)

        snode = db_controller.get_storage_node_by_id(snode.get_id())
        old_status = snode.status
        snode.status = StorageNode.STATUS_ONLINE
        snode.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        snode.online_since = str(datetime.datetime.now(datetime.timezone.utc))
        snode.write_to_db(db_controller.kv_store)

        storage_events.snode_status_change(snode, snode.status, old_status, caused_by="monitor")
        # distr_controller.send_node_status_event(snode, status)

        logger.info("Make other nodes connect to the node devices")
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        for node in snodes:
            if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                continue
            try:
                node.remote_devices = _connect_to_remote_devs(node)
            except RuntimeError:
                logger.error('Failed to connect to remote devices')
                return False
            node.write_to_db(kv_store)

        if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY,
                                  Cluster.STATUS_IN_EXPANSION]:
            logger.warning(
                f"The cluster status is not active ({cluster.status}), adding the node without distribs and lvstore")
            continue

        logger.info("Sending cluster map add node")
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for node_index, node in enumerate(snodes):
            if node.status != StorageNode.STATUS_ONLINE or node.get_id() == snode.get_id():
                continue
            ret = distr_controller.send_cluster_map_add_node(snode, node)

        # for dev in snode.nvme_devices:
        #     if dev.status == NVMeDevice.STATUS_ONLINE:
        #         device_controller.device_set_unavailable(dev.get_id())

        # logger.info("Setting node status to suspended")
        # set_node_status(snode.get_id(), StorageNode.STATUS_SUSPENDED)
        # logger.info("Done")

        logger.info("Setting node status to Active")
        set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE, caused_by="add_node")

        # In --expansion mode the caller (clibase) triggers expansion
        # migration explicitly *after* integrate_new_node_into_cluster has
        # built the post-rotation lvstore_stack and flipped cluster status
        # back to ACTIVE. Skipping it here avoids racing the half-built
        # rotation and double-queueing.
        if not expansion:
            for dev in snode.nvme_devices:
                if dev.status == NVMeDevice.STATUS_ONLINE:
                    tasks_controller.add_new_device_mig_task(dev.get_id())

        storage_events.snode_add(snode)

        cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_IN_EXPANSION)
    logger.info("Done")
    return "Success"


def get_number_of_online_devices(cluster_id):
    dev_count = 0
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    online_nodes = []
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1


def delete_storage_node(node_id, force=False):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_REMOVED:
        logger.error("Node must be in removed status")
        return False

    tasks = tasks_controller.get_active_node_tasks(snode.cluster_id, snode.get_id())
    if tasks:
        logger.error(f"Tasks found: {len(tasks)}, can not delete storage node, or use --force")
        if not force:
            return False
        for task in tasks:
            tasks_controller.cancel_task(task.uuid)
        time.sleep(1)

    snode.remove(db_controller.kv_store)

    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Sending cluster map to node: {node.get_id()}")
        send_cluster_map(node.get_id())

    storage_events.snode_delete(snode)
    logger.info("done")


def remove_storage_node(node_id, force_remove=False, force_migrate=False):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if snode.status == StorageNode.STATUS_ONLINE:
        logger.warning(f"Can not remove online node: {node_id}")
        return False

    tasks = tasks_controller.get_active_node_tasks(snode.cluster_id, snode.get_id())
    if tasks:
        logger.warning(f"Task found: {len(tasks)}, can not remove storage node, or use --force")
        if force_remove is False:
            return False
        for task in tasks:
            tasks_controller.cancel_task(task.uuid)

    lvols = db_controller.get_lvols_by_node_id(node_id)
    if lvols:
        if force_migrate:
            for lvol in lvols:
                pass
                # lvol_controller.migrate(lvol_id)
        elif force_remove:
            for lvol in lvols:
                lvol_controller.delete_lvol(lvol.get_id(), True)
        else:
            logger.warning("LVols found on the storage node, use --force-remove or --force-migrate")
            return False

    snaps = db_controller.get_snapshots()
    node_snaps = []
    for sn in snaps:
        if sn.lvol.node_id == node_id and sn.deleted is False:
            node_snaps.append(sn)

    if node_snaps:
        if force_migrate:
            logger.error("Not implemented!")
            return False
        elif force_remove:
            for sn in node_snaps:
                snapshot_controller.delete(sn.get_id())
        else:
            logger.error("Snapshots found on the storage node, use --force-remove or --force-migrate")
            return False

    if snode.nvme_devices:
        for dev in snode.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                distr_controller.disconnect_device(dev)

    if snode.jm_device and snode.jm_device.get_id() and snode.jm_device.status in [JMDevice.STATUS_ONLINE,
                                                                                   JMDevice.STATUS_UNAVAILABLE]:
        logger.info("Removing JM")
        device_controller.remove_jm_device(snode.jm_device.get_id(), force=True)

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)

    if cluster.mode == "docker":
        logger.info("Leaving swarm...")
        try:
            cluster_docker = utils.get_docker_client(snode.cluster_id)
            for node in cluster_docker.nodes.list():
                if node.attrs["Status"] and snode.mgmt_ip in node.attrs["Status"]["Addr"]:
                    node.remove(force=True)
        except Exception:
            pass

    try:
        if health_controller._check_node_api(snode):
            logger.info("Stopping SPDK container")
            snode_api = snode.client(timeout=20)
            snode_api.spdk_process_kill(snode.rpc_port, snode.cluster_id)
            snode_api.leave_swarm()
            pci_address = []
            for dev in snode.nvme_devices:
                if dev.pcie_address not in pci_address:
                    ret = snode_api.delete_dev_gpt_partitions(dev.pcie_address)
                    logger.debug(ret)
                    pci_address.append(dev.pcie_address)
    except Exception as e:
        logger.exception(e)

    set_node_status(node_id, StorageNode.STATUS_REMOVED)

    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_JM, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
            continue
        device_controller.device_set_failed(dev.get_id())

    logger.info("done")


def restart_storage_node(
        node_id, max_lvol=0, max_snap=0, max_prov=0,
        spdk_image=None, set_spdk_debug=None,
        small_bufsize=0, large_bufsize=0,
        force=False, node_address=None, reattach_volume=False, clear_data=False, new_ssd_pcie=[],
        force_lvol_recreate=False, spdk_proxy_image=None):
    """Wrapper that guarantees the node is reset to OFFLINE if the restart
    fails after THIS call set the RESTARTING status. Without this, any
    ``return False`` inside the inner logic leaves the node pinned in
    STATUS_RESTARTING, which blocks all future restart attempts.

    The cleanup is gated on pre-call status. The earlier version of this
    wrapper unconditionally wrote OFFLINE whenever the post-call status was
    RESTARTING, which corrupted concurrent in-flight restarts: a CLI retry
    bails fast in `_restart_storage_node_impl` (status != OFFLINE → return
    False) without acquiring the lock, but the wrapper would still see
    RESTARTING (held by the auto-restart task) and clobber it with OFFLINE.
    Peers then saw the node as OFFLINE while the running restart was still
    progressing, and `health_controller` flipped that node's local devices
    to UNAVAILABLE — leaving them stuck once the restart completed because
    the device-online block in the impl had already executed earlier.

    Pre-status of RESTARTING or IN_SHUTDOWN means another caller owns the
    transition; we must not clean up after them. Any other pre-status means
    the only way post-call status can be RESTARTING is that THIS call's
    `try_set_node_restarting` acquired the lock and a subsequent step
    failed — that's the case the cleanup is for."""
    db_ctrl = DBController()
    pre_status = None
    try:
        pre_status = db_ctrl.get_storage_node_by_id(node_id).status
    except Exception:
        logger.warning(f"Could not read pre-call status for {node_id}; "
                       f"skipping orphan-RESTARTING cleanup as a precaution")

    result = False
    try:
        result = _restart_storage_node_impl(
            node_id, max_lvol=max_lvol, max_snap=max_snap, max_prov=max_prov,
            spdk_image=spdk_image, set_spdk_debug=set_spdk_debug,
            small_bufsize=small_bufsize, large_bufsize=large_bufsize,
            force=force, node_address=node_address, reattach_volume=reattach_volume,
            clear_data=clear_data, new_ssd_pcie=new_ssd_pcie,
            force_lvol_recreate=force_lvol_recreate, spdk_proxy_image=spdk_proxy_image)
    except Exception:
        logger.exception("restart_storage_node raised unexpectedly")
    finally:
        # Trust the DB. If the impl raised after the ONLINE write was
        # already committed, the node IS factually online — peers see
        # ONLINE, IO is being served — and the only thing that "failed"
        # was a post-flip side-effect that bubbled an exception. Treating
        # that as failure caused the iteration-77 hang where the script
        # spent 8 minutes retrying restarts of an already-online node.
        try:
            post_node = db_ctrl.get_storage_node_by_id(node_id)
            if not result and post_node.status == StorageNode.STATUS_ONLINE:
                logger.warning(
                    f"Restart of {node_id} returned False but DB shows ONLINE; "
                    f"trusting the DB and treating as success."
                )
                result = True
            elif not result and pre_status not in (StorageNode.STATUS_RESTARTING,
                                                    StorageNode.STATUS_IN_SHUTDOWN,
                                                    None):
                # We owned the lock (pre_status was OFFLINE / DOWN / etc.),
                # the impl failed before reaching the ONLINE flip. Reset to
                # OFFLINE regardless of current status — a failed restart
                # can leave RESTARTING, but it can also leave intermediate
                # states; OFFLINE is the only safe wedge-free landing for
                # the next retry.
                logger.warning(
                    f"Restart of {node_id} failed (post-status={post_node.status}); "
                    f"resetting to OFFLINE to unblock future attempts"
                )
                # Force the OFFLINE write — bypass the state-machine guard
                # in set_node_status (which only restricts ONLINE writes
                # anyway, but we use a direct write here to avoid any
                # second-order effects from the helper).
                post_node.status = StorageNode.STATUS_OFFLINE
                post_node.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
                post_node.online_since = ""
                post_node.write_to_db(db_ctrl.kv_store)
                storage_events.snode_status_change(
                    post_node, StorageNode.STATUS_OFFLINE, post_node.status,
                    caused_by="restart_cleanup")
                distr_controller.send_node_status_event(post_node, StorageNode.STATUS_OFFLINE)
        except Exception as cleanup_exc:
            logger.error(f"Failed to reset node {node_id} after failed restart: {cleanup_exc}")
    return result


def _restart_storage_node_impl(
        node_id, max_lvol=0, max_snap=0, max_prov=0,
        spdk_image=None, set_spdk_debug=None,
        small_bufsize=0, large_bufsize=0,
        force=False, node_address=None, reattach_volume=False, clear_data=False, new_ssd_pcie=[],
        force_lvol_recreate=False, spdk_proxy_image=None):
    db_controller = DBController()
    logger.info("Restarting storage node")
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_OFFLINE and force is False:
        logger.error(f"Node must be offline: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_REMOVED:
        logger.error(f"Can not restart removed node: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_RESTARTING:
        logger.error(f"Node is in restart: {node_id}")
        if force is False:
            return False
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if cluster.status == Cluster.STATUS_IN_ACTIVATION:
        logger.error("Cluster is in activation status, can not restart node")
        return False

    # Guard: atomically check no peer is restarting/shutting down and set RESTARTING.
    # Uses a single FDB transaction to prevent TOCTOU race conditions.
    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not restart storage node")
        if force is False:
            return False

    logger.info("Pre-restart check: FDB transaction to verify no peer in restart/shutdown")
    acquired, reason = db_controller.try_set_node_restarting(snode.cluster_id, node_id)
    if not acquired:
        logger.error(f"Cannot restart {node_id}: {reason}")
        return False
    snode = db_controller.get_storage_node_by_id(node_id)

    if  node_address == snode.api_endpoint:
        node_address = None

    if node_address:
        logger.info(f"Restarting on new node with ip: {node_address}")
        snode_api = SNodeClient(node_address, timeout=5 * 60, retry=3)
        node_info, _ = snode_api.info()
        if not node_info:
            logger.error("Failed to get node info!")
            return False
        snode.api_endpoint = node_address
        snode.mgmt_ip = utils.resolve_address(node_address)
        data_nics = []
        for nic in snode.data_nics:
            if_name = nic["if_name"]
            device = node_info['network_interface'][if_name]
            data_nics.append(
                IFace({
                    'uuid': str(uuid.uuid4()),
                    'if_name': if_name,
                    'ip4_address': device['ip'],
                    'status': device['status'],
                    'net_type': device['net_type']}))
        snode.data_nics = data_nics
        snode.hostname = node_info['hostname']

        if snode.num_partitions_per_dev == 0 and reattach_volume:
            new_cloud_instance_id = node_info['cloud_instance']['id']
            detached_volumes = node_utils.detach_ebs_volumes(snode.cloud_instance_id)
            if not detached_volumes:
                logger.error("No volumes with matching tags were detached.")
                return False

            attached_volumes = node_utils.attach_ebs_volumes(new_cloud_instance_id, detached_volumes)
            if not attached_volumes:
                logger.error("Failed to attach volumes.")
                return False

            snode.cloud_instance_id = new_cloud_instance_id
            known_sn = [dev.serial_number for dev in snode.nvme_devices]
            if snode.jm_device and 'serial_number' in snode.jm_device.device_data_dict:
                known_sn.append(snode.jm_device.device_data_dict['serial_number'])

            node_info, _ = snode_api.info()
            for dev in node_info['nvme_devices']:
                if dev['serial_number'] in known_sn:
                    snode_api.bind_device_to_spdk(dev['address'])

    active_tcp = False
    active_rdma = False
    fabric_tcp = cluster.fabric_tcp
    fabric_rdma = cluster.fabric_rdma
    snode_api = snode.client(timeout=5 * 60, retry=3)
    for nic in snode.data_nics:
        if fabric_rdma and snode_api.ifc_is_roce(nic["if_name"]):
            nic.trtype = "RDMA"
            active_rdma = True
            if fabric_tcp and snode_api.ifc_is_tcp(nic["if_name"]):
                active_tcp = True
        elif fabric_tcp and snode_api.ifc_is_tcp(nic["if_name"]):
            nic.trtype = "TCP"
            active_tcp = True
    snode.active_tcp = active_tcp
    snode.active_rdma = active_rdma

    logger.info(f"Restarting Storage node: {snode.mgmt_ip}")
    node_info, _ = snode_api.info()
    logger.debug(f"Node info: {node_info}")

    logger.info("Restarting SPDK")

    if max_lvol:
        snode.max_lvol = max_lvol
    if max_snap:
        snode.max_snap = max_snap

    if not snode.l_cores:
        if node_info.get("nodes_config") and node_info["nodes_config"].get("nodes"):
            nodes = node_info["nodes_config"]["nodes"]
            for node in nodes:
                if node['cpu_mask'] == snode.spdk_cpu_mask:
                    snode.l_cores = node['l-cores']
                    break

    if max_prov > 0:
        try:
            max_prov = int(utils.parse_size(max_prov))
            snode.max_prov = max_prov
        except Exception as e:
            logger.debug(e)
            logger.error(f"Invalid max_prov value: {max_prov}")
            return False
    else:
        max_prov = snode.max_prov

    if spdk_image:
        snode.spdk_image = spdk_image

    # Calculate minimum huge page memory
    minimum_hp_memory = utils.calculate_minimum_hp_memory(snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
                                                          snode.max_lvol,
                                                          max_prov,
                                                          len(utils.hexa_to_cpu_list(snode.spdk_cpu_mask)))

    minimum_hp_memory = max(minimum_hp_memory, max_prov)

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
        logger.info(f"Minimum required huge pages memory is : {utils.humanbytes(minimum_hp_memory)}")
    else:
        logger.error("Cannot get memory info from the instance.. Exiting")
        return False

    # Calculate minimum sys memory
    # minimum_sys_memory = utils.calculate_minimum_sys_memory(snode.max_prov, memory_details['total'])
    # minimum_sys_memory = snode.minimum_sys_memory
    # satisfied, spdk_mem = utils.calculate_spdk_memory(minimum_hp_memory,
    #                                                  minimum_sys_memory,
    #                                                  int(memory_details['free']),
    #                                                  int(memory_details['huge_total']))
    # if not satisfied:
    #    logger.error(
    #        f"Not enough memory for the provided max_lvo: {snode.max_lvol}, max_snap: {snode.max_snap}, max_prov: {utils.humanbytes(snode.max_prov)}.. Exiting")
    minimum_sys_memory = snode.minimum_sys_memory or 0
    snode.spdk_mem = minimum_hp_memory

    spdk_debug = snode.spdk_debug
    if set_spdk_debug:
        spdk_debug = True
        snode.spdk_debug = spdk_debug

    if minimum_sys_memory:
        snode.minimum_sys_memory = minimum_sys_memory

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)

    if cluster.mode == "docker":
        cluster_docker = utils.get_docker_client(snode.cluster_id)
        cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]

    else:
        cluster_ip = utils.get_k8s_node_ip()

    total_mem = minimum_hp_memory
    for n in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if n.api_endpoint == snode.api_endpoint and n.socket == snode.socket and n.uuid != snode.uuid:
            total_mem += (n.spdk_mem + 500000000)

    if spdk_proxy_image:
        snode.spdk_proxy_image = spdk_proxy_image
    if not snode.spdk_proxy_image:
        snode.spdk_proxy_image = cluster.container_image_prefix + constants.SIMPLY_BLOCK_DOCKER_IMAGE

    results = None
    try:
        if new_ssd_pcie and type(new_ssd_pcie) is list:
            for new_ssd in new_ssd_pcie:
                if new_ssd not in snode.ssd_pcie:
                    try:
                        snode_api.bind_device_to_spdk(new_ssd)
                    except Exception as e:
                        logger.error(e)
                    snode.ssd_pcie.append(new_ssd)

        fdb_connection = cluster.db_connection
        results, err = snode_api.spdk_process_start(
            snode.l_cores, snode.spdk_mem, snode.spdk_image, spdk_debug, cluster_ip, fdb_connection,
            snode.namespace, snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password,
            multi_threading_enabled=constants.SPDK_PROXY_MULTI_THREADING_ENABLED, timeout=constants.SPDK_PROXY_TIMEOUT,
            ssd_pcie=snode.ssd_pcie, total_mem=total_mem, system_mem=minimum_sys_memory, cluster_mode=cluster.mode,
            socket=snode.socket, firewall_port=snode.firewall_port, cluster_id=snode.cluster_id,
            spdk_proxy_image=snode.spdk_proxy_image)

    except Exception as e:
        logger.error(e)
        return False
    req_cpu_count = len(utils.hexa_to_cpu_list(snode.spdk_cpu_mask))

    cores, _ = snode_api.read_allowed_list()
    logger.info(f"read_allowed list is {cores}")

    if len(cores) == req_cpu_count:
        new_distribution, _ = snode_api.recalculate_cores_distribution(cores, snode.number_of_alceml_devices)
        poller_cpu_cores = new_distribution.get("poller_cpu_cores")
        snode.alceml_cpu_cores = new_distribution.get("alceml_cpu_cores")
        snode.distrib_cpu_cores = new_distribution.get("distrib_cpu_cores")
        snode.alceml_worker_cpu_cores = new_distribution.get("alceml_worker_cpu_cores")
        jc_singleton_core = new_distribution.get("jc_singleton_core")
        app_thread_core = new_distribution.get("app_thread_core")
        jm_cpu_core = new_distribution.get("jm_cpu_core")
        snode.pollers_mask = utils.generate_mask(poller_cpu_cores)
        snode.app_thread_mask = utils.generate_mask(app_thread_core)

        if jc_singleton_core:
            snode.jc_singleton_mask = utils.decimal_to_hex_power_of_2(jc_singleton_core[0])
        snode.jm_cpu_mask = utils.generate_mask(jm_cpu_core)

    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False
    time.sleep(5)

    if small_bufsize:
        snode.iobuf_small_bufsize = small_bufsize
    if large_bufsize:
        snode.iobuf_large_bufsize = large_bufsize

    snode.write_to_db(db_controller.kv_store)

    rpc_client = snode.rpc_client(timeout=10 * 60, retry=10)

    # 1- set iobuf options
    if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
            snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
        ret = rpc_client.iobuf_set_options(
            snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
            snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
        if not ret:
            logger.error("Failed to set iobuf options")
            return False
    rpc_client.bdev_set_options(0, 0, 0, 0)
    rpc_client.accel_set_options()

    # 2- set socket implementation options
    bind_to_device = None
    if snode.data_nics and len(snode.data_nics) == 1:
        bind_to_device = snode.data_nics[0].if_name
    ret = rpc_client.sock_impl_set_options(bind_to_device)
    if not ret:
        logger.error("Failed socket implement set options")
        return False

    ret = rpc_client.nvmf_set_max_subsystems(constants.NVMF_MAX_SUBSYSTEMS)
    if not ret:
        logger.warning(f"Failed to set nvmf max subsystems {constants.NVMF_MAX_SUBSYSTEMS}")

    # 3- set nvme config
    if snode.pollers_mask:
        ret = rpc_client.nvmf_set_config(
            snode.pollers_mask,
            dhchap_digests=constants.DHCHAP_DIGESTS,
            dhchap_dhgroups=[constants.DHCHAP_DHGROUP],
        )
        if not ret:
            logger.error("Failed to set pollers mask")
            return False

    # 4- start spdk framework
    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    rpc_client.log_set_print_level("DEBUG")

    if snode.lvol_poller_mask:
        ret = rpc_client.bdev_lvol_create_poller_group(snode.lvol_poller_mask)
        if not ret:
            logger.error("Failed to set pollers mask")
            return False

    # 5- set app_thread cpu mask
    if snode.app_thread_mask:
        ret = rpc_client.thread_get_stats()
        app_thread_process_id = 0
        if ret.get("threads"):
            for entry in ret["threads"]:
                if entry['name'] == 'app_thread':
                    app_thread_process_id = entry['id']
                    break

        ret = rpc_client.thread_set_cpumask(app_thread_process_id, snode.app_thread_mask)
        if not ret:
            logger.error("Failed to set app thread mask")
            return False

    # 6- set nvme bdev options
    # bdev_nvme_set_options is a pure local SPDK config call; bound it at
    # 5 s so a stuck proxy can't consume the 10 min restart RPC budget.
    set_opts_rpc = snode.rpc_client(timeout=5, retry=0)
    ret = set_opts_rpc.bdev_nvme_set_options()
    if not ret:
        logger.error("Failed to set nvme options")
        return False

    qpair = cluster.qpair_count
    if cluster.fabric_tcp:
        ret = rpc_client.transport_create("TCP", qpair, 512 * (req_cpu_count + 1))
        if not ret:
            logger.error(f"Failed to create transport TCP with qpair: {qpair}")
            return False
    if cluster.fabric_rdma:
        ret = rpc_client.transport_create("RDMA", qpair, 512 * (req_cpu_count + 1))
        if not ret:
            logger.error(f"Failed to create transport RDMA with qpair: {qpair}")
            return False

    # 7- set jc singleton mask
    if snode.jc_singleton_mask:
        ret = rpc_client.jc_set_hint_lcpu_mask(snode.jc_singleton_mask)
        if not ret:
            logger.error("Failed to set jc singleton mask")
            return False

    node_info, _ = snode_api.info()
    if not snode.ssd_pcie:
        ssds = node_info['spdk_pcie_list']
    else:
        ssds = []
        for ssd in snode.ssd_pcie:
            if ssd in node_info['spdk_pcie_list']:
                ssds.append(ssd)

    nvme_devs = addNvmeDevices(rpc_client, snode, ssds)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    logger.info(f"Devices found: {len(nvme_devs)}")
    logger.debug(nvme_devs)

    logger.info(f"Devices in db: {len(snode.nvme_devices)}")
    logger.debug(snode.nvme_devices)

    new_devices = []
    active_devices = []
    removed_devices = []
    known_devices_sn = []
    devices_sn_dict = {d.serial_number: d for d in nvme_devs}
    for db_dev in snode.nvme_devices:
        known_devices_sn.append(db_dev.serial_number)
        if db_dev.status in [NVMeDevice.STATUS_FAILED_AND_MIGRATED, NVMeDevice.STATUS_FAILED,
                             NVMeDevice.STATUS_REMOVED]:
            removed_devices.append(db_dev)
            continue
        if db_dev.serial_number in devices_sn_dict.keys():
            logger.info(f"Device found: {db_dev.get_id()}, status {db_dev.status}")
            found_dev = devices_sn_dict[db_dev.serial_number]
            if not db_dev.is_partition and not found_dev.is_partition:
                db_dev.device_name = found_dev.device_name
                db_dev.nvme_bdev = found_dev.nvme_bdev
                db_dev.nvme_controller = found_dev.nvme_controller
                db_dev.pcie_address = found_dev.pcie_address

            # if db_dev.status in [ NVMeDevice.STATUS_ONLINE]:
            #     db_dev.status = NVMeDevice.STATUS_UNAVAILABLE
            active_devices.append(db_dev)
        else:
            logger.info(f"Device not found: {db_dev.get_id()}")
            if db_dev.status == NVMeDevice.STATUS_NEW:
                snode.nvme_devices.remove(db_dev)
            else:
                db_dev.status = NVMeDevice.STATUS_REMOVED
                removed_devices.append(db_dev)

    jm_dev_sn = ""
    if snode.jm_device and "serial_number" in snode.jm_device.device_data_dict:
        jm_dev_sn = snode.jm_device.device_data_dict['serial_number']
        known_devices_sn.append(jm_dev_sn)

    for dev in nvme_devs:
        if dev.serial_number == jm_dev_sn:
            logger.info(f"JM device found: {snode.jm_device.get_id()}")
            snode.jm_device.nvme_bdev = dev.nvme_bdev

        elif dev.serial_number not in known_devices_sn:
            logger.info(f"New device found: {dev.get_id()}")
            dev.status = NVMeDevice.STATUS_NEW
            new_devices.append(dev)
            snode.nvme_devices.append(dev)

    snode.write_to_db(db_controller.kv_store)
    if node_address and len(new_devices) > 0:
        # prepare devices on new node
        if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:

            jm_device = snode.nvme_devices[0]
            for index, nvme in enumerate(snode.nvme_devices):
                if nvme.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW] and nvme.size < jm_device.size:
                    jm_device = nvme
            jm_device.status = NVMeDevice.STATUS_JM

            if snode.jm_device and snode.jm_device.get_id():
                jm_device.uuid = snode.jm_device.get_id()

            ret = _prepare_cluster_devices_jm_on_dev(snode, snode.nvme_devices)
        else:
            ret = _prepare_cluster_devices_partitions(snode, snode.nvme_devices)
        if not ret:
            logger.error("Failed to prepare cluster devices")
            # return False
    else:
        ret = _prepare_cluster_devices_on_restart(snode, clear_data=clear_data)
        if not ret:
            logger.error("Failed to prepare cluster devices")
            return False

    snode.write_to_db()

    # set qos values if enabled
    if cluster.is_qos_set():
        logger.info("Setting Alcemls QOS weights")
        ret = rpc_client.alceml_set_qos_weights(qos_controller.get_qos_weights_list(snode.cluster_id))
        if not ret:
            logger.error("Failed to set Alcemls QOS")
            return False

    logger.info("Connecting to remote devices")
    try:
        snode.remote_devices = _connect_to_remote_devs(snode)
    except RuntimeError:
        logger.error('Failed to connect to remote devices')
        return False
    if snode.enable_ha_jm:
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)
    snode.lvstore_status = ""
    snode.write_to_db(db_controller.kv_store)

    snode = db_controller.get_storage_node_by_id(snode.get_id())
    for db_dev in snode.nvme_devices:
        if db_dev.status in [NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_ONLINE,
                             NVMeDevice.STATUS_CANNOT_ALLOCATE, NVMeDevice.STATUS_READONLY]:
            db_dev.status = NVMeDevice.STATUS_ONLINE
            if db_dev.previous_status and db_dev.previous_status == NVMeDevice.STATUS_CANNOT_ALLOCATE:
                records = db_controller.get_device_capacity(db_dev, 1)
                if records and records[0].size_util == 100:
                    db_dev.status = NVMeDevice.STATUS_CANNOT_ALLOCATE
            db_dev.health_check = True
            device_events.device_restarted(db_dev)
    snode.write_to_db(db_controller.kv_store)

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:

        # make other nodes connect to the new devices
        logger.info("Make other nodes connect to the node devices")
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        for node in snodes:
            if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                continue
            try:
                # Re-read node from DB to avoid overwriting concurrent changes
                node = db_controller.get_storage_node_by_id(node.get_id())
                node.remote_devices = _connect_to_remote_devs(node, reattach=True, force_connect_restarting_nodes=True)
            except RuntimeError:
                logger.error('Failed to connect to remote devices')
                return False
            node.write_to_db()

        logger.info("Sending device status event")
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        for db_dev in snode.nvme_devices:
            distr_controller.send_dev_status_event(db_dev, db_dev.status)

        if snode.jm_device and snode.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
            device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # ANA failback: demote secondaries BEFORE port unblock/online
        try:
            trigger_ana_failback_for_node(snode)
        except Exception as ana_e:
            logger.error("ANA failback during restart of %s failed: %s", snode.get_id(), ana_e)

        logger.info("Cluster is not ready yet")
        logger.info("Setting node status to Online")
        set_node_status(node_id, StorageNode.STATUS_ONLINE, caused_by="restart")
        _refresh_cluster_maps_after_node_recovery(snode)

        online_devices_list = []
        for dev in snode.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE,
                              NVMeDevice.STATUS_CANNOT_ALLOCATE,
                              NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                online_devices_list.append(dev.get_id())
        if online_devices_list:
            logger.info(f"Starting migration task for node {snode.get_id()}")
            tasks_controller.add_device_mig_task_for_node(snode.get_id())

        return True

    else:
        snode = db_controller.get_storage_node_by_id(snode.get_id())

        # Remote device connectivity is node-level and must be established before
        # any LVS recreation consumes remote alceml bdevs in distrib maps/stacks.
        logger.info("Make other nodes connect to the node devices")
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        for node in snodes:
            if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                continue

            try:
                # Re-read node from DB to avoid overwriting concurrent changes
                node = db_controller.get_storage_node_by_id(node.get_id())
                node.remote_devices = _connect_to_remote_devs(node, force_connect_restarting_nodes=True)
                if node.enable_ha_jm:
                    node.remote_jm_devices = _connect_to_remote_jm_devs(node)
            except RuntimeError:
                logger.error('Failed to connect to remote devices')
                return False
            node.write_to_db()

        # === LVS Recreation: clear sequential structure per design ===
        # No recursion. Process primary, secondary, tertiary LVS in order.
        # Before each, perform disconnect checks on the other two nodes.

        def _abort_restart(reason):
            """Kill SPDK and set offline on fatal error."""
            logger.error(f"Restart abort: {reason}")
            storage_events.snode_restart_failed(snode)
            snode_api_inner = snode.client(timeout=5, retry=5)
            snode_api_inner.spdk_process_kill(snode.rpc_port, snode.cluster_id)
            set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE)

        try:
            ret = recreate_all_lvstores(snode, force=force_lvol_recreate)
        except Exception as e:
            logger.error(e)
            _abort_restart(f"LVS recreation failed: {e}")
            return False
        if not ret:
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.lvstore_status = "failed"
            snode.write_to_db()
            set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE)
            return False

        # === Phase 10: Finalization — post all LVS recreation ===

        # Create S3 bdev for backup support (only if backup is configured)
        if cluster.backup_config:
            from simplyblock_core.controllers import backup_controller
            logger.info("Creating S3 bdev on restarted node")
            backup_controller.create_s3_bdev(snode, cluster.backup_config)

        # make other nodes connect to the new devices
        logger.info("Make other nodes connect to the node devices")
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        for node in snodes:
            if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                continue

            try:
                # Re-read node from DB to avoid overwriting concurrent changes
                node = db_controller.get_storage_node_by_id(node.get_id())
                node.remote_devices = _connect_to_remote_devs(node, force_connect_restarting_nodes=True)
                if node.enable_ha_jm:
                    node.remote_jm_devices = _connect_to_remote_jm_devs(node)
            except RuntimeError:
                logger.error('Failed to connect to remote devices')
                return False
            node.write_to_db()

        if snode.jm_device and snode.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
            device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # ANA failback: demote secondaries BEFORE port unblock/online
        try:
            trigger_ana_failback_for_node(snode)
        except Exception as ana_e:
            logger.error("ANA failback during restart of %s failed: %s", snode.get_id(), ana_e)

        logger.info("Setting node status to Online")
        set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE, caused_by="restart")

        logger.info("Sending device status event")
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        for db_dev in snode.nvme_devices:
            distr_controller.send_dev_status_event(db_dev, db_dev.status)

        _refresh_cluster_maps_after_node_recovery(snode)

        lvol_list = db_controller.get_lvols_by_node_id(snode.get_id())
        logger.info(f"Found {len(lvol_list)} lvols")

        # connect lvols to their respect pool
        for lvol in lvol_list:
            lvol_controller.connect_lvol_to_pool(lvol.uuid)

        # recreate pools
        pools = db_controller.get_pools()
        for pool in pools:
            ret = rpc_client.bdev_lvol_set_qos_limit(pool.numeric_id,
                                                     pool.max_rw_ios_per_sec,
                                                     pool.max_rw_mbytes_per_sec,
                                                     pool.max_r_mbytes_per_sec,
                                                     pool.max_w_mbytes_per_sec,
                                                     )
            if not ret:
                logger.error("RPC failed bdev_lvol_set_qos_limit")
                return False

        # Phase 10: start data migration, set node online
        online_devices_list = []
        for dev in snode.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE,
                              NVMeDevice.STATUS_CANNOT_ALLOCATE,
                              NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                online_devices_list.append(dev.get_id())
        if online_devices_list:
            logger.info(f"Starting migration task for node {snode.get_id()}")
            tasks_controller.add_device_mig_task_for_node(snode.get_id())
        return True


def _format_lvstore_ports(node):
    """Format per-lvstore ports for display."""
    if not node.lvstore_ports:
        return "-"
    parts = []
    for lvs_name, ports in node.lvstore_ports.items():
        lp = ports.get("lvol_subsys_port", "-")
        hp = ports.get("hublvol_port", "-")
        parts.append(f"{lvs_name}(L:{lp},H:{hp})")
    return " ".join(parts)


def list_storage_nodes(is_json, cluster_id=None):
    db_controller = DBController()
    if cluster_id:
        nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    else:
        nodes = db_controller.get_storage_nodes()
    data = []
    output = ""

    for node in nodes:
        logger.debug(node)
        logger.debug("*" * 20)
        total_devices = len(node.nvme_devices)
        online_devices = 0

        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1
        lvs = db_controller.get_lvols_by_node_id(node.get_id()) or []
        data.append({
            "UUID": node.uuid,
            "Hostname": node.hostname,
            "Management IP": node.mgmt_ip,
            "Dev": f"{total_devices}/{online_devices}",
            "LVols": f"{len(lvs)}",
            "Status": node.status,
            "Health": node.health_check,
            "Up time": utils.strfdelta(uptime) if (uptime := node.uptime()) is not None else "",
            "CPU": f"{len(utils.hexa_to_cpu_list(node.spdk_cpu_mask))}",
            "MEM": utils.humanbytes(node.spdk_mem),
            "SPDK P": node.rpc_port,
            "LVOL P": node.lvol_subsys_port,
            "DEV P": node.nvmf_port,
            "HUB P": node.hublvol.nvmf_port if node.hublvol else "-",
            "LVS Ports": _format_lvstore_ports(node),
            # "Cloud ID": node.cloud_instance_id,
            # "JM VUID": node.jm_vuid,
            # "Ext IP": node.cloud_instance_public_ip,
            "Secondary node ID": node.secondary_node_id,

        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def list_storage_devices(node_id, is_json):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("This storage node is not part of the cluster")
        return False

    storage_devices = []
    bdev_devices = []
    jm_devices = []
    remote_devices = []
    for device in snode.nvme_devices:
        logger.debug(device)
        logger.debug("*" * 20)
        storage_devices.append({
            "UUID": device.uuid,
            "StorgeID": device.cluster_device_order,
            "Name": device.alceml_name,
            "Size": utils.humanbytes(device.size),
            "Serial Number": device.serial_number,
            "PCIe": device.pcie_address,
            "Status": device.status,
            "IO Err": device.io_error,
            "Health": device.health_check
        })

    for bdev in snode.lvstore_stack:
        if bdev['type'] != "bdev_distr":
            continue
        logger.debug("*" * 20)
        distrib_params = bdev['params']
        bdev_devices.append({
            "VUID": distrib_params['vuid'],
            "Name": distrib_params['name'],
            "Size": utils.humanbytes(distrib_params['num_blocks'] * distrib_params['block_size']),
            "Block Size": distrib_params['block_size'],
            "Num Blocks": distrib_params['num_blocks'],
            "NDCS": f"{distrib_params['ndcs']}",
            "NPCS": f"{distrib_params['npcs']}",
            "Chunk": distrib_params['chunk_size'],
            "Page Size": distrib_params['pba_page_size'],
            "JM_VUID": distrib_params['jm_vuid'],
        })

    if snode.jm_device and snode.jm_device.get_id():
        jm_devices.append({
            "UUID": snode.jm_device.uuid,
            "Name": snode.jm_device.alceml_name,
            "Size": utils.humanbytes(snode.jm_device.size),
            "Status": snode.jm_device.status,
            "IO Err": snode.jm_device.io_error,
            "Health": snode.jm_device.health_check
        })

    for remote_device in snode.remote_devices:
        logger.debug(remote_device)
        logger.debug("*" * 20)
        name = remote_device.alceml_name

        remote_devices.append({
            "UUID": remote_device.uuid,
            "Name": name,
            "Size": utils.humanbytes(remote_device.size),
            "Node ID": remote_device.node_id,
            "Status": remote_device.status,
        })

    for remote_jm_device in snode.remote_jm_devices:
        logger.debug(remote_jm_device)
        logger.debug("*" * 20)
        remote_devices.append({
            "UUID": remote_jm_device.uuid,
            "Name": remote_jm_device.remote_bdev,
            "Size": utils.humanbytes(remote_jm_device.size),
            "Node ID": remote_jm_device.node_id,
            "Status": remote_jm_device.status,
        })

    data: dict[str, List[Any]] = {
        "Storage Devices": storage_devices,
        "JM Devices": jm_devices,
        "Remote Devices": remote_devices,
    }
    if bdev_devices:
        data["Distrib Block Devices"] = bdev_devices

    if is_json:
        return json.dumps(data, indent=2)
    else:
        out = "\n\n".join(
            f'{key}\n{utils.print_table(value)}\n\n'
            for key, value in data.items()
        )
        return out


def _check_ftt_allows_node_removal(node_id, db_controller):
    """Check whether FTT constraints allow removing (suspend/shutdown) a node.

    Returns (allowed: bool, reason: str).
    """
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return False, "Node not found"

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)

    if cluster.ha_type != "ha":
        return True, ""

    npcs = cluster.distr_npcs  # parity chunk count (1 or 2)
    ndcs = cluster.distr_ndcs  # data chunk count
    ft = cluster.max_fault_tolerance  # declared fault tolerance level

    # Count total active nodes (excluding in_creation and removed)
    total_active_nodes = sum(
        1 for node in snodes
        if node.status not in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]
    )

    # Block suspend/shutdown during rebalancing based on node headroom.
    # A cluster needs ndcs+npcs nodes minimum. During rebalancing:
    #   - With exactly ndcs+npcs nodes: no shutdowns allowed (no headroom)
    #   - With ndcs+npcs+1 nodes: one shutdown allowed (one spare)
    #   - With ndcs+npcs+2+ nodes: two shutdowns allowed, etc.
    # The number of allowed shutdowns during rebalancing is:
    #   total_active_nodes - (ndcs + npcs)
    # This must be greater than the number of already-not-online nodes.
    if cluster.is_re_balancing:
        not_online_already = sum(
            1 for node in snodes
            if node.get_id() != node_id
            and node.status != StorageNode.STATUS_ONLINE
            and node.status not in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]
        )
        headroom = total_active_nodes - (ndcs + npcs)
        if headroom <= not_online_already:
            return False, (
                f"Cluster is rebalancing with {total_active_nodes} active nodes "
                f"({not_online_already} already not online, "
                f"need >{ndcs + npcs} for ndcs={ndcs}, npcs={npcs}). "
                f"Wait for rebalancing to complete before removing a node."
            )

    # Count nodes that are not online (excluding the node being removed,
    # and excluding nodes in creation or already removed).
    not_online_nodes = []
    for node in snodes:
        if node.get_id() == node_id:
            continue
        if node.status in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            not_online_nodes.append(node)

    # Check for journal replication in progress on any online node.
    # A node with active journal replication counts as one additional not-online node.
    jm_replication_active = False
    for node in snodes:
        if node.get_id() == node_id:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            lvstores = node.rpc_client(timeout=5, retry=1).bdev_lvol_get_lvstores(node.lvstore)
            if lvstores:
                ret = node.rpc_client(timeout=5, retry=1).jc_get_jm_status(node.jm_vuid)
                for jm in ret:
                    if ret[jm] is False:
                        jm_replication_active = True
                        break
        except Exception:
            pass
        if jm_replication_active:
            break

    not_online_count = len(not_online_nodes)
    if jm_replication_active:
        not_online_count += 1

    if npcs == 1:
        # FTT=1: no room at all if anything is already not online or journal replicating
        if not_online_count > 0:
            return False, (
                f"FTT=1 (npcs=1): cannot remove node, cluster already has "
                f"{len(not_online_nodes)} not-online node(s)"
                f"{' and journal replication in progress' if jm_replication_active else ''}"
            )

    elif npcs == 2:
        if ft >= 2:
            # FTT=2: room for one not-online node, block if already have one+
            if not_online_count >= 2:
                return False, (
                    f"FTT=2 (npcs=2): cannot remove node, cluster already has "
                    f"{len(not_online_nodes)} not-online node(s)"
                    f"{' and journal replication in progress' if jm_replication_active else ''}"
                )
        else:
            # npcs=2, ft=1: like FTT=2 for capacity, but additionally
            # cannot remove both primary and its secondary
            if not_online_count >= 2:
                return False, (
                    f"npcs=2/ft=1: cannot remove node, cluster already has "
                    f"{len(not_online_nodes)} not-online node(s)"
                    f"{' and journal replication in progress' if jm_replication_active else ''}"
                )

            # Check primary-secondary pair constraint:
            # If the node being removed is a primary, check its secondary is online.
            # If the node being removed is a secondary, check its primary is online.
            for not_online_node in not_online_nodes:
                # Is any not-online node the secondary of the node we're removing?
                if snode.secondary_node_id == not_online_node.get_id():
                    return False, (
                        f"npcs=2/ft=1: cannot remove node {node_id}, "
                        f"its secondary {not_online_node.get_id()} is not online "
                        f"(status: {not_online_node.status})"
                    )
                if snode.tertiary_node_id == not_online_node.get_id():
                    return False, (
                        f"npcs=2/ft=1: cannot remove node {node_id}, "
                        f"its secondary {not_online_node.get_id()} is not online "
                        f"(status: {not_online_node.status})"
                    )

            # Is the node we're removing a secondary of any not-online primary?
            for not_online_node in not_online_nodes:
                if not_online_node.secondary_node_id == node_id:
                    return False, (
                        f"npcs=2/ft=1: cannot remove node {node_id}, "
                        f"it is secondary of not-online primary {not_online_node.get_id()} "
                        f"(status: {not_online_node.status})"
                    )
                if not_online_node.tertiary_node_id == node_id:
                    return False, (
                        f"npcs=2/ft=1: cannot remove node {node_id}, "
                        f"it is secondary of not-online primary {not_online_node.get_id()} "
                        f"(status: {not_online_node.status})"
                    )

    return True, ""


def _allow_shutdown_with_migration_tasks(snode, db_controller):
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    return (
        cluster.ha_type == "ha"
        and cluster.max_fault_tolerance >= 2
        and cluster.distr_npcs >= 2
    )


def shutdown_storage_node(node_id, force=False):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)

    # FTT check (suspend already checked this, but verify again for direct shutdown calls)
    if not force:
        allowed, reason = _check_ftt_allows_node_removal(node_id, db_controller)
        if not allowed:
            logger.error(f"Cannot shutdown node: {reason}")
            return False, reason

    # Guard: no concurrent shutdown + restart (design: mutual exclusion)
    for peer in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if peer.get_id() != node_id and peer.status == StorageNode.STATUS_RESTARTING:
            logger.error(
                f"Node {peer.get_id()} is restarting in this cluster, "
                f"cannot shutdown {node_id} concurrently")
            if force is False:
                return False
        if peer.get_id() != node_id and peer.status == StorageNode.STATUS_IN_SHUTDOWN:
            logger.error(
                f"Node {peer.get_id()} is already shutting down in this cluster, "
                f"cannot shutdown {node_id} concurrently")
            if force is False:
                return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not shutdown storage node")
        if force is False:
            return False

    tasks = tasks_controller.get_active_node_tasks(snode.cluster_id, snode.get_id())
    if tasks:
        if not force and _allow_shutdown_with_migration_tasks(snode, db_controller):
            logger.warning(
                "Migration task found: %s, proceeding with shutdown because FTT=2 allows node outage",
                len(tasks),
            )
        elif force:
            logger.warning(
                "Migration task found: %s, proceeding with forced shutdown and canceling tasks",
                len(tasks),
            )
            for task in tasks:
                if task.function_name != JobSchedule.FN_NODE_RESTART:
                    tasks_controller.cancel_task(task.uuid)
        else:
            logger.error(f"Migration task found: {len(tasks)}, can not shutdown storage node or use --force")
            return False

    # If node is not yet suspended and force is not used, run suspend first
    if snode.status != StorageNode.STATUS_SUSPENDED:
        if force:
            logger.warning("Node is not in suspended state, proceeding with force")
        elif snode.status == StorageNode.STATUS_ONLINE:
            logger.info("Node is online, suspending before shutdown")
            ret = suspend_storage_node(node_id, force=False, change_node_status=False)
            if not ret:
                logger.error("Failed to suspend node, cannot proceed with shutdown")
                return False
            snode = db_controller.get_storage_node_by_id(node_id)
        else:
            logger.error("Node is not in suspended or online state, use --force")
            return False

    logger.info("Shutting down node")
    set_node_status(node_id, StorageNode.STATUS_IN_SHUTDOWN)
    time.sleep(3)

    if snode.jm_device and snode.jm_device.status != JMDevice.STATUS_REMOVED:
        logger.info("Setting JM unavailable")
        device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)

    time.sleep(1)
    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_ONLINE,
                          NVMeDevice.STATUS_CANNOT_ALLOCATE, NVMeDevice.STATUS_READONLY]:
            # Default cause (CAUSE_OTHER) is correct here: a node-driven
            # shutdown must not count against the per-device flap budget.
            device_controller.device_set_unavailable(dev.get_id())

    time.sleep(1)
    logger.info("Stopping SPDK")
    try:
        snode.client(timeout=10, retry=10).spdk_process_kill(snode.rpc_port, snode.cluster_id)
    except SNodeClientException:
        logger.error('Failed to kill SPDK')
        return False
    pci_address = []
    for dev in snode.nvme_devices:
        if dev.pcie_address not in pci_address:
            try:
                ret = snode.client(timeout=30, retry=1).bind_device_to_nvme(dev.pcie_address)
                logger.debug(ret)
                pci_address.append(dev.pcie_address)
            except Exception as e:
                logger.debug(e)

    logger.info("Setting node status to offline")
    set_node_status(node_id, StorageNode.STATUS_OFFLINE)

    # ANA failover: update subsystem states on surviving nodes
    snode = db_controller.get_storage_node_by_id(node_id)
    try:
        trigger_ana_failover_for_node(snode)
    except Exception as ana_e:
        logger.error("ANA failover during shutdown of %s failed: %s", node_id, ana_e)

    tasks = db_controller.get_job_tasks(snode.cluster_id)
    for task in tasks:
        if task.node_id == node_id and task.status != JobSchedule.STATUS_DONE:
            if task.function_name in [JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG,
                                      JobSchedule.FN_NEW_DEV_MIG]:
                task.canceled = True
                task.write_to_db(db_controller.kv_store)

    logger.info("Done")
    return True


def suspend_storage_node(node_id, force=False, change_node_status=True):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_ONLINE and change_node_status:
        logger.error("Node is not in online state")
        if force is False:
            return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not suspend storage node")
        if force is False:
            return False

    tasks = tasks_controller.get_active_node_tasks(snode.cluster_id, snode.get_id())
    if tasks:
        if not force and _allow_shutdown_with_migration_tasks(snode, db_controller):
            logger.warning(
                "Migration task found: %s, proceeding with suspend because FTT=2 allows node outage",
                len(tasks),
            )
        elif force:
            logger.warning(
                "Migration task found: %s, proceeding with forced suspend and canceling tasks",
                len(tasks),
            )
            for task in tasks:
                tasks_controller.cancel_task(task.uuid)
        else:
            logger.error(f"Migration task found: {len(tasks)}, can not suspend storage node, use --force")
            return False

    if not force:
        allowed, reason = _check_ftt_allows_node_removal(node_id, db_controller)
        if not allowed:
            logger.error(f"Cannot suspend node: {reason}")
            return False, reason

    logger.info("Suspending node")

    rpc_client = snode.rpc_client()
    fw_api = FirewallClient(snode, timeout=20, retry=1)
    port_type = "tcp"
    if snode.active_rdma:
        port_type = "udp"

    # Track all blocked ports so we can revert on failure
    blocked_ports = []  # list of (port, port_type, rpc_port) tuples

    def _block_port(port):
        fw_api.firewall_set_port(port, port_type, "block", snode.rpc_port, is_reject=True)
        blocked_ports.append(port)

    def _revert_blocked_ports():
        for port in blocked_ports:
            try:
                fw_api.firewall_set_port(port, port_type, "unblock", snode.rpc_port)
            except Exception as revert_e:
                logger.error(f"Failed to revert port block for port {port}: {revert_e}")

    try:
        # Block per-lvstore ports for secondary lvstores hosted on this node
        if snode.lvstore_stack_secondary or snode.lvstore_stack_tertiary:
            nodes = db_controller.get_primary_storage_nodes_by_secondary_node_id(node_id)
            if nodes:
                for node in nodes:
                    sec_lvs_port = node.get_lvol_subsys_port(node.lvstore)
                    sec_hub_port = node.get_hublvol_port(node.lvstore)
                    if sec_hub_port:
                        _block_port(sec_hub_port)
                    _block_port(sec_lvs_port)
                    time.sleep(0.5)
                    rpc_client.bdev_lvol_set_leader(node.lvstore, leader=False)
                    rpc_client.bdev_distrib_force_to_non_leader(node.jm_vuid)

        # Block per-lvstore ports for this node's own primary lvstore
        own_lvs_port = snode.get_lvol_subsys_port(snode.lvstore)
        own_hub_port = snode.get_hublvol_port(snode.lvstore)
        if own_hub_port:
            _block_port(own_hub_port)
        _block_port(own_lvs_port)
        time.sleep(0.5)
        rpc_client.bdev_lvol_set_leader(snode.lvstore, leader=False)
        rpc_client.bdev_distrib_force_to_non_leader(snode.jm_vuid)
        time.sleep(1)
    except Exception as e:
        logger.error(f"Failed during suspend port blocking/leadership transfer: {e}")
        logger.info("Reverting blocked ports")
        _revert_blocked_ports()
        return False

    if change_node_status:
        logger.info("Setting node status to suspended")
        set_node_status(snode.get_id(), StorageNode.STATUS_SUSPENDED)

    logger.info("Done")
    return True


def resume_storage_node(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_SUSPENDED:
        logger.error("Node is not in suspended state")
        return False

    sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
    if sec_node and sec_node.status == StorageNode.STATUS_UNREACHABLE:
        logger.error("Secondary node is unreachable, cannot resume primary node")
        return False

    logger.info("Resuming node")
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
            device_controller.device_set_online(dev.get_id())

    for db_dev in snode.nvme_devices:
        distr_controller.send_dev_status_event(db_dev, db_dev.status)

    logger.info("Set JM Online")
    if snode.jm_device and snode.jm_device.get_id():
        device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

    logger.info("Connecting to remote devices")
    snode = db_controller.get_storage_node_by_id(node_id)
    try:
        snode.remote_devices = _connect_to_remote_devs(snode)
    except RuntimeError:
        logger.error('Failed to connect to remote devices')
        return False
    if snode.enable_ha_jm:
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)
    snode.write_to_db()
    _refresh_cluster_maps_after_node_recovery(snode)

    fw_api = FirewallClient(snode, timeout=20, retry=1)
    port_type = "tcp"
    if snode.active_rdma:
        port_type = "udp"
    nodes = db_controller.get_primary_storage_nodes_by_secondary_node_id(node_id)
    if nodes:
        for node in nodes:
            try:
                # Allow per-lvstore ports for secondary lvstores on this node
                sec_lvs_port = node.get_lvol_subsys_port(node.lvstore)
                sec_hub_port = node.get_hublvol_port(node.lvstore)
                fw_api.firewall_set_port(
                    sec_lvs_port, port_type, "allow", snode.rpc_port)
                if sec_hub_port:
                    fw_api.firewall_set_port(
                        sec_hub_port, port_type, "allow", snode.rpc_port)
            except Exception as e:
                logger.error(e)
                return False

    try:
        # Allow per-lvstore ports for this node's own primary lvstore
        own_lvs_port = snode.get_lvol_subsys_port(snode.lvstore)
        own_hub_port = snode.get_hublvol_port(snode.lvstore)
        fw_api.firewall_set_port(
            own_lvs_port, port_type, "allow", snode.rpc_port)
        if own_hub_port:
            fw_api.firewall_set_port(
                own_hub_port, port_type, "allow", snode.rpc_port)
    except Exception as e:
        logger.error(e)
        return False

    logger.info("Setting node status to online")
    set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE, caused_by="resume")
    logger.info("Done")
    return True


def get_node_capacity(node_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("Storage node Not found")
        return

    cap_stats_keys = [
        "date",
        "size_total",
        "size_prov",
        "size_used",
        "size_free",
        "size_util",
        "size_prov_util",
    ]
    prom_client = PromClient(node.cluster_id)
    records = prom_client.get_node_metrics(node_id, cap_stats_keys, history)
    new_records = utils.process_records(records, records_count, keys=cap_stats_keys)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Absolut": utils.humanbytes(record['size_total']),
            "Provisioned": utils.humanbytes(record['size_prov']),
            "Used": utils.humanbytes(record['size_used']),
            "Free": utils.humanbytes(record['size_free']),
            "Util %": f"{record['size_util']}%",
            "Prov Util %": f"{record['size_prov_util']}%",
        })
    return out


def get_node_iostats_history(node_id, history, records_count=20, parse_sizes=True, with_sizes=False):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False
    io_stats_keys = [
        "date",
        "read_bytes",
        "read_bytes_ps",
        "read_io_ps",
        "read_io",
        "read_latency_ps",
        "write_bytes",
        "write_bytes_ps",
        "write_io",
        "write_io_ps",
        "write_latency_ps",
    ]

    if with_sizes:
        io_stats_keys.extend(
            [
                "size_total",
                "size_prov",
                "size_used",
                "size_free",
                "size_util",
                "size_prov_util",
                "read_latency_ticks",
                "record_duration",
                "record_end_time",
                "record_start_time",
                "unmap_bytes",
                "unmap_bytes_ps",
                "unmap_io",
                "unmap_io_ps",
                "unmap_latency_ps",
                "unmap_latency_ticks",
                "write_bytes_ps",
                "write_latency_ticks",
            ]
        )
    prom_client = PromClient(node.cluster_id)
    records = prom_client.get_node_metrics(node_id, io_stats_keys, history)
    # combine records
    new_records = utils.process_records(records, records_count, keys=io_stats_keys)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record["read_io_ps"],
            "Read lat": record["read_latency_ps"],
            "Write speed": utils.humanbytes(record["write_bytes_ps"]),
            "Write IOPS": record["write_io_ps"],
            "Write lat": record["write_latency_ps"],
        })
    return out


def get_node_ports(node_id):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    out = []
    for nic in node.data_nics:
        out.append({
            "ID": nic.get_id(),
            "Device name": nic.if_name,
            "Address": nic.ip4_address,
            "Net type": nic.trtype,
            "Status": nic.status,
        })
    return utils.print_table(out)


def get_node_port_iostats(port_id, history=None, records_count=20):
    db_controller = DBController()
    nodes = db_controller.get_storage_nodes()
    nd = None
    port = None
    for node in nodes:
        for nic in node.data_nics:
            if nic.get_id() == port_id:
                port = nic
                nd = node
                break

    if port is None or nd is None:
        logger.error("Port not found")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records = db_controller.get_port_stats(nd.get_id(), port.get_id(), limit=records_number)
    new_records = utils.process_records(records, records_count)

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(record['date'])),
            "out_speed": utils.humanbytes(record['out_speed']),
            "in_speed": utils.humanbytes(record['in_speed']),
            "bytes_sent": utils.humanbytes(record['bytes_sent']),
            "bytes_received": utils.humanbytes(record['bytes_received']),
        })
    return utils.print_table(out)


def upgrade_automated_deployment_config():
    try:
        new_config = utils.load_config(constants.NODES_CONFIG_FILE)
        if not utils.validate_config(new_config, True):
            return False
        origin_config = utils.load_config(f"{constants.NODES_CONFIG_FILE}_read_only")
        updated_config = utils.regenerate_config(new_config, origin_config)
        if not updated_config or not updated_config.get("nodes"):
            return False
        utils.store_config_file(updated_config, constants.NODES_CONFIG_FILE, create_read_only_file=True)
        # Set Huge page memory
        huge_page_memory_dict: dict = {}
        for node_config in updated_config["nodes"]:
            numa = node_config["socket"]
            huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
        for numa, huge_page_memory in huge_page_memory_dict.items():
            num_pages = huge_page_memory // (2048 * 1024)
            utils.set_hugepages_if_needed(numa, num_pages)
        logger.info("Config regenerated successfully")
        return True
    except FileNotFoundError:
        logger.error("Error: Config file not found!")
        return False
    except json.JSONDecodeError:
        logger.error("Error: Config file is not valid JSON!")
        return False


def generate_automated_deployment_config(max_lvol, max_prov, sockets_to_use, nodes_per_socket, pci_allowed, pci_blocked,
                                         cores_percentage=0, force=False, device_model="", size_range="", nvme_names=None, k8s=False,
                                         calculate_hp_only=False, number_of_devices=0):
    if calculate_hp_only:
        minimum_hp_memory = utils.calculate_hp_only(max_lvol, number_of_devices, sockets_to_use, nodes_per_socket, cores_percentage)
        hp_number = math.ceil(minimum_hp_memory / 2)
        logger.info(f"The required number of huge pages on this host is: {hp_number} ({minimum_hp_memory} MB)")
        return True
    else:
        # we need minimum of 6 VPCs. RAM 4GB min. Plus 0.2% of the storage.
        total_cores = os.cpu_count() or 0
        if total_cores < 6:
            raise ValueError("Error: Not enough CPU cores to deploy storage node. Minimum 6 cores required.")

        # load vfio_pci and uio_pci_generic
        utils.load_kernel_module("vfio_pci")
        utils.load_kernel_module("uio_pci_generic")

        nodes_config, system_info = utils.generate_configs(max_lvol, max_prov, sockets_to_use, nodes_per_socket,
                                                           pci_allowed, pci_blocked, cores_percentage, force=force,
                                                           device_model=device_model, size_range=size_range, nvme_names=nvme_names)
        if not nodes_config or not nodes_config.get("nodes"):
            return False
        utils.store_config_file(nodes_config, constants.NODES_CONFIG_FILE, create_read_only_file=True)
        if system_info:
            utils.store_config_file(system_info, constants.SYSTEM_INFO_FILE)
        huge_page_memory_dict: dict = {}

        # Set Huge page memory
        for node_config in nodes_config["nodes"]:
            numa = node_config["socket"]
            huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
        if not k8s:
            utils.create_rpc_socket_mount()
        # for numa, huge_page_memory in huge_page_memory_dict.items():
        #    num_pages = huge_page_memory // (2048 * 1024)
        #    utils.set_hugepages_if_needed(numa, num_pages)
    return True


def deploy(ifname, isolate_cores=False):
    if not ifname:
        ifname = "eth0"

    dev_ip = utils.get_iface_ip(ifname)
    if not dev_ip:
        logger.error(f"Error getting interface ip: {ifname}")
        return False
    try:
        nodes_config = utils.load_config(constants.NODES_CONFIG_FILE)
        logger.info("Config loaded successfully.")
    except FileNotFoundError:
        logger.error("Error: Config file not found!")
        return False
    except json.JSONDecodeError:
        logger.error("Error: Config file is not valid JSON!")
        return False
    all_isolated_cores = utils.validate_config(nodes_config)
    if not all_isolated_cores:
        return False
    logger.info("Config Validated successfully.")

    logger.info("NVMe SSD devices found on node:")
    stream = os.popen(
        f"lspci -Dnn | grep -i '\\[{LINUX_DRV_MASS_STORAGE_ID:02}{LINUX_DRV_MASS_STORAGE_NVME_TYPE_ID:02}\\]'")
    for line in stream.readlines():
        logger.info(line.strip())

    logger.info("Installing dependencies...")
    scripts.install_deps(mode="docker")

    logger.info(f"Node IP: {dev_ip}")
    scripts.configure_docker(dev_ip)

    start_storage_node_api_container(dev_ip)

    if isolate_cores:
        utils.generate_realtime_variables_file(all_isolated_cores)
        utils.run_tuned()
        arch = platform.machine().lower()
        if "arm" in arch or "aarch64" in arch:
            utils.run_grubby(all_isolated_cores)
    return f"{dev_ip}:5000"


def start_storage_node_api_container(node_ip, cluster_ip=None):
    node_docker = docker.DockerClient(base_url=f"tcp://{node_ip}:2375", version="auto", timeout=60 * 5)
    # node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock', version="auto", timeout=60 * 5)
    logger.info(f"Pulling image {constants.SIMPLY_BLOCK_DOCKER_IMAGE}")
    pull_docker_image_with_retry(node_docker, constants.SIMPLY_BLOCK_DOCKER_IMAGE)

    logger.info("Recreating SNodeAPI container")

    # create the api container
    utils.remove_container(node_docker, '/SNodeAPI')

    if cluster_ip is not None:
        log_config = LogConfig(type=LogConfig.types.GELF, config={"gelf-address": f"tcp://{cluster_ip}:12202"})
    else:
        log_config = LogConfig(type=LogConfig.types.JOURNALD)

    node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "sudo -E python3 simplyblock_web/node_webapp.py storage_node",
        detach=True,
        privileged=True,
        name="SNodeAPI",
        network_mode="host",
        log_config=log_config,
        volumes=[
            '/etc/simplyblock:/etc/simplyblock',
            '/etc/foundationdb:/etc/foundationdb',
            '/var/tmp:/var/tmp',
            '/var/run:/var/run',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/sys:/sys',
            # Bind-mount the SPDK ramdisk so the spdk_process_is_up endpoint
            # can probe SPDK's JSON-RPC Unix socket directly at
            # /mnt/ramdisk/spdk_<port>/spdk.sock. Without this, the endpoint
            # has to fall through to dockerd, which can stall for 60-80s
            # during post-outage Swarm reconciliation (incident 2026-04-24).
            '/mnt/ramdisk:/mnt/ramdisk'],
        restart_policy={"Name": "always"},
        environment=[
            f"DOCKER_IP={node_ip}",
            "WITHOUT_CLOUD_INFO=True",
            "SIMPLYBLOCK_LOG_LEVEL=DEBUG",
        ]
    )
    logger.info(f"Pulling image {constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE}")
    pull_docker_image_with_retry(node_docker, constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    return True


def deploy_cleaner():
    scripts.deploy_cleaner()


def clean_devices(config_path, format=True, force=False, format_4k=False):
    utils.clean_devices(config_path, format=format, force=force, format_4k=format_4k)


def get_host_secret(node_id):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    return node.host_secret


def get_ctrl_secret(node_id):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    return node.ctrl_secret


def health_check(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    try:

        res = utils.ping_host(snode.mgmt_ip)
        if res:
            logger.info(f"Ping host: {snode.mgmt_ip}... OK")
        else:
            logger.error(f"Ping host: {snode.mgmt_ip}... Failed")

        # node_docker = docker.DockerClient(base_url=f"tcp://{snode.mgmt_ip}:2375", version="auto")
        # containers_list = node_docker.containers.list(all=True)
        # for cont in containers_list:
        #     name = cont.attrs['Name']
        #     state = cont.attrs['State']
        #
        #     if name in ['/spdk', '/spdk_proxy', '/SNodeAPI'] or name.startswith("/app_"):
        #         logger.debug(state)
        #         since = ""
        #         try:
        #             start = datetime.datetime.fromisoformat(state['StartedAt'].split('.')[0])
        #             since = str(datetime.datetime.now() - start).split('.')[0]
        #         except Exception:
        #             pass
        #         clean_name = name.split(".")[0].replace("/", "")
        #         logger.info(f"Container: {clean_name}, Status: {state['Status']}, Since: {since}")

    except Exception as e:
        logger.error(f"Failed to connect to node's docker: {e}")

    try:
        logger.info("Connecting to node's SPDK")
        rpc_client = snode.rpc_client(timeout=3, retry=1)

        ret = rpc_client.get_version()
        logger.info(f"SPDK version: {ret['version']}")

        ret = rpc_client.get_bdevs()
        logger.info(f"SPDK BDevs count: {len(ret)}")
        # for bdev in ret:
        #     name = bdev['name']
        #     product_name = bdev['product_name']
        #     driver = ""
        #     for d in bdev['driver_specific']:
        #         driver = d
        #         break
        #     # logger.info(f"name: {name}, product_name: {product_name}, driver: {driver}")

        logger.info("getting device bdevs")
        # for dev in snode.nvme_devices:
        #     nvme_bdev = rpc_client.get_bdevs(dev.nvme_bdev)
        #     if snode.enable_test_device:
        #         testing_bdev = rpc_client.get_bdevs(dev.testing_bdev)
        #     alceml_bdev = rpc_client.get_bdevs(dev.alceml_bdev)
        #     pt_bdev = rpc_client.get_bdevs(dev.pt_bdev)

        #     subsystem = rpc_client.subsystem_list(dev.nvmf_nqn)

        # dev.testing_bdev = test_name
        # dev.alceml_bdev = alceml_name
        # dev.pt_bdev = pt_name
        # # nvme.nvmf_nqn = subsystem_nqn
        # # nvme.nvmf_ip = IP
        # # nvme.nvmf_port = 4420

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")

    try:
        logger.info("Connecting to node's API")
        node_info, _ = snode.client().info()
        logger.info(f"Node info: {node_info['hostname']}")

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")


def get_info(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    node_info, _ = snode.client().info()
    return json.dumps(node_info, indent=2)


def get_spdk_info(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    rpc_client = snode.rpc_client()
    ret = rpc_client.ultra21_util_get_malloc_stats()
    if not ret:
        logger.error(f"Failed to get SPDK info for node {node_id}")
        return False
    data = []
    for key in ret.keys():
        data.append({
            "Key": key,
            "Value": ret[key],
            "Parsed": utils.humanbytes(ret[key])
        })
    return utils.print_table(data)


def get(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    data = snode.get_clean_dict()
    return json.dumps(data, indent=2, sort_keys=True)


# States from which a node may legally transition INTO STATUS_ONLINE.
# Going online is the most consequential write in the state machine: it
# tells peers the node is serving IO. To prevent stale paths (e.g. the
# monitor briefly losing then regaining health checks) from re-flipping
# a half-broken node to ONLINE, the only legal predecessors are the
# transient "active operation in progress" states. Anything else is an
# illegal transition and rejected.
_ALLOWED_PRE_STATUSES_FOR_ONLINE = (
    StorageNode.STATUS_RESTARTING,
    StorageNode.STATUS_IN_CREATION,
    StorageNode.STATUS_SUSPENDED,
)


def set_node_status(node_id, status, caused_by="monitor"):
    """Write a status transition for the node. Pure bookkeeping: emits
    the event, broadcasts to peers, and (on ONLINE) cancels any pending
    auto-restart tasks for this node. Does NOT do peer connects, hublvol
    wiring, or device-event broadcasts — those are the caller's job
    (the restart impl, resume_storage_node, etc. all already do them
    before calling this function)."""
    from simplyblock_core.controllers import tasks_controller

    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if snode.status == status:
        return True

    if status == StorageNode.STATUS_ONLINE and snode.status not in _ALLOWED_PRE_STATUSES_FOR_ONLINE:
        # Hard reject: ONLINE may only be reached from RESTARTING (restart
        # path), IN_CREATION (add_node path), or SUSPENDED (resume path).
        # Other paths must route through one of those states first.
        logger.error(
            f"Refusing illegal status transition for {node_id}: "
            f"{snode.status} -> ONLINE. Only {_ALLOWED_PRE_STATUSES_FOR_ONLINE} -> ONLINE is allowed."
        )
        return False

    old_status = snode.status
    snode.status = status
    snode.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
    if status == StorageNode.STATUS_ONLINE:
        snode.online_since = str(datetime.datetime.now(datetime.timezone.utc))
    else:
        snode.online_since = ""
    snode.write_to_db(db_controller.kv_store)
    storage_events.snode_status_change(snode, snode.status, old_status, caused_by=caused_by)
    distr_controller.send_node_status_event(snode, status)

    if status == StorageNode.STATUS_ONLINE:
        # The node is back online; obsolete auto-restart tasks must not
        # linger in the queue, or the dedup guard in
        # _validate_new_task_node_restart blocks every subsequent restart
        # attempt until the task runner happens to pick the orphan up.
        try:
            tasks_controller.cancel_pending_node_restart_tasks(snode.cluster_id, node_id)
        except Exception as e:
            logger.error(f"Failed to cancel pending node_restart tasks for {node_id}: {e}")

    return True


def _set_restart_phase(snode, lvs_name, phase, db_controller):
    """Persist the restart phase for a given LVS to FDB.

    Other services check this to gate sync deletes and create/clone/
    resize/snapshot registrations. All non-empty phases are treated as
    "restart in progress" — operations during any of them are queued and
    applied after the phase is cleared:

    - pre_block     : restart task has claimed the LVS but hasn't blocked
                      the client port yet. The primary-side operation can
                      still run; however the restarting peer's SPDK state
                      is about to be torn down and rebuilt, so fanning
                      the operation out to it now would be lost. Queue it.
    - blocked       : client port blocked, examine + hublvol wiring in
                      flight. Queue.
    - post_unblock  : port unblocked, but the subsystem re-registration
                      loop is still running on the restarting node — an
                      nvmf_subsystem_add_ns for a concurrently-created
                      lvol would race a subsystem_create in the restart
                      flow. Queue until the phase is cleared.
    - ""            : not in restart.

    When transitioning out of any non-empty phase to a phase that implies
    "the restart task is done with the queue", the queue is drained: from
    BLOCKED → POST_UNBLOCK (once the rebuild owns the node) and from
    POST_UNBLOCK → "" (once the per-lvol subsystem re-registration has
    finished). Operations are applied in FIFO order.
    """
    node_id = snode.get_id()
    snode = db_controller.get_storage_node_by_id(node_id)
    old_phase = snode.restart_phases.get(lvs_name, "") if snode.restart_phases else ""
    if not snode.restart_phases:
        snode.restart_phases = {}
    if phase:
        snode.restart_phases[lvs_name] = phase
    elif lvs_name in snode.restart_phases:
        del snode.restart_phases[lvs_name]
    snode.write_to_db()
    logger.info("Restart phase for %s on %s: %s", lvs_name, node_id[:8], phase or "cleared")

    # Drain queued operations whenever the phase advances past a queue-gating
    # state. Two drain points, both drain the same FIFO queue:
    #   1. BLOCKED → POST_UNBLOCK: rebuild is done enough that RPCs won't
    #      race the examine. Drain so ops queued during pre_block+blocked
    #      can execute before clients resume.
    #   2. POST_UNBLOCK → "": subsystem re-registration has also finished.
    #      Drain any ops that arrived between the previous drain and now
    #      (e.g. a create submitted during post_unblock) so they don't
    #      hit a partially-initialized node.
    # The queue is popped on drain so a second drain on an empty queue is
    # a no-op.
    if old_phase == StorageNode.RESTART_PHASE_BLOCKED and phase == StorageNode.RESTART_PHASE_POST_UNBLOCK:
        drain_restart_queue(node_id, lvs_name)
    elif old_phase == StorageNode.RESTART_PHASE_POST_UNBLOCK and phase == "":
        drain_restart_queue(node_id, lvs_name)


def get_restart_phase(node_id, lvs_name):
    """Get the current restart phase for a node/LVS. Used by other services.

    Returns the phase string, or "" if not in restart.
    """
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
        return node.restart_phases.get(lvs_name, "")
    except (KeyError, Exception):
        return ""


def wait_or_delay_for_restart_gate(node_id, lvs_name, timeout=30):
    """Gate for sync deletes and create/clone/resize registrations.

    Any non-empty restart phase (pre_block / blocked / post_unblock)
    returns ``"delay"`` — the caller must queue the op via
    :func:`queue_for_restart_drain`. The queue drains automatically on
    the ``BLOCKED → POST_UNBLOCK`` and ``POST_UNBLOCK → ""`` transitions
    in :func:`_set_restart_phase`, so the op lands on the rebuilt node in
    FIFO order after per-lvol subsystem re-registration has completed.

    Why all three non-empty phases delay:

    - ``pre_block``: restart task has claimed the LVS. The node's SPDK
      state is about to be torn down / rebuilt; applying a metadata op
      now would be lost by the rebuild.
    - ``blocked``: client port blocked, examine in flight. Applying a
      create/delete now can race examine's read of the primary's
      blobstore.
    - ``post_unblock``: client port unblocked but the per-lvol subsystem
      re-registration loop on the restarting node is still running.
      ``nvmf_subsystem_add_ns`` from a mgmt-side create would race the
      restart's own ``subsystem_create``.

    Normal (healthy) case: phase is empty → returns ``"proceed"``
    immediately. Operations execute in ms.
    """
    phase = get_restart_phase(node_id, lvs_name)
    if phase in (StorageNode.RESTART_PHASE_PRE_BLOCK,
                 StorageNode.RESTART_PHASE_BLOCKED,
                 StorageNode.RESTART_PHASE_POST_UNBLOCK):
        return "delay"
    return "proceed"


# Per-node ordered queue for operations delayed during port block.
# Key: (node_id, lvs_name), Value: list of (callable, description) in FIFO order.
_restart_op_queues: dict[tuple[str, str], list[tuple]] = {}
_restart_op_queues_lock = threading.Lock()


def queue_for_restart_drain(node_id, lvs_name, operation_fn, description=""):
    """Queue an operation for execution after port unblock.

    Called when wait_or_delay_for_restart_gate returns "delay".
    Operations are appended in order and will be drained sequentially
    by drain_restart_queue() after phase transitions to post_unblock.

    Args:
        node_id: target node
        lvs_name: LVS being restarted
        operation_fn: callable() that performs the actual RPC
        description: human-readable description for logging
    """
    key = (node_id, lvs_name)
    with _restart_op_queues_lock:
        if key not in _restart_op_queues:
            _restart_op_queues[key] = []
        _restart_op_queues[key].append((operation_fn, description))
    logger.info("Queued operation for post-unblock drain on %s/%s: %s",
                node_id[:8], lvs_name, description)


def drain_restart_queue(node_id, lvs_name):
    """Drain all queued operations for a node/LVS after port unblock.

    Called by the restart code after phase transitions to post_unblock.
    Executes operations in strict FIFO order, single-threaded.
    """
    key = (node_id, lvs_name)
    with _restart_op_queues_lock:
        queue = _restart_op_queues.pop(key, [])

    if not queue:
        return

    logger.info("Draining %d queued operations for %s/%s", len(queue), node_id[:8], lvs_name)
    for operation_fn, description in queue:
        try:
            logger.info("Executing queued operation: %s", description)
            operation_fn()
        except Exception as e:
            logger.error("Queued operation failed (%s): %s", description, e)


def _is_node_rpc_responsive(node, lvs_name, timeout=5, retry=2):
    """Check if a node's RPC interface is responsive.

    Returns True if RPC succeeds, False if it fails/times out.
    RPC is considered failing if it returns an error code or times out
    beyond the defined retries.
    """
    try:
        rpc = node.rpc_client(timeout=timeout, retry=retry)
        ret = rpc.bdev_lvol_get_lvstores(lvs_name)
        return ret is not None
    except Exception:
        return False


def _is_fabric_connected(node, lvs_peer_ids=None):
    """Check if a node's fabric is connected (JM quorum says NOT disconnected)."""
    return not _check_peer_disconnected(node, lvs_peer_ids=lvs_peer_ids)


def _count_fabric_disconnected_nodes(all_nodes, lvs_peer_ids=None):
    """Count how many nodes have disconnected fabric."""
    count = 0
    for n in all_nodes:
        if _check_peer_disconnected(n, lvs_peer_ids=lvs_peer_ids):
            count += 1
    return count


def find_leader_with_failover(all_nodes, lvs_name):
    """Detect the current leader and failover if needed.

    1. Try each node as leader via bdev_lvol_get_lvstores (leadership field)
    2. If leader's RPC is responsive → return it
    3. If leader's RPC times out BUT fabric is healthy:
       - Check if at least one non-leader has healthy fabric
       - If yes → force leadership change, return the new leader
       - If no → return None (reject)
    4. If no leader found → return first fabric-connected node as fallback

    Returns:
        (leader_node, non_leader_nodes) or (None, []) if all unreachable.
    """
    from simplyblock_core.controllers.lvol_controller import is_node_leader

    leader = None
    non_leaders = []

    # Find current leader
    for node in all_nodes:
        try:
            if is_node_leader(node, lvs_name):
                leader = node
                break
        except Exception:
            continue

    if leader is None:
        # No leader found via RPC — find first fabric-connected node
        for node in all_nodes:
            if _is_fabric_connected(node):
                leader = node
                break
        if leader is None:
            return None, []

    non_leaders = [n for n in all_nodes if n.get_id() != leader.get_id()]

    # Check if leader's RPC is responsive
    if _is_node_rpc_responsive(leader, lvs_name):
        return leader, non_leaders

    # Leader RPC failing — check if fabric is healthy
    if not _is_fabric_connected(leader):
        # Fabric disconnected — leader truly down, find new leader
        for nl in non_leaders:
            if _is_fabric_connected(nl) and _is_node_rpc_responsive(nl, lvs_name):
                logger.info("Leader %s fabric disconnected, failing over to %s",
                            leader.get_id(), nl.get_id())
                new_non_leaders = [n for n in all_nodes if n.get_id() != nl.get_id()]
                return nl, new_non_leaders
        return None, []

    # Leader fabric healthy but RPC failing — force leadership change
    # Need at least one non-leader with healthy fabric
    failover_target = None
    for nl in non_leaders:
        if _is_fabric_connected(nl) and _is_node_rpc_responsive(nl, lvs_name):
            failover_target = nl
            break

    if failover_target is None:
        logger.error("Leader %s RPC failing, fabric healthy, but no non-leader available for failover",
                     leader.get_id())
        return None, []

    # Force leadership change via fabric signal: send bdev_lvol_set_lvs_signal
    # FROM failover_target through the fabric TO the leader (whose mgmt is down
    # but data plane is healthy). The signal tells the leader's SPDK to drop
    # leadership for this LVS.
    try:
        rpc = failover_target.rpc_client(timeout=5, retry=2)
        rpc.bdev_lvol_set_lvs_signal(lvs_name)
        time.sleep(2)
        logger.info("Sent bdev_lvol_set_lvs_signal(%s) from %s to leader %s via fabric",
                    lvs_name, failover_target.get_id(), leader.get_id())
    except Exception as e:
        logger.error("Failed to send fabric signal for leadership change: %s", e)
        return None, []

    new_non_leaders = [n for n in all_nodes if n.get_id() != failover_target.get_id()]
    return failover_target, new_non_leaders


def check_non_leader_for_operation(node_id, lvs_name, operation_type="create",
                                    leader_op_completed=False, all_nodes=None):
    """Check a non-leader node's readiness for a sync operation.

    Args:
        node_id: the non-leader node to check
        lvs_name: the LVS name
        operation_type: "create" (create/clone/resize) or "delete"
        leader_op_completed: True if the operation was already executed on leader
        all_nodes: all nodes in the LVS group (for FTT check)

    Returns:
        "proceed" — execute now
        "skip" — disconnected, skip
        "reject" — unreachable+fabric healthy; reject entire operation
        "queue" — restart port blocked OR need to queue for retry
        "kill_and_wait" — kill node and wait for restart (FTT allows)
    """
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return "skip"

    # 1. Check disconnect state (JM quorum)
    lvs_peer_ids = [sid for sid in [node.secondary_node_id, node.tertiary_node_id] if sid]
    if _check_peer_disconnected(node, lvs_peer_ids=lvs_peer_ids):
        return "skip"

    # 2. Check restart phase — any non-empty phase means the restart task
    # owns the node's LVS state and the operation must be queued for the
    # post-rebuild drain. "skip" (the old pre_block behaviour) is incorrect
    # because the primary-side op runs unaffected by the LVS port block
    # (its mgmt RPC goes to port 8085, not 4436), so a pre_block skip can
    # lose a create/delete on the restarting node. See
    # _set_restart_phase for the drain timing.
    phase = get_restart_phase(node_id, lvs_name)
    if phase in (StorageNode.RESTART_PHASE_PRE_BLOCK,
                 StorageNode.RESTART_PHASE_BLOCKED,
                 StorageNode.RESTART_PHASE_POST_UNBLOCK):
        return "queue"

    # 3. Fabric is connected — check RPC responsiveness
    if _is_node_rpc_responsive(node, lvs_name):
        return "proceed"

    # 4. RPC failing but fabric connected
    logger.warning("Non-leader %s RPC failing but fabric connected", node_id[:8])

    # Check FTT — can we tolerate this node being unresponsive?
    if all_nodes:
        cluster = db_controller.get_cluster_by_id(node.cluster_id)
        max_ft = getattr(cluster, 'max_fault_tolerance', 1)
        disconnected_count = _count_fabric_disconnected_nodes(all_nodes, lvs_peer_ids)
        if disconnected_count + 1 > max_ft:
            # FTT would be violated — cannot proceed or kill
            if not leader_op_completed:
                logger.warning("Non-leader %s RPC failing, FTT would be violated "
                              "(disconnected=%d, max_ft=%d) — rejecting before leader op",
                              node_id[:8], disconnected_count, max_ft)
                return "reject"
            logger.warning("Cannot kill node %s: would violate FTT (disconnected=%d, max_ft=%d)",
                          node_id[:8], disconnected_count, max_ft)
            return "queue"

        if not leader_op_completed:
            # FTT allows — queue the registration for this non-leader and
            # let the leader operation proceed. The non-leader's
            # registration will be retried once it becomes RPC-responsive.
            logger.info("Non-leader %s RPC failing but FTT tolerates it "
                       "(disconnected=%d, max_ft=%d) — queueing, leader op can proceed",
                       node_id[:8], disconnected_count, max_ft)
            return "queue"

        # AFTER leader operation: FTT allows — kill node, wait for restart
        logger.info("Killing node %s (FTT allows: disconnected=%d, max_ft=%d)",
                    node_id[:8], disconnected_count, max_ft)
        return "kill_and_wait"

    # No all_nodes provided — safe default: queue
    return "queue"


def execute_on_leader_with_failover(all_nodes, lvs_name, operation_fn):
    """Execute an operation on the current leader with failover support.

    1. Find leader (with failover if needed)
    2. Execute operation_fn(leader_node)
    3. If operation fails, re-check leadership and retry on new leader
    4. Return (success, leader_node, result)

    Args:
        all_nodes: list of all StorageNode objects in the LVS group
        lvs_name: LVS name
        operation_fn: callable(leader_node) → result. Returns None/False on failure.

    Returns:
        (True, leader_node, result) on success
        (False, None, error_msg) on failure
    """
    leader, non_leaders = find_leader_with_failover(all_nodes, lvs_name)
    if leader is None:
        return False, None, "No leader available"

    # Execute on leader
    try:
        result = operation_fn(leader)
        if result is not None and result is not False:
            return True, leader, result
    except Exception as e:
        logger.warning("Operation failed on leader %s: %s — re-checking leadership",
                      leader.get_id(), e)

    # Operation failed — re-check leadership
    new_leader, _ = find_leader_with_failover(all_nodes, lvs_name)
    if new_leader is None:
        return False, None, "Operation failed and no leader available"

    if new_leader.get_id() == leader.get_id():
        # Same leader, operation truly failed
        return False, leader, "Operation failed on leader"

    # Leadership changed — retry on new leader
    logger.info("Leadership changed from %s to %s, retrying operation",
               leader.get_id(), new_leader.get_id())
    try:
        result = operation_fn(new_leader)
        if result is not None and result is not False:
            return True, new_leader, result
        return False, new_leader, "Operation failed on new leader"
    except Exception as e:
        return False, new_leader, f"Operation failed on new leader: {e}"


def _check_peer_disconnected(peer_node, lvs_peer_ids=None):
    """Check if a peer node should be treated as disconnected for the purpose
    of routing (takeover vs. non-leader path) and peer-port-block decisions.

    Returns True if peer is disconnected (should be skipped), False otherwise.

    Two signals, first match wins:

      1. Mgmt ground truth (FDB status). If FDB already says the peer is
         OFFLINE / REMOVED / UNREACHABLE, trust it immediately — mgmt has
         observed the peer leaving the cluster. Attempting to port-block
         such a peer's mgmt API will only hit ECONNREFUSED and, after 5×
         retries, abort the entire restart with a misleading "LVStore
         recovery failed" event. IN_SHUTDOWN / RESTARTING are deliberately
         NOT in this list — those are transient states the runner owns;
         preempting another node's leadership during its own restart
         would be incorrect.

      2. Data-plane JM quorum (legacy path). Only reached if mgmt says
         the peer is in an "alive" state. Useful to detect fabric
         partitions where mgmt is still reachable but the data plane
         isn't — the quorum reads NVMe controller state on surviving
         peers (see storage_node_monitor::_count_data_plane_votes).
    """
    from simplyblock_core.services.storage_node_monitor import is_node_data_plane_disconnected_quorum

    if peer_node.status in (StorageNode.STATUS_OFFLINE,
                            StorageNode.STATUS_REMOVED,
                            StorageNode.STATUS_UNREACHABLE):
        logger.info("Peer %s mgmt status is %s — treating as disconnected",
                    peer_node.get_id(), peer_node.status)
        return True

    if is_node_data_plane_disconnected_quorum(peer_node, lvs_peer_ids=lvs_peer_ids):
        logger.info("Peer %s is data-plane disconnected (NVMe-ctrlr quorum confirmed), will skip",
                     peer_node.get_id())
        return True

    logger.info("Peer %s is data-plane connected (NVMe-ctrlr quorum check)", peer_node.get_id())
    return False


def _check_hublvol_connected(snode, peer_node):
    """Method 2: Check if the hublvol to peer_node is still connected from snode.

    Per design: used as fallback when RPCs fail/timeout after the quorum check
    said the node was connected.
    - If hublvol IS connected: only management plane unreachable
    - If hublvol is NOT connected: node truly disconnected from fabric

    Returns True if hublvol is connected, False if disconnected.
    """
    try:
        rpc_client = snode.rpc_client(timeout=5, retry=1)
        if peer_node.hublvol and peer_node.hublvol.bdev_name:
            remote_bdev = f"{peer_node.hublvol.bdev_name}n1"
            bdevs = rpc_client.get_bdevs(remote_bdev)
            if bdevs:
                logger.info("HubLVol to %s is still connected from %s",
                            peer_node.get_id(), snode.get_id())
                return True
        logger.info("HubLVol to %s is NOT connected from %s",
                    peer_node.get_id(), snode.get_id())
        return False
    except Exception as e:
        logger.warning("Failed to check hublvol connection to %s: %s", peer_node.get_id(), e)
        return False


def _handle_rpc_failure_on_peer(snode, peer_node, lvs_jm_vuid, lvs_name=None):
    """Handle RPC failure to a peer during restart, per design decision tree.

    Called when RPCs to a previously-connected peer fail/timeout.

    Per design:
    Step 1: Check if hublvol to this node is still connected
      - If NOT connected → node is fabric-disconnected, skip it
      - If connected → only mgmt plane unreachable, go to step 2
    Step 2: Check if unreachable node is leader
      - If NOT leader → skip that node
      - If IS leader → send ``bdev_lvol_set_lvs_signal`` from snode through
        the fabric to the peer. This tells the peer's SPDK to drop
        leadership for the given LVS. Only relevant when the peer's data
        plane is healthy (hublvol connected). Wait 2 seconds for the
        signal to take effect, then continue.

    Returns:
        "skip" - node can be safely skipped
        "leader_dropped" - leadership was dropped via fabric, can continue
        "abort" - must abort restart (fabric connected but signal failed)
    """
    if not _check_hublvol_connected(snode, peer_node):
        logger.info("Peer %s hublvol disconnected after RPC failure, skipping", peer_node.get_id())
        return "skip"

    # Hublvol is connected — only mgmt plane is down, data plane healthy.
    # Send a fabric-level signal FROM snode TO the peer to drop leadership.
    if not lvs_name:
        logger.error("_handle_rpc_failure_on_peer: lvs_name required for fabric signal")
        return "abort"
    try:
        rpc_client = snode.rpc_client(timeout=5, retry=1)
        ret = rpc_client.bdev_lvol_set_lvs_signal(lvs_name)
        if ret:
            logger.info("Sent bdev_lvol_set_lvs_signal(%s) from %s to peer %s via fabric, waiting 2s",
                        lvs_name, snode.get_id(), peer_node.get_id())
            time.sleep(2)
            return "leader_dropped"
        else:
            logger.info("bdev_lvol_set_lvs_signal(%s) returned False — peer %s may not be leader, skipping",
                        lvs_name, peer_node.get_id())
            return "skip"
    except Exception as e:
        logger.error("Failed to send fabric signal to peer %s for LVS %s: %s — aborting restart",
                     peer_node.get_id(), lvs_name, e)
        return "abort"


def recreate_lvstore_on_non_leader(snode, leader_node, primary_node, activation_mode=False, force=False):
    """Recreate a non-leader LVS on snode.

    Per design: runs for secondary when primary is online, or for tertiary always.
    While snode examines its raid, the current leader must be quiesced:
    block the leader's port only, demote its lvs leadership, drain inflight
    IO, then examine. Non-leader peers (siblings) are never port-blocked.

    During the port-blocked window, all RPCs to the leader use timeout=0.2s
    with no retries. Any RPC failure in this window triggers an abort: kill
    the restarting SPDK, set node offline, unblock the leader port, raise.

    Args:
        snode: the restarting node (RPCs are executed here)
        leader_node: whoever currently leads this LVS
        primary_node: the original primary (for lvol list, lvstore name, etc.)
        activation_mode: when True, skip all peer operations (port blocking,
            hublvol creation/connection, leader demotion).  Used during
            cluster_activate() where not all LVS are ready yet.
    """
    db_controller = DBController()
    snode_rpc_client = snode.rpc_client()

    if activation_mode:
        # Soft prelude: reconnect any missing remote devices + remote JMs
        # before touching the LVS stack. Both helpers iterate existing bdevs
        # internally and no-op on controllers that are already attached.
        try:
            fresh_remote_devs = _connect_to_remote_devs(snode, reattach=False)
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.remote_devices = fresh_remote_devs or snode.remote_devices
            snode.write_to_db()
        except Exception as e:
            logger.warning("Soft reconnect of remote devices failed on %s: %s",
                           snode.get_id(), e)
        try:
            fresh_remote_jms = _connect_to_remote_jm_devs(snode)
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.remote_jm_devices = fresh_remote_jms or snode.remote_jm_devices
            snode.write_to_db()
        except Exception as e:
            logger.warning("Soft reconnect of remote JMs failed on %s: %s",
                           snode.get_id(), e)

    # Ensure snode has per-lvstore ports from primary
    lvstore_ports = {snode.lvstore: {
        "lvol_subsys_port": snode.lvol_subsys_port,
        "hublvol_port": snode.hublvol.nvmf_port,
    }}
    if snode.lvstore_stack_secondary:
        nd = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
        lvstore_ports[nd.lvstore] = {
            "lvol_subsys_port": nd.lvol_subsys_port,
            "hublvol_port": nd.hublvol.nvmf_port,
        }
    if snode.lvstore_stack_tertiary:
        nd = db_controller.get_storage_node_by_id(snode.lvstore_stack_tertiary)
        lvstore_ports[nd.lvstore] = {
            "lvol_subsys_port": nd.lvol_subsys_port,
            "hublvol_port": nd.hublvol.nvmf_port,
        }
    snode.lvstore_ports = lvstore_ports
    snode.write_to_db()

    lvol_list = []
    for lv in db_controller.get_lvols_by_node_id(primary_node.get_id()):
        if lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_IN_CREATION]:
            lvol_list.append(lv)

    ### 1- create distribs and raid
    # Set restart phase: pre_block — sync deletes and registrations can still complete.
    # IMPORTANT: every exit path after this point MUST clear the phase (either by
    # reaching the normal clear at the end, or via the except/finally below).
    # A stale pre_block causes check_non_leader_for_operation to return "skip"
    # for this LVS indefinitely, silently blocking all new volume subsystem
    # creation on this node.
    _set_restart_phase(snode, primary_node.lvstore, StorageNode.RESTART_PHASE_PRE_BLOCK, db_controller)

    ret, err = _create_bdev_stack(snode, primary_node.lvstore_stack, primary_node=primary_node)
    if err:
        logger.error(f"Failed to recreate non-leader lvstore on node {snode.get_id()}")
        logger.error(err)
        _set_restart_phase(snode, primary_node.lvstore, "", db_controller)
        primary_node.lvstore_status = "ready"
        primary_node.write_to_db()
        return False

    try:
        snode.connect_to_hublvol(primary_node, failover_node=None)
    except Exception as e:
        logger.error("Error establishing hublvol: %s", e)
        # return False

    # Resume JC compression for this LVS group on the restarting node
    ret, err = snode.rpc_client().jc_suspend_compression(jm_vuid=primary_node.jm_vuid, suspend=False)
    if not ret:
        logger.info("Failed to resume JC compression adding task...")
        tasks_controller.add_jc_comp_resume_task(
            snode.cluster_id, snode.get_id(), jm_vuid=primary_node.jm_vuid)

    ### 2- create lvols nvmf subsystems (idempotent: skip existing)
    is_tertiary = (primary_node.tertiary_node_id == snode.get_id())
    min_cntlid = 2000 if is_tertiary else 1000
    for lvol in lvol_list:
        allow_any = not bool(lvol.allowed_hosts)
        if _rpc_subsystem_exists(snode_rpc_client, lvol.nqn):
            logger.info("subsystem %s already exists on %s, skipping create",
                        lvol.nqn, snode.get_id())
        else:
            logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
            snode_rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                              max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS,
                                              allow_any_host=allow_any)
        if lvol.allowed_hosts:
            _reapply_allowed_hosts(lvol, snode, snode_rpc_client)

    port_type = "tcp"
    if leader_node.active_rdma:
        port_type = "udp"
    leader_lvs_port = primary_node.get_lvol_subsys_port(primary_node.lvstore)

    logger.info(f"[RESTART] Non-leader for {primary_node.lvstore} on {snode.get_id()[:8]}, "
                f"leader={leader_node.get_id()[:8]}, is_tert={is_tertiary}")

    # Set restart phase: blocked — sync deletes and registrations must be delayed until post_unblock
    _set_restart_phase(snode, primary_node.lvstore, StorageNode.RESTART_PHASE_BLOCKED, db_controller)

    # Resolve the secondary node for tertiary→secondary hublvol fallback
    secondary_node = None
    if primary_node.secondary_node_id and primary_node.secondary_node_id != snode.get_id():
        secondary_node = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)

    leader_port_blocked = False

    def _abort_and_unblock(reason):
        """Abort restart: kill SPDK on snode, set offline, unblock leader port, raise."""
        logger.error("Aborting non-leader restart on %s for %s: %s",
                     snode.get_id(), primary_node.lvstore, reason)
        try:
            storage_events.snode_restart_failed(snode)
            snode_api = snode.client(timeout=5, retry=5)
            snode_api.spdk_process_kill(snode.rpc_port, snode.cluster_id)
        except Exception as ke:
            logger.error("Failed to kill SPDK during abort: %s", ke)
        set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE)
        if leader_port_blocked:
            try:
                fw_api = FirewallClient(leader_node, timeout=5, retry=2)
                fw_api.firewall_set_port(leader_lvs_port, port_type, "allow", leader_node.rpc_port)
                tcp_ports_events.port_allowed(leader_node, leader_lvs_port)
            except Exception as ue:
                logger.error("Failed to unblock leader port during abort: %s", ue)
        _set_restart_phase(snode, primary_node.lvstore, "", db_controller)
        raise Exception(f"Abort non-leader restart: {reason}")

    # Quorum check on the current leader ONLY. Use a peer list that excludes the
    # restarting node (snode) — snode's JM is expected to be disconnected on peers
    # during restart, so including it would cause false negatives.
    lvs_peer_ids_excl_snode = [sid for sid in [primary_node.secondary_node_id, primary_node.tertiary_node_id]
                               if sid and sid != snode.get_id()]
    leader_has_quorum = not _check_peer_disconnected(leader_node, lvs_peer_ids=lvs_peer_ids_excl_snode)

    if not activation_mode and leader_has_quorum:
        ### 3- block leader port ONLY (no siblings)
        # Blocking the leader's LVS port is what quiesces its IO so this
        # restarting node can safely examine its raid0 without a write
        # racing into a half-reconstructed lvstore. Silently skipping the
        # block (as we used to do on ConnectionRefused) lets the leader
        # keep serving reads/writes while we examine — which has produced
        # CRC mismatches and lvol drops on the restarting peer. So retry,
        # and if it still can't land, abort the restart unless force=True.
        #
        # Budget: 3 attempts × FirewallClient(timeout=3, retry=1) × 1s sleep
        # between attempts → worst-case ~15s abort. Previously 5× ×
        # (timeout=5, retry=5) × 2s = ~140s, which made every iteration
        # against a dead-mgmt leader stall the restart task for minutes.
        # The FDB-status short-circuit in _check_peer_disconnected should
        # already route such peers to the takeover path before we reach
        # here; keeping a short local budget protects against stragglers.
        last_err = None
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                fw_api = FirewallClient(leader_node, timeout=3, retry=1)
                fw_api.firewall_set_port(leader_lvs_port, port_type, "block", leader_node.rpc_port)
                tcp_ports_events.port_deny(leader_node, leader_lvs_port)
                leader_port_blocked = True
                last_err = None
                break
            except Exception as e:
                last_err = e
                logger.warning(
                    "Port-block attempt %d/%d failed for leader %s on %s: %s",
                    attempt, attempts, leader_node.get_id(), primary_node.lvstore, e)
                if attempt < attempts:
                    time.sleep(1)
        if not leader_port_blocked:
            msg = (f"Failed to block leader {leader_node.get_id()} port "
                   f"{leader_lvs_port} after {attempts} attempts for "
                   f"{primary_node.lvstore}: {last_err}")
            if force:
                logger.warning(
                    "%s — force=True: proceeding without leader port block; "
                    "this allows leader-vs-restarter writes to race during "
                    "examine and can corrupt the rebuilt lvstore", msg)
            else:
                _abort_and_unblock(msg)

    if not activation_mode and leader_port_blocked:
        # Fixed 0.5s quiesce window instead of draining distrib-inflight.
        # bdev_distrib_check_inflight_io counts internal distrib IO (including
        # data-migration moves) which the port block does not pause, so polling
        # for it to hit zero can hold the leader's lvol port blocked long
        # enough to breach client fio max_latency. Migration IO does not touch
        # lvstore metadata, so a brief fixed wait is sufficient for the
        # secondary's examine to see a consistent superblock.
        time.sleep(0.5)

    elif not activation_mode and not leader_has_quorum:
        logger.info("Leader %s has no quorum for %s, skipping port block",
                    leader_node.get_id(), primary_node.lvstore)

    ### 4- examine (idempotent: skip only when raid AND lvstore already surfaced)
    raid_already = _rpc_bdev_exists(snode_rpc_client, primary_node.raid)
    lvstore_already = _rpc_lvstore_exists(snode_rpc_client, primary_node.lvstore)
    if raid_already and lvstore_already:
        logger.info(
            "Raid %s and lvstore %s already present on %s; skipping examine",
            primary_node.raid, primary_node.lvstore, snode.get_id())
    else:
        # Examine is required whenever the lvstore isn't surfaced — whether
        # the raid was freshly created by _create_bdev_stack (normal restart
        # path) or pre-existing with stale state (activation retry).
        snode_rpc_client.bdev_examine(primary_node.raid)

        ### 5- wait for examine
        ret = snode_rpc_client.bdev_wait_for_examine()
        if not ret:
            logger.warning("Failed to examine bdevs on non-leader node")

        # After examine, the lvstore MUST be present. If it isn't, SPDK
        # failed to rediscover the lvstore from its persisted metadata
        # (e.g. partial stack components left over, corrupt on-disk state).
        # During activation we can't safely recover — signal the caller
        # to reject the activation and ask for a restart of this node.
        if activation_mode and not _rpc_lvstore_exists(snode_rpc_client, primary_node.lvstore):
            raise LVSRestartRequiredError(
                snode.get_id(), primary_node.lvstore,
                detail=f"raid={primary_node.raid} present but lvstore did not recover"
                if raid_already else "examine did not produce lvstore")

    # Verify that examine actually rediscovered the lvstore and every lvol
    # the FDB expects to be present on this node. Mirrors the check in
    # recreate_lvstore() for the primary path. If an lvol blob did not
    # become durable on this peer's shard of raid0 before it was torn down
    # (e.g. the blob was committed on the primary/tertiary quorum but this
    # node missed the write window due to a simultaneous force-shutdown),
    # the examine won't surface it. Continuing would leave the lvol
    # subsystem bound without a namespace on this node — present on
    # primary/tertiary, missing here — and the divergence would never be
    # reconciled because there is no FDB↔SPDK lvol-set reconcile loop.
    if not activation_mode:
        if not snode_rpc_client.bdev_lvol_get_lvstores(primary_node.lvstore):
            logger.error(
                "Failed to recover lvstore %s on %s after examine",
                primary_node.lvstore, snode.get_id())
            if not force:
                _abort_and_unblock(
                    f"lvstore {primary_node.lvstore} did not recover after examine "
                    f"on non-leader {snode.get_id()}")

        registered_bdevs = snode_rpc_client.get_bdevs() or []
        bdev_names: set = set()
        for b in registered_bdevs:
            name = b.get('name')
            if name:
                bdev_names.add(name)
            for alias in (b.get('aliases') or []):
                bdev_names.add(alias)

        missing_lvols = []
        for lv in lvol_list:
            base_bdev_name = f"{lv.lvs_name}/{lv.lvol_bdev}"
            if lv.lvol_uuid in bdev_names or base_bdev_name in bdev_names:
                continue
            missing_lvols.append(lv)

        if missing_lvols:
            missing_repr = ", ".join(
                f"{lv.lvs_name}/{lv.lvol_bdev}(uuid={lv.lvol_uuid[:8]})"
                for lv in missing_lvols)
            logger.error(
                "Expected lvol bdevs missing on %s for %s after examine: %s",
                snode.get_id(), primary_node.lvstore, missing_repr)
            if not force:
                _abort_and_unblock(
                    f"Expected lvols not registered on {snode.get_id()} after "
                    f"examine of {primary_node.raid}: {missing_repr}. "
                    f"Re-run restart with force=True to proceed anyway "
                    f"(this peer will not serve these lvols).")
            else:
                logger.warning(
                    "force=True: proceeding with %d missing lvol(s) on %s for %s; "
                    "these lvols will not be served by this peer",
                    len(missing_lvols), snode.get_id(), primary_node.lvstore)

    # bdev_examine brings the LVS back with its metadata-persisted role
    # (primary). Leaving it as primary makes SPDK reject a later
    # bdev_lvol_connect_hublvol with "-22 nonsecondary node".
    sec_role = "tertiary" if is_tertiary else "secondary"
    if not snode_rpc_client.bdev_lvol_set_lvs_opts(
            primary_node.lvstore,
            groupid=primary_node.jm_vuid,
            subsystem_port=primary_node.get_lvol_subsys_port(primary_node.lvstore),
            role=sec_role,
    ):
        logger.error("bdev_lvol_set_lvs_opts(%s) failed for %s on %s",
                     sec_role, primary_node.lvstore, snode.get_id())

    # Track the deferred failover-path attach so we can run it AFTER the
    # leader port is unblocked. The in-freeze attach below uses a single
    # path only; the second path (if any) is reconciled out-of-band so the
    # 3 s INTER_ATTACH_SLEEP_SEC inside the coordinator never sits inside
    # the IO-impact window.
    deferred_failover_target = None
    deferred_failover_via = None

    if not activation_mode:
        ### 6- create hublvol on secondary (non-leader) for multipath failover
        # Secondary creates its own hublvol so the tertiary can use it as a failover path.
        if not is_tertiary:
            try:
                cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                snode.create_secondary_hublvol(leader_node, cluster.nqn)
                logger.info("Created secondary hublvol on restarting node %s for %s",
                            snode.get_id(), primary_node.lvstore)
            except Exception as e:
                logger.error("Error creating secondary hublvol on restarting node: %s", e)

        ### 7- single-path hublvol attach inside the freeze
        # Pre-flight reachability up front: the second path must NEVER be
        # attempted inside the leader-port-block window, and the single
        # attached path must target a known-alive peer. If the original
        # leader is offline (no quorum) we attach directly to the secondary
        # — the dead leader's IP is not even tried.
        try:
            if is_tertiary:
                secondary_alive = (secondary_node and not _check_peer_disconnected(
                    secondary_node, lvs_peer_ids=lvs_peer_ids_excl_snode))
                # leader_has_quorum was computed earlier (line ~4722). When
                # the leader has lost quorum, attaching to it would burn up
                # to fast_io_fail_timeout_sec (5s) inside the freeze.
                if leader_has_quorum:
                    sync_target = leader_node
                    if (secondary_alive and secondary_node is not None
                            and secondary_node.get_id() != leader_node.get_id()):
                        deferred_failover_target = secondary_node
                        deferred_failover_via = leader_node
                elif secondary_alive and secondary_node is not None and secondary_node.hublvol:
                    logger.info("Leader %s offline (no quorum); tertiary %s "
                                "connecting directly to secondary %s hublvol for %s",
                                leader_node.get_id(), snode.get_id(),
                                secondary_node.get_id(), primary_node.lvstore)
                    sync_target = secondary_node
                    # No deferred path: there is no live alternative peer
                    # to add as a failover. Once the original leader comes
                    # back online, its periodic hublvol reconciliation will
                    # add it as a path.
                else:
                    sync_target = None
                    logger.error(
                        "Tertiary %s rejoin %s: no reachable hublvol target "
                        "(leader=%s alive=%s, secondary alive=%s)",
                        snode.get_id(), primary_node.lvstore,
                        leader_node.get_id(), leader_has_quorum, secondary_alive)

                if sync_target is not None:
                    # Pass lvs_node=primary_node so LVS metadata (lvstore
                    # name, jm_vuid, port, hublvol NQN/bdev) comes from
                    # the configured primary of the LVS being recreated,
                    # *not* from sync_target — when the configured
                    # primary is offline and sync_target is a peer that
                    # took over leadership, sync_target.hublvol points at
                    # sync_target's OWN primary-LVS, which is the wrong
                    # LVS for our connection. (incident 2026-05-02
                    # 15:53:42: tertiary worker1 connecting through
                    # acting-leader worker5 for LVS_6207 set
                    # groupid=4729 — worker5's own primary — instead
                    # of 6207.)
                    snode.connect_to_hublvol(sync_target, failover_node=None,
                                             role=sec_role, rpc_timeout=0.2,
                                             lvs_node=primary_node)
            else:
                # Secondary: connect to leader (primary) hublvol — single path,
                # short timeout, no deferred failover (secondaries don't carry one).
                # Same lvs_node=primary_node rationale as the tertiary
                # branch above: when ``leader_node`` is a peer that took
                # over leadership of ``primary_node.lvstore``, its OWN
                # hublvol metadata would label the connection with the
                # wrong LVS.
                snode.connect_to_hublvol(leader_node, failover_node=None,
                                         role=sec_role, rpc_timeout=0.2,
                                         lvs_node=primary_node)
        except Exception as e:
            logger.error("Error connecting to hublvol: %s", e)

        ### 8- unblock leader port
        # If we blocked it, we MUST unblock — a stuck-blocked leader can't
        # serve client IO on that LVS. Retry until it lands; schedule a
        # port_allow task as a fallback if we still can't reach the leader
        # after our attempts so another retry loop keeps trying.
        if leader_port_blocked:
            unblocked = False
            attempts = 3
            for attempt in range(1, attempts + 1):
                try:
                    fw_api = FirewallClient(leader_node, timeout=3, retry=1)
                    fw_api.firewall_set_port(leader_lvs_port, port_type, "allow", leader_node.rpc_port)
                    tcp_ports_events.port_allowed(leader_node, leader_lvs_port)
                    unblocked = True
                    break
                except Exception as e:
                    logger.warning(
                        "Port-unblock attempt %d/%d failed for leader %s on %s: %s",
                        attempt, attempts, leader_node.get_id(), primary_node.lvstore, e)
                    if attempt < attempts:
                        time.sleep(1)
            if not unblocked:
                logger.error(
                    "Failed to unblock leader %s port %s for %s after %d attempts; "
                    "scheduling port_allow task",
                    leader_node.get_id(), leader_lvs_port, primary_node.lvstore, attempts)
                try:
                    tasks_controller.add_port_allow_task(
                        leader_node.cluster_id, leader_node.get_id(), leader_lvs_port)
                except Exception as sched_exc:
                    logger.error("Failed to schedule port_allow fallback: %s", sched_exc)
            leader_port_blocked = False

    # Set restart phase: post_unblock — delayed sync deletes and registrations can now proceed
    _set_restart_phase(snode, primary_node.lvstore, StorageNode.RESTART_PHASE_POST_UNBLOCK, db_controller)

    ### 8b- deferred failover-path attach (tertiary only, leader was alive)
    # The in-freeze attach above used a single path. Now that the leader
    # port is unblocked and IO is flowing again, top up the second path on
    # the multipath hublvol controller so a future primary loss has an
    # immediate failover. The coordinator's INTER_ATTACH_SLEEP_SEC (3 s)
    # cost lives here, OUTSIDE the IO-impact window — it doesn't sit inside
    # the leader-port-block freeze any more, so client IO is unaffected.
    if deferred_failover_target is not None and deferred_failover_via is not None:
        try:
            if snode.add_hublvol_failover_path(deferred_failover_via, deferred_failover_target):
                logger.info("Added deferred hublvol failover path to %s (via %s) on %s for %s",
                            deferred_failover_target.get_id(),
                            deferred_failover_via.get_id(),
                            snode.get_id(), primary_node.lvstore)
            else:
                logger.warning("Failed to add deferred hublvol failover path to %s on %s for %s",
                               deferred_failover_target.get_id(),
                               snode.get_id(), primary_node.lvstore)
        except Exception as e:
            logger.error("Error adding deferred hublvol failover path on %s: %s",
                         snode.get_id(), e)

    ### 9- add lvols to subsystems (always non_optimized for non-leader)
    executor = ThreadPoolExecutor(max_workers=50)
    for lvol in lvol_list:
        executor.submit(add_lvol_thread, lvol, snode, lvol_ana_state="non_optimized")
    executor.shutdown(wait=True)

    if not activation_mode:
        ### 10- add non-optimized path on tertiary to newly-restarted secondary's hublvol
        if not is_tertiary and primary_node.tertiary_node_id and leader_node.hublvol:
            tert_id = primary_node.tertiary_node_id
            if tert_id != snode.get_id() and tert_id != leader_node.get_id():
                tert_node = db_controller.get_storage_node_by_id(tert_id)
                if tert_node and not _check_peer_disconnected(tert_node, lvs_peer_ids=lvs_peer_ids_excl_snode):
                    try:
                        if tert_node.add_hublvol_failover_path(leader_node, snode):
                            logger.info("Added secondary %s hublvol path on tertiary %s for %s",
                                        snode.get_id(), tert_node.get_id(), primary_node.lvstore)
                        else:
                            logger.warning(
                                "Failed to add secondary %s hublvol path on tertiary %s for %s",
                                snode.get_id(), tert_node.get_id(), primary_node.lvstore)
                    except Exception as e:
                        logger.error("Error adding secondary hublvol path on tertiary: %s", e)

    # Clear restart phase for this LVS
    _set_restart_phase(snode, primary_node.lvstore, "", db_controller)

    primary_node = db_controller.get_storage_node_by_id(primary_node.get_id())
    primary_node.lvstore_status = "ready"
    primary_node.write_to_db()

    return True


def recreate_all_lvstores(snode, force=False):
    """Recreate all LVS stacks on a restarting node: primary, secondary, tertiary.

    This is the dispatch logic extracted from restart_storage_node() so it can
    be called independently (e.g. from tests) without the SPDK init preamble.
    """
    db_controller = DBController()

    # --- Step 1: Primary LVS ---
    logger.info("=== Phase: Primary LVS recreation ===")
    ret = recreate_lvstore(snode, force=force)
    snode = db_controller.get_storage_node_by_id(snode.get_id())
    if not ret:
        logger.error("Failed to recreate primary lvstore")
        return False

    # --- Step 2: Secondary LVS ---
    if snode.lvstore_stack_secondary:
        logger.info("=== Phase: Secondary LVS recreation ===")
        try:
            secondary_primary_node = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
            secondary_primary_node.lvstore_status = "in_creation"
            secondary_primary_node.write_to_db()

            sec_lvs_peer_ids = [sid for sid in [secondary_primary_node.secondary_node_id,
                                                 secondary_primary_node.tertiary_node_id] if sid]
            primary_disconnected = _check_peer_disconnected(secondary_primary_node, lvs_peer_ids=sec_lvs_peer_ids)

            if primary_disconnected:
                logger.info("Primary %s disconnected — %s taking leadership for %s",
                            secondary_primary_node.get_id(), snode.get_id(), secondary_primary_node.lvstore)
                ret = recreate_lvstore(snode, force=force, lvs_primary=secondary_primary_node)
            else:
                leader_node = secondary_primary_node
                logger.info("Non-leader for %s on %s (leader=%s)",
                            secondary_primary_node.lvstore, snode.get_id(), leader_node.get_id())
                ret = recreate_lvstore_on_non_leader(snode, leader_node, secondary_primary_node, force=force)
            if not ret:
                logger.error(f"Failed to recreate secondary LVS {secondary_primary_node.lvstore}")
        except Exception as e:
            logger.error("Secondary LVS recreation failed: %s", e)

    # --- Step 3: Tertiary LVS ---
    if snode.lvstore_stack_tertiary:
        logger.info("=== Phase: Tertiary LVS recreation ===")
        try:
            tertiary_primary_node = db_controller.get_storage_node_by_id(snode.lvstore_stack_tertiary)
            tertiary_primary_node.lvstore_status = "in_creation"
            tertiary_primary_node.write_to_db()

            tert_lvs_peer_ids = [sid for sid in [tertiary_primary_node.secondary_node_id,
                                                  tertiary_primary_node.tertiary_node_id] if sid]
            primary_disconnected = _check_peer_disconnected(tertiary_primary_node, lvs_peer_ids=tert_lvs_peer_ids)

            if primary_disconnected:
                sec_id = tertiary_primary_node.secondary_node_id
                sec_disconnected = True
                if sec_id and sec_id != snode.get_id():
                    sec_node_check = db_controller.get_storage_node_by_id(sec_id)
                    sec_disconnected = _check_peer_disconnected(sec_node_check, lvs_peer_ids=tert_lvs_peer_ids)

                if not sec_disconnected and sec_id:
                    leader_node = db_controller.get_storage_node_by_id(sec_id)
                    logger.info("Primary disconnected, secondary %s is leader for %s, "
                                "tertiary %s connects as non-leader",
                                leader_node.get_id(), tertiary_primary_node.lvstore, snode.get_id())
                    ret = recreate_lvstore_on_non_leader(snode, leader_node, tertiary_primary_node, force=force)
                else:
                    logger.warning("Both primary and secondary disconnected for tertiary LVS %s, skipping",
                                   tertiary_primary_node.lvstore)
                    ret = True
            else:
                leader_node = tertiary_primary_node
                logger.info("Non-leader (tertiary) for %s on %s (leader=%s)",
                            tertiary_primary_node.lvstore, snode.get_id(), leader_node.get_id())
                ret = recreate_lvstore_on_non_leader(snode, leader_node, tertiary_primary_node, force=force)
            if not ret:
                logger.error(f"Failed to recreate tertiary LVS {tertiary_primary_node.lvstore}")
        except Exception as e:
            logger.error("Tertiary LVS recreation failed: %s", e)

    return True


def recreate_lvstore(snode, force=False, lvs_primary=None, activation_mode=False):
    """Recreate LVStore as leader.

    Per design: runs for snode's own primary LVS, and also when snode
    takes over leadership from an offline primary (lvs_primary is set).

    Args:
        snode: the restarting node (RPCs are executed here)
        force: force recreation even on validation failure
        lvs_primary: when set, the original primary node (now offline)
            whose LVS this node is taking over.  When None, snode is the
            primary for its own LVS.
        activation_mode: when True, skip all peer operations (port blocking,
            hublvol creation/connection, leader demotion).  Used during
            cluster_activate() where peer LVS may not exist yet.  Hublvol
            setup is done in a separate pass after all lvstores are up.
    """
    db_controller = DBController()

    # --- LVS context: who owns the metadata for this lvstore? ---
    is_takeover = lvs_primary is not None
    lvs_node = lvs_primary if is_takeover else snode
    lvs_name = lvs_node.lvstore
    lvs_jm_vuid = lvs_node.jm_vuid
    lvs_raid = lvs_node.raid

    lvs_node.lvstore_status = "in_creation"
    lvs_node.write_to_db()

    if activation_mode:
        # Soft prelude: reconnect any missing remote devices + remote JMs
        # so the recreate path doesn't stumble on stale/absent controllers.
        # Both helpers iterate existing bdevs internally and no-op on
        # controllers that are already attached, so this is safe to call
        # every activation pass.
        try:
            fresh_remote_devs = _connect_to_remote_devs(snode, reattach=False)
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.remote_devices = fresh_remote_devs or snode.remote_devices
            snode.write_to_db()
        except Exception as e:
            logger.warning("Soft reconnect of remote devices failed on %s: %s",
                           snode.get_id(), e)

    if not is_takeover:
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)
        snode.write_to_db()

    # Gather peer nodes for this LVS, EXCLUDING snode itself
    sec_nodes = []
    lvs_all_peer_ids = [sid for sid in [lvs_node.secondary_node_id, lvs_node.tertiary_node_id] if sid]
    # Peer list for quorum checks: exclude snode (restarting node) since its JM
    # is expected to be disconnected on peers during restart.
    lvs_peer_ids = [sid for sid in lvs_all_peer_ids if sid != snode.get_id()]
    for sec_id in lvs_all_peer_ids:
        if sec_id != snode.get_id():
            sec = db_controller.get_storage_node_by_id(sec_id)
            if sec:
                sec_nodes.append(sec)

    # Per design: determine peer connectivity via disconnect state, NOT node status.
    # Method 1: JM quorum check for each peer.
    disconnected_peers = set()
    if activation_mode:
        # During activation peer LVS may not exist yet; skip all peer checks.
        current_leader = None
    else:
        for sec_node in sec_nodes:
            if _check_peer_disconnected(sec_node, lvs_peer_ids=lvs_peer_ids):
                disconnected_peers.add(sec_node.get_id())

        # Identify the current leader among connected peers.
        # Uses bdev_lvol_get_lvstores which returns "lvs leadership" field.
        # Compression and replication checks run only against the current leader.
        current_leader = None
        for sec_node in sec_nodes:
            if sec_node.get_id() in disconnected_peers:
                continue
            try:
                sec_rpc = sec_node.rpc_client(timeout=5, retry=2)
                ret = sec_rpc.bdev_lvol_get_lvstores(lvs_name)
                if ret and len(ret) > 0 and ret[0].get("lvs leadership"):
                    current_leader = sec_node
                    logger.info("Current leader for %s is %s", lvs_name, sec_node.get_id())
                    break
            except Exception as e:
                # Cannot tell "peer down" from "peer mgmt slow" at this stage:
                # snode has no peer-hublvol controller bdevs yet, so any
                # hublvol-presence check from snode would always say
                # "disconnected" and silently drop the leader. Abort and let
                # the next restart attempt re-evaluate peer state via the
                # data-plane check earlier in this function.
                raise Exception(
                    f"Abort restart: leader detection RPC to peer {sec_node.get_id()} failed: {e}")

        # Check compression and replication only on the current leader
        if current_leader:
            try:
                jc_compression_is_active = current_leader.rpc_client().jc_compression_get_status(lvs_jm_vuid)
                retries = 10
                while jc_compression_is_active:
                    if retries <= 0:
                        logger.warning("Timeout waiting for JC compression task to finish on leader %s",
                                       current_leader.get_id())
                        break
                    retries -= 1
                    logger.info(f"JC compression active on leader {current_leader.get_id()}, retrying in 60 seconds")
                    time.sleep(60)
                    jc_compression_is_active = current_leader.rpc_client().jc_compression_get_status(
                        current_leader.jm_vuid)
            except Exception as e:
                raise Exception(
                    f"Abort restart: jc_compression check on leader {current_leader.get_id()} failed: {e}")

    ### 1- create distribs and raid
    _set_restart_phase(snode, lvs_name, StorageNode.RESTART_PHASE_PRE_BLOCK, db_controller)

    if is_takeover:
        ret, err = _create_bdev_stack(snode, lvs_node.lvstore_stack, primary_node=lvs_node)
    else:
        ret, err = _create_bdev_stack(snode, [])

    if err:
        logger.error(f"Failed to recreate lvstore on node {snode.get_id()}")
        logger.error(err)
        _set_restart_phase(snode, lvs_name, "", db_controller)
        return False

    rpc_client = snode.rpc_client()

    lvol_list = []
    for lv in db_controller.get_lvols_by_node_id(lvs_node.get_id()):
        if lv.status == LVol.STATUS_IN_DELETION:
            if not is_takeover:
                lv.deletion_status = ''
                lv.write_to_db()
        elif lv.status in [LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE]:
            if lv.deletion_status == '':
                lvol_list.append(lv)

    lvol_ana_state = "optimized"

    ### 2- create lvols nvmf subsystems
    created_subsystems = []
    for lvol in lvol_list:
        if lvol.nqn not in created_subsystems:
            allow_any = not bool(lvol.allowed_hosts)
            logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
            ret = rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, 1,
                                              max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS,
                                              allow_any_host=allow_any)
            if ret:
                created_subsystems.append(lvol.nqn)
            if lvol.allowed_hosts:
                _reapply_allowed_hosts(lvol, snode, rpc_client)

    # ANA failback only when the original primary is coming back (not takeover)
    if not is_takeover and lvs_node.secondary_node_id and lvol_list:
        _failback_primary_ana(snode)

    snode_lvs_port = lvs_node.get_lvol_subsys_port(lvs_name)

    # Phase transition: blocked — sync deletes and registrations must be delayed
    _set_restart_phase(snode, lvs_name, StorageNode.RESTART_PHASE_BLOCKED, db_controller)

    # Peers whose LVS port is currently blocked. Client IO to any peer on
    # snode_lvs_port is rejected until that peer is removed from the list.
    # Every blocked peer MUST be unblocked — either per-peer after its
    # connect_to_hublvol succeeds, or en bloc on abort.
    blocked_peers: list = []

    def _unblock_peer_port(peer):
        """Remove the firewall block for snode_lvs_port on peer and drop
        the peer from blocked_peers. Safe to call if peer is not currently
        blocked (no-op). Tolerates RPC failure — logs and continues so
        other peers can still be unblocked."""
        try:
            _pt = "udp" if peer.active_rdma else "tcp"
            _fw = FirewallClient(peer, timeout=5, retry=2)
            _fw.firewall_set_port(snode_lvs_port, _pt, "allow", peer.rpc_port)
            tcp_ports_events.port_allowed(peer, snode_lvs_port)
        except Exception as ue:
            logger.error("Failed to unblock port %s on %s: %s",
                         snode_lvs_port, peer.get_id(), ue)
        finally:
            try:
                blocked_peers.remove(peer)
            except ValueError:
                pass

    def _kill_app():
        storage_events.snode_restart_failed(snode)
        snode_api = snode.client(timeout=5, retry=5)
        snode_api.spdk_process_kill(snode.rpc_port, snode.cluster_id)
        # spdk_process_kill returns as soon as the HTTP request is
        # queued — SPDK may keep serving IO for a short while after.
        # Block here until SPDK is actually gone so the subsequent
        # peer-port unblock in _abort_restart_and_unblock cannot race
        # client IO back into the secondary while a still-alive primary
        # on snode tries to serve as leader (→ writer conflict).
        # We hold the peer port blocks during this wait; cap it at 5 s
        # so a stuck kill does not leave peers permanently blocked.
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                up = snode_api.spdk_process_is_up(snode.rpc_port, snode.cluster_id)
            except Exception:
                up = False
            if not up:
                break
            time.sleep(0.25)
        else:
            logger.warning(
                "SPDK on %s still up 5s after kill signal; proceeding with unblock anyway",
                snode.get_id(),
            )
        set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE)

    def _abort_restart_and_unblock(reason):
        """Abort: kill SPDK, set offline, unblock every blocked peer, raise."""
        logger.error("Aborting recreate_lvstore on %s for %s: %s",
                     snode.get_id(), lvs_name, reason)
        _kill_app()
        for peer in list(blocked_peers):
            _unblock_peer_port(peer)
        raise Exception(f"Abort restart: {reason}")

    if not activation_mode:
        # Wait for replication to finish on the current leader only
        if current_leader and current_leader.get_id() not in disconnected_peers:
            try:
                ret = current_leader.wait_for_jm_rep_tasks_to_finish(lvs_jm_vuid)
                if not ret:
                    msg = f"JM replication task found on leader {current_leader.get_id()} for jm {lvs_jm_vuid}"
                    logger.error(msg)
                    storage_events.jm_repl_tasks_found(current_leader, lvs_jm_vuid)
            except Exception as e:
                raise Exception(
                    f"Abort restart: replication-wait on leader {current_leader.get_id()} failed: {e}")

        ### 3- block LVS port on every connected peer (leader + non-leaders).
        # Without blocking the tertiary, client IO can leak to it during the
        # leader flap: tertiary's LVOL listener stays open and serves writes
        # whose hublvol redirect target is mid-transition, producing
        # writer_conflict events on the journal. Each peer stays blocked
        # until its connect_to_hublvol succeeds in ### 8b.
        if current_leader and current_leader.get_id() not in disconnected_peers:
            try:
                current_leader.lvstore_status = "in_creation"
                current_leader.write_to_db()
                time.sleep(3)

                port_type = "tcp"
                if current_leader.active_rdma:
                    port_type = "udp"
                fw_api = FirewallClient(current_leader, timeout=5, retry=2)
                fw_api.firewall_set_port(snode_lvs_port, port_type, "block", current_leader.rpc_port)
                tcp_ports_events.port_deny(current_leader, snode_lvs_port)
                blocked_peers.append(current_leader)
            except Exception as e:
                # Failing to port-block the current leader means we cannot
                # safely promote snode: the old leader may still be serving
                # IO, and a parallel leader on snode would produce a writer
                # conflict (observed 2026-04-25, LVS_6609 incident).
                # _check_hublvol_connected from snode is meaningless here —
                # snode hasn't reconnected to peer hublvols yet — so we
                # cannot use it to discriminate "peer gone" from "peer slow".
                # Abort the attempt; the task runner will retry.
                _abort_restart_and_unblock(
                    f"Failed to port-block leader {current_leader.get_id()}: {e}")

        # Also block non-leader peers (tertiary). The leader's demote+drain
        # below is leader-specific; non-leaders just need the port shut so
        # IO can't leak to them during the flap.
        for sec_node in sec_nodes:
            if sec_node is current_leader:
                continue
            if sec_node.get_id() in disconnected_peers:
                continue
            if sec_node in blocked_peers:
                continue
            try:
                port_type = "udp" if sec_node.active_rdma else "tcp"
                fw_api = FirewallClient(sec_node, timeout=5, retry=2)
                fw_api.firewall_set_port(snode_lvs_port, port_type, "block", sec_node.rpc_port)
                tcp_ports_events.port_deny(sec_node, snode_lvs_port)
                blocked_peers.append(sec_node)
            except Exception as e:
                # Same rationale as the leader port-block: cannot safely
                # decide "peer gone" vs "peer slow" before snode has
                # reconnected to peer hublvols. A non-leader peer left
                # serving on snode_lvs_port during the leader flap can
                # accept client IO whose hublvol redirect is mid-transition,
                # producing a writer conflict.
                _abort_restart_and_unblock(
                    f"Failed to port-block non-leader peer {sec_node.get_id()}: {e}")

        if current_leader and current_leader in blocked_peers:
            # --- Inside port-blocked window: timeout=0.2s, retry=0, abort on failure ---
            leader_rpc = current_leader.rpc_client(timeout=0.2, retry=0)

            ### 4- drain in-flight IO BEFORE dropping leadership
            #
            # If we drop leadership while IO is still in distrib, those
            # in-flight IOs land on a non-leader lvstore and either get
            # redirected via the hub bdev (which may not be open yet on
            # the new follower) or aborted — both produce client-visible
            # IO errors and qpair tear-downs.  Concrete example: incident
            # 2026-05-02 (k8s_native_failover_ha-20260502-101452), worker1.
            # 123 state-9 IOs were in flight on its distribs at the moment
            # set_leader=False fired; the open of LVS_4729/hublvoln1
            # returned ENODEV; nvmf_tcp_qpair_set_recv_state floods and
            # disconnects followed ~1.6 s later.
            #
            # The drain runs while the leader's lvol port is iptables-
            # blocked, so we must not hold this open indefinitely.  The
            # earlier fixed 0.5 s sleep was a workaround put in place
            # after the original 10 s drain regression — but that
            # regression was on the recreate_lvstore_on_non_leader path,
            # where the blocked node is the configured primary and runs
            # data migration (which never pauses on port block, hence the
            # poll never settled).  *This* path blocks `current_leader`,
            # which is a secondary or tertiary that became acting leader
            # while the configured primary was out — and migration never
            # runs on a secondary/tertiary, so the inflight counter
            # genuinely drains.
            #
            # Bound at _DRAIN_BOUND_SEC anyway: a slow JM/distrib
            # completion shouldn't be allowed to hold the leader's port
            # blocked beyond client max_latency.  On timeout we proceed
            # with the drop and accept the same residual class of error
            # this is trying to prevent — but bounded.
            _DRAIN_BOUND_SEC = 2.0
            _DRAIN_POLL_SEC = 0.05
            deadline = time.time() + _DRAIN_BOUND_SEC
            drained = False
            while time.time() < deadline:
                try:
                    still_inflight = leader_rpc.bdev_distrib_check_inflight_io(lvs_jm_vuid)
                except Exception as e:
                    logger.warning(
                        "bdev_distrib_check_inflight_io poll failed for %s on %s: %s",
                        lvs_name, current_leader.get_id(), e)
                    break
                if not still_inflight:
                    drained = True
                    break
                time.sleep(_DRAIN_POLL_SEC)
            if not drained:
                # Continuing with the leadership drop while IO is still in
                # the distrib pipeline produces exactly the failure this
                # drain is meant to prevent (in-flight IO hitting a
                # non-leader lvstore at the moment of transition: hub-bdev
                # redirect failures, qpair tear-downs, client IO errors).
                # Abort cleanly: _abort_restart_and_unblock kills the
                # recovering node's SPDK, sets it OFFLINE, and unblocks
                # every peer port we just blocked above. The restart task
                # runner re-queues from there; on the next attempt the
                # cluster may have settled enough for drain to complete
                # within the bound.
                _abort_restart_and_unblock(
                    f"Inflight IO did not drain on acting-leader "
                    f"{current_leader.get_id()} within {_DRAIN_BOUND_SEC}s; "
                    f"refusing to drop leadership against a non-empty distrib "
                    f"pipeline")

            ### 5- drop leadership on current leader (drain complete)
            try:
                leader_rpc.bdev_lvol_set_leader(lvs_name, leader=False, bs_nonleadership=True)
                leader_rpc.bdev_distrib_force_to_non_leader(lvs_jm_vuid)
            except Exception as e:
                _abort_restart_and_unblock(f"Failed to demote leader {current_leader.get_id()}: {e}")

        if disconnected_peers:
            logger.info(f"Peers disconnected {disconnected_peers}, forcing journal replication on node: {snode.get_id()}")
            rpc_client.jc_explicit_synchronization(lvs_jm_vuid)

    ### 5- examine (idempotent: skip only when raid AND lvstore already surfaced)
    rpc_client.bdev_distrib_force_to_non_leader(lvs_jm_vuid)
    raid_already = _rpc_bdev_exists(rpc_client, lvs_raid)
    lvstore_already = _rpc_lvstore_exists(rpc_client, lvs_name)
    if raid_already and lvstore_already:
        logger.info(
            "Raid %s and lvstore %s already present on %s; skipping examine",
            lvs_raid, lvs_name, snode.get_id())
    else:
        # Examine is required whenever the lvstore isn't surfaced — whether
        # the raid was freshly created by _create_bdev_stack (normal restart
        # path) or pre-existing with stale state (activation retry). The
        # previous "raid_already → skip examine" shortcut broke the normal
        # restart path: _create_bdev_stack leaves the raid in place but does
        # not examine it, so the lvstore never surfaces and the subsequent
        # bdev_lvol_get_lvstores validation fails every time.
        rpc_client.bdev_examine(lvs_raid)

        ### 6- wait for examine
        rpc_client.bdev_wait_for_examine()

    # Validate lvstore recovery
    ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
    if not ret:
        logger.error(f"Failed to recover lvstore: {lvs_name} on node: {snode.get_id()}")
        if activation_mode:
            # In activation we can't safely patch partial on-disk state.
            # Tell the caller to restart this node before continuing.
            raise LVSRestartRequiredError(
                snode.get_id(), lvs_name,
                detail=f"raid={lvs_raid} present but lvstore did not recover"
                if raid_already else "examine did not produce lvstore")
        if not force:
            _abort_restart_and_unblock("Failed to recover lvstore")

    # Validate all bdev recovery
    ret = rpc_client.get_bdevs()
    node_bdev_names = {}
    if ret:
        for b in ret:
            node_bdev_names[b['name']] = b
            for al in b['aliases']:
                node_bdev_names[al] = b

    for lv in lvol_list:
        bdev_name = lv.lvol_uuid
        passed = health_controller.check_bdev(bdev_name, bdev_names=node_bdev_names)
        if not passed:
            logger.error(f"Failed to recover BDev: {bdev_name} on node: {snode.get_id()}")
            if not force:
                _abort_restart_and_unblock("Failed to recover lvstore")

    ### 7- take leadership
    # Derive the kernel-side role from snode's topology relative to lvs_node.
    # On takeover snode is acting as leader, but its kernel role must still
    # reflect topology so the peer view of the original primary stays
    # coherent. Hardcoding role="primary" caused the LVS_9060 follow-on
    # incident (2026-04-25 11:28:50 run): when the original primary later
    # rejoins, peers disagree on who the primary is and a writer conflict
    # follows.
    if snode.get_id() == lvs_node.get_id():
        snode_lvs_role = "primary"
    elif snode.get_id() == lvs_node.secondary_node_id:
        snode_lvs_role = "secondary"
    elif snode.get_id() == lvs_node.tertiary_node_id:
        snode_lvs_role = "tertiary"
    else:
        _abort_restart_and_unblock(
            f"snode {snode.get_id()} is not a registered peer of "
            f"lvstore {lvs_name} (lvs_node={lvs_node.get_id()})")
    ret = rpc_client.bdev_lvol_set_lvs_opts(
        lvs_name,
        groupid=lvs_jm_vuid,
        subsystem_port=lvs_node.get_lvol_subsys_port(lvs_name),
        role=snode_lvs_role,
    )
    ret = rpc_client.bdev_lvol_set_leader(lvs_name, leader=True)
    leader_restored = False
    for _ in range(10):
        try:
            ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
            if ret and len(ret) > 0 and ret[0].get("lvs leadership"):
                leader_restored = True
                break
        except Exception:
            pass
        time.sleep(0.2)
    if not leader_restored:
        logger.error("Failed to restore leadership for %s on node %s", lvs_name, snode.get_id())
        if not force:
            _abort_restart_and_unblock(f"Failed to restore leadership for {lvs_name}")

    if not activation_mode:
        ### 8- create hublvol and expose via subsystem with listeners
        if sec_nodes:
            if is_takeover:
                try:
                    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                    snode.adopt_hublvol(lvs_node, cluster.nqn)
                    logger.info("Adopted hublvol on new leader %s for %s", snode.get_id(), lvs_name)
                except Exception as e:
                    logger.error("Error adopting hublvol on new leader: %s", e)
                    _abort_restart_and_unblock(f"adopt_hublvol on new leader failed: {e}")
            else:
                try:
                    if not snode.recreate_hublvol():
                        _abort_restart_and_unblock(
                            f"recreate_hublvol returned False on {snode.get_id()}")
                except RPCException as e:
                    logger.error("Error creating hublvol: %s", e.message)
                    _abort_restart_and_unblock(f"recreate_hublvol raised: {e.message}")

        ### 8b- connect peers to hublvol WITHIN port-blocked window
        # The old leader must be set to secondary role (via set_lvs_opts + connect_hublvol)
        # BEFORE we unblock its port.  Otherwise new IO can arrive and trigger
        # spdk_lvs_trigger_leadership_switch, re-promoting the old leader and
        # causing a writer conflict.
        cluster = db_controller.get_cluster_by_id(snode.cluster_id)

        # Identify the topological secondary owner (sec_1) of this LVS by
        # looking at lvs_node, NOT by sec_nodes ordering. The previous
        # index-based code (sec_nodes[0]) routed sec_1 work to whichever
        # peer happened to be first after disconnected_peers filtering —
        # which on the LVS_9060 takeover (2026-04-25 11:28:50) wasn't even
        # the right LVS, since create_secondary_hublvol read the lvstore
        # name off snode.lvstore (snode's own primary, not the LVS being
        # taken over).
        sec1_id = lvs_node.secondary_node_id
        sec1_node = next((s for s in sec_nodes if s.get_id() == sec1_id), None)
        sec1_online = bool(sec1_node and sec1_node.get_id() not in disconnected_peers)

        # Create the sec_1 hublvol only if sec_1 is a peer (not snode itself)
        # and it's online. When snode IS the topological sec_1 (secondary
        # owner taking leadership), there is no separate node to expose
        # the secondary hublvol on — the leader's primary hublvol on snode
        # is the only path until the original primary returns.
        if sec1_online and sec1_node is not None:
            try:
                sec1_node.create_secondary_hublvol(lvs_node, cluster.nqn)
            except Exception as e:
                logger.error("Error creating secondary hublvol on sec_1: %s", e)
                _abort_restart_and_unblock(
                    f"create_secondary_hublvol on {sec1_node.get_id()} raised: {e}")

        # Track tertiary→secondary failover-path attaches to run AFTER the
        # peer port unblock — keeping the in-freeze attach single-path with
        # a 0.2 s RPC budget and pushing the second-path INTER_ATTACH_SLEEP
        # outside the IO-impact window. ``deferred_tertiary_paths`` holds
        # ``(tert_node, primary_node, sec1_node)`` tuples to apply later.
        deferred_tertiary_paths = []

        for sec_node in sec_nodes:
            if sec_node.get_id() in disconnected_peers:
                continue
            # Role and failover are determined by topology, not by index.
            # An index-based assignment (sec_nodes[0] -> 'secondary',
            # rest -> 'tertiary') breaks when the original primary is
            # filtered out via disconnected_peers and shifts the
            # remaining peers up one slot.
            if sec_node.get_id() == lvs_node.secondary_node_id:
                sec_role = "secondary"
            elif sec_node.get_id() == lvs_node.tertiary_node_id:
                sec_role = "tertiary"
                # Defer the tertiary→secondary path; in-freeze attach is
                # single-path against the (returning) primary only.
                if sec1_online:
                    deferred_tertiary_paths.append((sec_node, snode, sec1_node))
            else:
                logger.warning(
                    "Skipping hublvol connect for %s: not a registered "
                    "peer of %s (lvs_node=%s)",
                    sec_node.get_id(), lvs_name, lvs_node.get_id())
                continue
            try:
                # Single-path attach against ``snode`` (the leader). The
                # secondary failover for tertiary is appended in a
                # post-unblock pass via ``add_hublvol_failover_path``.
                ok = sec_node.connect_to_hublvol(snode, failover_node=None, role=sec_role,
                                                 rpc_timeout=0.2)
            except Exception as e:
                logger.error("Error establishing hublvol on %s: %s", sec_node.get_id(), e)
                _abort_restart_and_unblock(
                    f"connect_to_hublvol on {sec_node.get_id()} raised: {e}")
            if not ok:
                _abort_restart_and_unblock(
                    f"connect_to_hublvol returned False on {sec_node.get_id()} ({sec_role})")

            ### 8c- unblock this peer's port only after its hublvol is connected
            if sec_node in blocked_peers:
                _unblock_peer_port(sec_node)

    ### 9- add lvols to subsystems
    executor = ThreadPoolExecutor(max_workers=50)
    for lvol in lvol_list:
        executor.submit(add_lvol_thread, lvol, snode, lvol_ana_state)
    executor.shutdown(wait=True)

    # Phase transition: post_unblock — delayed sync deletes and registrations can now proceed
    _set_restart_phase(snode, lvs_name, StorageNode.RESTART_PHASE_POST_UNBLOCK, db_controller)

    ### 10b- deferred tertiary→secondary hublvol failover paths
    # The in-freeze attach above used a single path (tertiary → primary).
    # Now that every peer's port is unblocked and IO is flowing again,
    # top up the multipath controller on each tertiary so a future primary
    # loss has an immediate failover. The coordinator's
    # INTER_ATTACH_SLEEP_SEC (3 s) cost lives here, OUTSIDE the IO-impact
    # window — it doesn't sit inside the leader-port-block freeze any more.
    if not activation_mode and deferred_tertiary_paths:
        for tert_node, primary_node, sec1_failover in deferred_tertiary_paths:
            if sec1_failover is None:
                # Only appended when ``sec1_online`` was True (meaning
                # ``sec1_node`` was non-None at the time), so this branch
                # should be unreachable in practice — guard for mypy.
                continue
            try:
                if tert_node.add_hublvol_failover_path(primary_node, sec1_failover):
                    logger.info("Added deferred secondary %s hublvol path on tertiary %s for %s",
                                sec1_failover.get_id(), tert_node.get_id(), lvs_name)
                else:
                    logger.warning("Failed to add deferred secondary %s hublvol path on tertiary %s for %s",
                                   sec1_failover.get_id(), tert_node.get_id(), lvs_name)
            except Exception as e:
                logger.error("Error adding deferred hublvol failover path on tertiary %s: %s",
                             tert_node.get_id(), e)

    if not activation_mode:
        ### 11- demote old leader's subsystems to non_optimized (async)
        # Per design: after restarting node takes leadership, the old leader must
        # start demoting all its lvol subsystems to non_optimized.
        for sec_node in sec_nodes:
            if sec_node.get_id() in disconnected_peers:
                continue
            try:
                sec_rpc = sec_node.rpc_client(timeout=10, retry=2)
                for lvol in lvol_list:
                    listener_port = sec_node.get_lvol_subsys_port(lvol.lvs_name)
                    for iface in sec_node.data_nics:
                        if iface.ip4_address:
                            tr_type = "RDMA" if sec_node.active_rdma and iface.trtype == "RDMA" else "TCP"
                            sec_rpc.listeners_create(
                                lvol.nqn, tr_type, iface.ip4_address, listener_port,
                                ana_state="non_optimized")
                logger.info("Demoted subsystems to non_optimized on old leader %s", sec_node.get_id())
            except Exception as e:
                logger.warning("Failed to demote subsystems on %s: %s", sec_node.get_id(), e)

        ### finish
        for sec_node in sec_nodes:
            if sec_node.get_id() not in disconnected_peers:
                sec_node = db_controller.get_storage_node_by_id(sec_node.get_id())
                sec_node.lvstore_status = "ready"
                sec_node.write_to_db()

    # Clear restart phase for this LVS
    _set_restart_phase(snode, lvs_name, "", db_controller)

    lvs_node = db_controller.get_storage_node_by_id(lvs_node.get_id())
    lvs_node.lvstore_status = "ready"
    lvs_node.write_to_db()

    # reset snapshot delete status (only for own primary LVS)
    if not is_takeover:
        for snap in db_controller.get_snapshots_by_node_id(snode.get_id()):
            if snap.status == SnapShot.STATUS_IN_DELETION:
                snap.deletion_status = ''
                snap.write_to_db()

    return True


def add_lvol_thread(lvol, snode, lvol_ana_state="optimized"):
    db_controller = DBController()

    rpc_client = snode.rpc_client(timeout=10, retry=2)

    if "crypto" in lvol.lvol_type:
        base = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
        ret = lvol_controller._create_crypto_lvol(
            rpc_client, lvol.crypto_bdev, base, lvol.crypto_key1, lvol.crypto_key2)
        if not ret:
            msg = f"Failed to create crypto lvol on node {snode.get_id()}"
            logger.error(msg)
            return False, msg

    # Add NS to subsystem (idempotent: skip if already bound with matching NSID).
    if _rpc_subsystem_has_ns(rpc_client, lvol.nqn, nsid=lvol.ns_id, bdev_name=lvol.top_bdev):
        logger.info("Namespace nsid=%s already on subsystem %s, skipping add_ns",
                    lvol.ns_id, lvol.nqn)
    else:
        logger.info("Add BDev to subsystem " + f"{lvol.vuid:016X}")
        rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid, nsid=lvol.ns_id)

    # Use per-lvstore port for this lvol's lvstore
    listener_port = snode.get_lvol_subsys_port(lvol.lvs_name)
    for iface in snode.data_nics:
        if iface.ip4_address and lvol.fabric == iface.trtype.lower():
            tr = iface.trtype
        elif iface.ip4_address and lvol.fabric == "tcp" and snode.active_tcp:
            tr = "TCP"
        else:
            continue
        if _rpc_subsystem_has_listener(rpc_client, lvol.nqn, tr, iface.ip4_address, listener_port):
            logger.info("Listener %s %s:%s already on %s, skipping",
                        tr, iface.ip4_address, listener_port, lvol.nqn)
            continue
        logger.info("adding listener for %s on IP %s (%s)", lvol.nqn, iface.ip4_address, tr)
        rpc_client.listeners_create(
            lvol.nqn, tr, iface.ip4_address, listener_port, ana_state=lvol_ana_state)

    lvol_obj = db_controller.get_lvol_by_id(lvol.get_id())
    lvol_obj.status = LVol.STATUS_ONLINE
    lvol_obj.io_error = False
    lvol_obj.health_check = True
    lvol_obj.write_to_db()
    # set QOS
    if lvol.rw_ios_per_sec or lvol.rw_mbytes_per_sec or lvol.r_mbytes_per_sec or lvol.w_mbytes_per_sec:
        lvol_controller.set_lvol(lvol.uuid, lvol.rw_ios_per_sec, lvol.rw_mbytes_per_sec,
                                 lvol.r_mbytes_per_sec, lvol.w_mbytes_per_sec)
    return True, None


def get_sorted_ha_jms(current_node):
    db_controller = DBController()
    jm_count = {}
    jm_dev_to_mgmt_ip = {}

    for node in db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id):
        if node.get_id() == current_node.get_id():  # pass
            continue

        if node.jm_device and node.jm_device.status == JMDevice.STATUS_ONLINE and node.jm_device.get_id():
            jm_count[node.jm_device.get_id()] = 0
            jm_dev_to_mgmt_ip[node.jm_device.get_id()] = node.mgmt_ip

    for node in db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id):
        if node.get_id() == current_node.get_id():  # pass
            continue
        if not node.jm_ids:
            continue
        for rem_jm_id in node.jm_ids:
            if rem_jm_id in jm_count:
                jm_count[rem_jm_id] += 1

    mgmt_ips = []
    jm_count = dict(sorted(jm_count.items(), key=lambda x: x[1]))
    out = []
    for jm_id in jm_count.keys():
        if jm_id:
            if jm_dev_to_mgmt_ip[jm_id] in mgmt_ips:
                continue
            if jm_dev_to_mgmt_ip[jm_id] == current_node.mgmt_ip:
                continue
            mgmt_ips.append(jm_dev_to_mgmt_ip[jm_id])
            out.append(jm_id)
    return out[:current_node.ha_jm_count - 1]


def get_node_jm_names(current_node, remote_node=None):
    jm_list = []
    if current_node.jm_device:
        if remote_node:
            jm_list.append(f"remote_{current_node.jm_device.jm_bdev}n1")
        else:
            jm_list.append(current_node.jm_device.jm_bdev)
    else:
        jm_list.append("JM_LOCAL")

    if current_node.enable_ha_jm:
        for jm_id in current_node.jm_ids:
            if not jm_id:
                continue

            if remote_node:
                if remote_node.jm_device.get_id() == jm_id:
                    jm_list.append(remote_node.jm_device.jm_bdev)
                    continue

            jm_dev = DBController().get_jm_device_by_id(jm_id)
            jm_list.append(f"remote_{jm_dev.jm_bdev}n1")

    return jm_list[:current_node.ha_jm_count]


def get_secondary_nodes(current_node, exclude_ids=None):
    if exclude_ids is None:
        exclude_ids = []
    db_controller = DBController()
    nodes = []
    nod_found = False
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id)
    if len(all_nodes) == 2:
        for node in all_nodes:
            if node.get_id() != current_node.get_id() and node.get_id() not in exclude_ids:
                return [node.get_id()]

    for node in all_nodes:
        if node.get_id() == current_node.get_id() or node.get_id() in exclude_ids:
            if node.get_id() == current_node.get_id():
                nod_found = True
            continue
        elif node.status == StorageNode.STATUS_ONLINE and node.mgmt_ip != current_node.mgmt_ip:
            # elif node.status == StorageNode.STATUS_ONLINE :
            if node.is_secondary_node:
                nodes.append(node.get_id())

            elif not node.lvstore_stack_secondary:
                nodes.append(node.get_id())
                if nod_found:
                    return [node.get_id()]

    return nodes


def get_secondary_nodes_2(current_node, exclude_ids=None, exclude_mgmt_ips=None):
    """Get candidate nodes for second secondary assignment (dual fault tolerance).
    Unlike get_secondary_nodes, this checks lvstore_stack_tertiary instead of
    lvstore_stack_secondary, since nodes that already serve as first secondary
    for another primary are still eligible as second secondary.

    The tertiary must be host-disjoint from both the primary (current_node) and
    the already-picked first secondary, otherwise a single host outage would
    take out two of the four HA journal members and violate the cluster's
    fault-tolerance guarantee. Caller passes the secondary's mgmt_ip via
    exclude_mgmt_ips to enforce this.
    """
    if exclude_ids is None:
        exclude_ids = []
    forbidden_ips = {current_node.mgmt_ip}
    if exclude_mgmt_ips:
        forbidden_ips.update(exclude_mgmt_ips)
    db_controller = DBController()
    nodes = []
    nod_found = False
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id)
    if len(all_nodes) == 2:
        for node in all_nodes:
            if node.get_id() != current_node.get_id() and node.get_id() not in exclude_ids:
                return [node.get_id()]

    for node in all_nodes:
        if node.get_id() == current_node.get_id() or node.get_id() in exclude_ids:
            if node.get_id() == current_node.get_id():
                nod_found = True
            continue
        elif node.status == StorageNode.STATUS_ONLINE and node.mgmt_ip not in forbidden_ips:
            if node.is_secondary_node:
                nodes.append(node.get_id())

            elif not node.lvstore_stack_tertiary:
                nodes.append(node.get_id())
                if nod_found:
                    return [node.get_id()]

    return nodes


def create_lvstore(snode, ndcs, npcs, distr_bs, distr_chunk_bs, page_size_in_blocks, max_size):
    db_controller = DBController()
    lvstore_stack: List[dict] = []
    distrib_list = []
    distrib_vuids = []
    size = max_size // snode.number_of_distribs
    distr_page_size = page_size_in_blocks
    # distr_page_size = (ndcs + npcs) * page_size_in_blocks
    # cluster_sz = ndcs * page_size_in_blocks
    cluster_sz = page_size_in_blocks * constants.LVOL_CLUSTER_RATIO
    strip_size_kb = int((ndcs + npcs) * 2048)
    strip_size_kb = utils.nearest_upper_power_of_2(strip_size_kb)
    jm_vuid = 1
    jm_ids = []
    lvol_subsys_port, hublvol_port = utils.get_next_lvstore_ports(snode.cluster_id)
    if snode.enable_ha_jm:
        jm_vuid = utils.get_random_vuid()
        jm_ids = get_sorted_ha_jms(snode)
        logger.debug(f"online_jms: {str(jm_ids)}")
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode, jm_ids)
        snode.jm_ids = jm_ids
        snode.jm_vuid = jm_vuid
        snode.write_to_db()

    write_protection = False
    if ndcs > 1:
        write_protection = True
    for _ in range(snode.number_of_distribs):
        distrib_vuid = utils.get_random_vuid()
        while distrib_vuid in distrib_vuids:
            distrib_vuid = utils.get_random_vuid()

        distrib_name = f"distrib_{distrib_vuid}"
        lvstore_stack.extend(
            [
                {
                    "type": "bdev_distr",
                    "name": distrib_name,
                    "params": {
                        "name": distrib_name,
                        "jm_vuid": jm_vuid,
                        "vuid": distrib_vuid,
                        "ndcs": ndcs,
                        "npcs": npcs,
                        "num_blocks": size // distr_bs,
                        "block_size": distr_bs,
                        "chunk_size": distr_chunk_bs,
                        "pba_page_size": distr_page_size,
                        "write_protection": write_protection,
                    }
                }
            ]
        )
        distrib_list.append(distrib_name)
        distrib_vuids.append(distrib_vuid)

    if len(distrib_list) == 1:
        raid_device = distrib_list[0]
    else:
        raid_device = f"raid0_{jm_vuid}"
        lvstore_stack.append(
            {
                "type": "bdev_raid",
                "name": raid_device,
                "params": {
                    "name": raid_device,
                    "raid_level": "0",
                    "base_bdevs": distrib_list,
                    "strip_size_kb": strip_size_kb
                },
                "distribs_list": distrib_list,
                "jm_ids": jm_ids,
                "jm_vuid": jm_vuid,
            }
        )

    lvs_name = f"LVS_{jm_vuid}"
    lvstore_stack.append(
        {
            "type": "bdev_lvstore",
            "name": lvs_name,
            "params": {
                "name": lvs_name,
                "bdev_name": raid_device,
                "cluster_sz": cluster_sz,
                "clear_method": "none",
                "num_md_pages_per_cluster_ratio": 50,
            }
        }
    )

    snode.lvstore = lvs_name
    snode.lvstore_stack = lvstore_stack
    snode.raid = raid_device
    snode.lvol_subsys_port = lvol_subsys_port
    # Re-read lvstore_ports from DB to preserve ports propagated by other
    # nodes' create_lvstore calls (the in-memory snode may be stale).
    fresh = db_controller.get_storage_node_by_id(snode.get_id())
    snode.lvstore_ports = fresh.lvstore_ports if fresh.lvstore_ports else {}
    snode.lvstore_ports[lvs_name] = {
        "lvol_subsys_port": lvol_subsys_port,
        "hublvol_port": hublvol_port,
    }
    snode.lvstore_status = "in_creation"
    snode.write_to_db()

    ret, err = _create_bdev_stack(snode, lvstore_stack)
    if err:
        logger.error(f"Failed to create lvstore on node {snode.get_id()}")
        logger.error(err)
        return False

    rpc_client = snode.rpc_client()
    ret = rpc_client.bdev_lvol_set_lvs_opts(
        snode.lvstore,
        groupid=snode.jm_vuid,
        subsystem_port=snode.get_lvol_subsys_port(snode.lvstore),
        role="primary"
    )
    ret = rpc_client.bdev_lvol_set_leader(snode.lvstore, leader=True)

    secondary_ids = []
    if snode.secondary_node_id:
        secondary_ids.append(snode.secondary_node_id)
    if snode.tertiary_node_id:
        secondary_ids.append(snode.tertiary_node_id)

    for sec_node_id in secondary_ids:
        sec_node = db_controller.get_storage_node_by_id(sec_node_id)

        # Propagate per-lvstore ports to secondary node
        if not sec_node.lvstore_ports:
            sec_node.lvstore_ports = {}
        sec_node.lvstore_ports[lvs_name] = snode.lvstore_ports[lvs_name].copy()

        # creating lvstore on secondary
        sec_node.remote_jm_devices = _connect_to_remote_jm_devs(sec_node)
        sec_node.write_to_db()
        ret, err = _create_bdev_stack(sec_node, lvstore_stack, primary_node=snode)
        if err:
            logger.error(f"Failed to create lvstore on node {sec_node.get_id()}")
            logger.error(err)
            return False

        # sending to the other node (sec_node) with the primary group jm_vuid (snode.jm_vuid)
        ret, err = sec_node.rpc_client().jc_suspend_compression(jm_vuid=snode.jm_vuid, suspend=False)
        if not ret:
            logger.info("Failed to resume JC compression adding task...")
            tasks_controller.add_jc_comp_resume_task(sec_node.cluster_id, sec_node.get_id(), jm_vuid=snode.jm_vuid)

        sec_rpc_client = sec_node.rpc_client()
        sec_rpc_client.bdev_examine(snode.raid)
        sec_rpc_client.bdev_wait_for_examine()

        sec_node.write_to_db()

    # Create hublvol on primary after all secondaries have their stacks
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    try:
        snode.create_hublvol(cluster_nqn=cluster.nqn)
    except RPCException as e:
        logger.error("Error establishing hublvol: %s", e.message)
        # return False

    if secondary_ids:
        # Create secondary hublvol on sec_1 so tertiary can multipath
        sec1 = db_controller.get_storage_node_by_id(secondary_ids[0])
        if sec1 and sec1.status == StorageNode.STATUS_ONLINE:
            try:
                cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                sec1.create_secondary_hublvol(snode, cluster.nqn)
            except Exception as e:
                logger.error("Error creating secondary hublvol on sec_1: %s", e)

        for i, sec_node_id in enumerate(secondary_ids):
            sec_node = db_controller.get_storage_node_by_id(sec_node_id)
            if sec_node.status == StorageNode.STATUS_ONLINE:
                try:
                    time.sleep(1)
                    # tertiary gets multipath failover to sec_1
                    failover_node = sec1 if i >= 1 and sec1 and sec1.status == StorageNode.STATUS_ONLINE else None
                    sec_role = "tertiary" if i >= 1 else "secondary"
                    sec_node.connect_to_hublvol(snode, failover_node=failover_node, role=sec_role)
                except Exception as e:
                    logger.error("Error establishing hublvol: %s", e)
                    # return False

    storage_events.node_ports_changed(snode)
    return True



def _create_bdev_stack(snode, lvstore_stack=None, primary_node=None):
    def _create_distr(snode, name, params):
        try:
            rpc_client.bdev_distrib_create(**params)
        except Exception:
            logger.error("Failed to create bdev distrib")
        ret = distr_controller.send_cluster_map_to_distr(snode, name)
        if not ret:
            logger.error("Failed to send cluster map")

    rpc_client = snode.rpc_client()
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    created_bdevs: list = []
    if not lvstore_stack:
        # Restart case
        stack = snode.lvstore_stack
    else:
        stack = lvstore_stack

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]
    else:
        node_bdev_names = []

    thread_list = []
    for bdev in stack:
        type = bdev['type']
        name = bdev['name']
        params = bdev['params']
        if name in node_bdev_names:
            continue

        elif type == "bdev_distr":
            if primary_node:
                params['jm_names'] = get_node_jm_names(primary_node, remote_node=snode)
            else:
                params['jm_names'] = get_node_jm_names(snode)

            if snode.distrib_cpu_cores:
                distrib_cpu_mask = utils.decimal_to_hex_power_of_2(snode.distrib_cpu_cores[snode.distrib_cpu_index])
                params['distrib_cpu_mask'] = distrib_cpu_mask
                snode.distrib_cpu_index = (snode.distrib_cpu_index + 1) % len(snode.distrib_cpu_cores)

            params['full_page_unmap'] = cluster.full_page_unmap
            t = threading.Thread(target=_create_distr, args=(snode, name, params,))
            thread_list.append(t)
            t.start()
            ret = True

        elif type == "bdev_lvstore" and lvstore_stack and not primary_node:
                ret = rpc_client.create_lvstore(**params)

        elif type == "bdev_ptnonexcl":
            ret = rpc_client.bdev_PT_NoExcl_create(**params)

        elif type == "bdev_raid":
            if thread_list:
                for t in thread_list:
                    t.join()
            distribs_list = bdev["distribs_list"]
            strip_size_kb = params["strip_size_kb"]
            ret = rpc_client.bdev_raid_create(name, distribs_list, strip_size_kb=strip_size_kb)

        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if ret:
            bdev['status'] = "created"
            created_bdevs.insert(0, bdev)
        else:
            if created_bdevs:
                # rollback
                _remove_bdev_stack(created_bdevs[::-1], rpc_client)
            return False, f"Failed to create BDev: {name}"

    if thread_list:
        for t in thread_list:
            t.join()
    return True, None


def _remove_bdev_stack(bdev_stack, rpc_client, remove_distr_only=False):
    for bdev in reversed(bdev_stack):
        if 'status' in bdev and bdev['status'] == 'deleted':
            continue
        type = bdev['type']
        name = bdev['name']
        if type == "bdev_distr":
            ret = rpc_client.bdev_distrib_delete(name)
        elif type == "bdev_raid":
            ret = rpc_client.bdev_raid_delete(name)
        elif type == "bdev_lvstore" and not remove_distr_only:
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
        elif type == "bdev_ptnonexcl":
            ret = rpc_client.bdev_PT_NoExcl_delete(name)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue
        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
        # time.sleep(1)


def teardown_non_leader_lvstore(donor_node, primary_node, slot=None):
    """Tear down a non-leader (secondary or tertiary) LVStore stack on
    ``donor_node`` for the LVS owned by ``primary_node``, in-place.

    This is the inverse of ``recreate_lvstore_on_sec`` for a single
    (primary, donor) pair and is used by the single-node-expansion orchestrator
    when re-homing a sec/tert role from one node to another.

    Pre-conditions (caller's responsibility — not enforced here):
      * A replacement holder for the same role has already been created
        elsewhere and is in sync (so the LVS still meets FTT after this
        teardown).
      * IO quiescing / port-blocking is *not* performed here. Callers needing
        coordinated leadership transitions must do it before calling this
        function; this helper only removes the donor-side bdevs/subsystems.

    Slot discovery
    --------------
    Two modes:

    * ``slot=None`` (default): the helper auto-detects the slot from
      ``primary_node.secondary_node_id`` / ``_2``. Use this when the
      primary's pointer has not yet been moved away from the donor.
    * ``slot="_1"`` or ``slot="_2"``: the caller asserts which slot the
      donor previously occupied. Used by the expansion executor, which
      flips the primary's pointer to the recipient *before* tearing down
      the donor (so the discovered pointer no longer matches).

    Steps performed:
      1. Delete per-lvol nvmf subsystems on the donor for every lvol owned
         by ``primary_node``.
      2. Remove the donor's bdev stack (distrib + raid + lvstore + ptnonexcl)
         via ``_remove_bdev_stack`` using ``primary_node.lvstore_stack`` as
         the structural template.
      3. Detach the hublvol nvme controller on the donor (best-effort —
         may already be gone if the controller was never attached).
      4. Clear the corresponding back-reference field
         (``lvstore_stack_secondary_1`` or ``_2``) on the donor and persist.

    What this does NOT do (orchestrator's responsibility):
      * Update ``primary_node.secondary_node_id`` / ``_2`` pointers — the
        orchestrator knows the new holder and will overwrite there.
      * Reconfigure the sibling sec/tert (e.g., when sec_1 is being torn
        down, sec_2's multipath controller still references the donor and
        must be re-attached separately).
      * Remove ``donor_node.lvstore_ports[primary.lvstore]`` — left in place
        so a subsequent re-add to the same donor reuses the same ports.

    Returns
    -------
    bool
        True if all donor-side cleanup completed; False if the donor was
        not actually a sec/tert for this primary (no-op refused) or the
        bdev stack delete returned an error.
    """
    if slot  == "_1":
        sec_attr = f"lvstore_stack_secondary"
    elif slot == "_2":
        sec_attr = f"lvstore_stack_tertiary"
    elif slot is None:
        if primary_node.secondary_node_id == donor_node.get_id():
            sec_attr = 'lvstore_stack_secondary'
        elif primary_node.lvstore_stack_tertiary == donor_node.get_id():
            sec_attr = 'lvstore_stack_tertiary'
        else:
            logger.error(
                f"teardown_non_leader_lvstore: donor {donor_node.get_id()} "
                f"is not sec_1 nor sec_2 for primary {primary_node.get_id()}; "
                f"refusing")
            return False
    else:
        raise ValueError(
            f"teardown_non_leader_lvstore: slot must be None, '_1', or '_2', "
            f"got {slot!r}")

    db_controller = DBController()
    rpc_client = donor_node.rpc_client()

    # 1. Delete per-lvol subsystems on the donor.
    for lvol in db_controller.get_lvols_by_node_id(primary_node.get_id()):
        if lvol.status == LVol.STATUS_IN_DELETION:
            continue
        try:
            rpc_client.subsystem_delete(lvol.nqn)
        except Exception as e:
            logger.warning(
                f"teardown_non_leader_lvstore: subsystem_delete({lvol.nqn}) "
                f"on {donor_node.get_id()} raised {e}; continuing")

    # 2. Best-effort: detach the hublvol nvme controller.
    if primary_node.hublvol and primary_node.hublvol.bdev_name:
        try:
            rpc_client.bdev_nvme_detach_controller(
                primary_node.hublvol.bdev_name)
        except Exception as e:
            logger.debug(
                f"teardown_non_leader_lvstore: hublvol detach raised {e} "
                f"(likely already detached)")

    # 3. Remove the bdev stack. The donor instantiated the stack from
    #    primary_node.lvstore_stack; we use the same list as the structural
    #    template so _remove_bdev_stack walks it in the right order.
    if primary_node.lvstore_stack:
        # _remove_bdev_stack mutates 'status' fields — work on a shallow copy
        # of the dicts so we don't accidentally persist 'deleted' markers
        # back into primary_node.lvstore_stack on subsequent writes.
        stack_copy = [dict(b) for b in primary_node.lvstore_stack]
        _remove_bdev_stack(stack_copy, rpc_client)


    # 4. Clear the back-reference on the donor and persist. Re-fetch so we
    #    don't clobber unrelated concurrent edits to the donor record.
    fresh_donor = db_controller.get_storage_node_by_id(donor_node.get_id())
    setattr(fresh_donor, sec_attr, "")
    if fresh_donor.lvstore_ports and primary_node.lvstore in fresh_donor.lvstore_ports:
        del fresh_donor.lvstore_ports[primary_node.lvstore]
    fresh_donor.write_to_db()

    logger.info(
        f"teardown_non_leader_lvstore: tore down {sec_attr} for primary "
        f"{primary_node.get_id()} on donor {donor_node.get_id()}")
    return True


def reattach_sibling_failover(sibling_node, primary_node,
                              old_failover_node, new_failover_node):
    """Surgically reconfigure a sibling secondary's NVMe-oF multipath group
    so its failover path points at the new sec_1 holder instead of the old
    one.

    Used by the single-node-expansion executor when ``primary_node``'s
    sec_1 is re-homed from ``old_failover_node`` to ``new_failover_node``.
    The sec_2 node (``sibling_node``) currently has an NVMe controller
    attached to ``primary_node``'s hublvol bdev with paths
    ``{primary, old_failover}``; after this call the paths are
    ``{primary, new_failover}``.

    The two RPCs are issued in additive-then-subtractive order so that the
    sibling never has fewer than the prior path count: the new failover
    path is attached first, then the old one is removed. If the additive
    step fails on every NIC the function raises (the operator must
    intervene); if only the subtractive cleanup fails it logs and returns
    success — the dead path will be inert once the donor's stack is torn
    down.
    """
    if primary_node.hublvol is None or not primary_node.hublvol.bdev_name:
        logger.debug(
            "reattach_sibling_failover: primary %s has no hublvol; nothing to do",
            primary_node.get_id())
        return

    bdev_name = primary_node.hublvol.bdev_name
    nqn = primary_node.hublvol.nqn
    port = primary_node.hublvol.nvmf_port
    rpc_client = sibling_node.rpc_client()

    def _tr_type_for(node, iface):
        if node.active_rdma and iface.trtype == "RDMA":
            return "RDMA"
        if not node.active_rdma and node.active_tcp and iface.trtype == "TCP":
            return "TCP"
        return None

    # Add new failover path(s).
    new_attached = 0
    for iface in new_failover_node.data_nics:
        tr_type = _tr_type_for(new_failover_node, iface)
        if tr_type is None:
            continue
        try:
            ret = rpc_client.bdev_nvme_attach_controller(
                bdev_name, nqn, iface.ip4_address, port, tr_type,
                multipath="multipath")
            if ret:
                new_attached += 1
        except Exception as e:
            logger.warning(
                f"reattach_sibling_failover: attach new failover path "
                f"{iface.ip4_address}:{port} on {sibling_node.get_id()} "
                f"raised {e}")

    if new_attached == 0:
        raise RuntimeError(
            f"reattach_sibling_failover: failed to attach any new failover "
            f"path for primary {primary_node.get_id()} on sibling "
            f"{sibling_node.get_id()}")

    # Remove old failover path(s). Best-effort: the dead path is also
    # naturally inert once the donor's stack is torn down.
    for iface in old_failover_node.data_nics:
        tr_type = _tr_type_for(old_failover_node, iface)
        if tr_type is None:
            continue
        try:
            rpc_client.bdev_nvme_remove_trid(
                bdev_name, iface.ip4_address, port, trtype=tr_type)
        except Exception as e:
            logger.warning(
                f"reattach_sibling_failover: remove old failover path "
                f"{iface.ip4_address}:{port} on {sibling_node.get_id()} "
                f"raised {e} (path will be inert after donor teardown)")

    logger.info(
        f"reattach_sibling_failover: sibling {sibling_node.get_id()} for "
        f"LVS@{primary_node.get_id()} repointed failover "
        f"{old_failover_node.get_id()} -> {new_failover_node.get_id()}")


def send_cluster_map(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("snode not found")
        return False

    logger.info("Sending cluster map")
    return distr_controller.send_cluster_map_to_node(snode)


def get_cluster_map(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("snode not found")
        return False

    distribs_list = []
    nodes = [snode]

    if snode.secondary_node_id:
        try:
            nodes.append(db_controller.get_storage_node_by_id(snode.secondary_node_id))
        except KeyError:
            pass

    for bdev in snode.lvstore_stack:
        type = bdev['type']
        if type == "bdev_raid":
            distribs_list.extend(bdev["distribs_list"])

    for node in nodes:
        logger.info(f"getting cluster map from node: {node.get_id()}")
        rpc_client = node.rpc_client()
        for distr in distribs_list:
            ret = rpc_client.distr_get_cluster_map(distr)
            if not ret:
                logger.error(f"Failed to get distr cluster map: {distr}")
                return False
            logger.debug(ret)
            print("*" * 100)
            print(distr)
            results, is_passed = distr_controller.parse_distr_cluster_map(ret)
            print(utils.print_table(results))
    return True


def make_sec_new_primary(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("snode not found")
        return False

    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_NEW:
            device_controller.add_device(dev.get_id(), add_migration_task=False)

    time.sleep(5)
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_REMOVED:
            device_controller.device_set_failed(dev.get_id())

    snode = db_controller.get_storage_node_by_id(node_id)
    snode.primary_ip = snode.mgmt_ip
    snode.write_to_db(db_controller.kv_store)

    for lvol in db_controller.get_lvols_by_node_id(node_id):
        lvol.hostname = snode.hostname
        lvol.write_to_db()

    return True


def dump_lvstore(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if not snode.lvstore:
        logger.error("Storage node does not have lvstore")
        return False

    rpc_client = snode.rpc_client(timeout=120)
    logger.info(f"Dumping lvstore data on node: {snode.get_id()}")
    file_name = f"LVS_dump_{snode.hostname}_{snode.lvstore}_{str(datetime.datetime.now().isoformat())}.txt"
    file_path = f"/etc/simplyblock/{file_name}"
    ret = rpc_client.bdev_lvs_dump(snode.lvstore, file_path)
    if not ret:
        logger.warning("faild to dump lvstore data")
    #     return False

    logger.info(f"LVS dump file will be here: {file_path}")
    return True


def set_value(node_id, attr, value):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if attr in snode.get_attrs_map():
        try:
            value = snode.get_attrs_map()[attr]['type'](value)
            logger.info(f"Setting {attr} to {value}")
            setattr(snode, attr, value)
            snode.write_to_db()
        except Exception:
            pass

    return True


def safe_delete_bdev(name, node_id):
    # On primary node
    #./ rpc.py bdev_lvol_delete lvsname / name
    # check the statue code of the following command it must be 0
    #./ rpc.py bdev_lvol_get_lvol_delete_status lvsname / name
    # #./ rpc.py bdev_lvol_delete lvsname / name - s

    # On secondary:
    #./ rpc.py bdev_lvol_delete lvsname / name - s

    db_controller = DBController()
    primary_node = db_controller.get_storage_node_by_id(node_id)
    secondary_node = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)
    bdev_name = f"{primary_node.lvstore}/{name}"
    logger.info(f"deleting from primary: {bdev_name}")
    ret, _ = primary_node.rpc_client().delete_lvol(bdev_name)
    if not ret:
        logger.error(f"Failed to delete bdev: {bdev_name} from node: {primary_node.get_id()}")
        return False

    time.sleep(1)

    while True:
        try:
            ret = primary_node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
        except Exception as e:
            logger.error(e)
            return False

        if ret == 1:  # Async lvol deletion is in progress or queued
            logger.info(f"deletion in progress: {bdev_name}")
            time.sleep(1)

        elif ret == 0 or ret == 2:  # Lvol may have already been deleted (not found) or delete completed
            ret, _ = primary_node.rpc_client().delete_lvol(bdev_name, del_async=True)
            if not ret:
                logger.error(f"Failed to delete bdev: {bdev_name} from node: {primary_node.get_id()}")
                return False

            logger.info(f"deletion completed on primary: {bdev_name}")
            logger.info(f"deleting from secondary: {bdev_name}")
            ret, _ = secondary_node.rpc_client().delete_lvol(bdev_name, del_async=True)
            if not ret:
                logger.error(f"Failed to delete bdev: {bdev_name} from node: {secondary_node.get_id()}")
                return False
            else:
                logger.info(f"deletion completed on secondary: {bdev_name}")
            return True
        else:
            logger.error(f"failed to delete bdev: {bdev_name}, status code: {ret}")
            return False


def auto_repair(node_id, validate_only=False, force_remove_inconsistent=False, force_remove_worng_ref=False):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_ONLINE:
        logger.error("Storage node is not online")
        return False

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if cluster.status not in [Cluster.STATUS_DEGRADED, Cluster.STATUS_ACTIVE]:
        logger.error("Cluster is not in degraded or active state")
        return False

    ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
    if not ret:
        logger.error("Failed to get LVol info")
        return False
    lvs_info = ret[0]
    if "uuid" in lvs_info and lvs_info['uuid']:
        lvs_uuid =  lvs_info['uuid']
    else:
        logger.error("Failed to get lvstore uuid")
        return False

    # get the lvstore uuid
    # ./spdk/scripts/rpc.py -s /mnt/ramdisk/spdk_8080/spdk.sock bdev_lvs_dump_tree  --uuid=1dc5fb34-5ff6-4be6-ab46-eb9f006f5d47 > out_8080.json
    lvstore_dump = snode.rpc_client().bdev_lvs_dump_tree(lvs_uuid)

    # #sbctl sn list-lvols d4577fa7-545f-4506-b127-7e81fc3a6e34 --json > lvols_8080.json
    # with open('lvols_8082.json', 'r') as file:
    #     lvols = json.load(file)
    lvols = lvol_controller.list_by_node(node_id, is_json=True)
    if lvols:
        lvols = json.loads(lvols)

    # #sbctl sn list-snapshots d4577fa7-545f-4506-b127-7e81fc3a6e34 --json > snaps_8080.json
    # with open('snaps_8082.json', 'r') as file:
    #     snaps = json.load(file)
    snaps = snapshot_controller.list_by_node(node_id, is_json=True)
    if snaps:
        snaps = json.loads(snaps)

    out_blobid_dict = {}
    lvols_blobid_dict = {}
    snaps_blobid_dict = {}
    diff_list = []
    diff_lvol_dict = {}
    diff_snap_dict = {}
    diff_clone_dict = {}
    manual_del = {}
    inconsistent_dict = {}
    mgmt_diff_dict = {}


    for dump in lvstore_dump["lvols"]:
        out_blobid_dict[dump["blobid"]] = {"uuid": dump["uuid"], "name": dump["name"], "ref": dump["ref"]}

    for lvol in lvols:
        lvols_blobid_dict[lvol["BlobID"]] = {"uuid": lvol["BDdev UUID"], "name": lvol["BDev"]}
    for snap in snaps:
        snaps_blobid_dict[snap["BlobID"]] = {"uuid": snap["BDdev UUID"], "name": snap["BDev"]}

    out_blobid_dict_keys = list(out_blobid_dict.keys())
    lvols_blobid_dict_keys = list(lvols_blobid_dict.keys())
    snaps_blobid_dict_keys = list(snaps_blobid_dict.keys())

    for blob in out_blobid_dict_keys:
        if blob not in (lvols_blobid_dict_keys + snaps_blobid_dict_keys):
            if out_blobid_dict[blob]["name"] == "hublvol":
                continue
            else:
                # all blob ID in spdk but not in mgmt
                diff_list.append(blob)
        else:
            if blob  in lvols_blobid_dict_keys:
                if out_blobid_dict[blob]["name"] != lvols_blobid_dict[blob]["name"] or out_blobid_dict[blob]["uuid"] != lvols_blobid_dict[blob]["uuid"]:
                    inconsistent_dict[blob] = out_blobid_dict[blob]
                    inconsistent_dict[blob]["type"] = "lvol|clone"
            if blob in snaps_blobid_dict_keys:
                if out_blobid_dict[blob]["name"] != snaps_blobid_dict[blob]["name"] or out_blobid_dict[blob]["uuid"] != snaps_blobid_dict[blob]["uuid"]:
                    inconsistent_dict[blob] = out_blobid_dict[blob]
                    inconsistent_dict[blob]["type"] = "snap"


    for blob in lvols_blobid_dict_keys:
        if blob not in out_blobid_dict_keys:
            # All blob in mgmt but not in SPDK
            mgmt_diff_dict[blob] = lvols_blobid_dict[blob]
            mgmt_diff_dict[blob]["type"] = "lvol|clone"

    for blob in snaps_blobid_dict_keys:
        if blob not in out_blobid_dict_keys:
            # All blob in mgmt but not in SPDK
            mgmt_diff_dict[blob] = snaps_blobid_dict[blob]
            mgmt_diff_dict[blob]["type"] = "snap"

    print(f"All diff count is: {len(diff_list)}")
    print(f"All mgmt diff count is: {len(mgmt_diff_dict.keys())}")

    for blob in diff_list:
        if "LVOL" in out_blobid_dict[blob]["name"]:
            if out_blobid_dict[blob]["ref"] !=1:
                manual_del[blob] = out_blobid_dict[blob]
            else:
                diff_lvol_dict[blob] = out_blobid_dict[blob]
        elif "SNAP" in out_blobid_dict[blob]["name"]:
            if out_blobid_dict[blob]["ref"] != 2:
                manual_del[blob] = out_blobid_dict[blob]
            else:
                diff_snap_dict[blob] = out_blobid_dict[blob]
        elif "CLN" in out_blobid_dict[blob]["name"]:
            if out_blobid_dict[blob]["ref"] !=1:
                manual_del[blob] = out_blobid_dict[blob]
            else:
                diff_clone_dict[blob] = out_blobid_dict[blob]

    if not validate_only:
        cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_IN_ACTIVATION)
        time.sleep(3)

    print(f"safe lvols to be deleted count is {len(diff_lvol_dict.keys())}")
    print(f"safe snaps to be deleted count is {len(diff_snap_dict.keys())}")
    print(f"safe clone to be deleted count is {len(diff_clone_dict.keys())}")
    print(f"manual bdevs to be deleted count is {len(manual_del.keys())}")
    print(f"inconsistent bdevs to be checked count is {len(inconsistent_dict.keys())}")
    print("#########################################")
    print("Safe lvols to be deleted:")
    for blob, value in diff_lvol_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Safe snaps to be deleted:")
    for blob, value in diff_snap_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Safe clones to be deleted:")
    for blob, value in diff_clone_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Manual bdeves to be deleted that have wrong ref number:")
    for blob, value in manual_del.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only and force_remove_worng_ref:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Inconsistent bdeves to be checked:")
    for blob, value in inconsistent_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only and force_remove_inconsistent:
            safe_delete_bdev(value['name'], node_id)

    if not validate_only:
        cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_ACTIVE)

    print("#########################################")
    print("All mgmt bdeves to be checked:")
    print(mgmt_diff_dict)
    for blob, value in mgmt_diff_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['type']}")

    if validate_only:
        return not(diff_lvol_dict or diff_snap_dict or diff_clone_dict or manual_del or inconsistent_dict or mgmt_diff_dict)

    return True


def lvs_dump_tree(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_ONLINE:
        logger.error("Storage node is not online")
        return False

    ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
    if not ret:
        logger.error("Failed to get LVol info")
        return False
    lvs_info = ret[0]
    if "uuid" in lvs_info and lvs_info['uuid']:
        lvs_uuid =  lvs_info['uuid']
    else:
        logger.error("Failed to get lvstore uuid")
        return False

    ret = snode.rpc_client().bdev_lvs_dump_tree(lvs_uuid)
    if not ret:
        logger.error("Failed to dump lvstore tree")
        return False

    return json.dumps(ret, indent=2)
