# coding=utf-8
"""Baseline cluster-state builders for the expansion simulator.

Each scenario builder writes a freshly-activated cluster to FDB AND
populates the per-node :class:`RpcServerSim` instances so that the state
matches what would exist after a successful ``cluster_activate`` — without
actually walking through the activation code path. The expansion code
under test starts from the post-activation state.

Phase 1 provides the 4-node FTT1 baseline. Phase 4 will add the 6-node
FTT2 (2+2) and 8-node baselines.
"""

from __future__ import annotations

import datetime
from typing import Dict, List, Tuple

from tests.expansion_sim._rpc_sim import (
    BdevSim,
    ClusterSim,
    RpcServerSim,
    SubsystemSim,
)


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _make_cluster_record(uuid: str, ftt: int):
    from simplyblock_core.models.cluster import Cluster
    c = Cluster()
    c.uuid = uuid
    c.cluster_name = f"sim-cluster-{uuid[:8]}"
    c.mode = "docker"
    c.ha_type = "ha"
    c.max_fault_tolerance = ftt
    c.distr_ndcs = 2
    c.distr_npcs = max(ftt, 2)  # FTT1 → 1 chunk; min 2 in this codebase
    c.distr_bs = 4096
    c.distr_chunk_bs = 4096
    c.page_size_in_blocks = 2097152
    c.client_qpair_count = 3
    c.status = Cluster.STATUS_ACTIVE
    c.nqn = f"nqn.2023-02.io.simplyblock:{uuid}"
    c.nvmf_base_port = 4420
    c.rpc_base_port = 8080
    c.snode_api_port = 50001
    c.create_dt = _now_iso()
    return c


def _make_node_record(uuid: str, cluster_id: str,
                      mgmt_ip: str, rpc_port: int,
                      lvstore: str = "",
                      secondary_node_id: str = "",
                      secondary_node_id_2: str = "",
                      lvstore_stack_secondary_1: str = "",
                      lvstore_stack_secondary_2: str = "",
                      jm_vuid: int = 0):
    from simplyblock_core.models.storage_node import StorageNode
    from simplyblock_core.models.iface import IFace
    from simplyblock_core.models.hublvol import HubLVol

    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster_id
    n.hostname = f"sim-{uuid[:8]}"
    n.status = StorageNode.STATUS_ONLINE
    n.mgmt_ip = mgmt_ip
    n.rpc_port = rpc_port
    n.rpc_username = "u"
    n.rpc_password = "p"
    n.lvstore = lvstore
    n.lvstore_status = "ready" if lvstore else ""
    n.lvol_subsys_port = 4420
    n.secondary_node_id = secondary_node_id
    n.secondary_node_id_2 = secondary_node_id_2
    n.lvstore_stack_secondary_1 = lvstore_stack_secondary_1
    n.lvstore_stack_secondary_2 = lvstore_stack_secondary_2
    n.lvstore_stack = []
    n.lvstore_ports = {lvstore: {"lvol_subsys_port": 4420,
                                  "hublvol_port": 4421}} if lvstore else {}
    n.active_tcp = True
    n.active_rdma = False
    n.jm_vuid = jm_vuid
    n.enable_ha_jm = False
    n.raid = "raid0"
    n.number_of_distribs = 1  # one distrib per node — keeps lvstore_stack simple
    n.alceml_cpu_cores = []
    n.alceml_worker_cpu_cores = []
    n.alceml_cpu_index = 0
    n.alceml_worker_cpu_index = 0
    n.create_dt = _now_iso()
    nic = IFace()
    nic.ip4_address = mgmt_ip
    nic.trtype = "TCP"
    nic.if_name = "eth0"
    nic.status = "online"
    n.data_nics = [nic]
    if lvstore:
        n.hublvol = HubLVol({
            "nvmf_port": 4421,
            "uuid": f"hub-{uuid}",
            "nqn": f"nqn.hub.{uuid}",
            "bdev_name": f"hub_{uuid}",
            "model_number": "sim",
            "nguid": "0" * 32,
        })
    else:
        n.hublvol = None
    return n


def _populate_sim_for_primary(sim: RpcServerSim, node, primary_lvstore_stack: List[dict]):
    """Reflect the post-activation state of a primary node in its
    simulator: lvstore bdev, hublvol bdev, hublvol subsystem with listener,
    and cluster_id-derived JM bdev. Phase 1 keeps this minimal."""
    # Hublvol bdev + subsystem + listener
    if node.hublvol is not None:
        sim.bdevs[node.hublvol.bdev_name] = BdevSim(
            name=node.hublvol.bdev_name, type="hublvol")
        sim.subsystems[node.hublvol.nqn] = SubsystemSim(
            nqn=node.hublvol.nqn,
            serial_number=node.hublvol.uuid,
            model_number="sim",
            min_cntlid=1,
            listeners=[(node.mgmt_ip, node.hublvol.nvmf_port, "optimized")],
        )
    # Lvstore bdev
    if node.lvstore:
        sim.bdevs[node.lvstore] = BdevSim(name=node.lvstore, type="bdev_lvstore")
    # Reflect the lvstore_stack entries as bdevs of their declared type.
    for bdev_dict in primary_lvstore_stack:
        name = bdev_dict.get("name")
        btype = bdev_dict.get("type")
        if name and name not in sim.bdevs:
            sim.bdevs[name] = BdevSim(name=name, type=btype, params=dict(bdev_dict))


def build_4_node_ftt1_baseline(cluster_sim: ClusterSim, db) -> Tuple[str, List[str], str]:
    """4-node FTT1 cluster + a 5th node ready to be added.

    Layout (as cluster_activate would produce):
        n1: primary=LVS_n1, sec_1 of LVS_n4
        n2: primary=LVS_n2, sec_1 of LVS_n1
        n3: primary=LVS_n3, sec_1 of LVS_n2
        n4: primary=LVS_n4, sec_1 of LVS_n3

    Writes ``Cluster`` + 5 ``StorageNode`` records to FDB, registers 5
    :class:`RpcServerSim` instances on ``cluster_sim``, and populates the
    4 primaries' simulator state to mimic post-activation.

    Returns
    -------
    tuple
        (cluster_id, [n1..n4 ids], n5_id)
    """
    cluster_id = "cl-ftt1-4"
    cluster = _make_cluster_record(cluster_id, ftt=1)
    cluster.write_to_db(db)

    # Write a ClusterStatObject so DBController.get_cluster_capacity
    # returns a usable size_total (the executor's create-primary path
    # consults it to compute max_size for create_lvstore).
    from simplyblock_core.models.stats import ClusterStatObject
    stat = ClusterStatObject()
    stat.uuid = cluster_id
    stat.cluster_id = cluster_id
    stat.size_total = 4 * 1024 ** 4  # 4 TiB
    stat.size_used = 0
    stat.size_free = stat.size_total
    stat.create_dt = _now_iso()
    stat.write_to_db(db)

    node_ids = [f"n{i}" for i in range(1, 5)]  # n1..n4
    new_node_id = "n5"

    # Rotation: secondary(LVS_i) = nodes[i mod k]
    # LVS_1 sec on n2, LVS_2 sec on n3, LVS_3 sec on n4, LVS_4 sec on n1.
    sec_of = {  # primary_id -> node hosting that LVS as sec_1
        "n1": "n2", "n2": "n3", "n3": "n4", "n4": "n1",
    }
    # Inverse: which primary's LVS each node hosts as sec_1
    primary_of_sec = {v: k for k, v in sec_of.items()}

    # Build the node records.
    nodes_by_id = {}
    for i, nid in enumerate(node_ids):
        n = _make_node_record(
            uuid=nid,
            cluster_id=cluster_id,
            mgmt_ip=f"10.10.0.{i + 1}",
            rpc_port=8080 + i,
            lvstore=f"LVS_{nid}",
            secondary_node_id=sec_of[nid],
            lvstore_stack_secondary_1=primary_of_sec.get(nid, ""),
            jm_vuid=1000 + i,
        )
        # Realistic-ish primary lvstore_stack: one distrib + one raid +
        # one lvstore. Each distrib carries a unique vuid so
        # ``utils.get_random_vuid`` can dedupe across the cluster when
        # creating new distribs for the newcomer.
        distrib_name = f"distr_{nid}_0"
        raid_name = f"raid_{nid}"
        distrib_vuid = 5000 + i  # deterministic, unique per existing primary
        n.lvstore_stack = [
            {"type": "bdev_distr", "name": distrib_name,
             "params": {"vuid": distrib_vuid, "name": distrib_name}},
            {"type": "bdev_raid", "name": raid_name,
             "params": {"strip_size_kb": 32},
             "distribs_list": [distrib_name]},
            {"type": "bdev_lvstore", "name": f"LVS_{nid}", "params": {}},
        ]
        n.write_to_db(db)
        nodes_by_id[nid] = n

    # Newcomer: registered, online, no lvstore yet.
    n5 = _make_node_record(
        uuid=new_node_id, cluster_id=cluster_id,
        mgmt_ip="10.10.0.5", rpc_port=8084,
        lvstore="", jm_vuid=1004,
    )
    n5.write_to_db(db)
    nodes_by_id[new_node_id] = n5

    # Spin up simulators for all 5 nodes.
    for nid, node in nodes_by_id.items():
        sim = RpcServerSim(
            node_id=nid,
            mgmt_ip=node.mgmt_ip,
            rpc_port=node.rpc_port,
        )
        cluster_sim.add_server(sim)
        _populate_sim_for_primary(sim, node, node.lvstore_stack)

    return cluster_id, node_ids, new_node_id
