# coding=utf-8
"""Edge-cluster data models (docs/edge_clusters_spec.md §3).

All records use cluster-prefixed composite keys ({cluster_id}/{uuid}) so every
read is a bounded FDB range read — no full-table scans.
"""
from typing import List

from pydantic import SecretStr

from simplyblock_core.models.base_model import BaseModel, BaseNodeObject
from simplyblock_edge import constants as edge_constants


class EdgePartition(BaseModel):
    """A partition/device a node contributes to its local stack (nested on
    EdgeNode, not persisted standalone)."""

    STATUS_ONLINE = 'online'
    STATUS_FAILED = 'failed'
    STATUS_NEW = 'new'          # added, awaiting raid grow
    STATUS_REMOVED = 'removed'

    device_path: str = ""       # e.g. /dev/nvme0n1p4
    size: int = 0
    bdev_name: str = ""         # assigned by the stack planner
    status: str = STATUS_ONLINE


class EdgeNode(BaseNodeObject):
    """One edge worker node. Status vocabulary is inherited from
    BaseNodeObject (online/offline/unreachable/down/in_creation/in_restart/
    removed) — see spec §6.1 for which transitions the monitor owns."""

    cluster_id: str = ""
    hostname: str = ""          # kubernetes node name (nodeSelector + liveness key)
    mgmt_ip: str = ""           # node InternalIP; RPC endpoint
    data_ip: str = ""           # nvmf listener address (defaults to mgmt_ip)
    rpc_port: int = edge_constants.EDGE_RPC_PORT
    rpc_username: str = ""
    rpc_password: SecretStr = SecretStr("")
    nvmf_port: int = edge_constants.EDGE_NVMF_PORT
    repl_port: int = edge_constants.EDGE_REPL_PORT
    partitions: List[EdgePartition] = []
    # The primary hosts the lvstore and the client subsystems; the first node
    # added to the cluster becomes primary.
    is_primary: bool = False
    # Primary only: the bdev the lvstore was created on (empty = no lvstore
    # yet). Created lazily — at first volume create, or at second-node add so
    # it can sit on the cross-node mirror (spec §5.2/§10). Also encodes the
    # topology for idempotent reassembly after restarts.
    lvstore_base: str = ""
    online_since: str = ""

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def get_data_ip(self):
        return self.data_ip or self.mgmt_ip


class EdgeVolume(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'

    cluster_id: str = ""
    # NB: not "name" — BaseModel reserves self.name for the class name, which
    # is part of the FDB key (object/{name}/{id}); shadowing it corrupts the
    # keyspace (same reason Cluster uses cluster_name).
    volume_name: str = ""       # unique per cluster (enforced at create)
    size: int = 0
    lvol_bdev: str = ""         # "{lvs}/{name}"
    nqn: str = ""
    ns_id: int = 1
    status: str = STATUS_ONLINE

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)
