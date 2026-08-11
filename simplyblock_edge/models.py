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
    STATUS_REMOVED = 'removed'  # permanently gone (replaced); slot is retired
    # Gracefully removed by the operator (device-remove); comes back via
    # device-restart.
    STATUS_OFFLINE = 'offline'
    # The monitor detected the backing device is gone/faulted (e.g. EBS
    # force-detach) while the record says it should be serving. IO continues
    # on raid redundancy; device-restart brings it back after reattach.
    STATUS_UNAVAILABLE = 'unavailable'

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
    # Deploy-time choice, 1..6: SPDK reactor cores on this node. Thread
    # placement (app / lvs poller / nvmf pollers) derives from it — see
    # stack.plan_cpu_layout.
    spdk_cpus: int = edge_constants.EDGE_POD_CPU
    partitions: List[EdgePartition] = []
    # The first node added; store index 0 (its store's client port is
    # nvmf_port + 0, the second node's store is nvmf_port + 1).
    is_primary: bool = False
    # The bdev this node's OWN lvstore was created on (empty = not created
    # yet). 2-node: the store mirror; 1-node: the local top. Encodes the
    # topology for idempotent reassembly after restarts.
    lvstore_base: str = ""
    # lvs names this node currently LEADS (fork leadership). Normally its own
    # store only; after a fail-over the survivor also leads the peer's store
    # until fail-back returns it.
    leader_of: List[str] = []
    online_since: str = ""
    # Why the node is in its current (failure) state. Set whenever a flow
    # gives up on a node: without it the ONLY signal a caller gets is a
    # status flip to offline, so an API client can do nothing but poll until
    # its own timeout and report "timed out" — which is what happened on the
    # first live edge run (2026-08-11), hiding the real error entirely.
    status_reason: str = ""

    @property
    def store_index(self) -> int:
        return 0 if self.is_primary else 1

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
    # The node whose lvstore homes this volume (placement is balanced across
    # the two stores on 2-node clusters). Leadership — and therefore which
    # path is ANA-optimized — normally follows the home node.
    home_node_id: str = ""
    client_port: int = 0        # the home store's per-store client port
    status: str = STATUS_ONLINE
    # Optional encryption: a crypto bdev between the lvol and the fabric.
    # AES_XTS keys live in the cluster's KMS (external Vault or LocalKMS) —
    # same key handling as hyperscale lvols; the key name/path derive from
    # the volume uuid (stack.crypto_key_name / stack.volume_dek_path).
    crypto: bool = False
    crypto_bdev: str = ""

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)
