# coding=utf-8
import os.path
from typing import List, Optional

from pydantic import SecretStr

from simplyblock_core import constants
from simplyblock_core.models.base_model import BaseModel


class HashicorpVaultSettings(BaseModel):
    base_url: str = ""
    transit_mount: str = "simplyblock/transit"
    kv_mount: str = "simplyblock/kv"
    cert_role: str = "simplyblock-webappapi"


class Cluster(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_READONLY = 'read_only'
    STATUS_INACTIVE = "inactive"
    STATUS_SUSPENDED = "suspended"
    STATUS_DEGRADED = "degraded"
    STATUS_UNREADY = "unready"
    STATUS_IN_ACTIVATION = "in_activation"
    STATUS_IN_EXPANSION = "in_expansion"

    STATUS_CODE_MAP = {
        STATUS_ACTIVE: 1,
        STATUS_INACTIVE: 2,
        STATUS_READONLY: 3,

        STATUS_SUSPENDED: 10,
        STATUS_DEGRADED: 11,
        STATUS_UNREADY: 12,
        STATUS_IN_ACTIVATION: 13,
        STATUS_IN_EXPANSION: 14,

    }

    auth_hosts_only: bool = False
    blk_size: int = 0
    cap_crit: int = 90
    # ISO-8601 UTC timestamp of when the cluster last entered IN_ACTIVATION.
    # Stamped/cleared by cluster_ops.set_cluster_status and consumed by the
    # storage_node_monitor watchdog to detect and revert a wedged activation
    # (incident 2026-06-25). Empty string means "not currently activating".
    in_activation_since: str = ""
    # ISO-8601 UTC timestamp refreshed every ~60s by the heartbeat thread that
    # cluster_activate runs for its whole duration. Lets the watchdog tell a
    # LIVE long activation (heartbeat fresh — leave it alone, up to the
    # absolute budget) from a DEAD one (driver process/container gone,
    # heartbeat stale — revert after minutes instead of the node-scaled
    # budget, which is 42 min on a 32-node cluster; incident 2026-07-13).
    # Empty on records from before this field existed -> watchdog falls back
    # to the absolute budget alone.
    activation_heartbeat: str = ""
    cap_warn: int = 80
    cli_pass: SecretStr = SecretStr("")
    cluster_max_devices: int = 0
    cluster_max_nodes: int = 0
    cluster_max_size: int = 0
    db_connection: SecretStr = SecretStr("")
    dhchap: str = ""
    distr_bs: int = 0
    distr_chunk_bs: int = 0
    distr_ndcs: int = 0
    distr_npcs: int = 0
    enable_node_affinity: bool = False
    # Undocumented chaos-testing switch: when set at cluster create, a transparent
    # SPDK delay bdev is inserted between each device's nvme bdev and its alceml so
    # a device can be made to "hang" on demand (see device_controller device-hang).
    enable_hang_device: bool = False
    grafana_endpoint: str = ""
    mode: str = "docker"
    grafana_secret: SecretStr = SecretStr("")
    contact_point: str = ""
    ha_type: str = "single"
    inflight_io_threshold: int = 4
    iscsi: str = ""
    max_queue_size: int = 128
    model_ids: List[str] = []
    cluster_name: str = None # type: ignore[assignment]
    nqn: str = ""
    page_size_in_blocks: int = 2097152
    prov_cap_crit: int = 190
    prov_cap_warn: int = 180
    qpair_count: int = 32
    fabric_tcp: bool = True
    fabric_rdma: bool = False
    client_qpair_count: int = 3
    secret: SecretStr = SecretStr("")
    cr_name: str = ""
    cr_namespace: str = ""
    cr_plural: str = ""
    disable_monitoring: bool = False
    strict_node_anti_affinity: bool = False
    tls: bool = False
    tls_config: dict = {}
    is_re_balancing: bool = False
    # Suspend-recovery drain phase marker.
    #
    # When a cluster becomes SUSPENDED, recovering it by auto-restarting nodes
    # piecemeal is unsafe (stale/half state, e.g. a node left lvstore_status
    # "in_creation" by a failed activation that then never gets health-checked).
    # Instead the monitor first force-shuts-down every still-up node so the
    # cluster reaches a clean all-offline slate, and PAUSES auto-restart until
    # then; only afterwards is the existing auto-restart allowed to bring the
    # nodes back (all of them except the ones deliberately stopped by an
    # operator, i.e. auto_restart_disabled).
    #
    # False  -> drain still in progress (or no suspension): auto-restart paused,
    #           auto-shutdown actively draining ONLINE/DOWN nodes.
    # True   -> the all-offline drain completed for this suspension episode:
    #           auto-restart resumes and the auto-shutdown stops (it must not
    #           re-kill nodes that are restarting back up).
    # Reset to False when the cluster returns to a healthy status
    # (active/degraded/read_only) in cluster_ops.set_cluster_status.
    suspend_drain_complete: bool = False
    # Cluster-wide data placement-binding mode for distrib bdevs.
    #   False = legacy per-page placement binding (default, safe everywhere)
    #   True  = new per-chunk placement binding (opt-in, propagated to every
    #           bdev_distrib_create at restart and flipped at runtime via the
    #           distr_shared_placement RPC)
    # Set by cluster_ops.set_shared_placement after a preflight check
    # (status=active, not rebalancing, all nodes online). Persisted here so
    # subsequent restarts re-create distrib bdevs with the same flag.
    shared_placement: bool = False
    # Armed when the cluster should be auto-switched to per-chunk placement
    # once it is fully settled (ACTIVE, not rebalancing, all nodes online):
    #   * set at cluster creation (new clusters migrate after first rebalance)
    #   * set by cluster_ops.update_cluster ONLY after every node restart of an
    #     upgrade has completed — never mid rolling-restart, so the monitor
    #     cannot fire on a transiently-quiet cluster
    # storage_node_monitor consumes this flag, calls set_shared_placement once,
    # and clears it. shared_placement (above) is the durable "done" marker.
    shared_placement_migration_pending: bool = False
    full_page_unmap: bool = True
    is_single_node: bool = False
    # Failure-domain anti-affinity. When True, every storage node carries an
    # operator-supplied failure_domain tag (rack/cabinet/DC) and placement
    # spreads data/parity chunks, HA journal copies and secondary/tertiary
    # nodes across distinct failure domains (best-effort: falls back to
    # host-disjoint placement when a domain-disjoint placement is impossible).
    # Deploy-time only — set at cluster create/add, never toggled at runtime;
    # an existing cluster must be redeployed to gain the feature.
    enable_failure_domain: bool = False
    snapshot_replication_target_cluster: str = ""
    snapshot_replication_target_pool: str = ""
    snapshot_replication_timeout: int = 60*10
    client_data_nic: str = ""
    max_fault_tolerance: int = 1
    backup_config: dict = {}
    backup_source: str = ""  # active backup source cluster_id ("" = local)
    backup_timeout_seconds: int = 14400  # 4 hours default
    nvmf_base_port: int = 4420
    rpc_base_port: int = 8080
    snode_api_port: int = 50001
    container_image_prefix: str = ""
    hashicorp_vault_settings: Optional[HashicorpVaultSettings] = None

    # Single-node-expansion resumability cursor. Empty dict means no expansion
    # is in flight. Populated/advanced/cleared via the helpers in
    # ``simplyblock_core.controllers.cluster_expansion.planner``. See the feature plan
    # ``single_node_expansion_plan.md`` for the schema.
    expand_state: dict = {}
    backup_local_path: str = constants.KVD_DB_BACKUP_PATH
    backup_frequency_seconds: int = 3*60*60
    backup_s3_bucket: str = ""
    backup_s3_region: str = ""
    backup_s3_cred: str = ""

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1

    def is_qos_set(self) -> bool:
        # Import is here is to avoid circular import dependency
        from simplyblock_core.db_controller import DBController
        db_controller = DBController()
        qos_classes = db_controller.get_qos(self.get_id())
        if len(qos_classes) > 1:
            return True
        return False

    def get_backup_path(self, path=""):
        if self.backup_s3_bucket and self.backup_s3_cred:
            backup_path = f"blobstore://{self.backup_s3_cred}@s3.{self.backup_s3_region}.amazonaws.com/{path}?bucket={self.backup_s3_bucket}" \
                          + f"&region={self.backup_s3_region}&sc=0"
        elif self.backup_local_path:
            backup_path = os.path.join(self.backup_local_path, path)
        else:
            backup_path = os.path.join(constants.KVD_DB_BACKUP_PATH, self.uuid, path)
        return backup_path


class ClusterAddNodeLock(BaseModel):
    """Cluster-scoped mutex held while a node-add performs its cross-node mesh
    wiring (connect to remote devices, go ONLINE, make peers reverse-connect,
    push the cluster map). Only this section needs serializing; the slow,
    node-local part of add_node (SPDK boot, local device prep) runs in parallel
    across concurrent node-add tasks.

    Keyed by ``cluster_id`` so there is at most one in-flight mesh section per
    cluster. ``heartbeat_at`` is refreshed by the holder while the section runs;
    a lock whose heartbeat is older than ``CLUSTER_ADD_LOCK_TTL_SEC`` is
    considered abandoned (holder crashed) and may be reclaimed.
    """

    cluster_id: str = ""
    owner: str = ""
    acquired_at: int = 0
    heartbeat_at: int = 0

    def get_id(self):
        return self.cluster_id or self.uuid


class PortReservation(BaseModel):
    """Short-lived reservation of an NVMe-oF port during node add.

    ``add_node`` allocates a node's rpc/nvmf ports long before the node record
    is persisted (SPDK boots in between), so two concurrent adds would otherwise
    read the same "next free" port. A reservation makes the chosen port visible
    to other allocators immediately and transactionally. Once the node record is
    persisted its own port fields keep the port reserved; the reservation is
    redundant after that and is reclaimed by ``created_at`` TTL
    (``PORT_RESERVATION_TTL_SEC``) — no explicit release is required, and a
    crashed add self-heals when the reservation goes stale.
    """

    cluster_id: str = ""
    port: int = 0
    owner: str = ""
    created_at: int = 0

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.port)
