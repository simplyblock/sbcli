# coding=utf-8

from typing import List

from simplyblock_core import constants
from simplyblock_core.models.base_model import BaseModel


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
    cap_warn: int = 80
    cli_pass: str = ""
    cluster_max_devices: int = 0
    cluster_max_nodes: int = 0
    cluster_max_size: int = 0
    db_connection: str = ""
    dhchap: str = ""
    distr_bs: int = 0
    distr_chunk_bs: int = 0
    distr_ndcs: int = 0
    distr_npcs: int = 0
    enable_node_affinity: bool = False
    grafana_endpoint: str = ""
    mode: str = "docker"
    grafana_secret: str = ""
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
    secret: str = ""
    disable_monitoring: bool = False
    strict_node_anti_affinity: bool = False
    tls: bool = False
    is_re_balancing: bool = False
    full_page_unmap: bool = True
    is_single_node: bool = False
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

    def get_clean_dict(self):
        data = super(Cluster, self).get_clean_dict()
        data['status_code'] = self.get_status_code()
        return data

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
        else:
            backup_path =  path.join([self.backup_local_path, path])
        return backup_path
