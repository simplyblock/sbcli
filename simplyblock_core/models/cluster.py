# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel


class Cluster(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_READONLY = 'read_only'
    STATUS_INACTIVE = "inactive"
    STATUS_SUSPENDED = "suspended"
    STATUS_DEGRADED = "degraded"
    STATUS_UNREADY = "unready"
    STATUS_IN_ACTIVATION = "in_activation"

    STATUS_CODE_MAP = {
        STATUS_ACTIVE: 1,
        STATUS_INACTIVE: 2,
        STATUS_READONLY: 3,

        STATUS_SUSPENDED: 10,
        STATUS_DEGRADED: 11,

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
    enable_qos: bool = False
    grafana_endpoint: str = ""
    grafana_secret: str = ""
    ha_type: str = "single"
    inflight_io_threshold: int = 4
    iscsi: str = ""
    max_queue_size: int = 128
    model_ids: List[str] = []
    nqn: str = ""
    page_size_in_blocks: int = 2097152
    prov_cap_crit: int = 190
    prov_cap_warn: int = 180
    qpair_count: int = 6
    secret: str = ""
    status: str = ""
    strict_node_anti_affinity: bool = False
    tls: bool = False
    updated_at: str = ""
    uuid: str = ""

    def __init__(self, data=None):
        super(Cluster, self).__init__(data=data)

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1

    def get_clean_dict(self):
        data = super(Cluster, self).get_clean_dict()
        data['status_code'] = self.get_status_code()
        return data




lst = []
for k, v in Cluster().attributes.items():
    if hasattr(v['type'], "__origin__"):
        if hasattr(v['type'], "__args__"):
            lst.append(f"{k}: {v['type']._name}[{v['type'].__args__[0].__name__}] = {v['default']} ")
        else:
            lst.append(f"{k}: {v['type']._name} = {v['default']} ")
        # print(v['type'].__dict__)
    elif v['type'] == str:
        lst.append(f"{k}: {v['type'].__name__} = \"{v['default']}\"")

    else:
        lst.append(f"{k}: {v['type'].__name__} = {v['default']}")

lst.sort()
for l in lst:
    print(l)
