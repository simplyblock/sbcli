# coding=utf-8
from simplyblock_core.models.base_model import BaseModel

class Deployer(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_READONLY = 'read_only'
    STATUS_INACTIVE = "inactive"
    STATUS_SUSPENDED = "suspended"
    STATUS_DEGRADED = "degraded"

    DOCKER_PULL = "dockerpull"
    TF_STATE_INIT = "tfstate_init"
    TF_PLAN = "tfplan"
    TF_APPLY = "tfapply"

    STATUS_CODE_MAP = {
        DOCKER_PULL: 0,
        TF_STATE_INIT: 1,
        TF_PLAN: 2,
        TF_APPLY: 3,
    }

    availability_zone: str = ""
    ecr_account_id: str = ""
    ecr_image_tag: str = ""
    ecr_region: str = ""
    ecr_repository_name: str = ""
    extra_nodes: int = 0
    extra_nodes_instance_type: str = ""
    mgmt_nodes: int = 0
    mgmt_nodes_instance_type: str = ""
    nr_hugepages: int = 0
    region: str = ""
    sbcli_cmd: str = ""
    sbcli_pkg_version: str = ""
    storage_nodes: int = 0
    storage_nodes_ebs_size1: int = 0
    storage_nodes_ebs_size2: int = 0
    storage_nodes_instance_type: str = ""
    tf_logs_bucket_name: str = ""
    tf_output: str = ""
    tf_state_bucket_name: str = ""
    tf_state_bucket_region: str = ""
    tf_workspace: str = ""
    volumes_per_storage_nodes: int = 0
    whitelist_ips: str = ""


    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1


