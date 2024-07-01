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

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "region": {"type": str, 'default': ""},
        "availability_zone": {"type": str, 'default': ""},
        "sbcli_cmd": {"type": str, 'default': ""},
        "sbcli_pkg_version": {"type": str, 'default': ""},
        "whitelist_ips": {"type": str, 'default': ""},  # todo: make this a list
        "mgmt_nodes": {"type": int, 'default': 0},
        "storage_nodes": {"type": int, 'default': 0},
        "extra_nodes": {"type": int, 'default': 0},
        "mgmt_nodes_instance_type": {"type": str, 'default': ""},
        "storage_nodes_instance_type": {"type": str, 'default': ""},
        "extra_nodes_instance_type": {"type": str, 'default': ""},
        "storage_nodes_ebs_size1": {"type": int, 'default': 0},  # size in GB
        "storage_nodes_ebs_size2": {"type": int, 'default': 0},  # size in GB
        "volumes_per_storage_nodes": {"type": int, 'default': 0},
        "nr_hugepages": {"type": int, 'default': 0},
        "tf_state_bucket_name": {"type": str, 'default': ""},
        "tf_state_bucket_region": {"type": str, 'default': ""},
        "tf_workspace": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},
        "tf_logs_bucket_name": {"type": str, 'default': ""},
        "ecr_account_id": {"type": str, 'default': ""},
        "ecr_region": {"type": str, 'default': ""},
        "ecr_repository_name": {"type": str, 'default': ""},
        "ecr_image_tag": {"type": str, 'default': ""},
        "tf_output": {"type": str, 'default': ""},
    }

    def __init__(self, data=None):
        super(Deployer, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_status_code(self):
        return self.status
