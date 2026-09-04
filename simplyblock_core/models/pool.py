
from typing import ClassVar

from pydantic import SecretStr

from simplyblock_core.models.base_model import BaseModel, default_factory


class Pool(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_INACTIVE = "inactive"

    _STATUS_CODE_MAP: ClassVar[dict] = {
        STATUS_ACTIVE: 1,
        STATUS_INACTIVE: 2,
    }

    cluster_id: str = ""
    groups: list[str] = default_factory(list)
    lvol_max_size: int = 0
    lvols: int = 0
    max_r_mbytes_per_sec: int = 0
    max_rw_ios_per_sec: int = 0
    max_rw_mbytes_per_sec: int = 0
    max_w_mbytes_per_sec: int = 0
    pool_max_size: int = 0
    pool_name: str = ""
    numeric_id: int = 0
    secret: SecretStr = SecretStr("")  # unused
    users: list[str] = default_factory(list)
    qos_host: str = ""
    cr_name: str = ""
    cr_namespace: str = ""
    cr_plural: str = ""
    lvols_cr_name: str = ""
    lvols_cr_namespace: str = ""
    lvols_cr_plural: str = ""
    sec_options: dict = default_factory(dict)
    dhchap: bool = False
    dhchap_key: SecretStr = SecretStr("")
    dhchap_ctrlr_key: SecretStr = SecretStr("")
    allowed_hosts: list[str] = default_factory(list)


    def has_qos(self):
        return 0 < (self.max_rw_ios_per_sec + self.max_rw_mbytes_per_sec + self.max_r_mbytes_per_sec + self.max_w_mbytes_per_sec)
