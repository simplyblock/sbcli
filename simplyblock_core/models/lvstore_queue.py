# coding=utf-8
import datetime

from simplyblock_core.models.base_model import BaseModel


class LVStoreQueueTask(BaseModel):

    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_SUSPENDED = 'suspended'
    STATUS_DONE = 'done'

    FN_LVOL_ADD = "LVOL_ADD"
    FN_SNAPSHOT_ADD = "SNAPSHOT_ADD"

    cluster_id: str = ""
    date: int = 0
    function_name: str = ""
    function_params: dict = {}
    function_result: str = ""
    retry: int = 0
    primary_node_id: str = ""
    secondary_node_id: str = ""
    lvstore: str = ""

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)
