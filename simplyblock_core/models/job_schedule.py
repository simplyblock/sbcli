# coding=utf-8
import datetime
import time

from simplyblock_core.models.base_model import BaseModel


class JobSchedule(BaseModel):

    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_SUSPENDED = 'suspended'
    STATUS_DONE = 'done'

    FN_DEV_RESTART = "device_restart"
    FN_NODE_RESTART = "node_restart"
    FN_DEV_MIG = "device_migration"
    FN_FAILED_DEV_MIG = "failed_device_migration"
    FN_NEW_DEV_MIG = "new_device_migration"
    FN_NODE_ADD = "node_add"

    canceled: bool = False
    cluster_id: str = ""
    date: int = 0
    device_id: str = ""
    function_name: str = ""
    function_params: dict = {}
    function_result: str = ""
    max_retry: int = -1
    node_id: str = ""
    retry: int = 0

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)


    def get_id(self):
        return "%s/%s/%s" % (self.cluster_id, self.date, self.uuid)
