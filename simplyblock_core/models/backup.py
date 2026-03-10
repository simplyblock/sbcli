# coding=utf-8
import datetime
from typing import List

from simplyblock_core.models.base_model import BaseModel


class Backup(BaseModel):

    STATUS_PENDING = 'pending'
    STATUS_IN_PROGRESS = 'in_progress'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'
    STATUS_MERGING = 'merging'
    STATUS_DELETING = 'deleting'

    _STATUS_CODE_MAP = {
        STATUS_PENDING: 0,
        STATUS_IN_PROGRESS: 1,
        STATUS_COMPLETED: 2,
        STATUS_FAILED: 3,
        STATUS_MERGING: 4,
        STATUS_DELETING: 5,
    }

    s3_id: int = 0
    cluster_id: str = ""
    lvol_id: str = ""
    lvol_name: str = ""
    snapshot_id: str = ""
    snapshot_name: str = ""
    node_id: str = ""
    prev_backup_id: str = ""
    pool_uuid: str = ""
    size: int = 0
    created_at: int = 0
    completed_at: int = 0
    error_message: str = ""
    # S3 metadata written to metadata bucket
    s3_metadata: dict = {}

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)


class BackupPolicy(BaseModel):

    STATUS_ACTIVE = 'active'
    STATUS_INACTIVE = 'inactive'

    _STATUS_CODE_MAP = {
        STATUS_ACTIVE: 0,
        STATUS_INACTIVE: 1,
    }

    cluster_id: str = ""
    policy_name: str = ""
    max_versions: int = 0
    max_age_seconds: int = 0
    max_age_display: str = ""

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)


class BackupPolicyAttachment(BaseModel):
    """Links a BackupPolicy to a pool or lvol."""

    cluster_id: str = ""
    policy_id: str = ""
    target_type: str = ""  # "pool" or "lvol"
    target_id: str = ""

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)
