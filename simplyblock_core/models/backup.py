# coding=utf-8
import datetime
from typing import List

from simplyblock_core.models.backup_config import BackupLocation
from simplyblock_core.models.base_model import BaseModel


class Backup(BaseModel):

    STATUS_PENDING = 'pending'
    STATUS_IN_PROGRESS = 'in_progress'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'
    STATUS_MERGING = 'merging'
    STATUS_MERGED = 'merged'
    STATUS_DELETING = 'deleting'

    _STATUS_CODE_MAP = {
        STATUS_PENDING: 0,
        STATUS_IN_PROGRESS: 1,
        STATUS_COMPLETED: 2,
        STATUS_FAILED: 3,
        STATUS_MERGING: 4,
        STATUS_DELETING: 5,
        STATUS_MERGED: 6,
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
    # Security params from the source lvol (for cross-cluster restore)
    allowed_hosts: List[dict] = []
    #: Where this backup's objects live and how to interpret them, as a
    #: ``BackupLocation``. Stored as a dict because ``BaseModel`` cannot nest
    #: pydantic models; read it through :meth:`get_location`.
    location: dict = {}
    encrypted: bool = False
    #: Which KMS holds this backup's key, and under what path. A
    #: ``backup_manifest.Encryption``; stored as a dict for the same reason
    #: ``location`` is. Empty for an unencrypted backup.
    encryption: dict = {}

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def get_location(self) -> BackupLocation:
        """Validate and return where this backup's objects live.

        Raises:
            ValueError: The backup predates self-describing locations, or its
                recorded location is not valid. Either way it cannot be read
                without knowing what wrote it. ``ValidationError`` is a
                ``ValueError``, so one except clause covers both.
        """
        if not self.location:
            raise ValueError(
                f"Backup {self.uuid} has no recorded location "
                "(created before backups became self-describing)")

        return BackupLocation.model_validate(self.location)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)


class BackupChainLock(BaseModel):
    """Short-lived lock used while enqueuing a snapshot backup chain."""

    snapshot_id: str = ""
    requested_snapshot_id: str = ""
    lvol_id: str = ""
    created_at: int = 0

    def get_id(self):
        return self.snapshot_id or self.uuid


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
    backup_schedule: str = ""  # e.g. "15m,4 60m,11 24h,7"

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
