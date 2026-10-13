import datetime
from typing import ClassVar

from simplyblock_core.models.backup_config import BackupLocation
from simplyblock_core.models.base_model import BaseModel, default_factory
from simplyblock_core.models.indices import Index


class Backup(BaseModel):

    _WATCHED = True

    # get_id() is "<cluster>/<uuid>", so a caller holding only the uuid cannot
    # point-read the record; `uuid` is the index that lets it.
    _INDEXES: ClassVar[tuple] = (
        Index('uuid'),
        Index('lvol_id'),
        Index('snapshot_id'),
        Index('prev_backup_id'),
        # A standalone backup's group_backup_id is blank, which the index drops
        # (models/indices.py), so only the members of a group backup are indexed.
        Index('group_backup_id'),
    )

    STATUS_PENDING = 'pending'
    STATUS_IN_PROGRESS = 'in_progress'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'
    STATUS_MERGING = 'merging'
    STATUS_MERGED = 'merged'
    STATUS_DELETING = 'deleting'

    _STATUS_CODE_MAP: ClassVar[dict] = {
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
    #: Where this backup's objects live and how to interpret them, as a
    #: ``BackupLocation``. Stored as a dict because ``BaseModel`` cannot nest
    #: pydantic models; read it through :meth:`get_location`.
    location: dict = default_factory(dict)
    #: Kept beside `encryption` rather than derived from it, because a record
    #: written before backups described their own keys says only this much, and
    #: reading that as "not encrypted" would restore a plaintext volume over
    #: ciphertext.
    encrypted: bool = False

    #: Which KMS holds this backup's key, and under what path. A
    #: ``backup_manifest.KeyDescriptor``; stored as a dict for the same reason
    #: ``location`` is. Empty for an unencrypted backup, and for one whose
    #: record predates self-describing backups.
    encryption: dict = default_factory(dict)

    #: When this backup is a member of a group backup -- a consistency-group
    #: generation backed up as one unit -- the ``GroupBackup.get_id()`` it
    #: belongs to and the generation sequence. Blank/0 for a standalone
    #: single-volume backup, so the ``group_backup_id`` index skips standalone
    #: backups (design-consistency-group-backup.md §4.1).
    group_backup_id: str = ""
    group_seq: int = 0

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def watch_scope(self):
        return (self.cluster_id,)

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
        self.updated_at = str(datetime.datetime.now(datetime.UTC))
        super().write_to_db(kv_store)


class GroupBackup(BaseModel):
    """A consistency-group backup: the member snapshots of one group generation
    backed up to S3 as one all-or-nothing unit.

    The crash-consistency comes from the generation, not this record: the member
    snapshots are a single point in time because one frozen
    ``bdev_lvol_snapshot_group`` call took them (design-consistency-groups.md
    §5). This record ties the N per-member :class:`Backup` records together and
    is the control-plane half of the group manifest written to
    ``groups/<uuid>.json``. It is ``completed`` only once every member backup is
    completed and the group manifest is published -- the commit barrier
    (design-consistency-group-backup.md §3.3).
    """

    _WATCHED = True

    _INDEXES: ClassVar[tuple] = (
        # uuid: a caller holding only the uuid (the mirror, a restore) can point
        # read. consistency_group_id: "the group backups of this group" (listing,
        # retention) without a scan.
        Index('uuid'),
        Index('consistency_group_id'),
    )

    STATUS_PENDING = 'pending'
    STATUS_IN_PROGRESS = 'in_progress'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'
    STATUS_DELETING = 'deleting'

    _STATUS_CODE_MAP: ClassVar[dict] = {
        STATUS_PENDING: 0,
        STATUS_IN_PROGRESS: 1,
        STATUS_COMPLETED: 2,
        STATUS_FAILED: 3,
        STATUS_DELETING: 4,
    }

    cluster_id: str = ""
    #: The source consistency group and generation. ``consistency_group_id`` is
    #: ``ConsistencyGroup.get_id()`` ("<cluster>/<uuid>"); ``group_seq`` is the
    #: generation (``last_group_seq`` at the take).
    consistency_group_id: str = ""
    consistency_group_name: str = ""
    group_seq: int = 0
    #: The member backups, one per member snapshot of the generation. Each entry
    #: is ``{"lvol_id": <member lvol id>, "backup_id": <Backup.get_id()>}``.
    members: list = default_factory(list)
    #: Homogeneous across members (design §3.4): a serialized ``BackupLocation``
    #: (never credentials), read through :meth:`get_location`, and one encryption
    #: disposition shared by every member.
    location: dict = default_factory(dict)
    encrypted: bool = False
    size: int = 0
    created_at: int = 0
    completed_at: int = 0
    error_message: str = ""

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def watch_scope(self):
        return (self.cluster_id,)

    def get_location(self) -> BackupLocation:
        """Where the member backups live, shared by all members (design §3.4).

        Raises:
            ValueError: no location was recorded.
        """
        if not self.location:
            raise ValueError(f"GroupBackup {self.uuid} has no recorded location")
        return BackupLocation.model_validate(self.location)

    def member_backup_ids(self) -> list:
        """The ``Backup.get_id()`` of every member, in record order."""
        return [m["backup_id"] for m in self.members]

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.UTC))
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

    _WATCHED = True

    _INDEXES: ClassVar[tuple] = (
        Index('uuid'),
    )

    STATUS_ACTIVE = 'active'
    STATUS_INACTIVE = 'inactive'

    _STATUS_CODE_MAP: ClassVar[dict] = {
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

    def watch_scope(self):
        return (self.cluster_id,)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.UTC))
        super().write_to_db(kv_store)


class BackupPolicyAttachment(BaseModel):

    _INDEXES: ClassVar[tuple] = (
        Index(('target_type', 'target_id')),
    )
    """Links a BackupPolicy to a pool or lvol."""

    cluster_id: str = ""
    policy_id: str = ""
    target_type: str = ""  # "pool" or "lvol"
    target_id: str = ""

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)


class DBBackup(BaseModel):
    """FDB backup metadata."""

    backup_name: str = ""
    cluster_id: str = ""

