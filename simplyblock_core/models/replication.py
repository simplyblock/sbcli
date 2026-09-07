"""Replication targets and policies.

Three levels: a source cluster has any number of **targets** (named
destinations), each target has one or more **policies** (which own the cadence),
and a volume optionally references one policy. Structurally this mirrors
``BackupPolicy`` / ``BackupPolicyAttachment`` in ``models/backup.py``, including
the ``cluster_id/uuid`` composite id.
"""
from typing import ClassVar
import datetime

from simplyblock_core.models.base_model import BaseModel


class ReplicationTarget(BaseModel):
    """A named replication destination of a source cluster.

    Replaces the single ``Cluster.snapshot_replication_target_*`` triple, which
    could only hold one destination and was overwritten by every
    ``cluster add-replication``.
    """

    STATUS_ACTIVE = 'active'
    STATUS_INACTIVE = 'inactive'

    _STATUS_CODE_MAP: ClassVar[dict] = {
        STATUS_ACTIVE: 0,
        STATUS_INACTIVE: 1,
    }

    cluster_id: str = ""          # SOURCE cluster this target belongs to
    target_name: str = ""         # unique per source cluster
    target_cluster_id: str = ""
    # Always a UUID: resolving a pool NAME lazily is what made add_replication
    # raise KeyError despite advertising "ID or name".
    target_pool_uuid: str = ""
    timeout_sec: int = 60 * 10
    status: str = STATUS_ACTIVE

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.UTC))
        super().write_to_db(kv_store)


class ReplicationPolicy(BaseModel):
    """Cadence, mode and retention shared by a group of volumes."""

    STATUS_ACTIVE = 'active'
    STATUS_INACTIVE = 'inactive'

    _STATUS_CODE_MAP: ClassVar[dict] = {
        STATUS_ACTIVE: 0,
        STATUS_INACTIVE: 1,
    }

    MODE_FAILOVER = "failover"
    MODE_MIGRATION = "migration"

    # A replicated snapshot holds only its own clusters; deleting one
    # swap-merges its segments into the successor CHAINED to it. Keep fewer than
    # a pair and an arriving snapshot has nothing to chain onto, so the target
    # ends up holding the newest delta over holes (see commit b34bb8d96).
    MIN_KEEP_REPLICATED = 2

    cluster_id: str = ""          # SOURCE cluster
    policy_name: str = ""         # unique per source cluster
    target_id: str = ""           # ReplicationTarget.get_id()
    interval_min: int = 1         # internal snapshot cadence, 0 = user snaps only
    mode: str = MODE_FAILOVER
    keep_replicated: int = MIN_KEEP_REPLICATED
    #: Tiered retention, e.g. "15m:2h,1h:11h,1d:7d" -- one snapshot every 15
    #: minutes for the last 2 hours, then hourly for 11 hours, then daily for
    #: 7 days. Empty keeps the flat keep_replicated behaviour. A schedule
    #: never overrides MIN_KEEP_REPLICATED: the newest pair is always kept so
    #: an arriving delta has a predecessor to chain onto.
    retention_schedule: str = ""
    status: str = STATUS_ACTIVE

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.UTC))
        super().write_to_db(kv_store)
