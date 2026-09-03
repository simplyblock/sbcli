# coding=utf-8
"""Replication targets and policies.

Three levels: a source cluster has any number of **targets** (named
destinations), each target has one or more **policies** (which own the cadence),
and a volume optionally references one policy. Structurally this mirrors
``BackupPolicy`` / ``BackupPolicyAttachment`` in ``models/backup.py``, including
the ``cluster_id/uuid`` composite id.
"""
from typing import ClassVar
import datetime

from simplyblock_core.models.base_model import BaseModel, default_factory


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
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
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
    #: All volumes attached to this policy form ONE consistency group: they
    #: must share an LVS, cadence snapshots are taken as one frozen group
    #: (bdev_lvol_snapshot_group), and fail-over generations are resolved
    #: group-wide. Auto-creates/deletes a ConsistencyGroup record.
    consistency_group: bool = False
    status: str = STATUS_ACTIVE

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)


class ConsistencyGroup(BaseModel):
    """Auto-managed group record behind a consistency-group policy.

    Created with the policy and removed with it. ``members`` maps lvol id to
    its membership EPOCH:

        {"joined_seq": N, "removed_seq": M}

    A member is included in group generation ``seq`` iff
    ``joined_seq <= seq`` and (``removed_seq == 0`` or ``seq <= removed_seq``).
    Late joiners deliberately do NOT inherit history: they join at
    ``last_group_seq + 1``, i.e. the first group snapshot taken AFTER the
    attach, because earlier group snapshots simply do not contain them.
    """

    cluster_id: str = ""
    policy_id: str = ""           # ReplicationPolicy.get_id()
    #: pinned placement: every member volume lives on this node / LVS. Set by
    #: the first member and enforced for all others.
    node_id: str = ""
    lvs_name: str = ""
    #: monotonically increasing generation counter; group snapshot N stamps
    #: every member snapshot it takes with group_seq = N.
    last_group_seq: int = 0
    members: dict = default_factory(dict)
    status: str = "active"

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)

    def included_in_seq(self, lvol_id, seq):
        m = (self.members or {}).get(lvol_id)
        if not m or not seq:
            return False
        if m.get("joined_seq", 0) > seq:
            return False
        removed = m.get("removed_seq", 0)
        return removed == 0 or seq <= removed
