"""A snapshot-tree change an lvstore member still owes (replica_chain).

``bdev_lvol_add_clone`` / ``bdev_lvol_convert`` change the snapshot tree. The
leader persists the change; every other member keeps its own in-memory tree
and learns of the change only from the same call issued to it. A member that
could not be told (it did not answer, or it did not hold the bdev yet) keeps a
stale tree for as long as it does not reload the lvstore, and acts on it once
it leads (bf23 2026-11-12, LVS_16).

One record per (member, op, bdev). The id starts with the member's node id,
so a member's whole backlog is one prefix read. A newer record for the same
bdev and op replaces the older one: the latest intent is what the leader has.
"""

from simplyblock_core.models.base_model import BaseModel


class ReplicaChainPending(BaseModel):

    OP_ADD_CLONE = "add_clone"
    OP_CONVERT = "convert"

    REASON_UNREACHABLE = "unreachable"   # the member did not answer
    REASON_ABSENT = "absent"             # it answered, without the bdev
    REASON_FAILED = "failed"             # it answered an error on a non-fatal step

    node_id: str = ""            # the member that owes the change
    role: str = ""               # secondary / tertiary / peer, for the logs
    primary_node_id: str = ""    # owner of the lvstore (membership is read from it)
    lvs_name: str = ""
    op: str = ""
    bdev_name: str = ""          # composite lvs/name at the time of the call
    bdev_uuid: str = ""          # the bdev's uuid, the same on every member
    parent_name: str = ""        # add_clone only
    parent_uuid: str = ""
    reason: str = ""
    seq: int = 0                 # time.time_ns() of the call: replay order
    attempts: int = 0
    last_error: str = ""

    def get_id(self):
        return f"{self.node_id}:{self.op}:{self.bdev_uuid or self.bdev_name}"
