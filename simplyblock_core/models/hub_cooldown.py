"""
hub_cooldown.py — cross-process detach-cooldown record for migration hub
NVMe-oF controllers.

TasksRunnerLVolMigration and TasksRunnerBatchMigration are separate
processes, each running its own HubControllerManager instance. Each
instance's own in-memory view of "which hub controllers are attached" only
needs to be correct within its own process, but the mandatory cooldown
between a detach and the next attach for a given (src_node_id, tgt_node_id)
pair must be honored across BOTH processes — otherwise a batch migration
detaching a hub and a solo migration re-attaching it (or vice versa) within
the same window can race the NVMe TCP disconnect handshake and fail to
attach. This tiny record persists just the detach timestamp so every
process's HubControllerManager enforces the same cooldown window, regardless
of which process performed the detach.
"""

from simplyblock_core.models.base_model import BaseModel


class HubDetachCooldown(BaseModel):
    # f"{src_node_id}:{tgt_node_id}" — the same pair-key HubControllerManager
    # already uses internally for its own in-memory _entries dict.
    pair_key: str = ""

    # time.time() (wall clock, NOT time.monotonic() — this is compared across
    # processes, possibly on different hosts, so it must be a wall-clock
    # timestamp rather than a per-process monotonic counter) of the most
    # recent detach for this pair.
    detach_ts: float = 0.0

    def get_id(self):
        return self.pair_key
