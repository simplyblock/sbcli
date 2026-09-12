"""Hard object limits: volume size, snapshots per volume, clones per snapshot.

Pure functions -- no DB, no RPC -- so every create/resize path (CLI and API)
can call them and they stay unit-testable. The limits themselves live in
``constants`` (``MAX_LVOL_SIZE``, ``MAX_SNAPSHOTS_PER_LVOL``,
``MAX_CLONES_PER_SNAPSHOT``); see the comment block there for the rationale.

Counting is done over the *mini* records (``LVolMini`` / ``SnapShotMini``) that
the create paths already hold via ``ttl_cache.cached_mini_*``. Never pull the
full tables for this: a full ``SnapShot`` embeds the entire ``LVol`` dict and a
per-create full-table scan reached 15 s at 10k snapshots (run 2026-07-21). The
helpers accept full records too -- they only touch fields both shapes carry.

"Active" follows the existing per-lvstore object cap: deleted objects never
count, objects still *in* deletion do (they are in the blob chain until the
delete completes).
"""
from typing import Iterable, Optional

from simplyblock_core import constants, utils
from simplyblock_core.models.lvol_model import LVol


def _lvol_id(lvol) -> str:
    """uuid of an LVol or LVolMini (minis carry it as ``lvol_uuid``)."""
    if lvol is None:
        return ""
    return getattr(lvol, "uuid", "") or getattr(lvol, "lvol_uuid", "")


def is_active_snapshot(snap) -> bool:
    return not getattr(snap, "deleted", False)


def is_active_lvol(lvol) -> bool:
    return getattr(lvol, "status", "") != LVol.STATUS_DELETED


# --------------------------------------------------------------------------- size

def check_lvol_size(size: int, what: str = "Volume size") -> Optional[str]:
    """None if ``size`` is within MAX_LVOL_SIZE, else an error message.

    ``what`` names the value in the message ("Volume size", "New size",
    "Clone size", "Volume max size")."""
    limit = constants.MAX_LVOL_SIZE
    if size and size > limit:
        return (f"{what} {utils.humanbytes(size)} exceeds the maximum of "
                f"{utils.humanbytes(limit)} per volume")
    return None


# ---------------------------------------------------------------- snapshots / lvol

def count_active_snapshots(lvol_id: str, snapshots: Iterable) -> int:
    """Active (non-deleted) snapshots taken from ``lvol_id``."""
    return sum(1 for s in snapshots
               if is_active_snapshot(s) and _lvol_id(getattr(s, "lvol", None)) == lvol_id)


def check_snapshot_limit(lvol_id: str, snapshots: Iterable, new_objects: int = 1) -> Optional[str]:
    """None if ``lvol_id`` can take ``new_objects`` more snapshots, else an error."""
    limit = constants.MAX_SNAPSHOTS_PER_LVOL
    count = count_active_snapshots(lvol_id, snapshots)
    if count + new_objects > limit:
        return (f"Snapshot limit reached for volume {lvol_id}: {count} active "
                f"snapshots; the hard limit is {limit} per volume. Delete "
                f"snapshots before creating more")
    return None


# ------------------------------------------------------------- clones / snapshot

def count_active_clones(snapshot_id: str, lvols: Iterable) -> int:
    """Active (non-deleted) clones created from ``snapshot_id``."""
    return sum(1 for lv in lvols
               if is_active_lvol(lv) and getattr(lv, "cloned_from_snap", "") == snapshot_id)


def check_clone_limit(snapshot_id: str, lvols: Iterable, new_objects: int = 1) -> Optional[str]:
    """None if ``snapshot_id`` can take ``new_objects`` more clones, else an error."""
    limit = constants.MAX_CLONES_PER_SNAPSHOT
    count = count_active_clones(snapshot_id, lvols)
    if count + new_objects > limit:
        return (f"Clone limit reached for snapshot {snapshot_id}: {count} active "
                f"clones; the hard limit is {limit} per snapshot")
    return None
