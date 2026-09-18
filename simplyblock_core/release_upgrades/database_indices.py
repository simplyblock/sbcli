"""Build the declared secondary indices as part of a cluster upgrade.

A cluster upgraded from a release that predates a given index declaration holds
records the index does not describe yet: live writes have maintained it since
the new code started running, but the records written before that are only
covered once the backfill has walked them. Until then every read of that index
falls back to the table scan the index exists to remove, which is correct but
slow — and invisible without the fallback counter.

This runs the backfill so the indices are usable once the upgrade is over
without a separate operator step, and drops the hand-rolled key families the
declared indices replaced.

Both happen in ``upgrade_complete``, not in ``pre_update``. ``pre_update`` runs
before a single container image has been replaced, so every API instance,
service and task runner is still executing the previous release: it maintains
no index, and it still reads the key families below. Flipping an index to
``ready`` there would publish it cluster-wide while writers that do not
maintain it are live — every record they create in the window is missing from
it for good — and clearing the old key families there makes the running code
answer "name free" for every name it holds. ``upgrade_complete`` is the first
point at which the new code is the only code, so the backfill walk covers the
window and the old families have no reader left.

Unlike the other plugins in this package, this one is NOT deleted in the
following release and does not gate on ``to_release``: the backfill is
idempotent and cheap once complete (it skips every index already ``ready``),
and it has to run on *every* upgrade path, including one that adds an index to
a class that already had some.
"""

from simplyblock_core import constants, index_ops, utils
from simplyblock_core.release_upgrades import UpgradePlugin

logger = utils.get_logger(__name__)

STATE_KEY = "database_indices"

#: Key families the declared indices subsumed. Each was maintained from its own
#: call sites, outside the entity's write transaction; once the rollout is
#: complete they have no readers left, so the upgrade clears them rather than
#: leaving dead keyspace behind.
OBSOLETE_PREFIXES = (
    b'name_index/',
    b'lvol_snaps/',
)


class DatabaseIndices(UpgradePlugin):
    name = "database-indices"
    to_release = constants.SIMPLY_BLOCK_VERSION
    STATE_KEY = STATE_KEY

    def applies(self, cluster) -> bool:
        """Always. See the module docstring: this is not a one-release step."""
        return True

    def pre_update(self, cluster) -> None:
        """Claim the upgrade. All the work is in ``upgrade_complete``."""
        from simplyblock_core.db_controller import DBController

        db = DBController()
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.release_upgrade_state[STATE_KEY] = {"to_release": self.to_release}
        cluster.write_to_db(db.kv_store)
        logger.info("Secondary indices will be built by `cluster upgrade-complete`; "
                    "until then every indexed read falls back to a table scan")

    def upgrade_complete(self, cluster) -> list:
        from simplyblock_core.db_controller import DBController

        db = DBController()
        messages = [index_ops.backfill_lvol_cluster_id(log=logger.info)]
        for line in index_ops.build_indices(log=logger.info):
            logger.info(line)
            messages.append(line)

        for prefix in OBSOLETE_PREFIXES:
            db.kv_store.clear_range_startswith(prefix)
            messages.append(f"Cleared obsolete key family {prefix.decode()}")

        # Persisted here rather than on the object the caller holds: the caller
        # re-reads the cluster before stamping the installed release, so an
        # in-memory pop would not survive.
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.release_upgrade_state.pop(STATE_KEY, None)
        cluster.write_to_db(db.kv_store)
        return messages
