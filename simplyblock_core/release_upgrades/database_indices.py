"""Build the declared secondary indices as part of a cluster upgrade.

A cluster upgraded from a release that predates a given index declaration holds
records the index does not describe yet: live writes have maintained it since
the new code started running, but the records written before that are only
covered once the backfill has walked them. Until then every read of that index
falls back to the table scan the index exists to remove, which is correct but
slow — and invisible without the fallback counter.

This runs the backfill so no operator step stands between an upgrade and the
indices being usable, and drops the three hand-rolled key families the declared
indices replaced.

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
#: call sites, outside the entity's write transaction; they have no readers
#: left, so the upgrade clears them rather than leaving dead keyspace behind.
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
        from simplyblock_core.db_controller import DBController

        db = DBController()
        index_ops.backfill_lvol_cluster_id(log=logger.info)
        for line in index_ops.build_indices(log=logger.info):
            logger.info(line)

        for prefix in OBSOLETE_PREFIXES:
            db.kv_store.clear_range_startswith(prefix)
            logger.info("Cleared obsolete key family %s", prefix.decode())

    def upgrade_complete(self, cluster) -> list:
        """Nothing to complete: ``pre_update`` leaves no state behind.

        Selected by the presence of ``STATE_KEY`` in the cluster's upgrade
        state, which this plugin never sets, so this is only reachable if a
        future version starts using it.
        """
        cluster.release_upgrade_state.pop(STATE_KEY, None)
        return []
