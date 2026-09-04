"""Release-specific upgrade plug-ins.

Some releases need one-off steps around ``cluster update`` that must not
become permanent features of the general upgrade path (e.g. suspending JC
compression while rolling onto a release that changes the journal
compaction format). Each such step lives in its own module in this package
as an :class:`UpgradePlugin`, is registered in :data:`PLUGINS`, and is
DELETED again in the release after the one it serves — together with any
one-line guards it placed in general code paths.

Contract per plugin:
  * ``to_release`` (mandatory): the release the plugin ships with, matched
    against the running software's SIMPLY_BLOCK_VERSION. The plugin's
    ``pre_update`` only runs when it matches. Versions are compared as
    ``.``/``-``-separated parts with prefix semantics, so ``"RC26.3"``
    covers ``RC26.3-RC1``, ``RC26.3-RC2`` and the final ``RC26.3`` build.
  * ``from_release`` (optional): restrict to upgrades coming from a given
    release, matched against ``cluster.installed_release``. A cluster with
    no stamp (pre-dating this framework) always matches.
  * ``pre_update(cluster)``: runs as the FIRST step of
    ``cluster_ops.update_cluster``, before anything is changed. Raise
    :class:`ReleaseUpgradeError` to abort the upgrade cleanly. Any state
    the plugin needs across the upgrade window is persisted under its own
    ``STATE_KEY`` in ``cluster.release_upgrade_state``.
  * ``upgrade_complete(cluster)``: runs from ``sbctl cluster
    upgrade-complete`` and must clear the plugin's key from
    ``cluster.release_upgrade_state``. It is selected by the presence of
    that key, not by version match, so a pending upgrade can always be
    completed. Returns a list of human-readable result lines.
"""

from simplyblock_core import constants, utils

logger = utils.get_logger(__name__)


class ReleaseUpgradeError(Exception):
    """A release-specific upgrade step refused to proceed."""


class UpgradePlugin:
    name: str = ""
    to_release: str = ""    # mandatory, e.g. "RC26.3" (covers RC26.3-RC1, -RC2, ...)
    from_release: str = ""  # optional, e.g. "R26.2"
    STATE_KEY: str = ""     # key owned by the plugin in cluster.release_upgrade_state

    def applies(self, cluster) -> bool:
        if not _release_matches(constants.SIMPLY_BLOCK_VERSION, self.to_release):
            return False
        if self.from_release and cluster.installed_release \
                and not _release_matches(cluster.installed_release, self.from_release):
            return False
        return True

    def pre_update(self, cluster) -> None:
        raise NotImplementedError

    def upgrade_complete(self, cluster) -> list:
        raise NotImplementedError


def _release_matches(running: str, wanted: str) -> bool:
    def parts(version):
        return str(version).replace("-", ".").split(".")
    running_parts = parts(running)
    wanted_parts = parts(wanted)
    return running_parts[:len(wanted_parts)] == wanted_parts


def _plugins():
    # Imported lazily so deleting a plugin module next release only requires
    # removing its entry here.
    from simplyblock_core.release_upgrades import jc_compression_upgrade
    return [jc_compression_upgrade.JCCompressionUpgrade()]


def run_pre_update(cluster) -> None:
    """First step of cluster_ops.update_cluster. Raises ReleaseUpgradeError
    to abort the upgrade before anything was changed."""
    for plugin in _plugins():
        if plugin.applies(cluster):
            logger.info(f"Running release-upgrade pre-update step: {plugin.name} "
                        f"(to_release={plugin.to_release})")
            plugin.pre_update(cluster)
        else:
            # Loud on purpose: a to_release/SIMPLY_BLOCK_VERSION mismatch must
            # not silently skip a mandatory upgrade step.
            logger.warning(f"Skipping release-upgrade step {plugin.name}: "
                           f"to_release={plugin.to_release}, "
                           f"from_release={plugin.from_release or 'any'}, "
                           f"running={constants.SIMPLY_BLOCK_VERSION}, "
                           f"cluster installed_release={cluster.installed_release or 'unset'}")


def run_upgrade_complete(cluster) -> list:
    """Backs ``sbctl cluster upgrade-complete``. Runs every plugin that left
    state on the cluster, then stamps the installed release."""
    from simplyblock_core.db_controller import DBController
    db = DBController()

    messages = []
    for plugin in _plugins():
        if plugin.STATE_KEY in cluster.release_upgrade_state:
            logger.info(f"Running release-upgrade completion step: {plugin.name}")
            messages.extend(plugin.upgrade_complete(cluster))

    cluster = db.get_cluster_by_id(cluster.get_id())
    cluster.installed_release = constants.SIMPLY_BLOCK_VERSION
    cluster.write_to_db(db.kv_store)
    messages.append(f"Cluster stamped with release {constants.SIMPLY_BLOCK_VERSION}")
    return messages
