"""Rewrite a cluster's legacy ``hashicorp_vault_settings`` into ``kms_settings``.

``Cluster.get_kms_settings()`` already falls back to the legacy field, so this
is not required for correctness on its own -- it is what retires that
fallback, the same way ``database_indices.py`` retires its own obsolete key
families once the backfill is done. See the module docstring there and
``Cluster.kms_settings``'s comment for the full reasoning.

Like ``DatabaseIndices``, and unlike the one-release plugins in this package:
not gated to a single ``to_release``, not deleted afterward, idempotent, and
``applies()`` unconditionally -- it has to run on every upgrade path,
including one that skips several releases before its next ``cluster update``.

The rewrite itself happens in ``upgrade_complete``, not ``pre_update``, for
the same reason the index backfill does: during the roll, some processes are
still running old code that only reads ``hashicorp_vault_settings``, and
clearing it out from under them would make that code (correctly, rather than
incorrectly) see no KMS configuration at all. ``upgrade_complete`` is the
first point at which the new code is the only code.
"""

from simplyblock_core import constants, utils
from simplyblock_core.release_upgrades import UpgradePlugin

logger = utils.get_logger(__name__)

STATE_KEY = "kms_settings_migration"


class KMSSettingsMigration(UpgradePlugin):
    name = "kms-settings-migration"
    to_release = constants.SIMPLY_BLOCK_VERSION
    STATE_KEY = STATE_KEY

    def applies(self, cluster) -> bool:
        """Always. See the module docstring: this is not a one-release step."""
        return True

    def pre_update(self, cluster) -> None:
        """Claim the upgrade. The rewrite itself is in ``upgrade_complete``."""
        from simplyblock_core.db_controller import DBController

        db = DBController()
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.release_upgrade_state[STATE_KEY] = {"to_release": self.to_release}
        cluster.write_to_db(db.kv_store)
        logger.info("kms_settings will be migrated by `cluster upgrade-complete`; "
                    "until then kms_settings reads fall back to hashicorp_vault_settings")

    def upgrade_complete(self, cluster) -> list:
        from simplyblock_core.db_controller import DBController

        db = DBController()
        cluster = db.get_cluster_by_id(cluster.get_id())

        messages = []
        if not cluster.kms_settings and cluster.hashicorp_vault_settings is not None:
            legacy = cluster.hashicorp_vault_settings
            cluster.set_kms_settings({
                "provider": "vault",
                "base_url": legacy.base_url,
                "transit_mount": legacy.transit_mount,
                "kv_mount": legacy.kv_mount,
                "cert_role": legacy.cert_role,
            })
            cluster.hashicorp_vault_settings = None
            messages.append("Migrated hashicorp_vault_settings to kms_settings")
        else:
            messages.append("No legacy hashicorp_vault_settings to migrate")

        cluster.release_upgrade_state.pop(STATE_KEY, None)
        cluster.write_to_db(db.kv_store)
        return messages
