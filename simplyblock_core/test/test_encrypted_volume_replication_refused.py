"""An encrypted volume is refused at every point replication is enabled.

Nothing moves a volume's data encryption key to the destination cluster, so a
replicated copy there is ciphertext with no key: a fail-over clone cannot build
its crypto layer, and a clone taken from the replicated snapshot comes up
exporting blocks nothing can read. The three entry points below each enable
replication on their own, so each carries the refusal.
"""
import pytest

from simplyblock_core.controllers import lvol_controller, replication_policy_controller
from simplyblock_core.models.lvol_model import LVol


def _lvol(crypto):
    lvol = LVol()
    lvol.uuid = "LV1"
    lvol.crypto_bdev = crypto
    return lvol


class _DB:
    def __init__(self, lvol):
        self._lvol = lvol

    def get_lvol_by_id(self, _):
        return self._lvol


@pytest.mark.parametrize("replication_kwargs", [
    {"do_replicate": True},
    {"replication_cluster_id": "CL_TGT"},
    {"replication_policy": "nightly"},
])
def test_creating_an_encrypted_volume_with_replication_is_refused(monkeypatch, replication_kwargs):
    monkeypatch.setattr(lvol_controller, "DBController", lambda: _DB(_lvol("crypto_LV1")))

    created, error = lvol_controller.add_lvol_ha(
        "vol1", 1024, "N_SRC", "single", "P_SRC", use_crypto=True, **replication_kwargs)

    assert created is False
    assert "encrypted" in error


@pytest.mark.parametrize("attach", [
    lambda lvol_id: replication_policy_controller.attach_policy(lvol_id, "nightly"),
    lambda lvol_id: replication_policy_controller.start_member_replication(lvol_id, None, None),
])
def test_putting_an_encrypted_volume_under_a_policy_is_refused(monkeypatch, attach):
    monkeypatch.setattr(replication_policy_controller, "db", _DB(_lvol("crypto_LV1")))

    with pytest.raises(replication_policy_controller.ReplicationConfigError) as excinfo:
        attach("LV1")

    assert "encrypted" in str(excinfo.value)
