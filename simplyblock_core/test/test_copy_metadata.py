"""The metadata each replicated copy carries for a fail-over on its own side
(control-plane split, step 3): the source volume's record, its host keys as
secrets on the wire, and the group generation's members."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.protocol import (
    CAPABILITIES,
    FinishRequest,
    SourceGroup,
    SourceHostAccess,
    SourceSnapshot,
    SourceVolume,
)
from simplyblock_core.services import snapshot_replication as sr

KEY = "DHHC-1:00:c2VjcmV0LWtleS1tYXRlcmlhbA==:"


def _volume():
    lv = LVol()
    lv.uuid = "LV1"
    lv.lvol_name = "pvc-data"
    lv.nqn = "nqn.2023-02.io.simplyblock:vol"
    lv.size = 10 << 30
    lv.allowed_hosts = [{"nqn": "nqn.host:a", "dhchap_key": KEY}, {"nqn": "nqn.host:b"}]
    return lv


def _snapshot(group_id="", group_seq=0):
    s = SnapShot()
    s.uuid = "S1"
    s.cluster_id = "SRC"
    s.lvol = _volume()
    s.group_id = group_id
    s.group_seq = group_seq
    return s


def test_host_keys_are_masked_in_python_and_clear_on_the_wire():
    host = SourceHostAccess(nqn="nqn.host:a", dhchap_key=SecretStr(KEY))
    assert KEY not in repr(host)
    assert KEY not in str(host.model_dump())
    wire = host.model_dump(mode="json")
    assert wire["dhchap_key"] == KEY
    # The receiving side parses the wire form back into a secret.
    parsed = SourceHostAccess.model_validate(wire).dhchap_key
    assert parsed is not None and parsed.get_secret_value() == KEY


def test_the_runner_sends_the_volume_record_without_its_host_keys_inside():
    volume = sr._source_volume(_snapshot())
    assert volume.record["nqn"] == "nqn.2023-02.io.simplyblock:vol"
    assert volume.record["lvol_name"] == "pvc-data"
    assert "allowed_hosts" not in volume.record
    assert [h.nqn for h in volume.allowed_hosts] == ["nqn.host:a", "nqn.host:b"]
    key = volume.allowed_hosts[0].dhchap_key
    assert key is not None and key.get_secret_value() == KEY
    assert KEY not in repr(volume)


def test_a_snapshot_with_no_embedded_volume_sends_none():
    s = _snapshot()
    s.lvol = None
    assert sr._source_volume(s) is None


def _group():
    g = ConsistencyGroup()
    g.uuid = "G1"
    g.cluster_id = "SRC"
    g.group_name = "gitea"
    g.policy_id = "SRC/P1"
    g.members = {
        "LV1": {"joined_seq": 1, "removed_seq": 0},
        "LV2": {"joined_seq": 1, "removed_seq": 0},
        "LV3": {"joined_seq": 5, "removed_seq": 0},   # joined after generation 4
        "LV4": {"joined_seq": 1, "removed_seq": 3},   # left before generation 4
    }
    return g


def test_the_group_sends_the_members_of_the_snapshots_generation(monkeypatch):
    db = MagicMock()
    db.get_consistency_group_by_id.return_value = _group()
    monkeypatch.setattr(sr, "db", db)
    group = sr._source_group(_snapshot("SRC/G1", 4))
    assert group == SourceGroup(group_name="gitea", policy_id="SRC/P1", members=["LV1", "LV2"])
    db.get_consistency_group_by_id.assert_called_once_with("SRC/G1")


def test_a_snapshot_outside_a_group_sends_no_group(monkeypatch):
    db = MagicMock()
    monkeypatch.setattr(sr, "db", db)
    assert sr._source_group(_snapshot()) is None
    db.get_consistency_group_by_id.assert_not_called()


def test_a_gone_group_sends_no_member_list(monkeypatch):
    db = MagicMock()
    db.get_consistency_group_by_id.side_effect = KeyError("SRC/G1")
    monkeypatch.setattr(sr, "db", db)
    assert sr._source_group(_snapshot("SRC/G1", 4)) is None


def _record(source):
    db = MagicMock()
    db.get_snapshots.return_value = []
    landing = SimpleNamespace(pool_uuid="P", top_bdev="LVS/REP_s", lvol_uuid="U", blobid=7)
    leader = SimpleNamespace(cluster_id="DST")
    request = FinishRequest(source_cluster_id="SRC", landing_lvol_id="L", link="source",
                            snapshot=source)
    with patch.object(SnapShot, "write_to_db"):
        return peer_service._record_copy(db, request, landing, leader, None, "C1")


def test_the_copy_records_the_source_volume_and_group():
    volume = SourceVolume(record={"uuid": "LV1", "nqn": "nqn.x"},
                          allowed_hosts=[SourceHostAccess(nqn="nqn.host:a", dhchap_key=SecretStr(KEY))])
    group = SourceGroup(group_name="gitea", policy_id="SRC/P1", members=["LV1", "LV2"])
    copy = _record(SourceSnapshot(snapshot_id="S1", snap_name="s", size=1,
                                  group_id="SRC/G1", group_seq=4, volume=volume, group=group))
    assert copy.source_volume == {"uuid": "LV1", "nqn": "nqn.x",
                                  "allowed_hosts": [{"nqn": "nqn.host:a", "dhchap_key": KEY}]}
    assert copy.source_group == {"group_name": "gitea", "policy_id": "SRC/P1",
                                 "members": ["LV1", "LV2"]}


def test_the_metadata_survives_the_wire():
    """The HTTP peer sends model_dump(mode="json") and the serving side parses
    it: the copy must end up with the keys in clear, as the source volume holds them."""
    source = SourceSnapshot(snapshot_id="S1", snap_name="s", size=1,
                            volume=sr._source_volume(_snapshot()))
    wire = FinishRequest(source_cluster_id="SRC", landing_lvol_id="L", link="source",
                         snapshot=source).model_dump(mode="json")
    copy = _record(FinishRequest.model_validate(wire).snapshot)
    assert copy.source_volume["allowed_hosts"] == [
        {"nqn": "nqn.host:a", "dhchap_key": KEY}, {"nqn": "nqn.host:b"}]
    assert copy.source_volume["lvol_name"] == "pvc-data"


def test_a_copy_from_an_older_source_records_no_metadata():
    copy = _record(SourceSnapshot(snapshot_id="S1", snap_name="s", size=1))
    assert copy.source_volume == {} and copy.source_group == {}


def test_the_peer_advertises_copy_metadata():
    assert "copy_metadata" in CAPABILITIES
