"""Partial (dirty-bitmap delta) replication transfers.

The in-memory dirty bitmap has been in the SPDK fork for a long time but never
executed: ``bdev_lvol_transfer`` takes an optional ``allow_partial`` and the
control plane never sent it. It could not be sent, either -- a partial transfer
ships only the ranges written since the previous snapshot, so the destination
has to hold everything else already, and this pipeline landed every transfer in
a FRESH EMPTY volume and only chained it afterwards. A delta into an empty
volume silently loses every cluster it does not cover.

These tests pin the change that makes it safe: chain the landing volume onto the
destination's copy of the previous snapshot BEFORE the transfer, and ask for a
delta only when that actually succeeded on every online member of the target's
HA pair.
"""
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr


# --------------------------------------------------------------------------
# fakes
# --------------------------------------------------------------------------

def _mk_snap(uuid, created_at, lvol_uuid, node_id, target="", source="",
             status=SnapShot.STATUS_ONLINE, snap_type=None):
    lv = LVol()
    lv.uuid = lvol_uuid
    lv.node_id = node_id
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.status = status
    s.snap_ref_id = ""
    s.target_replicated_snap_uuid = target
    s.source_replicated_snap_uuid = source
    s.snap_bdev = f"LVS/{uuid}"
    s.lvol = lv
    if snap_type is not None:
        s.snap_type = snap_type
    return s


class _FakeRPC:
    def __init__(self, owner, add_clone_ok=True):
        self._owner = owner
        self._add_clone_ok = add_clone_ok

    def bdev_lvol_add_clone(self, clone_bdev, snap_bdev):
        self._owner.add_clone_calls.append((clone_bdev, snap_bdev))
        return self._add_clone_ok


class _Node:
    def __init__(self, uuid, secondary_node_id=None, add_clone_ok=True,
                 status=StorageNode.STATUS_ONLINE):
        self._uuid = uuid
        self.secondary_node_id = secondary_node_id
        self.status = status
        self.add_clone_calls = []
        self._add_clone_ok = add_clone_ok

    def get_id(self):
        return self._uuid

    def rpc_client(self):
        return _FakeRPC(self, self._add_clone_ok)


class _FakeDB:
    def __init__(self, snaps, nodes):
        self._snaps = list(snaps)
        self._nodes = dict(nodes)

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._snaps if s.lvol.node_id == node_id]

    def get_snapshot_by_id(self, uuid):
        for s in self._snaps:
            if s.uuid == uuid:
                return s
        raise KeyError(uuid)

    def get_storage_node_by_id(self, uuid):
        if uuid not in self._nodes:
            raise KeyError(uuid)
        return self._nodes[uuid]


class _Task:
    def __init__(self, **params):
        self.function_params = dict(params)
        self.writes = 0

    def write_to_db(self):
        self.writes += 1
        return True


class _LandingLV:
    top_bdev = "LVS/REP_SNAP_2"

    def __init__(self, uuid="REP_LV_1"):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


# --------------------------------------------------------------------------
# the landing volume becomes a clone of the predecessor when eligible
# --------------------------------------------------------------------------

def _eligible_setup(monkeypatch, primary_ok=True, secondary_ok=True,
                    secondary_online=True):
    """A snapshot with a replicated predecessor whose remote copy sits on the
    receiving leader -- the case a delta is legitimate in."""
    cur = _mk_snap("SNAP_2", 200, "LV1", "N_SRC")
    prev = _mk_snap("SNAP_1", 100, "LV1", "N_SRC", target="T_SNAP_1")
    # the predecessor's copy on the destination, on the receiving leader
    remote_copy = _mk_snap("T_SNAP_1", 150, "REP_LV", "N_TGT")

    sec = _Node("N_TGT_SEC", add_clone_ok=secondary_ok,
                status=(StorageNode.STATUS_ONLINE if secondary_online
                        else StorageNode.STATUS_OFFLINE))
    leader = _Node("N_TGT", secondary_node_id="N_TGT_SEC",
                   add_clone_ok=primary_ok)
    monkeypatch.setattr(sr, "db", _FakeDB([cur, prev, remote_copy],
                                          {"N_TGT": leader, "N_TGT_SEC": sec}))
    return cur, leader, sec


def test_landing_volume_is_chained_to_predecessor_when_eligible(monkeypatch):
    cur, leader, sec = _eligible_setup(monkeypatch)
    task = _Task()

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is True

    # chained on BOTH members, onto the predecessor's remote copy
    assert leader.add_clone_calls == [("LVS/REP_SNAP_2", "LVS/T_SNAP_1")]
    assert sec.add_clone_calls == [("LVS/REP_SNAP_2", "LVS/T_SNAP_1")]
    # and recorded so the finish step does not add the entry a second time
    assert task.function_params["prechained_node_ids"] == ["N_TGT", "N_TGT_SEC"]


def test_no_predecessor_falls_back_to_a_fresh_full_transfer(monkeypatch):
    """First snapshot of a volume: nothing on the destination to build on."""
    cur = _mk_snap("SNAP_1", 100, "LV1", "N_SRC")
    leader = _Node("N_TGT", secondary_node_id="N_TGT_SEC")
    sec = _Node("N_TGT_SEC")
    monkeypatch.setattr(sr, "db",
                        _FakeDB([cur], {"N_TGT": leader, "N_TGT_SEC": sec}))
    task = _Task()

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is False

    assert leader.add_clone_calls == []
    assert sec.add_clone_calls == []
    assert "prechained_node_ids" not in task.function_params


def test_unreplicated_predecessor_falls_back(monkeypatch):
    """A predecessor exists but was never replicated -- no remote copy to
    clone from, so this snapshot starts the chain."""
    cur = _mk_snap("SNAP_2", 200, "LV1", "N_SRC")
    prev = _mk_snap("SNAP_1", 100, "LV1", "N_SRC")   # no target copy
    leader = _Node("N_TGT", secondary_node_id="N_TGT_SEC")
    sec = _Node("N_TGT_SEC")
    monkeypatch.setattr(sr, "db", _FakeDB([cur, prev],
                                          {"N_TGT": leader, "N_TGT_SEC": sec}))
    task = _Task()

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is False
    assert leader.add_clone_calls == []


def test_offline_secondary_forces_a_full_transfer(monkeypatch):
    """The transfer lands on the leader and the lvstore mirrors it to the
    secondary. A secondary that is not there to be chained would read zeros
    wherever the delta did not write, so a degraded pair gets a full copy."""
    cur, leader, sec = _eligible_setup(monkeypatch, secondary_online=False)
    task = _Task()

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is False
    # nothing was chained at all -- we bail before touching either node
    assert leader.add_clone_calls == []
    assert sec.add_clone_calls == []


def test_failed_primary_chain_forces_a_full_transfer(monkeypatch):
    cur, leader, sec = _eligible_setup(monkeypatch, primary_ok=False)
    task = _Task()

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is False
    # the secondary is not touched once the primary refused
    assert sec.add_clone_calls == []


def test_failed_secondary_chain_forces_full_but_records_the_primary(monkeypatch):
    """A full transfer into a half-chained volume is still correct, but the
    entry that DID land must be remembered or the finish step adds it twice."""
    cur, leader, sec = _eligible_setup(monkeypatch, secondary_ok=False)
    task = _Task()

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is False
    assert task.function_params["prechained_node_ids"] == ["N_TGT"]


def test_prechain_is_not_repeated_for_a_node_already_chained(monkeypatch):
    """A retried attempt must not add the same clone entry a second time."""
    cur, leader, sec = _eligible_setup(monkeypatch)
    task = _Task(prechained_node_ids=["N_TGT"])

    assert sr._prechain_landing_volume(
        task, cur, False, _LandingLV(), leader) is True
    assert leader.add_clone_calls == []          # already done
    assert sec.add_clone_calls == [("LVS/REP_SNAP_2", "LVS/T_SNAP_1")]


def test_decision_is_cached_for_the_same_landing_volume(monkeypatch):
    """A resumed transfer keeps the mode its first attempt used, without
    re-issuing add_clone."""
    cur, leader, sec = _eligible_setup(monkeypatch)
    lv = _LandingLV("REP_LV_1")
    task = _Task()

    assert sr._partial_transfer_decision(task, cur, False, lv, leader) is True
    assert len(leader.add_clone_calls) == 1

    # second pass over the same landing volume: cached, nothing re-chained
    leader.add_clone_calls.clear()
    sec.add_clone_calls.clear()
    assert sr._partial_transfer_decision(task, cur, False, lv, leader) is True
    assert leader.add_clone_calls == []
    assert sec.add_clone_calls == []


def test_a_replaced_landing_volume_re_derives_and_never_inherits_partial(monkeypatch):
    """An interrupted attempt can delete a half-created landing volume and
    build a fresh, UNCHAINED one. Inheriting the old "partial is fine" verdict
    would ship a delta into something holding nothing.
    """
    cur = _mk_snap("SNAP_2", 200, "LV1", "N_SRC")
    prev = _mk_snap("SNAP_1", 100, "LV1", "N_SRC")   # predecessor NOT replicated
    leader = _Node("N_TGT", secondary_node_id="N_TGT_SEC")
    sec = _Node("N_TGT_SEC")
    monkeypatch.setattr(sr, "db", _FakeDB([cur, prev],
                                          {"N_TGT": leader, "N_TGT_SEC": sec}))
    # a stale verdict from a landing volume that no longer exists
    task = _Task(allow_partial=True, allow_partial_landing="REP_LV_OLD",
                 prechained_node_ids=["N_TGT", "N_TGT_SEC"])

    got = sr._partial_transfer_decision(
        task, cur, False, _LandingLV("REP_LV_NEW"), leader)

    assert got is False, "a replaced landing volume must not inherit partial"
    assert task.function_params["allow_partial"] is False
    assert task.function_params["allow_partial_landing"] == "REP_LV_NEW"
    # the stale chain record is discarded too
    assert task.function_params["prechained_node_ids"] == []


def test_prechained_nodes_are_ignored_for_a_different_landing_volume():
    """Skipping add_clone on the strength of a stale record would leave the
    volume unchained."""
    task = _Task(allow_partial=True, allow_partial_landing="REP_LV_1",
                 prechained_node_ids=["N_TGT", "N_TGT_SEC"])

    assert sr._prechained_nodes_for(task, _LandingLV("REP_LV_1")) == {
        "N_TGT", "N_TGT_SEC"}
    assert sr._prechained_nodes_for(task, _LandingLV("REP_LV_2")) == set()


# --------------------------------------------------------------------------
# allow_partial reaches the RPC only when it was asked for
# --------------------------------------------------------------------------

def _transfer_params(**kwargs):
    """Run the REAL bdev_lvol_transfer body and return the params it would
    put on the wire."""
    from simplyblock_core.rpc_client import RPCClient

    class _C(RPCClient):
        def __init__(self):
            self.sent: tuple = ()

        def _request(self, method, params=None, request_timeout=None):
            self.sent = (method, params)
            return True

    c = _C()
    c.bdev_lvol_transfer(name="LVS/SNAP_2", offset=0, batch_size=16,
                         bdev_name="hub0", operation="replicate", lvol_id=7,
                         **kwargs)
    method, params = c.sent
    assert method == "bdev_lvol_transfer"
    return params


def test_allow_partial_is_never_sent_while_the_fork_workaround_holds():
    """The delta path is DISABLED: bdev_lvol_transfer never emits allow_partial,
    so every transfer is a full one regardless of what the caller requests
    (commit 52e75afb2, 2026-08-31 — the SPDK fork's fragment write path
    corrupts partial transfers). When the fork is fixed and the emission in
    rpc_client.bdev_lvol_transfer is re-enabled, restore the opt-in
    assertions: allow_partial=True must put the key on the wire, and
    False/default must keep it absent."""
    assert "allow_partial" not in _transfer_params(allow_partial=True)
    assert "allow_partial" not in _transfer_params(allow_partial=False)
    assert "allow_partial" not in _transfer_params()


def test_transfer_still_carries_the_routing_fields():
    """allow_partial must not disturb the map-id routing the hub demux needs."""
    p = _transfer_params(allow_partial=True)
    assert p["lvol_name"] == "LVS/SNAP_2"
    assert p["lvol_id"] == 7
    assert p["gateway"] == "hub0"
    assert p["operation"] == "replicate"
    assert p["cluster_batch"] == 16


# --------------------------------------------------------------------------
# retention keeps the COW parent
# --------------------------------------------------------------------------

def test_retention_floor_keeps_the_cow_parent_alive():
    """The delta's correctness depends on the PREVIOUS replicated snapshot
    still existing on the destination when the next one lands -- it is the COW
    parent the landing volume is chained onto. Retention must therefore never
    prune down to a single replicated internal snapshot.
    """
    from simplyblock_core.models.replication import ReplicationPolicy

    # the flat default and the policy floor both keep a PAIR
    assert sr._KEEP_REPLICATED_INTERNAL >= 2
    assert ReplicationPolicy.MIN_KEEP_REPLICATED >= 2
    # a policy cannot be configured below the floor
    assert ReplicationPolicy.keep_replicated >= ReplicationPolicy.MIN_KEEP_REPLICATED


def test_retention_keep_count_never_drops_below_the_floor(monkeypatch):
    """_keep_replicated_for clamps a policy that asks for fewer than a pair."""
    from simplyblock_core.models.replication import ReplicationPolicy

    class _Policy:
        keep_replicated = 1          # below the floor
        retention_schedule = None

        def get_id(self):
            return "P1"

    class _DB:
        def get_replication_policy_for_lvol(self, lvol):
            return _Policy()

    monkeypatch.setattr(sr, "db", _DB())
    lv = LVol()
    lv.uuid = "LV1"
    assert sr._keep_replicated_for(lv) == ReplicationPolicy.MIN_KEEP_REPLICATED


def test_scheduled_retention_still_keeps_the_newest_pair():
    """The ladder thins history but always_keep_newest protects the COW parent
    regardless of how coarse the schedule's finest tier is."""
    from simplyblock_core.snapshot_retention import parse_schedule, select_retained

    tiers = parse_schedule("1h:24h")
    now = 1_787_900_000.0
    # a fast cadence: two snapshots a minute apart, far finer than the tier
    history = [now - 60, now - 120]
    keep = select_retained(history, tiers, now, always_keep_newest=2)
    # BOTH survive: the newest is the next delta's base, the one before it is
    # the COW parent the current landing volume is chained onto
    assert set(keep) == set(history)
