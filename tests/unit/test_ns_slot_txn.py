"""Transactional namespace-slot allocation (claim_lvol_ns_slot /
release_lvol_ns_slot).

The subsystem pick and the in_creation record write used to be separate
reads/writes: two concurrent creates could both count the same shared
subsystem as having one free namespace slot and both join it. The claim now
runs pick + record write in ONE FDB transaction, with the record itself
acting as the slot claim (occupancy is recounted from records inside the
transaction, so the conflict-retry loser sees the winner's record).

These tests drive the transaction body through the transactionless adapter
(_NoTxnStore over a dict-backed store) — the fdb.transactional machinery
itself is not unit-testable without libfdb_c; what is verified here is the
claim/recount/persist logic and its atomic pairing.
"""

from types import SimpleNamespace

from simplyblock_core.db_controller import DBController, SubsystemCapacityError
from simplyblock_core.models.lvol_model import LVol, LVolMini


class FakeKV:
    """Dict-backed store implementing the surface BaseModel and _NoTxnStore
    use: get/set/clear + get_range_startswith (read_from_db's fallback for
    stores without raw range reads)."""

    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def clear(self, key):
        self.data.pop(key, None)

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        items = sorted((k, v) for k, v in self.data.items() if k.startswith(prefix))
        if reverse:
            items = items[::-1]
        if limit:
            items = items[:limit]
        return items


def make_dbc(fake):
    dbc = DBController()
    dbc.kv_store = fake  # no create_transaction attr -> _NoTxnStore path
    return dbc


def make_node(node_id="node-1", max_lvol=10):
    return SimpleNamespace(get_id=lambda: node_id, max_lvol=max_lvol)


def seed_lvol(fake, uuid, nqn, node_id="node-1", status=LVol.STATUS_ONLINE,
              max_ns=2):
    lv = LVol()
    lv.uuid = uuid
    lv.lvol_name = "lv-" + uuid
    lv.node_id = node_id
    lv.nqn = nqn
    lv.status = status
    lv.max_namespace_per_subsys = max_ns
    lv.write_to_db(fake)
    return lv


def new_lvol(uuid="new-1", node_id="node-1"):
    lv = LVol()
    lv.uuid = uuid
    lv.lvol_name = "lv-" + uuid
    lv.node_id = node_id
    lv.status = LVol.STATUS_IN_CREATION
    lv.max_namespace_per_subsys = 2
    return lv


STANDALONE_NQN = "nqn.cluster:lvol:new"


class TestClaim:
    def test_joins_subsystem_with_free_slot_and_persists_record(self):
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=2)
        dbc = make_dbc(fake)
        lv = new_lvol()

        joined = dbc.claim_lvol_ns_slot(lv, make_node(), True, STANDALONE_NQN)

        assert joined is True
        assert lv.nqn == "nqnA"
        assert lv.namespace == "a1"
        # the record write is part of the claim
        assert LVol().read_from_db(fake, id="new-1")
        assert LVolMini().read_from_db(fake, id="new-1")
        # allocator key bumped
        assert fake.get(b"ns_slot_alloc/node-1") == b"1"

    def test_in_creation_records_occupy_slots(self):
        # A concurrent create's committed in_creation record must count:
        # that IS the race fix (the retry recounts with the record present).
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=2)
        seed_lvol(fake, "a2", "nqnA", status=LVol.STATUS_IN_CREATION, max_ns=2)
        dbc = make_dbc(fake)
        lv = new_lvol()

        joined = dbc.claim_lvol_ns_slot(lv, make_node(), True, STANDALONE_NQN)

        assert joined is False
        assert lv.nqn == STANDALONE_NQN
        assert lv.namespace == ""

    def test_sequential_claims_do_not_double_book_last_slot(self):
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=2)
        dbc = make_dbc(fake)

        first = new_lvol("c1")
        second = new_lvol("c2")
        assert dbc.claim_lvol_ns_slot(first, make_node(), True, STANDALONE_NQN) is True
        joined = dbc.claim_lvol_ns_slot(second, make_node(), True,
                                        "nqn.cluster:lvol:c2")

        # first took nqnA's last slot; second must NOT join it
        assert first.nqn == "nqnA"
        assert joined is False
        assert second.nqn == "nqn.cluster:lvol:c2"

    def test_exclude_nqns_skips_spdk_rejected_subsystem(self):
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=2)
        dbc = make_dbc(fake)
        lv = new_lvol()

        joined = dbc.claim_lvol_ns_slot(lv, make_node(), True, STANDALONE_NQN,
                                        exclude_nqns={"nqnA"})

        assert joined is False
        assert lv.nqn == STANDALONE_NQN

    def test_capacity_error_writes_nothing(self):
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=1)  # full subsystem
        dbc = make_dbc(fake)
        lv = new_lvol()
        before = dict(fake.data)

        try:
            dbc.claim_lvol_ns_slot(lv, make_node(max_lvol=1), True,
                                   STANDALONE_NQN)
            raised = False
        except SubsystemCapacityError:
            raised = True

        assert raised
        assert fake.data == before

    def test_allowed_hosts_applied_on_standalone_reset_on_join(self):
        hosts = [{"nqn": "nqn.host:1"}]

        fake = FakeKV()
        dbc = make_dbc(fake)
        lv = new_lvol()
        dbc.claim_lvol_ns_slot(lv, make_node(), True, STANDALONE_NQN,
                               standalone_allowed_hosts=hosts)
        assert lv.allowed_hosts == hosts

        fake2 = FakeKV()
        seed_lvol(fake2, "a1", "nqnA", max_ns=2)
        dbc2 = make_dbc(fake2)
        lv2 = new_lvol("new-2")
        lv2.allowed_hosts = hosts  # stale standalone value from a prior try
        dbc2.claim_lvol_ns_slot(lv2, make_node(), True, STANDALONE_NQN,
                                standalone_allowed_hosts=hosts)
        # joined lvols inherit the subsystem root's host config
        assert lv2.allowed_hosts == []

    def test_non_namespaced_claim_is_standalone(self):
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=2)
        dbc = make_dbc(fake)
        lv = new_lvol()

        joined = dbc.claim_lvol_ns_slot(lv, make_node(), False, STANDALONE_NQN)

        assert joined is False
        assert lv.nqn == STANDALONE_NQN


class TestRelease:
    def test_release_removes_record_and_mini(self):
        fake = FakeKV()
        dbc = make_dbc(fake)
        lv = new_lvol()
        dbc.claim_lvol_ns_slot(lv, make_node(), True, STANDALONE_NQN)
        assert LVol().read_from_db(fake, id="new-1")

        dbc.release_lvol_ns_slot(lv)

        assert not LVol().read_from_db(fake, id="new-1")
        assert not LVolMini().read_from_db(fake, id="new-1")

    def test_released_slot_is_reclaimable(self):
        fake = FakeKV()
        seed_lvol(fake, "a1", "nqnA", max_ns=2)
        dbc = make_dbc(fake)

        first = new_lvol("c1")
        dbc.claim_lvol_ns_slot(first, make_node(), True, STANDALONE_NQN)
        assert first.nqn == "nqnA"
        dbc.release_lvol_ns_slot(first)

        second = new_lvol("c2")
        joined = dbc.claim_lvol_ns_slot(second, make_node(), True,
                                        "nqn.cluster:lvol:c2")
        assert joined is True
        assert second.nqn == "nqnA"
