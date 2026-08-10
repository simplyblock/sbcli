"""Operator-facing topology labels resolve to the integer ids the CP keys off.

The integer stays the internal identity (placement, distrib map, expansion
planner); these tests pin the translation layer: syntax, the digits-are-still-ids
compatibility rule, allocation, and rendering.
"""
import pytest

from simplyblock_core import topology_labels as tl


# --- syntax ----------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("RACK1", "RACK1"),
    ("rack1", "RACK1"),      # case is not meaningful
    ("Rack1", "RACK1"),
    ("  AZ2  ", "AZ2"),      # operators paste with whitespace
    ("HOST1", "HOST1"),
    ("DC-EU-WEST_1", "DC-EU-WEST_1"),
    ("A", "A"),
    ("R" + "1" * 31, "R" + "1" * 31),   # 32 chars, the limit
])
def test_normalize_accepts(raw, expected):
    assert tl.normalize_label(raw) == expected


@pytest.mark.parametrize("raw", [
    "",             # empty
    "   ",
    None,
    "1RACK",        # must start with a letter
    "RACK 1",       # no spaces inside
    "RACK.1",       # no dots
    "RACK/1",
    "R" + "1" * 32,  # 33 chars, one over
    "RÄCK1",        # non-ASCII
])
def test_normalize_rejects(raw):
    with pytest.raises(tl.InvalidLabelError):
        tl.normalize_label(raw)


# --- the compatibility rule ------------------------------------------------

def test_digits_stay_an_id_not_a_label():
    # Scripts, CI bootstraps and the k8s operator pass integers. They must keep
    # meaning THAT id — not a label whose name happens to be "7".
    assert tl.parse_failure_domain_arg("7") == (7, None)
    assert tl.parse_failure_domain_arg(7) == (7, None)
    assert tl.parse_failure_domain_arg("0") == (0, None)
    assert tl.parse_failure_domain_arg("-1") == (-1, None)


def test_names_are_labels():
    assert tl.parse_failure_domain_arg("RACK1") == (None, "RACK1")
    assert tl.parse_failure_domain_arg("az2") == (None, "AZ2")


def test_absent_is_neither():
    assert tl.parse_failure_domain_arg(None) == (None, None)
    assert tl.parse_failure_domain_arg("") == (None, None)


def test_invalid_label_raises_rather_than_silently_becoming_an_id():
    with pytest.raises(tl.InvalidLabelError):
        tl.parse_failure_domain_arg("rack 1")


# --- allocation ------------------------------------------------------------

def test_first_id_is_zero():
    assert tl.next_free_id([]) == 0


def test_allocation_never_reuses_a_retired_id():
    # One past the highest, NOT the lowest free: reusing 1 after RACK2 was
    # removed would silently move nodes into a domain that meant something else.
    assert tl.next_free_id([0, 2]) == 3
    assert tl.next_free_id([5]) == 6


def test_allocation_spans_ids_that_are_only_on_nodes():
    # An id chosen by a legacy integer call is not in the registry, but must
    # still never be handed to a new label.
    assert tl.next_free_id([0, 1, 42]) == 43


def test_allocation_ignores_none():
    assert tl.next_free_id([0, None, 3]) == 4


# --- rendering -------------------------------------------------------------

def test_render_prefers_the_label():
    assert tl.render({"RACK1": 0, "RACK2": 1}, 1) == "RACK2"


def test_render_falls_back_to_the_id():
    # A cluster not yet backfilled, or a domain created by id, still displays.
    assert tl.render({}, 3) == "3"
    assert tl.render({"RACK1": 0}, 3) == "3"


def test_render_unset_is_blank():
    assert tl.render({"RACK1": 0}, -1) == ""


def test_label_for_id():
    assert tl.label_for_id({"AZ1": 4}, 4) == "AZ1"
    assert tl.label_for_id({"AZ1": 4}, 5) is None
    assert tl.label_for_id(None, 4) is None
    assert tl.label_for_id({"AZ1": 4}, -1) is None


# --- backfill naming -------------------------------------------------------

def test_backfill_names_are_derived_from_the_id():
    assert tl.backfill_label(tl.FAILURE_DOMAIN, 0) == "FD0"
    assert tl.backfill_label(tl.FAILURE_DOMAIN, 7) == "FD7"
    assert tl.backfill_label(tl.PHYSICAL, 3) == "HOST3"


def test_backfill_names_are_valid_labels():
    # Whatever the backfill invents must survive normalize_label, or `sn add-node`
    # could not name the domain it just created.
    for kind in (tl.FAILURE_DOMAIN, tl.PHYSICAL):
        for id_ in (0, 1, 99):
            label = tl.backfill_label(kind, id_)
            assert tl.normalize_label(label) == label


def test_every_kind_has_a_registry_field_and_prefix():
    for kind in (tl.FAILURE_DOMAIN, tl.PHYSICAL):
        assert kind in tl.REGISTRY_FIELD
        assert kind in tl.BACKFILL_PREFIX


# --- transactional claim ---------------------------------------------------
#
# Exercised through _NoTxnStore (the same path the unit-tier fdb stub takes):
# same logic as the FDB transaction, minus atomicity.

import json  # noqa: E402

from simplyblock_core.db_controller import DBController  # noqa: E402
from simplyblock_core.models.cluster import Cluster  # noqa: E402


class _FakeKV(dict):
    """Minimal key-value store: what _NoTxnStore needs is get/set/clear."""

    def get(self, key):
        return dict.get(self, key)

    def set(self, key, value):
        self[key] = value

    def clear(self, key):
        self.pop(key, None)


@pytest.fixture
def db_with_cluster():
    cluster = Cluster()
    cluster.uuid = "cluster-1"
    kv = _FakeKV()
    # unwrap_secrets: what write_to_db persists — get_clean_dict keeps SecretStr
    # objects, which json.dumps cannot serialize.
    kv.set(cluster.get_db_id().encode(),
           json.dumps(cluster.to_dict(unwrap_secrets=True)).encode())

    db = DBController.__new__(DBController)
    db.kv_store = kv
    return db, cluster


def _registry(db, cluster, kind):
    raw = db.kv_store.get(cluster.get_db_id().encode())
    return getattr(Cluster().from_dict(json.loads(raw)), tl.REGISTRY_FIELD[kind])


def test_claim_allocates_then_is_idempotent(db_with_cluster):
    db, cluster = db_with_cluster
    first = db.claim_topology_label("cluster-1", tl.FAILURE_DOMAIN, "RACK1")
    assert first == 0
    # Same label again: same id, and no second entry.
    assert db.claim_topology_label("cluster-1", tl.FAILURE_DOMAIN, "RACK1") == 0
    assert _registry(db, cluster, tl.FAILURE_DOMAIN) == {"RACK1": 0}


def test_claim_gives_each_new_label_its_own_id(db_with_cluster):
    db, _ = db_with_cluster
    ids = [db.claim_topology_label("cluster-1", tl.FAILURE_DOMAIN, label)
           for label in ("RACK1", "RACK2", "AZ9")]
    assert ids == [0, 1, 2]
    assert len(set(ids)) == 3


def test_claim_skips_ids_already_on_nodes(db_with_cluster):
    db, _ = db_with_cluster
    # An id chosen by a legacy `--failure-domain 5` call: not in the registry,
    # but a new label must not be handed the same id.
    claimed = db.claim_topology_label(
        "cluster-1", tl.FAILURE_DOMAIN, "RACK1", extra_used=[5])
    assert claimed == 6


def test_claim_with_desired_id_names_an_existing_id(db_with_cluster):
    db, cluster = db_with_cluster
    # The backfill path: id 3 exists on nodes, name it rather than allocate.
    assert db.claim_topology_label(
        "cluster-1", tl.FAILURE_DOMAIN, "FD3", desired_id=3) == 3
    assert _registry(db, cluster, tl.FAILURE_DOMAIN) == {"FD3": 3}


def test_desired_id_does_not_let_two_labels_own_one_id(db_with_cluster):
    db, cluster = db_with_cluster
    db.claim_topology_label("cluster-1", tl.FAILURE_DOMAIN, "RACK1", desired_id=2)
    # A second name for id 2 is refused; the first name wins and the call is a
    # no-op, so rendering stays unambiguous.
    assert db.claim_topology_label(
        "cluster-1", tl.FAILURE_DOMAIN, "FD2", desired_id=2) == 2
    assert _registry(db, cluster, tl.FAILURE_DOMAIN) == {"RACK1": 2}


def test_the_two_kinds_have_independent_registries(db_with_cluster):
    db, cluster = db_with_cluster
    db.claim_topology_label("cluster-1", tl.FAILURE_DOMAIN, "RACK1")
    db.claim_topology_label("cluster-1", tl.PHYSICAL, "HOST1")
    assert _registry(db, cluster, tl.FAILURE_DOMAIN) == {"RACK1": 0}
    assert _registry(db, cluster, tl.PHYSICAL) == {"HOST1": 0}


def test_claim_on_a_missing_cluster_raises(db_with_cluster):
    db, _ = db_with_cluster
    with pytest.raises(KeyError):
        db.claim_topology_label("no-such-cluster", tl.FAILURE_DOMAIN, "RACK1")
