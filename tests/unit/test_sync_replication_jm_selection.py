"""Sync-replication journal rules as pure logic: per-site counts, remote JM
names and their sort order, and the per-site pick of journal copies.

The DB-backed flows (get_sorted_ha_jms, get_node_jm_names, the distrib stack,
node removal) are in tests/integration/test_sync_replication_jm_selection.py.
"""
import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import JMDevice
from simplyblock_core.models.storage_node import StorageNode


def _cluster(sync=True, ftt=1, fd=False, single_node=False):
    cluster = Cluster()
    cluster.uuid = "c1"
    cluster.sync_replication = sync
    cluster.max_fault_tolerance = ftt
    cluster.enable_failure_domain = fd
    cluster.is_single_node = single_node
    return cluster


# -- journal size -----------------------------------------------------------------

@pytest.mark.parametrize("ftt,fd,expected", [(1, False, 6), (1, True, 8), (2, False, 8)])
def test_sync_journal_is_the_per_site_rule_on_each_site(ftt, fd, expected):
    assert ops.get_required_ha_jm_count(_cluster(ftt=ftt, fd=fd)) == expected
    assert ops.resolve_ha_jm_count(_cluster(ftt=ftt, fd=fd), None) == expected


@pytest.mark.parametrize("ftt,fd,expected", [(1, False, 3), (1, True, 4), (2, False, 4)])
def test_non_sync_journal_size_is_unchanged(ftt, fd, expected):
    assert ops.get_required_ha_jm_count(_cluster(sync=False, ftt=ftt, fd=fd)) == expected


def test_explicit_even_sync_journal_size_is_kept():
    assert ops.resolve_ha_jm_count(_cluster(), 8) == 8


@pytest.mark.parametrize("count,reason", [(7, "even"), (10, "at most 8"), (4, "too low")])
def test_invalid_sync_journal_size_is_refused(count, reason):
    with pytest.raises(ValueError, match=reason):
        ops.resolve_ha_jm_count(_cluster(), count)


def test_odd_journal_size_stays_valid_without_sync():
    assert ops.resolve_ha_jm_count(_cluster(sync=False), 5) == 5


@pytest.mark.parametrize("asked", [False, None, True])
def test_sync_cluster_always_journals_across_sites(asked):
    assert ops.resolve_enable_ha_jm(_cluster(), asked) is True


def test_ha_journal_toggle_is_unchanged_without_sync():
    assert ops.resolve_enable_ha_jm(_cluster(sync=False), False) is False
    assert ops.resolve_enable_ha_jm(_cluster(sync=False), True) is True
    assert ops.resolve_enable_ha_jm(_cluster(sync=False, single_node=True), True) is False


# -- remote JM names ----------------------------------------------------------------

def test_remote_jm_names_by_site():
    assert ops.remote_jm_controller_name("a", "a", "jm_x") == "remote_jm_x"
    assert ops.remote_jm_controller_name("a", "b", "jm_x") == "remote_xs_jm_x"
    assert ops.remote_jm_controller_name("", "", "jm_x") == "remote_jm_x"


def test_names_sort_own_then_same_site_then_other_site():
    # JC sorts the names and counts the first jm_n_local as local; ids are
    # chosen so that the id order alone would put every name the wrong way.
    own = "jm_zzz"
    same = ops.remote_jm_controller_name("a", "a", "jm_mmm") + "n1"
    other = ops.remote_jm_controller_name("a", "b", "jm_aaa") + "n1"
    assert sorted([other, same, own]) == [own, same, other]


# -- per-site pick --------------------------------------------------------------------

def _cand(ip, fd=-1, label=0, site="a"):
    return ops._JMCandidate(ip, fd, label, site)


def _pick(candidates, total, target, taken, fd_enabled=False, local_fd=-1):
    jm_count = dict.fromkeys(candidates, 0)
    return ops._pick_ha_jms(candidates, jm_count, total, target, taken, fd_enabled, local_fd, "n")


def test_pick_never_puts_two_copies_on_one_host():
    candidates = {"j1": _cand("h1"), "j2": _cand("h1"), "j3": _cand("h2")}
    assert _pick(candidates, 3, 3, []) == ["j1", "j3"]


def test_pick_skips_the_hosts_already_holding_a_copy():
    candidates = {"j1": _cand("h1"), "j2": _cand("h2")}
    assert _pick(candidates, 3, 2, [("h1", -1, 0)]) == ["j2"]


def test_pick_balances_four_copies_over_two_domains_with_the_local_one():
    # own site of a 4 + 4 journal: the local JM is in domain 0, so the three
    # copies picked here must end 2-2 across the site's two domains.
    candidates = {"a0": _cand("h1", fd=0), "a1": _cand("h2", fd=0),
                  "b0": _cand("h3", fd=1), "b1": _cand("h4", fd=1), "b2": _cand("h5", fd=1)}
    picked = _pick(candidates, 4, 3, [("h0", 0, 0)], fd_enabled=True, local_fd=0)
    fds = [candidates[j].fd for j in picked] + [0]
    assert len(picked) == 3
    assert fds.count(0) == 2 and fds.count(1) == 2


def test_pick_balances_the_other_site_without_the_local_domain():
    # other site of a 4 + 4 journal: nothing taken there yet, domain ids
    # of the local site mean nothing -> 2-2 inside that site.
    candidates = {"a0": _cand("h1", fd=0), "a1": _cand("h2", fd=0), "a2": _cand("h3", fd=0),
                  "b0": _cand("h4", fd=1), "b1": _cand("h5", fd=1)}
    picked = _pick(candidates, 4, 4, [], fd_enabled=True, local_fd=-1)
    fds = [candidates[j].fd for j in picked]
    assert fds.count(0) == 2 and fds.count(1) == 2


# -- replacement on node removal ---------------------------------------------------

def _node(node_id, site, host, fd=-1, jm=True, jm_ids=(), ha_jm_count=6,
          status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = "c1"
    node.status = status
    node.site = site
    node.mgmt_ip = host
    node.failure_domain = fd
    node.ha_jm_count = ha_jm_count
    node.jm_ids = list(jm_ids)
    if jm:
        dev = JMDevice()
        dev.uuid = f"jm-{node_id}"
        dev.node_id = node_id
        dev.jm_bdev = f"jm_{node_id}"
        dev.status = JMDevice.STATUS_ONLINE
        node.jm_device = dev
    return node


def _replacement_setup(extra_b=()):
    """Owner a1 journals on a2, a3 (site a) and b1, b2, b3 (site b); b1 goes away."""
    owner = _node("a1", "a", "ha1", jm_ids=["jm-a2", "jm-a3", "jm-b1", "jm-b2", "jm-b3"])
    nodes = [owner, _node("a2", "a", "ha2"), _node("a3", "a", "ha3"), _node("a4", "a", "ha4"),
             _node("b1", "b", "hb1"), _node("b2", "b", "hb2"), _node("b3", "b", "hb3"),
             *extra_b]
    return owner, nodes


def test_replacement_comes_from_the_removed_jms_site_only():
    owner, nodes = _replacement_setup()
    # a4 is free on the other site; b-site has no free JM -> nothing, never a4.
    assert ops._pick_site_replacement_jm(owner, "jm-b1", "b", nodes, False) is None
    owner, nodes = _replacement_setup(extra_b=[_node("b4", "b", "hb4")])
    assert ops._pick_site_replacement_jm(owner, "jm-b1", "b", nodes, False) == "jm-b4"


def test_replacement_is_never_on_a_surviving_members_host():
    # b4 shares b2's host; b5 is on a host of its own.
    owner, nodes = _replacement_setup(extra_b=[_node("b4", "b", "hb2"), _node("b5", "b", "hb5")])
    assert ops._pick_site_replacement_jm(owner, "jm-b1", "b", nodes, False) == "jm-b5"


def test_replacement_restores_the_domain_balance_of_the_site():
    # 4 + 4 journal; site b holds b2 (fd 0), b3, b4 (fd 1) after losing b1 (fd 0).
    owner = _node("a1", "a", "ha1", ha_jm_count=8,
                  jm_ids=["jm-a2", "jm-a3", "jm-a4", "jm-b1", "jm-b2", "jm-b3", "jm-b4"])
    nodes = [owner, _node("a2", "a", "ha2"), _node("a3", "a", "ha3"), _node("a4", "a", "ha4"),
             _node("b1", "b", "hb1", fd=0), _node("b2", "b", "hb2", fd=0),
             _node("b3", "b", "hb3", fd=1), _node("b4", "b", "hb4", fd=1),
             _node("b5", "b", "hb5", fd=1), _node("b6", "b", "hb6", fd=0)]
    assert ops._pick_site_replacement_jm(owner, "jm-b1", "b", nodes, True) == "jm-b6"


def test_replacement_on_the_owners_own_site_avoids_the_owners_host():
    owner, nodes = _replacement_setup(extra_b=[])
    nodes.append(_node("a5", "a", "ha1"))   # shares the owner's host
    nodes.append(_node("a6", "a", "ha6"))
    owner.jm_ids = ["jm-a2", "jm-a3", "jm-b1", "jm-b2", "jm-b3"]
    nodes.remove(next(n for n in nodes if n.get_id() == "a4"))
    # a2 goes away: a5 comes first but sits on the owner's host; a6 qualifies.
    assert ops._pick_site_replacement_jm(owner, "jm-a2", "a", nodes, False) == "jm-a6"


def test_replacement_skips_a_jm_whose_owner_cannot_be_attached():
    # a forced shutdown leaves b4 offline with its JM status still ONLINE
    owner, nodes = _replacement_setup(extra_b=[_node("b4", "b", "hb4", status=StorageNode.STATUS_OFFLINE),
                                               _node("b5", "b", "hb5")])
    assert ops._pick_site_replacement_jm(owner, "jm-b1", "b", nodes, False) == "jm-b5"
