"""Pure rules of a site loss on a sync-replication cluster: the veto reading
of the data-plane vote (how many answering peers still report a node's JM
link enabled), the management probe of the forced promote's proof (which
probe, if any, got an answer from a node's SPDK), the probe of the veto dict
and the cluster verdict built on the two dicts. The DB-backed flows are in
tests/integration/test_sync_replication_disaster.py.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import storage_node_monitor
from simplyblock_core.services import tasks_runner_sync_promote as runner
from simplyblock_core.snode_client import SNodeClientException

API_UP = "the node API reports the SPDK process up"
RPC_ANSWERS = "its RPC answers"


def _node(node_id="a0"):
    node = MagicMock(spec=StorageNode)
    node.get_id.return_value = node_id
    node.cluster_id = "cluster-1"
    node.rpc_port = 8080
    return node


# ---------------------------------------------------------------------------
# the vote as a veto
# ---------------------------------------------------------------------------

class TestSeenVotes:

    @pytest.mark.parametrize("votes, seen", [
        ((0, 0), (0, 0)),       # nobody to ask, or nobody holds the link
        ((2, 3), (1, 3)),       # one peer still reports it enabled
        ((3, 3), (0, 3)),
        ((0, 3), (3, 3)),
    ])
    def test_enabled_is_total_minus_disconnected(self, votes, seen):
        node = _node()
        with patch.object(storage_node_monitor, "_data_plane_votes", return_value=votes) as taken:
            assert storage_node_monitor.data_plane_seen_votes(node) == seen
        taken.assert_called_once_with(node, False)

    def test_fresh_is_passed_through(self):
        node = _node()
        with patch.object(storage_node_monitor, "_data_plane_votes", return_value=(2, 3)) as taken:
            assert storage_node_monitor.data_plane_seen_votes(node, fresh=True) == (1, 3)
        taken.assert_called_once_with(node, True)


# ---------------------------------------------------------------------------
# the management probe
# ---------------------------------------------------------------------------

_REFUSED = SNodeClientException("connection refused")
_VERSION = {"version": "SPDK v25.05"}


class TestSpdkAnswers:

    @staticmethod
    def _probe(node_api, rpc):
        """``_spdk_answers`` of a node whose node API gives ``node_api`` and
        whose ``spdk_get_version`` gives ``rpc`` (an exception is raised)."""
        node = _node()
        is_up = node.client.return_value.spdk_process_is_up
        version = node.rpc_client.return_value.get_version
        for mock, answer in ((is_up, node_api), (version, rpc)):
            if isinstance(answer, Exception):
                mock.side_effect = answer
            else:
                mock.return_value = answer
        return runner._spdk_answers(node), node

    def test_the_node_api_reporting_spdk_up_is_an_answer(self):
        answered, node = self._probe((True, None), RPCException("connection error"))
        assert answered == API_UP
        node.client.return_value.spdk_process_is_up.assert_called_once_with(8080, "cluster-1")
        node.rpc_client.assert_not_called()

    @pytest.mark.parametrize("node_api", [_REFUSED, (False, None), (None, "no such process")])
    def test_a_version_from_the_rpc_is_an_answer(self, node_api):
        answered, _ = self._probe(node_api, _VERSION)
        assert answered == RPC_ANSWERS

    @pytest.mark.parametrize("node_api", [_REFUSED, (False, None)])
    def test_a_live_proxy_in_front_of_a_dead_spdk_is_no_answer(self, node_api):
        # The RPC proxy is up and returns no result, so get_version() comes
        # back None without raising.
        answered, node = self._probe(node_api, None)
        assert answered == ""
        node.rpc_client.return_value.get_version.assert_called_once_with()

    @pytest.mark.parametrize("node_api", [_REFUSED, (False, None)])
    def test_nothing_answering_is_no_answer(self, node_api):
        answered, _ = self._probe(node_api, RPCException("connection error"))
        assert answered == ""


# ---------------------------------------------------------------------------
# the probe of the veto dict
# ---------------------------------------------------------------------------

class TestCollectSiteGone:

    PROBED = (StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_IN_SHUTDOWN)
    NOT_PROBED = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_RESTARTING,
                  StorageNode.STATUS_OFFLINE, StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_SUSPENDED,
                  StorageNode.STATUS_REMOVED)

    def test_a_transient_node_without_an_enabled_vote_is_gone(self):
        nodes, seen = [], {}
        for status in self.PROBED:
            for votes in ((0, 0), (1, 3)):
                node = _node(f"{status}-{votes[0]}")
                node.status = status
                nodes.append(node)
                seen[node.get_id()] = votes
        for status in self.NOT_PROBED:
            node = _node(status)
            node.status = status
            nodes.append(node)
        with patch.object(storage_node_monitor, "data_plane_seen_votes",
                          side_effect=lambda node: seen[node.get_id()]) as probe:
            gone = storage_node_monitor._collect_site_gone(nodes)
        assert gone == {node_id: votes[0] == 0 for node_id, votes in seen.items()}
        assert sorted(c.args[0].get_id() for c in probe.call_args_list) == sorted(seen)


# ---------------------------------------------------------------------------
# the cluster verdict
# ---------------------------------------------------------------------------

SITE_A, SITE_B = "site-a", "site-b"
ONLINE, OFFLINE = StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE
UNREACHABLE, RESTARTING = StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_RESTARTING
SCHEDULABLE = StorageNode.STATUS_SCHEDULABLE
ACTIVE, DEGRADED, SUSPENDED = Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_SUSPENDED
DRAINING, DOWN = Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN


def _sync_cluster(record=("", "")):
    """FTT 1 per site (ndcs 1, npcs 1), ``record`` the stored lost-site pair."""
    cluster = Cluster()
    cluster.uuid = "cluster-1"
    cluster.sync_replication = True
    cluster.distr_ndcs, cluster.distr_npcs = 1, 1
    cluster.lost_site, cluster.lost_site_state = record
    return cluster


def _storage_node(node_id, site, status):
    """A node with one device - unavailable once the node is OFFLINE (as
    set_node_offline leaves it), online otherwise."""
    node = StorageNode()
    node.uuid, node.site, node.status = node_id, site, status
    node.mgmt_ip = f"10.0.{0 if site == SITE_A else 1}.{node_id[1:]}"
    dev = NVMeDevice()
    dev.uuid = f"{node_id}-d0"
    dev.status = NVMeDevice.STATUS_UNAVAILABLE if status == OFFLINE else NVMeDevice.STATUS_ONLINE
    node.nvme_devices = [dev]
    return node


def _layout(a=(ONLINE,) * 3, b=(ONLINE,) * 3):
    """Site A's nodes a0.., site B's b0.., in the given statuses."""
    return ([_storage_node(f"a{i}", SITE_A, st) for i, st in enumerate(a)]
            + [_storage_node(f"b{i}", SITE_B, st) for i, st in enumerate(b)])


def _verdict(cluster, nodes, majority=(), veto=()):
    """``majority``: the node ids a peer majority reports disconnected (main's
    dict); ``veto``: those no answering peer reports enabled (the veto dict)."""
    return storage_node_monitor._sync_cluster_status(
        cluster, nodes, {}, {n: True for n in majority}, {n: True for n in veto}, [])


def _proposed(verdict):
    return verdict.status, verdict.site, verdict.state


class TestSyncClusterStatus:
    """3 + 3 nodes, FTT 1 per site, one site-loss case per test."""

    # the site goes

    def test_two_nodes_offline_and_one_up_is_a_site_draining(self):
        verdict = _verdict(_sync_cluster(), _layout(a=(OFFLINE, OFFLINE, ONLINE)))
        assert _proposed(verdict) == (DEGRADED, SITE_A, DRAINING)

    def test_every_node_offline_is_a_site_down(self):
        verdict = _verdict(_sync_cluster(), _layout(a=(OFFLINE,) * 3))
        assert _proposed(verdict) == (DEGRADED, SITE_A, DOWN)

    def test_every_node_unreachable_unvetoed_is_a_site_down(self):
        a = ("a0", "a1", "a2")
        verdict = _verdict(_sync_cluster(), _layout(a=(UNREACHABLE,) * 3), majority=a, veto=a)
        assert _proposed(verdict) == (DEGRADED, SITE_A, DOWN)

    def test_one_vetoed_node_keeps_the_site_draining(self):
        # a0: one peer still reports its link enabled - a majority reports it
        # disconnected all the same, which alone would make the site "down"
        verdict = _verdict(_sync_cluster(), _layout(a=(UNREACHABLE,) * 3),
                           majority=("a0", "a1", "a2"), veto=("a1", "a2"))
        assert _proposed(verdict) == (DEGRADED, SITE_A, DRAINING)

    def test_three_vetoed_unreachable_nodes_are_no_site_loss(self):
        nodes = _layout(a=(UNREACHABLE,) * 3)
        verdict = _verdict(_sync_cluster(), nodes)
        assert _proposed(verdict) == (ACTIVE, "", "")
        assert verdict.gone == {}

    # the site is recorded

    @pytest.mark.parametrize("first", [RESTARTING, ONLINE])
    def test_a_recorded_site_with_one_node_up_is_not_judged(self, first):
        verdict = _verdict(_sync_cluster((SITE_A, DOWN)), _layout(a=(first, OFFLINE, OFFLINE)))
        assert _proposed(verdict) == (DEGRADED, "", "")

    def test_a_recorded_site_coming_back_stays_degraded(self):
        cluster = _sync_cluster((SITE_A, DOWN))
        a = [OFFLINE] * 3
        for i in range(4):
            assert _proposed(_verdict(cluster, _layout(a=tuple(a)))) == (DEGRADED, "", ""), a
            if i < 3:
                a[i] = ONLINE

    def test_a_recorded_site_and_the_other_out_suspend(self):
        verdict = _verdict(_sync_cluster((SITE_A, DOWN)),
                           _layout(a=(OFFLINE,) * 3, b=(OFFLINE, OFFLINE, ONLINE)))
        assert _proposed(verdict) == (SUSPENDED, "", "")

    def test_both_sites_beyond_their_ftt_suspend(self):
        verdict = _verdict(_sync_cluster(), _layout(a=(OFFLINE, OFFLINE, ONLINE), b=(OFFLINE, OFFLINE, ONLINE)))
        assert _proposed(verdict) == (SUSPENDED, "", "")

    # no witness: the management node lost its own network

    def test_all_nodes_unwitnessed_fall_back_to_majority(self):
        """Gone by the veto only, so the majority dict (nobody gone) decides."""
        nodes = _layout(a=(UNREACHABLE,) * 3, b=(UNREACHABLE,) * 3)
        verdict = _verdict(_sync_cluster(), nodes, veto=[n.get_id() for n in nodes])
        assert _proposed(verdict) == (ACTIVE, "", "")
        assert verdict.gone == {}

    def test_two_per_site_unwitnessed_fall_back_to_majority(self):
        """Gone by the veto only, so the majority dict (nobody gone) decides."""
        nodes = _layout(a=(UNREACHABLE, UNREACHABLE, ONLINE), b=(UNREACHABLE, UNREACHABLE, ONLINE))
        verdict = _verdict(_sync_cluster(), nodes, veto=("a0", "a1", "b0", "b1"))
        assert _proposed(verdict) == (ACTIVE, "", "")

    def test_a_recorded_site_without_a_witness_stays_degraded(self):
        majority: dict = {}
        verdict = storage_node_monitor._sync_cluster_status(
            _sync_cluster((SITE_A, DOWN)), _layout(a=(OFFLINE,) * 3, b=(UNREACHABLE,) * 3),
            {}, majority, {"b0": True, "b1": True, "b2": True}, [])
        assert _proposed(verdict) == (DEGRADED, "", "")
        assert verdict.gone == {}

    # no healthy site by the veto rule, a recorded site: the majority dict
    # decides the status, a node is escalated only when both dicts call it
    # gone. A: a0 UNREACHABLE, the two others ONLINE (the voters); B: b0, b1
    # UNREACHABLE and abstained on (gone by the veto only) -> B is out by
    # the veto rule and live by the majority.

    @pytest.mark.parametrize("status", [UNREACHABLE, SCHEDULABLE])
    @pytest.mark.parametrize("state", [DRAINING, DOWN])
    @pytest.mark.parametrize("majority, veto, gone", [
        (("a0",), ("b0", "b1"), {}),                          # a peer still reports a0 enabled
        (("a0",), ("a0", "b0", "b1"), {"a0": True}),          # gone by both
        ((), ("a0", "b0", "b1"), {}),                         # gone by the veto only
    ])
    def test_with_no_healthy_site_escalation_needs_both_dicts(
            self, status, state, majority, veto, gone):
        verdict = _verdict(_sync_cluster((SITE_A, state)),
                           _layout(a=(status, ONLINE, ONLINE), b=(UNREACHABLE, UNREACHABLE, ONLINE)),
                           majority=majority, veto=veto)
        assert _proposed(verdict) == (DEGRADED, "", "")
        assert verdict.gone == gone

    # degenerate snapshots: the computation of main

    def test_no_node_with_a_site_suspends(self):
        majority: dict = {}
        verdict = storage_node_monitor._sync_cluster_status(_sync_cluster(), [], {}, majority, {}, [])
        assert _proposed(verdict) == (SUSPENDED, "", "")
        assert verdict.gone == {}

    def test_a_lone_gone_site_suspends_with_no_proposal(self):
        majority: dict = {}
        nodes = [n for n in _layout(a=(OFFLINE,) * 3) if n.site == SITE_A]
        verdict = storage_node_monitor._sync_cluster_status(_sync_cluster(), nodes, {}, majority, {}, [])
        assert _proposed(verdict) == (SUSPENDED, "", "")
        assert verdict.gone == {}
