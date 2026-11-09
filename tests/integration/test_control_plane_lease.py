"""The stretched control plane's lease against real FoundationDB
(docs/stretched-control-plane.md): two sites' agents run their cycles over
one database, the lease follows ``leader_site`` through drain and release,
the services of each site follow the lease, a takeover is refused while the
declared-dead site answers and moves the lease once it does not, and the
site-bound failure domains of a 1+1 / 1+2 stretched cluster are assigned
per site and capped by the scheme.

Real DB, mocked probes (``utils.ping_host``) and clocks; no storage node is
started.
"""
import datetime
import uuid

import pytest

from simplyblock_core import constants, utils
from simplyblock_core.controllers import control_plane_controller as cpc
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.control_plane_lease import ControlPlaneLease
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.storage_node import StorageNode

db = DBController()


def _cluster(*, stretched=True, leader_site="a", sync_replication=True, ndcs=1, npcs=1, fd=False):
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.cluster_name = f"stretch-{cluster.uuid[:8]}"
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "ha"
    cluster.distr_ndcs, cluster.distr_npcs = ndcs, npcs
    cluster.sync_replication = sync_replication
    cluster.enable_failure_domain = fd
    cluster.stretched_control_plane = stretched
    cluster.leader_site = leader_site
    cluster.site_api_endpoints = {"a": "https://a.example:443", "b": "https://b.example:443"}
    cluster.write_to_db(db.kv_store)
    return cluster


def _mgmt(cluster, site, status=MgmtNode.STATUS_ONLINE, ip=None):
    node = MgmtNode()
    node.uuid = str(uuid.uuid4())
    node.cluster_id = cluster.get_id()
    node.hostname = f"mgmt-{site}-{node.uuid[:4]}"
    node.mgmt_ip = ip or f"10.{ord(site)}.0.{len(db.get_mgmt_nodes()) + 1}"
    node.site = site
    node.status = status
    node.create_dt = str(datetime.datetime.now())
    node.write_to_db(db.kv_store)
    return node


def _snode(cluster, site, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = str(uuid.uuid4())
    node.cluster_id = cluster.get_id()
    node.site = site
    node.status = status
    node.mgmt_ip = f"10.{ord(site)}.1.{node.uuid[:2]}"
    node.write_to_db(db.kv_store)
    return node


def _cycle(site, host=None):
    """One agent cycle as the control plane of ``site`` would run it."""
    cpc._agent_cycle(db, cpc._AgentState(), site, host or f"mgmt-{site}")


def _as_site(monkeypatch, site):
    monkeypatch.setenv(constants.CONTROL_PLANE_SITE_ENV, site)
    cpc.reset_identity_cache()


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    cpc.reset_identity_cache()
    monkeypatch.setattr(cpc, "_fdb_status", lambda _db: None)
    yield
    cpc.reset_identity_cache()


def _drained(cluster):
    """Age the drain so the holder's next cycle releases."""
    lease = db.get_control_plane_lease(cluster.get_id())
    past = (cpc._now() - datetime.timedelta(seconds=constants.CONTROL_PLANE_DRAIN_SEC + 1)).isoformat()
    db.atomic_update(lease, lambda fresh: setattr(fresh, "state_since", past))


class TestLeaseFollowsLeaderSite:

    def test_the_leader_site_acquires_and_the_other_waits(self, monkeypatch):
        cluster = _cluster(leader_site="a")
        _cycle("b")
        assert db.get_control_plane_lease(cluster.get_id()) is None
        _cycle("a")
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.holder_site == "a" and lease.state == ControlPlaneLease.STATE_HELD and lease.epoch == 1
        _as_site(monkeypatch, "a")
        assert cpc.services_enabled() == (True, "leader: site a holds the lease")
        _as_site(monkeypatch, "b")
        enabled, reason = cpc.services_enabled()
        assert not enabled and reason.startswith("standby")

    def test_renewal_moves_the_expiry_and_keeps_the_epoch(self):
        cluster = _cluster()
        _cycle("a")
        first = db.get_control_plane_lease(cluster.get_id())
        _cycle("a")
        second = db.get_control_plane_lease(cluster.get_id())
        assert second.epoch == first.epoch == 1
        assert second.expires_at >= first.expires_at

    def test_a_planned_move_drains_releases_and_hands_over(self, monkeypatch):
        cluster = _cluster(leader_site="a")
        _cycle("a")
        db.atomic_update(cluster, lambda fresh: setattr(fresh, "leader_site", "b"))
        _cycle("b")  # b must not take a lease a still holds
        assert db.get_control_plane_lease(cluster.get_id()).holder_site == "a"
        _cycle("a")
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.state == ControlPlaneLease.STATE_DRAINING
        _as_site(monkeypatch, "a")
        enabled, reason = cpc.services_enabled()
        assert not enabled and reason.startswith("draining")
        _cycle("b")  # still a's: draining, not released
        assert db.get_control_plane_lease(cluster.get_id()).holder_site == "a"
        _drained(cluster)
        _cycle("a")
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.state == ControlPlaneLease.STATE_RELEASED and lease.released_by == "handover"
        _cycle("b")
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.holder_site == "b" and lease.state == ControlPlaneLease.STATE_HELD and lease.epoch == 2
        _as_site(monkeypatch, "b")
        assert cpc.services_enabled()[0]
        _as_site(monkeypatch, "a")
        assert not cpc.services_enabled()[0]

    def test_an_expired_lease_of_a_dead_holder_is_taken_by_the_desired_site(self):
        cluster = _cluster(leader_site="a")
        _cycle("a")
        lease = db.get_control_plane_lease(cluster.get_id())
        past = (cpc._now() - datetime.timedelta(seconds=1)).isoformat()
        db.atomic_update(lease, lambda fresh: setattr(fresh, "expires_at", past))
        _cycle("b")  # b is not desired: it waits even for an expired lease
        assert db.get_control_plane_lease(cluster.get_id()).holder_site == "a"
        db.atomic_update(cluster, lambda fresh: setattr(fresh, "leader_site", "b"))
        _cycle("b")
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.holder_site == "b" and lease.epoch == 2

    def test_a_cluster_that_is_not_stretched_has_no_lease_and_runs_everywhere(self, monkeypatch):
        cluster = _cluster(stretched=False)
        _cycle("a")
        assert db.get_control_plane_lease(cluster.get_id()) is None
        _as_site(monkeypatch, "b")
        assert cpc.services_enabled() == (True, "single: no stretched cluster")


class TestApiOnAStandby:

    def test_mutations_are_redirected_to_the_leader_endpoint(self, monkeypatch):
        cluster = _cluster(leader_site="a")
        _cycle("a")
        _as_site(monkeypatch, "b")
        verdict = cpc.api_verdict("POST", f"/api/v2/clusters/{cluster.get_id()}/storage-pools", cluster.get_id())
        assert verdict.action == "redirect"
        assert verdict.location == f"https://a.example:443/api/v2/clusters/{cluster.get_id()}/storage-pools"
        assert cpc.api_verdict("GET", f"/api/v2/clusters/{cluster.get_id()}/", cluster.get_id()).action == "serve"


class TestPlannedMoveGates:

    def test_a_site_with_an_offline_node_is_refused_and_force_overrides(self):
        cluster = _cluster(leader_site="a", sync_replication=False, fd=True)
        _mgmt(cluster, "a"), _mgmt(cluster, "b")
        _snode(cluster, "a"), _snode(cluster, "b", status=StorageNode.STATUS_OFFLINE)
        _cycle("a")
        with pytest.raises(cpc.ControlPlaneNotReadyError) as e:
            cpc.move_leader_site(cluster.get_id(), "b")
        assert any("not online" in p for p in e.value.problems)
        assert db.get_cluster_by_id(cluster.get_id()).leader_site == "a"
        readiness = cpc.move_leader_site(cluster.get_id(), "b", force=True)
        assert readiness.problems
        assert db.get_cluster_by_id(cluster.get_id()).leader_site == "b"

    def test_a_ready_site_is_accepted(self):
        cluster = _cluster(leader_site="a", sync_replication=False, fd=True)
        _mgmt(cluster, "a"), _mgmt(cluster, "b")
        _snode(cluster, "a"), _snode(cluster, "b")
        _cycle("a")
        readiness = cpc.move_leader_site(cluster.get_id(), "b")
        assert readiness.ready
        assert db.get_cluster_by_id(cluster.get_id()).leader_site == "b"

    def test_a_site_without_a_control_plane_is_refused(self):
        cluster = _cluster(leader_site="a", sync_replication=False, fd=True)
        _mgmt(cluster, "a")
        _snode(cluster, "c")
        with pytest.raises(cpc.ControlPlaneError, match="no control plane"):
            cpc.move_leader_site(cluster.get_id(), "c")


class TestTakeover:

    def test_refused_while_the_declared_dead_site_answers(self, monkeypatch):
        cluster = _cluster(leader_site="a", sync_replication=False, fd=True)
        _mgmt(cluster, "a"), _mgmt(cluster, "b")
        _snode(cluster, "a")
        _cycle("a")
        monkeypatch.setattr(utils, "ping_host", lambda ip: ip.startswith("10.97."))  # 'a' == 97
        with pytest.raises(cpc.ControlPlaneTakeoverRefusedError) as e:
            cpc.takeover(cluster.get_id(), "b", "a", operator="test")
        assert any("answers ping" in line for line in e.value.alive)
        assert db.get_control_plane_lease(cluster.get_id()).holder_site == "a"

    def test_moves_the_lease_once_nothing_answers(self, monkeypatch):
        cluster = _cluster(leader_site="a", sync_replication=False, fd=True)
        _mgmt(cluster, "a"), _mgmt(cluster, "b")
        _snode(cluster, "a"), _snode(cluster, "b")
        _cycle("a")
        monkeypatch.setattr(utils, "ping_host", lambda ip: False)
        _as_site(monkeypatch, "b")
        status = cpc.takeover(cluster.get_id(), "b", "a", operator="test")
        assert status.leader_site == "b"
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.state == ControlPlaneLease.STATE_RELEASED and lease.released_by == "takeover:test"
        assert lease.epoch == 2
        _cycle("b")
        lease = db.get_control_plane_lease(cluster.get_id())
        assert lease.holder_site == "b" and lease.state == ControlPlaneLease.STATE_HELD and lease.epoch == 3
        # The old holder's renewal, should it come back, fails on the epoch.
        _cycle("a")
        assert db.get_control_plane_lease(cluster.get_id()).holder_site == "b"

    def test_the_confirmation_must_name_the_leading_site(self, monkeypatch):
        cluster = _cluster(leader_site="a", sync_replication=False, fd=True)
        _mgmt(cluster, "a"), _mgmt(cluster, "b")
        _cycle("a")
        monkeypatch.setattr(utils, "ping_host", lambda ip: False)
        with pytest.raises(cpc.ControlPlaneError, match="leads"):
            cpc.takeover(cluster.get_id(), "a", "b")
        with pytest.raises(cpc.ControlPlaneError, match="other site"):
            cpc.takeover(cluster.get_id(), "b", "b")


class TestSiteBoundFailureDomains:

    def test_sites_get_distinct_domains_up_to_the_scheme(self):
        cluster = _cluster(sync_replication=False, fd=True, ndcs=1, npcs=1)
        assert cpc.site_failure_domain(cluster.get_id(), "a") == 0
        assert cpc.site_failure_domain(cluster.get_id(), "b") == 1
        assert cpc.site_failure_domain(cluster.get_id(), "a") == 0
        with pytest.raises(ValueError, match="spans 2"):
            cpc.site_failure_domain(cluster.get_id(), "c")

    def test_one_plus_two_spans_three_sites(self):
        cluster = _cluster(sync_replication=False, fd=True, ndcs=1, npcs=2)
        for site, fd in (("a", 0), ("b", 1), ("c", 2)):
            assert cpc.site_failure_domain(cluster.get_id(), site) == fd
        with pytest.raises(ValueError, match="spans 3"):
            cpc.site_failure_domain(cluster.get_id(), "d")
