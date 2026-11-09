"""The stretched control plane's pure logic (docs/stretched-control-plane.md):
the lease agent's decision, the lease expiry, the FDB status reading, the
scheme rule, and the API verdict over a mocked mode. No database is touched;
the lease against real FoundationDB is in
tests/integration/test_control_plane_lease.py.
"""
import datetime

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import control_plane_controller as cpc
from simplyblock_core.models.control_plane_lease import ControlPlaneLease

NOW = datetime.datetime(2026, 11, 9, 12, 0, 0, tzinfo=datetime.UTC)
DRAIN = constants.CONTROL_PLANE_DRAIN_SEC


def _lease(holder="a", state=ControlPlaneLease.STATE_HELD, expires_in=10, since_ago=0, epoch=1):
    lease = ControlPlaneLease()
    lease.uuid = lease.cluster_id = "cluster-1"
    lease.holder_site = holder
    lease.holder_host = f"{holder}-host"
    lease.state = state
    lease.epoch = epoch
    lease.expires_at = (NOW + datetime.timedelta(seconds=expires_in)).isoformat()
    lease.state_since = (NOW - datetime.timedelta(seconds=since_ago)).isoformat()
    return lease


def _plan(local, desired, lease):
    return cpc.plan_step(local=local, desired=desired, lease=lease, now=NOW, drain_sec=DRAIN)


class TestPlanStep:

    def test_the_desired_site_acquires_a_missing_lease(self):
        assert _plan("a", "a", None).action == "acquire"

    def test_the_other_site_waits_for_a_missing_lease(self):
        assert _plan("b", "a", None).action == "wait"

    def test_a_control_plane_without_a_site_never_acts(self):
        assert _plan("", "a", None).action == "none"
        assert _plan("", "a", _lease(expires_in=-5)).action == "none"

    def test_the_holder_renews_while_desired(self):
        assert _plan("a", "a", _lease()).action == "renew"

    def test_a_reverted_drain_is_renewed_back_to_held(self):
        assert _plan("a", "a", _lease(state=ControlPlaneLease.STATE_DRAINING)).action == "renew"

    def test_the_holder_drains_when_another_site_is_desired(self):
        step = _plan("a", "b", _lease())
        assert step.action == "drain"
        assert "b" in step.reason

    def test_the_holder_waits_out_the_drain_then_releases(self):
        early = _lease(state=ControlPlaneLease.STATE_DRAINING, since_ago=DRAIN - 1)
        assert _plan("a", "b", early).action == "wait"
        late = _lease(state=ControlPlaneLease.STATE_DRAINING, since_ago=DRAIN)
        assert _plan("a", "b", late).action == "release"

    def test_the_desired_site_acquires_a_released_lease(self):
        assert _plan("b", "b", _lease(state=ControlPlaneLease.STATE_RELEASED)).action == "acquire"

    def test_nobody_else_acquires_a_released_lease(self):
        assert _plan("a", "b", _lease(state=ControlPlaneLease.STATE_RELEASED)).action == "wait"

    def test_the_desired_site_acquires_an_expired_lease(self):
        assert _plan("b", "b", _lease(holder="a", expires_in=-1)).action == "acquire"

    def test_a_live_lease_of_another_site_is_never_taken(self):
        """The core safety rule: the desired site waits for the holder's own
        drain, or for the expiry that only a holder cut off from FDB suffers."""
        assert _plan("b", "b", _lease(holder="a")).action == "wait"
        assert _plan("b", "b", _lease(holder="a", state=ControlPlaneLease.STATE_DRAINING)).action == "wait"


class TestLeaseExpiry:

    def test_no_lease_is_expired(self):
        assert cpc.lease_expired(None, NOW)

    def test_an_unparseable_expiry_is_expired(self):
        lease = _lease()
        lease.expires_at = "garbage"
        assert cpc.lease_expired(lease, NOW)

    def test_a_future_expiry_is_live(self):
        assert not cpc.lease_expired(_lease(expires_in=1), NOW)

    def test_the_boundary_is_expired(self):
        assert cpc.lease_expired(_lease(expires_in=0), NOW)


class TestFdbStatus:

    def test_region_lag_and_data_state_are_read(self):
        status = {"cluster": {"datacenter_lag": {"seconds": 0.25, "versions": 250000},
                              "data": {"state": {"name": "healthy", "healthy": True}}}}
        assert cpc.fdb_region_lag(status) == (0.25, "healthy")

    def test_a_single_region_status_has_no_lag(self):
        assert cpc.fdb_region_lag({"cluster": {"data": {"state": {"name": "healthy"}}}}) == (None, "healthy")

    def test_no_document_reads_as_unknown(self):
        assert cpc.fdb_region_lag(None) == (None, "")
        assert cpc.fdb_region_lag({}) == (None, "")


class TestScheme:

    @pytest.mark.parametrize("kwargs,expected", [
        (dict(sync_replication=True, enable_failure_domain=False, distr_ndcs=4, distr_npcs=2), "sync_replication"),
        (dict(sync_replication=False, enable_failure_domain=True, distr_ndcs=1, distr_npcs=1), "fd_1p1"),
        (dict(sync_replication=False, enable_failure_domain=True, distr_ndcs=1, distr_npcs=2), "fd_1p2"),
        (dict(sync_replication=False, enable_failure_domain=True, distr_ndcs=2, distr_npcs=1), ""),
        (dict(sync_replication=False, enable_failure_domain=False, distr_ndcs=1, distr_npcs=1), ""),
    ])
    def test_the_three_schemes(self, kwargs, expected):
        assert cpc.stretch_scheme(**kwargs) == expected

    def test_an_unsupported_scheme_is_refused_naming_the_three(self):
        with pytest.raises(ValueError) as e:
            cpc.validate_stretched_control_plane(
                stretched_control_plane=True, sync_replication=False, enable_failure_domain=True,
                distr_ndcs=2, distr_npcs=1, ha_type="ha", is_single_node=False)
        for scheme in cpc.STRETCH_SCHEMES.values():
            assert scheme in str(e.value)

    def test_non_ha_is_refused(self):
        with pytest.raises(ValueError):
            cpc.validate_stretched_control_plane(
                stretched_control_plane=True, sync_replication=True, enable_failure_domain=False,
                distr_ndcs=1, distr_npcs=1, ha_type="single", is_single_node=False)

    def test_not_stretched_is_not_checked(self):
        cpc.validate_stretched_control_plane(
            stretched_control_plane=False, sync_replication=False, enable_failure_domain=False,
            distr_ndcs=2, distr_npcs=1, ha_type="single", is_single_node=True)


class TestApiVerdict:

    @pytest.fixture(autouse=True)
    def _fresh(self):
        cpc.reset_identity_cache()
        yield
        cpc.reset_identity_cache()

    def test_a_leader_serves_everything(self, monkeypatch):
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict("leader", "holds"))
        assert cpc.api_verdict("POST", "/api/v2/clusters/x/storage-pools").action == "serve"

    def test_a_standby_serves_reads(self, monkeypatch):
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict(
            "standby", "no lease", redirects={"c1": "https://a.example:443"}))
        assert cpc.api_verdict("GET", "/api/v2/clusters/c1/").action == "serve"

    def test_a_standby_redirects_mutations_to_the_leader(self, monkeypatch):
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict(
            "standby", "no lease", redirects={"c1": "https://a.example:443/"}))
        verdict = cpc.api_verdict("POST", "/api/v2/clusters/c1/storage-pools", "c1")
        assert verdict.action == "redirect"
        assert verdict.location == "https://a.example:443/api/v2/clusters/c1/storage-pools"

    def test_a_standby_without_a_known_leader_endpoint_answers_503(self, monkeypatch):
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict(
            "standby", "no lease", redirects={"c1": ""}))
        verdict = cpc.api_verdict("DELETE", "/api/v2/clusters/c1/storage-pools/p", "c1")
        assert verdict.action == "unavailable"
        assert "site_api_endpoints" in verdict.message

    def test_control_plane_routes_are_always_served_locally(self, monkeypatch):
        """A takeover is issued against the standby; it must never bounce."""
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict(
            "standby", "no lease", redirects={"c1": "https://a.example"}))
        assert cpc.api_verdict("POST", "/api/v2/clusters/c1/control-plane/takeover", "c1").action == "serve"

    def test_a_disconnected_control_plane_answers_503_but_keeps_health(self, monkeypatch):
        def boom():
            raise RuntimeError("fdb timeout")
        monkeypatch.setattr(cpc, "current_mode", boom)
        assert cpc.api_verdict("GET", "/api/v2/clusters/").action == "unavailable"
        assert cpc.api_verdict("GET", "/api/v2/_meta/health").action == "serve"
        assert cpc.api_verdict("GET", "/_meta/metrics").action == "serve"

    def test_services_are_paused_while_disconnected(self, monkeypatch):
        def boom():
            raise RuntimeError("fdb timeout")
        monkeypatch.setattr(cpc, "current_mode", boom)
        enabled, reason = cpc.services_enabled()
        assert not enabled
        assert reason.startswith("disconnected")

    def test_services_run_on_a_single_control_plane(self, monkeypatch):
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict("single", "no stretched cluster"))
        assert cpc.services_enabled()[0]

    def test_services_pause_while_draining(self, monkeypatch):
        monkeypatch.setattr(cpc, "current_mode", lambda: cpc.ModeVerdict(
            "draining", "handing over", redirects={"c1": ""}))
        assert not cpc.services_enabled()[0]


class TestModeWithoutDatabase:

    def test_no_database_means_single(self):
        """The unit tier's fdb stub: nothing is stretched as far as the
        process knows, so every service runs as it always did."""
        cpc.reset_identity_cache()
        assert cpc.current_mode().mode == "single"
