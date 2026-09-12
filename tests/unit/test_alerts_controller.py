# coding=utf-8
"""Active-alert derivation.

Two things are being pinned here, and the second one is the one that matters.

The first is that each condition an operator must act on produces an alert.
The second is that each condition an operator CAUSED produces none. Every
alert in this set is also generated many times a day by ordinary work -- an
operator shuts a node down, so it is offline and the cluster is degraded; a
node restarts, so its peers go briefly down; an operator pulls a device, so
it is unavailable. A feed that fires on those is noise, and a noisy feed is
one nobody reads, so the suppressions are load-bearing, not polish.

Resolution is structural rather than tested by a "resolve" path: alerts are
recomputed from current state on every call, so an alert stops existing the
moment its condition does. The tests assert that absence directly.
"""

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from simplyblock_core.controllers import alerts_controller as ac
from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


NOW = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)
CLUSTER_ID = "cl-1"


def _iso(seconds_ago):
    return str(NOW - timedelta(seconds=seconds_ago))


def _cluster(status=Cluster.STATUS_ACTIVE):
    c = SimpleNamespace(status=status, updated_at=_iso(600))
    c.get_id = lambda: CLUSTER_ID
    return c


def _device(dev_id="dev-1", status=NVMeDevice.STATUS_ONLINE, admin_removed=False):
    d = SimpleNamespace(status=status, admin_removed=admin_removed,
                        serial_number="SN1")
    d.get_id = lambda: dev_id
    return d


def _node(node_id="node-1", status=StorageNode.STATUS_ONLINE, *,
          auto_restart_disabled=False, updated_at=None, down_since="",
          devices=None, hostname=""):
    n = SimpleNamespace(
        status=status,
        hostname=hostname,
        auto_restart_disabled=auto_restart_disabled,
        updated_at=updated_at if updated_at is not None else _iso(10),
        down_since=down_since,
        nvme_devices=devices if devices is not None else [],
    )
    n.get_id = lambda: node_id
    return n


def _event(domain, level, event="", message="", node_id="node-1", seconds_ago=60):
    e = EventObj()
    e.domain = domain
    e.event_level = level
    e.event = event
    e.message = message
    e.node_id = node_id
    e.date = round((NOW - timedelta(seconds=seconds_ago)).timestamp() * 1000)
    return e


def _kinds(alerts):
    return {a["kind"] for a in alerts}


def _run(cluster=None, nodes=(), events=(), api_mean=None):
    return ac.evaluate_alerts(cluster or _cluster(), list(nodes), list(events),
                              now=NOW, api_mean_seconds=api_mean)


# ==========================================================================
# node offline
# ==========================================================================

class TestNodeOffline(unittest.TestCase):
    def test_an_unexplained_offline_node_alerts(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_OFFLINE)])
        self.assertIn(ac.NODE_OFFLINE, _kinds(alerts))
        self.assertEqual(alerts[0]["message"], "node node-1 offline")
        self.assertEqual(alerts[0]["severity"], ac.SEVERITY_CRITICAL)

    def test_an_operator_shutdown_does_not_alert(self):
        """`sn shutdown` sets auto_restart_disabled and it stays set for as
        long as the node is down, so it is exactly the record of intent."""
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_OFFLINE,
                                   auto_restart_disabled=True)])
        self.assertNotIn(ac.NODE_OFFLINE, _kinds(alerts))

    def test_it_resolves_when_the_node_is_online_again(self):
        self.assertEqual(_run(nodes=[_node(status=StorageNode.STATUS_ONLINE)]), [])

    def test_the_hostname_is_used_when_known(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_OFFLINE,
                                   hostname="worker-3")])
        self.assertEqual(alerts[0]["message"], "node worker-3 offline")


# ==========================================================================
# node hung in restart
# ==========================================================================

class TestNodeRestartHung(unittest.TestCase):
    def test_a_normal_restart_does_not_alert(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_RESTARTING,
                                   updated_at=_iso(30))])
        self.assertEqual(alerts, [])

    def test_past_three_minutes_it_alerts(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_RESTARTING,
                                   updated_at=_iso(ac.RESTART_HUNG_SEC + 1))])
        self.assertIn(ac.NODE_RESTART_HUNG, _kinds(alerts))
        self.assertEqual(alerts[0]["message"], "node node-1 hanging in restart")

    def test_the_threshold_is_three_minutes(self):
        self.assertEqual(ac.RESTART_HUNG_SEC, 180.0)

    def test_an_unparseable_entry_time_does_not_alert(self):
        """Without a start time there is no duration, and guessing one would
        alert on every restart in the cluster."""
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_RESTARTING,
                                   updated_at="")])
        self.assertEqual(alerts, [])


# ==========================================================================
# node unavailable / down
# ==========================================================================

class TestNodeUnavailable(unittest.TestCase):
    def test_unreachable_alerts(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_UNREACHABLE)])
        self.assertIn(ac.NODE_UNAVAILABLE, _kinds(alerts))
        self.assertEqual(alerts[0]["message"], "node node-1 unavailable")


class TestNodeDown(unittest.TestCase):
    def test_a_sustained_down_alerts(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_DOWN,
                                   down_since=_iso(ac.DOWN_GRACE_SEC + 1))])
        self.assertIn(ac.NODE_DOWN, _kinds(alerts))
        self.assertEqual(alerts[0]["message"], "node node-1 down")

    def test_a_fresh_down_is_within_the_grace_window(self):
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_DOWN,
                                   down_since=_iso(5))])
        self.assertEqual(alerts, [])

    def test_a_peer_restart_explains_it(self):
        """The expected short-term case: another node is restarting, and this
        one is down as a consequence."""
        nodes = [_node("node-1", StorageNode.STATUS_DOWN,
                       down_since=_iso(ac.DOWN_GRACE_SEC + 1)),
                 _node("node-2", StorageNode.STATUS_RESTARTING, updated_at=_iso(20))]
        self.assertNotIn(ac.NODE_DOWN, _kinds(_run(nodes=nodes)))

    def test_a_peer_shutdown_explains_it(self):
        nodes = [_node("node-1", StorageNode.STATUS_DOWN,
                       down_since=_iso(ac.DOWN_GRACE_SEC + 1)),
                 _node("node-2", StorageNode.STATUS_IN_SHUTDOWN)]
        self.assertNotIn(ac.NODE_DOWN, _kinds(_run(nodes=nodes)))

    def test_its_own_transition_does_not_explain_itself(self):
        """Only a PEER mid-operation excuses a DOWN. A lone down node is a
        fault however the set is computed."""
        nodes = [_node("node-1", StorageNode.STATUS_DOWN,
                       down_since=_iso(ac.DOWN_GRACE_SEC + 1))]
        self.assertIn(ac.NODE_DOWN, _kinds(_run(nodes=nodes)))

    def test_a_missing_down_since_still_alerts(self):
        """Matches the monitor's own _down_longer_than: never silently ignore
        a DOWN whose entry time cannot be established."""
        nodes = [_node("node-1", StorageNode.STATUS_DOWN, down_since="")]
        self.assertIn(ac.NODE_DOWN, _kinds(_run(nodes=nodes)))


# ==========================================================================
# cluster
# ==========================================================================

class TestClusterAlerts(unittest.TestCase):
    def test_degraded_alerts(self):
        alerts = _run(cluster=_cluster(Cluster.STATUS_DEGRADED))
        self.assertIn(ac.CLUSTER_DEGRADED, _kinds(alerts))
        self.assertEqual(alerts[0]["message"], f"cluster {CLUSTER_ID} degraded")

    def test_degraded_by_an_operator_shutdown_does_not_alert(self):
        alerts = _run(cluster=_cluster(Cluster.STATUS_DEGRADED),
                      nodes=[_node(status=StorageNode.STATUS_OFFLINE,
                                   auto_restart_disabled=True)])
        self.assertNotIn(ac.CLUSTER_DEGRADED, _kinds(alerts))

    def test_degraded_with_an_unexplained_offline_node_still_alerts(self):
        alerts = _run(cluster=_cluster(Cluster.STATUS_DEGRADED),
                      nodes=[_node(status=StorageNode.STATUS_OFFLINE)])
        self.assertIn(ac.CLUSTER_DEGRADED, _kinds(alerts))
        self.assertIn(ac.NODE_OFFLINE, _kinds(alerts))

    def test_suspended_always_alerts(self):
        """No suppression: a suspended cluster serves no IO, whatever caused
        it."""
        alerts = _run(cluster=_cluster(Cluster.STATUS_SUSPENDED),
                      nodes=[_node(status=StorageNode.STATUS_OFFLINE,
                                   auto_restart_disabled=True)])
        self.assertIn(ac.CLUSTER_SUSPENDED, _kinds(alerts))

    def test_an_active_cluster_is_quiet(self):
        self.assertEqual(_run(cluster=_cluster(Cluster.STATUS_ACTIVE)), [])


# ==========================================================================
# devices
# ==========================================================================

class TestDeviceAlerts(unittest.TestCase):
    def test_an_unavailable_device_alerts(self):
        node = _node(devices=[_device(status=NVMeDevice.STATUS_UNAVAILABLE)])
        alerts = _run(nodes=[node])
        self.assertIn(ac.DEVICE_UNAVAILABLE, _kinds(alerts))
        self.assertEqual(
            alerts[0]["message"],
            "device dev-1 on node node-1 unavailable: "
            "potential device failure / removal")
        self.assertEqual(alerts[0]["device_id"], "dev-1")

    def test_an_operator_removed_device_does_not_alert(self):
        node = _node(devices=[_device(status=NVMeDevice.STATUS_REMOVED,
                                      admin_removed=True)])
        self.assertEqual(_run(nodes=[node]), [])

    def test_an_unsolicited_removal_does_alert(self):
        node = _node(devices=[_device(status=NVMeDevice.STATUS_REMOVED)])
        self.assertIn(ac.DEVICE_UNAVAILABLE, _kinds(_run(nodes=[node])))

    def test_a_failed_device_alerts(self):
        node = _node(devices=[_device(status=NVMeDevice.STATUS_FAILED)])
        self.assertIn(ac.DEVICE_UNAVAILABLE, _kinds(_run(nodes=[node])))

    def test_only_an_online_node_raises_device_alerts(self):
        """A device is reported unavailable for all sorts of reasons that are
        about the NODE: a graceful shutdown marks every one of them
        unavailable on purpose, a restart takes them down with the node, and
        an offline or down node cannot say anything meaningful about its
        devices. In each of those the node is the alert, and one node
        condition must not also become N device alerts."""
        devs = [_device(f"dev-{i}", NVMeDevice.STATUS_UNAVAILABLE) for i in range(6)]
        for status in (StorageNode.STATUS_IN_SHUTDOWN,
                       StorageNode.STATUS_RESTARTING,
                       StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_REMOVED,
                       StorageNode.STATUS_IN_CREATION):
            node = _node(status=status, auto_restart_disabled=True,
                         updated_at=_iso(5), down_since=_iso(5), devices=devs)
            self.assertNotIn(ac.DEVICE_UNAVAILABLE, _kinds(_run(nodes=[node])),
                             f"device alert raised on a {status} node")

    def test_an_online_node_with_a_bad_device_does_alert(self):
        devs = [_device(f"dev-{i}", NVMeDevice.STATUS_UNAVAILABLE) for i in range(3)]
        node = _node(status=StorageNode.STATUS_ONLINE, devices=devs)
        alerts = [a for a in _run(nodes=[node]) if a["kind"] == ac.DEVICE_UNAVAILABLE]
        self.assertEqual(len(alerts), 3)

    def test_an_online_device_is_quiet(self):
        node = _node(devices=[_device()])
        self.assertEqual(_run(nodes=[node]), [])


# ==========================================================================
# event-derived
# ==========================================================================

class TestEventDerivedAlerts(unittest.TestCase):
    def test_jm_compression_error_alerts(self):
        events = [_event(ec.DOMAIN_JM, EventObj.LEVEL_ERROR, event="jm_compression",
                         message="compression_failed (error_code=11)")]
        alerts = _run(events=events)
        self.assertIn(ac.NODE_JC_COMPRESSION_ERROR, _kinds(alerts))
        self.assertEqual(alerts[0]["message"],
                         "node node-1: jc compression error - critical!")

    def test_a_successful_compression_event_does_not_alert(self):
        events = [_event(ec.DOMAIN_JM, EventObj.LEVEL_INFO, event="jm_compression",
                         message="compression_finished")]
        self.assertEqual(_run(events=events), [])

    def test_other_jm_errors_are_the_jc_error_alert(self):
        events = [_event(ec.DOMAIN_JM, EventObj.LEVEL_ERROR, event="jm_fault")]
        alerts = _run(events=events)
        self.assertIn(ac.NODE_JC_ERROR, _kinds(alerts))
        self.assertEqual(alerts[0]["message"],
                         "node node-1: unrecoverable JC error")

    def test_a_distr_error_is_an_unrecoverable_io_error(self):
        events = [_event(ec.DOMAIN_DISTR, EventObj.LEVEL_ERROR, event="error_write")]
        alerts = _run(events=events)
        self.assertIn(ac.NODE_IO_ERROR, _kinds(alerts))
        self.assertEqual(alerts[0]["message"],
                         "node node-1: unrecoverable io error")

    def test_a_storm_collapses_to_one_alert_per_node(self):
        """A distrib fault is reported many times. One alert per report would
        be the event log again under another name."""
        events = [_event(ec.DOMAIN_DISTR, EventObj.LEVEL_ERROR, seconds_ago=i)
                  for i in range(1, 200)]
        alerts = _run(events=events)
        self.assertEqual(len([a for a in alerts if a["kind"] == ac.NODE_IO_ERROR]), 1)

    def test_separate_nodes_are_separate_alerts(self):
        events = [_event(ec.DOMAIN_DISTR, EventObj.LEVEL_ERROR, node_id="node-1"),
                  _event(ec.DOMAIN_DISTR, EventObj.LEVEL_ERROR, node_id="node-2")]
        alerts = _run(events=events)
        self.assertEqual(len([a for a in alerts if a["kind"] == ac.NODE_IO_ERROR]), 2)

    def test_an_old_event_has_aged_out(self):
        events = [_event(ec.DOMAIN_DISTR, EventObj.LEVEL_ERROR,
                         seconds_ago=ac.EVENT_WINDOW_SEC + 60)]
        self.assertEqual(_run(events=events), [])

    def test_capacity_critical_is_a_warning(self):
        events = [_event(ec.DOMAIN_CLUSTER, EventObj.LEVEL_CRITICAL,
                         event=ec.EVENT_CAPACITY,
                         message="Cluster absolute capacity reached: 92%")]
        alerts = _run(events=events)
        self.assertIn(ac.CLUSTER_CAPACITY_CRITICAL, _kinds(alerts))
        self.assertEqual(alerts[0]["severity"], ac.SEVERITY_WARNING)
        self.assertEqual(alerts[0]["message"],
                         f"warning: cluster {CLUSTER_ID} critical capacity limit reached")

    def test_provisioned_capacity_is_its_own_alert(self):
        events = [_event(ec.DOMAIN_CLUSTER, EventObj.LEVEL_CRITICAL,
                         event=ec.EVENT_CAPACITY,
                         message="Cluster provisioned capacity reached: 195%")]
        alerts = _run(events=events)
        self.assertIn(ac.CLUSTER_PROV_CAPACITY_CRITICAL, _kinds(alerts))
        self.assertEqual(
            alerts[0]["message"],
            f"warning: cluster {CLUSTER_ID} provisioned capacity limit reached")

    def test_a_capacity_warning_is_not_the_critical_alert(self):
        events = [_event(ec.DOMAIN_CLUSTER, EventObj.LEVEL_WARN,
                         event=ec.EVENT_CAPACITY,
                         message="Cluster absolute capacity reached: 82%")]
        self.assertEqual(_run(events=events), [])


# ==========================================================================
# api latency
# ==========================================================================

class TestApiSlow(unittest.TestCase):
    def test_a_slow_api_alerts(self):
        alerts = _run(api_mean=ac.API_SLOW_MEAN_SEC + 1)
        self.assertIn(ac.API_SLOW, _kinds(alerts))
        self.assertEqual(alerts[0]["message"], "simplyblock api critically slow")

    def test_a_healthy_api_is_quiet(self):
        self.assertEqual(_run(api_mean=0.05), [])

    def test_no_measurement_does_not_alert(self):
        """With no requests yet the mean is 0/0. Guessing is worse than
        staying quiet."""
        self.assertEqual(_run(api_mean=None), [])

    def test_the_threshold_matches_the_grafana_rule(self):
        self.assertEqual(ac.API_SLOW_MEAN_SEC, 15.0)


# ==========================================================================
# shape
# ==========================================================================

class TestAlertShape(unittest.TestCase):
    def test_ids_are_stable_across_evaluations(self):
        nodes = [_node(status=StorageNode.STATUS_OFFLINE)]
        first = _run(nodes=nodes)
        second = _run(nodes=nodes)
        self.assertEqual([a["id"] for a in first], [a["id"] for a in second])

    def test_one_id_per_object_and_kind(self):
        nodes = [_node("node-1", StorageNode.STATUS_OFFLINE),
                 _node("node-2", StorageNode.STATUS_OFFLINE)]
        ids = [a["id"] for a in _run(nodes=nodes)]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertIn(f"{ac.NODE_OFFLINE}:node-1", ids)

    def test_critical_sorts_before_warning(self):
        events = [_event(ec.DOMAIN_CLUSTER, EventObj.LEVEL_CRITICAL,
                         event=ec.EVENT_CAPACITY,
                         message="Cluster absolute capacity reached: 92%")]
        alerts = _run(nodes=[_node(status=StorageNode.STATUS_OFFLINE)], events=events)
        self.assertEqual(alerts[0]["severity"], ac.SEVERITY_CRITICAL)
        self.assertEqual(alerts[-1]["severity"], ac.SEVERITY_WARNING)

    def test_every_alert_carries_the_cluster(self):
        alerts = _run(cluster=_cluster(Cluster.STATUS_SUSPENDED),
                      nodes=[_node(status=StorageNode.STATUS_UNREACHABLE)])
        self.assertTrue(alerts)
        for alert in alerts:
            self.assertEqual(alert["cluster_id"], CLUSTER_ID)
            self.assertIn(alert["severity"],
                          (ac.SEVERITY_CRITICAL, ac.SEVERITY_WARNING))

    def test_a_healthy_cluster_produces_nothing(self):
        nodes = [_node("node-1", devices=[_device()]),
                 _node("node-2", devices=[_device("dev-2")])]
        self.assertEqual(_run(nodes=nodes, api_mean=0.01), [])


if __name__ == "__main__":
    unittest.main()


# ==========================================================================
# firing / resolution lifecycle
# ==========================================================================

def _raised(alert_id="node_offline:node-1", severity=ac.SEVERITY_CRITICAL):
    return {"id": alert_id, "kind": "node_offline", "severity": severity,
            "message": "node node-1 offline", "cluster_id": CLUSTER_ID,
            "node_id": "node-1", "device_id": "", "since": _iso(10),
            "details": {}}


class TestReconcile(unittest.TestCase):
    """An alert that only ever appears is a problem report with no end.
    Whoever was paged has no way to learn from the same channel that it is
    over, so every alert has to be chased by hand. The resolution is the
    other half of the pair, and it has to fire like the alert does."""

    def test_a_new_condition_is_raised_once(self):
        out = ac.reconcile({}, [_raised()], NOW)
        self.assertEqual(out["raised_ids"], ["node_offline:node-1"])
        self.assertEqual(out["resolved_ids"], [])
        self.assertEqual(out["active"]["node_offline:node-1"]["status"],
                         ac.STATUS_FIRING)

    def test_a_persisting_condition_is_not_re_raised(self):
        """The flooding guard: a condition that lasts a week is two events,
        not one per poll."""
        state = ac.reconcile({}, [_raised()], NOW)
        for _ in range(50):
            state = ac.reconcile(state, [_raised()], NOW)
            self.assertEqual(state["raised_ids"], [])
            self.assertEqual(state["resolved_ids"], [])

    def test_first_seen_survives_across_polls(self):
        """first_seen is when the condition started, not when this evaluation
        ran."""
        first = ac.reconcile({}, [_raised()], NOW)
        started = first["active"]["node_offline:node-1"]["first_seen"]
        later = ac.reconcile(first, [_raised()],
                             NOW + timedelta(seconds=600))
        self.assertEqual(later["active"]["node_offline:node-1"]["first_seen"],
                         started)

    def test_a_cleared_condition_resolves_once(self):
        state = ac.reconcile({}, [_raised()], NOW)
        out = ac.reconcile(state, [], NOW)
        self.assertEqual(out["resolved_ids"], ["node_offline:node-1"])
        self.assertEqual(out["active"], {})
        entry = out["resolved"]["node_offline:node-1"]
        self.assertEqual(entry["status"], ac.STATUS_RESOLVED)
        self.assertTrue(entry["resolved_at"])

    def test_a_resolution_is_not_repeated(self):
        state = ac.reconcile({}, [_raised()], NOW)
        state = ac.reconcile(state, [], NOW)
        again = ac.reconcile(state, [], NOW)
        self.assertEqual(again["resolved_ids"], [])
        # ...but it stays visible for the retention window
        self.assertIn("node_offline:node-1", again["resolved"])

    def test_a_resolution_ages_out_of_the_history(self):
        state = ac.reconcile({}, [_raised()], NOW)
        state = ac.reconcile(state, [], NOW)
        later = ac.reconcile(state, [],
                             NOW + timedelta(seconds=ac.HISTORY_RETENTION_SEC + 60))
        self.assertEqual(later["resolved"], {})

    def test_history_is_capped(self):
        """The window alone does not bound this: one document per cluster
        holds the whole set, and a flapping alert adds an entry every time it
        clears."""
        many = [_raised(f"node_down:node-{i}") for i in range(ac.HISTORY_MAX_ENTRIES + 50)]
        state = ac.reconcile({}, many, NOW)
        state = ac.reconcile(state, [], NOW)
        self.assertEqual(len(state["resolved"]), ac.HISTORY_MAX_ENTRIES)


class TestHistoryWindow(unittest.TestCase):
    def test_no_window_returns_everything_retained(self):
        resolved = {"a": {"resolved_at": _iso(10)}, "b": {"resolved_at": _iso(100000)}}
        self.assertEqual(len(ac._history(resolved, None, NOW)), 2)

    def test_a_window_drops_older_entries(self):
        resolved = {"a": {"resolved_at": _iso(10)}, "b": {"resolved_at": _iso(7200)}}
        kept = ac._history(resolved, 3600, NOW)
        self.assertEqual([e["resolved_at"] for e in kept], [_iso(10)])

    def test_an_entry_without_an_end_time_is_not_in_any_window(self):
        self.assertEqual(ac._history({"a": {"resolved_at": ""}}, 3600, NOW), [])

    def test_a_re_firing_condition_leaves_the_resolved_set(self):
        state = ac.reconcile({}, [_raised()], NOW)
        state = ac.reconcile(state, [], NOW)
        again = ac.reconcile(state, [_raised()], NOW)
        self.assertEqual(again["raised_ids"], ["node_offline:node-1"])
        self.assertNotIn("node_offline:node-1", again["resolved"])

    def test_warnings_get_the_same_pair(self):
        warn = _raised("cluster_capacity_critical:cl-1", ac.SEVERITY_WARNING)
        state = ac.reconcile({}, [warn], NOW)
        self.assertEqual(state["raised_ids"], ["cluster_capacity_critical:cl-1"])
        out = ac.reconcile(state, [], NOW)
        self.assertEqual(out["resolved_ids"], ["cluster_capacity_critical:cl-1"])

    def test_independent_alerts_transition_independently(self):
        a, b = _raised("node_offline:node-1"), _raised("node_down:node-2")
        state = ac.reconcile({}, [a, b], NOW)
        out = ac.reconcile(state, [a], NOW)
        self.assertEqual(out["resolved_ids"], ["node_down:node-2"])
        self.assertEqual(out["raised_ids"], [])
        self.assertIn("node_offline:node-1", out["active"])


class TestTransitionEvents(unittest.TestCase):
    """Both transitions reach the cluster event log, and only transitions do."""

    def _run(self, raised_ids, resolved_ids, state):
        from unittest.mock import patch
        cluster = _cluster()
        with patch.object(ec, "log_event_cluster") as log:
            ac._log_transitions(cluster, state, raised_ids, resolved_ids)
        return log

    def test_a_raise_is_logged_at_the_alert_severity(self):
        state = ac.reconcile({}, [_raised()], NOW)
        log = self._run(state["raised_ids"], [], state)
        log.assert_called_once()
        kwargs = log.call_args.kwargs
        self.assertEqual(kwargs["event"], ac.ALERT_RAISED)
        self.assertEqual(kwargs["event_level"], EventObj.LEVEL_CRITICAL)
        self.assertIn("node node-1 offline", kwargs["message"])

    def test_a_warning_raise_is_logged_as_a_warning(self):
        warn = _raised("cluster_capacity_critical:cl-1", ac.SEVERITY_WARNING)
        state = ac.reconcile({}, [warn], NOW)
        log = self._run(state["raised_ids"], [], state)
        self.assertEqual(log.call_args.kwargs["event_level"], EventObj.LEVEL_WARN)

    def test_a_resolution_is_logged_as_info(self):
        state = ac.reconcile({}, [_raised()], NOW)
        state = ac.reconcile(state, [], NOW)
        log = self._run([], state["resolved_ids"], state)
        log.assert_called_once()
        kwargs = log.call_args.kwargs
        self.assertEqual(kwargs["event"], ac.ALERT_RESOLVED)
        self.assertEqual(kwargs["event_level"], EventObj.LEVEL_INFO)
        self.assertIn("node node-1 offline", kwargs["message"])

    def test_nothing_is_logged_without_a_transition(self):
        state = ac.reconcile({}, [_raised()], NOW)
        steady = ac.reconcile(state, [_raised()], NOW)
        log = self._run(steady["raised_ids"], steady["resolved_ids"], steady)
        log.assert_not_called()

    def test_a_logging_failure_does_not_break_the_feed(self):
        from unittest.mock import patch
        state = ac.reconcile({}, [_raised()], NOW)
        with patch.object(ec, "log_event_cluster", side_effect=RuntimeError("fdb")):
            ac._log_transitions(_cluster(), state, state["raised_ids"], [])


class TestFeedOrder(unittest.TestCase):
    def test_firing_sorts_before_resolved(self):
        firing = dict(_raised("node_down:node-2"), status=ac.STATUS_FIRING)
        gone = dict(_raised("node_offline:node-1"), status=ac.STATUS_RESOLVED)
        ordered = sorted([gone, firing], key=ac._feed_sort_key)
        self.assertEqual(ordered[0]["status"], ac.STATUS_FIRING)

    def test_critical_sorts_before_warning_within_firing(self):
        warn = dict(_raised("cluster_capacity_critical:cl-1", ac.SEVERITY_WARNING),
                    status=ac.STATUS_FIRING)
        crit = dict(_raised("node_offline:node-1"), status=ac.STATUS_FIRING)
        ordered = sorted([warn, crit], key=ac._feed_sort_key)
        self.assertEqual(ordered[0]["severity"], ac.SEVERITY_CRITICAL)
