"""Regression tests for the v1 /cluster/metrics Prometheus exposition.

Two distinct faults are locked down here, both of which shipped because no
test ever exercised this endpoint with more than one entity:

  * **Series leak.** The gauges used to be cached at module scope, so a Gauge
    child created for a label set -- and these label sets carry entity
    identity (lvol/pvc_name/device/...) -- survived for the worker process's
    whole life. Volumes deleted days earlier kept being exported. Observed on
    a customer cluster: 94 live volumes but ~350k series / 83 MB / ~5 s per
    scrape, growing ~7 MB/day at ~1.2k volume deletions/day, until Prometheus
    could no longer usefully ingest it.

  * **DuplicateTimeseries.** A Gauge registers its name with the registry when
    it is constructed, so building a gauge family inside the per-entity loops
    raises ``DuplicateTimeseries`` on the *second* node/device/pool/volume. An
    earlier attempt at fixing the leak moved construction into the request but
    left it inside those loops, which would have 500'd every real cluster.

The fix is both halves together: a registry per request, and each gauge family
built exactly once per request, before the loops.
"""

import math
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_web.api.v1 import metrics


def _stats_record():
    """A stats row: every io_stats key present with a numeric value."""
    rec = MagicMock()
    rec.get_clean_dict.return_value = {k: 1 for k in metrics.io_stats_keys}
    return rec


def _cluster(uuid="cl-1"):
    cl = MagicMock()
    cl.get_id.return_value = uuid
    cl.cluster_name = "test-cluster"
    cl.get_status_code.return_value = 1
    cl.get_clean_dict.return_value = {"prov_cap_crit": 0, "cap_crit": 0}
    return cl


def _device(uuid, health_check=True):
    dev = MagicMock()
    dev.uuid = uuid
    dev.get_id.return_value = uuid
    dev.status = "online"
    dev.get_status_code.return_value = 1
    dev.health_check = health_check
    return dev


def _node(uuid, devices, health_check=True):
    node = MagicMock()
    node.get_id.return_value = uuid
    node.hostname = f"host-{uuid}"
    node.status = "online"
    node.get_status_code.return_value = 1
    node.health_check = health_check
    node.nvme_devices = devices
    rpc = MagicMock()
    rpc.framework_get_reactors.return_value = {"reactors": [
        {"lcore": 0, "idle": 100, "busy": 100, "irq": 0, "sys": 0,
         "lw_threads": [{"id": 1, "name": "app_thread"}]},
        {"lcore": 1, "idle": 100, "busy": 100, "irq": 0, "sys": 0,
         "lw_threads": [{"id": 2, "name": "poller_0"}]},
    ]}
    rpc.thread_get_stats.return_value = {"threads": [{"id": 1, "busy": 50}, {"id": 2, "busy": 25}]}
    node.rpc_client.return_value = rpc
    return node


def _pool(uuid):
    pool = MagicMock()
    pool.get_id.return_value = uuid
    pool.pool_name = f"pool-{uuid}"
    pool.get_status_code.return_value = 1
    return pool


def _lvol(uuid, health_check=True):
    lvol = MagicMock()
    lvol.get_id.return_value = uuid
    lvol.lvol_name = f"vol-{uuid}"
    lvol.pvc_name = f"pvc-{uuid}"
    lvol.pool_name = "pool-p1"
    lvol.get_status_code.return_value = 1
    lvol.health_check = health_check
    return lvol


def _fake_db(nodes, pools, lvols, clusters=None):
    db = MagicMock()
    db.get_clusters.return_value = clusters if clusters is not None else [_cluster()]
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    db.get_pools.return_value = pools
    db.get_lvols.return_value = lvols
    db.get_cluster_stats.return_value = [_stats_record()]
    db.get_node_stats.return_value = [_stats_record()]
    db.get_device_stats.return_value = [_stats_record()]
    db.get_pool_stats.return_value = [_stats_record()]
    db.get_lvol_stats.return_value = [_stats_record()]
    return db


def _scrape(db):
    with patch.object(metrics, "db", db):
        return metrics.get_data().get_data(as_text=True)


class TestMultipleEntities(unittest.TestCase):
    """The case that was never tested, and that both the original bug and the
    first attempted fix fell down on."""

    def test_many_entities_do_not_raise_duplicate_timeseries(self):
        db = _fake_db(
            nodes=[_node("n1", [_device("d1"), _device("d2")]),
                   _node("n2", [_device("d3"), _device("d4")])],
            pools=[_pool("p1"), _pool("p2")],
            lvols=[_lvol("l1"), _lvol("l2"), _lvol("l3")],
        )
        body = _scrape(db)   # must not raise DuplicateTimeseries
        for uuid in ("n1", "n2", "d1", "d4", "p1", "p2", "l1", "l3"):
            self.assertIn(uuid, body, f"{uuid} missing from exposition")

    def test_every_entity_is_reported_exactly_once_per_metric(self):
        db = _fake_db(nodes=[_node("n1", [_device("d1")])], pools=[_pool("p1")],
                      lvols=[_lvol("l1"), _lvol("l2")])
        body = _scrape(db)
        self.assertEqual(body.count('lvol_read_bytes{'), 2, "one series per volume")
        self.assertEqual(body.count('device_read_bytes{'), 1)
        self.assertEqual(body.count('pool_read_bytes{'), 1)


class TestNoSeriesLeak(unittest.TestCase):

    def test_deleted_volume_disappears_from_the_next_scrape(self):
        nodes, pools = [_node("n1", [_device("d1")])], [_pool("p1")]
        before = _scrape(_fake_db(nodes, pools, [_lvol("keep"), _lvol("doomed")]))
        self.assertIn("doomed", before)

        # "doomed" is deleted; the very next scrape must not mention it.
        after = _scrape(_fake_db(nodes, pools, [_lvol("keep")]))
        self.assertIn("keep", after)
        self.assertNotIn("doomed", after,
                         "deleted volume still exported -- the series leak is back")

    def test_payload_does_not_grow_across_repeated_scrapes(self):
        """The leak showed up in production as a monotonically growing payload
        (+~7 MB/day). Same entities in, same bytes out, every time."""
        db = _fake_db(nodes=[_node("n1", [_device("d1")])], pools=[_pool("p1")],
                      lvols=[_lvol("l1"), _lvol("l2")])
        sizes = {len(_scrape(db)) for _ in range(4)}
        self.assertEqual(len(sizes), 1, f"payload size drifted across scrapes: {sizes}")

    def test_churn_does_not_accumulate(self):
        """147 volumes came and went in 3 h on the affected cluster. Replaying
        that shape must leave the exposition the size of the live set."""
        nodes, pools = [_node("n1", [_device("d1")])], [_pool("p1")]
        baseline = len(_scrape(_fake_db(nodes, pools, [_lvol("live-1"), _lvol("live-2")])))
        for i in range(20):   # 20 generations of ephemeral volumes
            _scrape(_fake_db(nodes, pools, [_lvol("live-1"), _lvol("live-2"), _lvol(f"tmp-{i}")]))
        after = len(_scrape(_fake_db(nodes, pools, [_lvol("live-1"), _lvol("live-2")])))
        self.assertEqual(after, baseline,
                         "exposition grew after volume churn -- entities are being retained")

    def test_no_module_level_registry(self):
        """Drift guard: a registry (or gauge dict) at module scope is exactly
        what made deleted entities immortal. It must not come back."""
        from prometheus_client import CollectorRegistry, Gauge
        for name, value in vars(metrics).items():
            self.assertNotIsInstance(
                value, CollectorRegistry,
                f"module-level CollectorRegistry '{name}' reintroduces the leak")
            self.assertNotIsInstance(
                value, Gauge,
                f"module-level Gauge '{name}' reintroduces the leak")


class TestAbsentValuesStayAbsent(unittest.TestCase):
    """prometheus_client creates the child series on .labels(), and an un-set
    child exports as 0. So .labels() must only be called once a value is known
    -- otherwise a metric with no source value silently reads as a real zero."""

    def test_metrics_missing_from_the_stats_row_are_not_exported_as_zero(self):
        db = _fake_db(nodes=[_node("n1", [_device("d1")])], pools=[_pool("p1")],
                      lvols=[_lvol("l1")])
        # A stats row that only carries read_bytes: nothing else has a value.
        sparse = MagicMock()
        sparse.get_clean_dict.return_value = {"read_bytes": 7}
        db.get_lvol_stats.return_value = [sparse]

        body = _scrape(db)
        self.assertIn('lvol_read_bytes{', body)
        self.assertNotIn('lvol_write_bytes{', body,
                         "absent stat exported as 0 -- would read as real data in Grafana")

    def test_cpu_gauges_are_not_zero_filled_from_the_node_stats_loop(self):
        """cpu_busy_percentage/cpu_core_utilization are set from reactor data,
        not from the node stats row; the stats loop must skip them rather than
        create an empty child for the node label set."""
        db = _fake_db(nodes=[_node("n1", [_device("d1")])], pools=[], lvols=[])
        body = _scrape(db)
        # Only the per-thread / per-core label sets, never a bare node-level one.
        self.assertNotIn('snode_cpu_busy_percentage{cluster="cl-1",cluster_name="test-cluster",'
                         'hostname="host-n1",snode="n1"}', body)
        self.assertEqual(body.count('snode_cpu_busy_percentage{'), 2)


class TestCpuMetrics(unittest.TestCase):

    def test_one_core_utilization_series_per_reactor(self):
        """The reactor walk used to sit inside the per-gauge-name loop, so it
        re-walked every reactor ~33x per node. One series per core, per node."""
        db = _fake_db(nodes=[_node("n1", [_device("d1")])], pools=[], lvols=[])
        body = _scrape(db)
        self.assertEqual(body.count('snode_cpu_core_utilization{'), 2, "two reactors -> two series")
        self.assertEqual(body.count('snode_cpu_busy_percentage{'), 2, "two threads -> two series")

    def test_busy_percentage_is_computed_from_thread_and_core_cycles(self):
        db = _fake_db(nodes=[_node("n1", [_device("d1")])], pools=[], lvols=[])
        body = _scrape(db)
        # thread 1 busy=50, core busy+idle = 200 -> 25%
        self.assertIn('thread_name="app_thread"} 25.0', body)


class TestHealthCheckNotApplicable(unittest.TestCase):
    """health_check is Optional[bool]; None means "not applicable". Gauge.set(None)
    raises TypeError, which would have failed the entire scrape."""

    def test_none_health_check_reports_nan_instead_of_failing(self):
        db = _fake_db(
            nodes=[_node("n1", [_device("d1", health_check=None)], health_check=None)],
            pools=[_pool("p1")],
            lvols=[_lvol("l1", health_check=None)],
        )
        body = _scrape(db)   # must not raise TypeError
        for metric in ("snode_health_check{", "device_health_check{", "lvol_health_check{"):
            line = next(ln for ln in body.splitlines() if ln.startswith(metric))
            self.assertTrue(math.isnan(float(line.rsplit(" ", 1)[1])),
                            f"{metric} should be NaN when not applicable, got: {line}")
