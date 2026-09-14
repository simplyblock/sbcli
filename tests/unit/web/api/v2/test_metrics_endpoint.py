"""Unit tests for the v2 Prometheus exporter.

The exporter's contract is not only "emits the right numbers" but also "emits
nothing for objects that are gone" — the property the v1 exporter could not
hold, because retained Gauge children outlive the objects they describe. The
statelessness tests below are the regression guard for that.
"""

from simplyblock_core.models.stats import (
    ClusterStatObject,
    DeviceStatObject,
    LVolStatObject,
    NodeStatObject,
    PoolStatObject,
)
from simplyblock_core.models.storage_node import StorageNode


METRICS_URL = '/api/v2/metrics'


def _samples(body, name):
    """Every `metric_name{labels} value` line for one metric name."""
    prefix = name + '{'
    return [
        line for line in body.splitlines()
        if line.startswith((prefix, name + ' '))
    ]


def _value(body, name, **labels):
    """The single value of `name` whose label set contains all of `labels`."""
    matches = [
        line for line in _samples(body, name)
        if all(f'{k}="{v}"' in line for k, v in labels.items())
    ]
    assert len(matches) == 1, f'expected one {name} sample for {labels}, got {matches}'
    return float(matches[0].rsplit(' ', 1)[1])


def _no_stats(db):
    for accessor in (
        'get_cluster_stats', 'get_node_stats', 'get_device_stats',
        'get_pool_stats', 'get_lvol_stats',
    ):
        getattr(db, accessor).return_value = []


class TestExportedValues:

    def test_cluster_io_counters_and_capacity(self, client, db, cluster):
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        db.get_cluster_stats.return_value = [ClusterStatObject(data={
            'uuid': cluster.get_id(),
            'cluster_id': cluster.get_id(),
            'read_bytes': 4096,
            'read_io': 8,
            'read_latency_ticks': 800,
            'size_total': 1000,
            'size_used': 250,
            'size_free': 750,
        })]

        body = client.get(METRICS_URL).text

        assert _value(body, 'simplyblock_cluster_read_bytes_total', cluster=cluster.get_id()) == 4096
        assert _value(body, 'simplyblock_cluster_read_operations_total', cluster=cluster.get_id()) == 8
        assert _value(body, 'simplyblock_cluster_read_latency_ticks_total', cluster=cluster.get_id()) == 800
        assert _value(body, 'simplyblock_cluster_size_used_bytes', cluster=cluster.get_id()) == 250

    def test_io_counters_are_typed_as_counters(self, client, db, cluster):
        """`rate()` needs the counter type to handle an SPDK restart."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvols.return_value = []

        body = client.get(METRICS_URL).text

        assert '# TYPE simplyblock_cluster_read_bytes_total counter' in body
        assert '# TYPE simplyblock_cluster_size_used_bytes gauge' in body

    def test_derived_rate_fields_are_not_exported(self, client, db, cluster):
        """The collectors' `_ps` fields are superseded by rate() over counters."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        db.get_cluster_stats.return_value = [ClusterStatObject(data={
            'uuid': cluster.get_id(), 'read_bytes_ps': 999, 'read_latency_ps': 42,
        })]

        body = client.get(METRICS_URL).text

        assert 'read_bytes_ps' not in body
        assert 'read_latency_ps' not in body
        # `date` and the never-populated record_* fields are metadata, not metrics
        assert 'simplyblock_cluster_date' not in body
        assert 'record_duration' not in body

    def test_capacity_percentages_are_not_exported(self, client, db, cluster):
        """size_util is int-truncated in FDB; PromQL divides the byte gauges exactly."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        db.get_cluster_stats.return_value = [ClusterStatObject(data={
            'uuid': cluster.get_id(), 'size_util': 25, 'size_prov_util': 50,
        })]

        body = client.get(METRICS_URL).text

        assert 'size_util' not in body
        assert 'size_prov_util' not in body

    def test_thresholds_exported_as_ratios(self, client, db, cluster):
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        cluster.cap_crit = 90
        cluster.prov_cap_crit = 190

        body = client.get(METRICS_URL).text

        assert _value(
            body, 'simplyblock_cluster_capacity_critical_threshold_ratio',
            cluster=cluster.get_id(),
        ) == 0.9
        assert _value(
            body, 'simplyblock_cluster_provisioned_capacity_critical_threshold_ratio',
            cluster=cluster.get_id(),
        ) == 1.9

    def test_provisioned_size_omitted_where_collector_never_sets_it(
        self, client, db, cluster, storage_node, device,
    ):
        """A zero default must not be published as a real measurement."""
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        db.get_device_stats.return_value = [DeviceStatObject(data={
            'uuid': device.get_id(), 'size_total': 100, 'size_used': 10,
        })]

        body = client.get(METRICS_URL).text

        assert _samples(body, 'simplyblock_device_size_total_bytes')
        assert not _samples(body, 'simplyblock_device_size_provisioned_bytes')
        assert 'simplyblock_snode_size_provisioned_bytes' in body


class TestStatusAndHealth:

    def test_offline_node_still_reports_status(self, client, db, cluster, storage_node):
        """v1 skipped non-ONLINE nodes entirely, so a node going down emitted
        nothing and its series merely went stale — indistinguishable from a
        failed scrape."""
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.status = StorageNode.STATUS_OFFLINE
        storage_node.nvme_devices = []

        body = client.get(METRICS_URL).text

        assert _value(
            body, 'simplyblock_snode_status',
            snode=storage_node.get_id(), status=StorageNode.STATUS_OFFLINE,
        ) == 1

    def test_node_with_no_devices_still_reports_status(self, client, db, cluster, storage_node):
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []

        body = client.get(METRICS_URL).text

        assert _samples(body, 'simplyblock_snode_status')

    def test_health_check_omitted_when_undetermined(self, client, db, cluster, storage_node):
        """v1 reported NaN here; absence says the same without a float sentinel."""
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        storage_node.health_check = None

        body = client.get(METRICS_URL).text

        assert not _samples(body, 'simplyblock_snode_health_check')

    def test_health_check_is_binary(self, client, db, cluster, storage_node):
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        storage_node.health_check = False

        body = client.get(METRICS_URL).text

        assert _value(
            body, 'simplyblock_snode_health_check', snode=storage_node.get_id(),
        ) == 0


class TestLabelling:

    def test_pools_scoped_to_their_cluster(self, client, db, cluster, pool):
        """v1 called an unfiltered get_pools() inside its per-cluster loop, so
        every pool was emitted once per cluster under a different label."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_lvols.return_value = []
        db.get_pool_stats.return_value = [PoolStatObject(data={
            'uuid': pool.get_id(), 'size_used': 7,
        })]

        client.get(METRICS_URL)

        db.get_pools.assert_called_with(cluster_id=cluster.get_id())

    def test_volume_pool_label_is_the_pool_uuid(self, client, db, cluster, pool, volume):
        """v1 set `pool` to the pool name on volume metrics but to the pool id
        on pool metrics, so the two levels could not be joined on it."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_lvol_stats.return_value = [LVolStatObject(data={
            'uuid': volume.get_id(), 'pool_id': pool.get_id(), 'size_used': 3,
        })]
        db.get_pool_stats.return_value = [PoolStatObject(data={
            'uuid': pool.get_id(), 'size_used': 3,
        })]

        body = client.get(METRICS_URL).text

        assert _value(
            body, 'simplyblock_lvol_size_used_bytes',
            lvol=volume.get_id(), pool=pool.get_id(),
        ) == 3
        assert _value(
            body, 'simplyblock_pool_size_used_bytes', pool=pool.get_id(),
        ) == 3

    def test_volume_carries_pool_name_for_human_facing_filters(
        self, client, db, cluster, pool, volume,
    ):
        """The lvols dashboard's pool dropdown reads this label; v1 got names
        from `pool` itself, which now holds the uuid."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvol_stats.return_value = [LVolStatObject(data={
            'uuid': volume.get_id(), 'pool_id': pool.get_id(), 'size_used': 3,
        })]

        body = client.get(METRICS_URL).text

        assert _value(
            body, 'simplyblock_lvol_size_used_bytes',
            lvol=volume.get_id(), pool_name=volume.pool_name,
        ) == 3


class TestCpuMetrics:

    def test_every_reactor_and_thread_counter_reports(
        self, client, db, cluster, storage_node,
    ):
        """No RPC on the scrape path: CPU is read from FDB like everything else.

        Asserts all six counters with distinct values. mypy cannot check the key
        of a TypedDict `.get()`, so a typo in one of the accessors in
        `_REACTOR_COUNTERS` / `_THREAD_COUNTERS` would silently read as absent —
        this test is what catches that.
        """
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        db.get_node_stats.return_value = [NodeStatObject(data={
            'uuid': storage_node.get_id(),
            'cpu_dict': {'reactors': [{
                'lcore': 3, 'busy': 700, 'idle': 300, 'irq': 5, 'sys': 10,
                'threads': [{'id': 1, 'name': 'app_thread', 'busy': 400, 'idle': 600}],
            }]},
        })]

        body = client.get(METRICS_URL).text
        node, core = storage_node.get_id(), '3'

        for metric, expected in [
            ('reactor_busy_ticks_total', 700),
            ('reactor_idle_ticks_total', 300),
            ('reactor_irq_ticks_total', 5),
            ('reactor_sys_ticks_total', 10),
        ]:
            assert _value(
                body, f'simplyblock_snode_{metric}', snode=node, core_id=core,
            ) == expected, metric

        for metric, expected in [
            ('thread_busy_ticks_total', 400),
            ('thread_idle_ticks_total', 600),
        ]:
            assert _value(
                body, f'simplyblock_snode_{metric}',
                snode=node, core_id=core, thread_name='app_thread',
            ) == expected, metric

    def test_cpu_exported_as_raw_ticks_not_a_percentage(
        self, client, db, cluster, storage_node,
    ):
        """SPDK counts ticks cumulatively since reactor start, so a ratio taken
        at collection time is a lifetime average, not current load."""
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        db.get_node_stats.return_value = [NodeStatObject(data={
            'uuid': storage_node.get_id(),
            'cpu_dict': {'reactors': [{'lcore': 0, 'busy': 700, 'idle': 300, 'threads': []}]},
        })]

        body = client.get(METRICS_URL).text

        assert 'cpu_busy_percentage' not in body
        assert 'cpu_core_utilization' not in body
        assert '# TYPE simplyblock_snode_reactor_busy_ticks_total counter' in body

    def test_absent_counter_is_omitted_rather_than_reported_as_zero(
        self, client, db, cluster, storage_node,
    ):
        """CpuStats marks every key optional because records come back from FDB
        and may predate a field. A missing counter must produce no sample: 0
        would be indistinguishable from a genuine zero, and on a counter it
        reads as a reset."""
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        db.get_node_stats.return_value = [NodeStatObject(data={
            'uuid': storage_node.get_id(),
            'cpu_dict': {'reactors': [{      # no irq/sys, as an older record
                'lcore': 0, 'busy': 700, 'idle': 300,
                'threads': [{'id': 1, 'name': 'app_thread', 'busy': 400}],
            }]},
        })]

        body = client.get(METRICS_URL).text

        assert _value(
            body, 'simplyblock_snode_reactor_busy_ticks_total', core_id='0') == 700
        assert not _samples(body, 'simplyblock_snode_reactor_irq_ticks_total')
        assert not _samples(body, 'simplyblock_snode_reactor_sys_ticks_total')
        # the present sibling still reports, and idle is untouched
        assert _value(
            body, 'simplyblock_snode_thread_busy_ticks_total',
            thread_name='app_thread') == 400
        assert not _samples(body, 'simplyblock_snode_thread_idle_ticks_total')

    def test_reactor_without_lcore_is_skipped(self, client, db, cluster, storage_node):
        """There is no correct core_id to label the series with."""
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        db.get_node_stats.return_value = [NodeStatObject(data={
            'uuid': storage_node.get_id(),
            'cpu_dict': {'reactors': [{'busy': 700, 'idle': 300}]},
        })]

        body = client.get(METRICS_URL).text

        assert not _samples(body, 'simplyblock_snode_reactor_busy_ticks_total')

    def test_thread_without_name_is_skipped(self, client, db, cluster, storage_node):
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        db.get_node_stats.return_value = [NodeStatObject(data={
            'uuid': storage_node.get_id(),
            'cpu_dict': {'reactors': [{
                'lcore': 0, 'busy': 700, 'threads': [{'id': 1, 'busy': 400}]}]},
        })]

        body = client.get(METRICS_URL).text

        assert _samples(body, 'simplyblock_snode_reactor_busy_ticks_total')
        assert not _samples(body, 'simplyblock_snode_thread_busy_ticks_total')

    def test_node_without_cpu_data_emits_no_cpu_series(
        self, client, db, cluster, storage_node,
    ):
        _no_stats(db)
        db.get_pools.return_value = []
        db.get_lvols.return_value = []
        storage_node.nvme_devices = []
        db.get_node_stats.return_value = [NodeStatObject(data={'uuid': storage_node.get_id()})]

        body = client.get(METRICS_URL).text

        assert not _samples(body, 'simplyblock_snode_reactor_busy_ticks_total')


class TestStatelessness:

    def test_deleted_object_disappears_from_the_next_scrape(
        self, client, db, cluster, pool, volume,
    ):
        """The core regression guard. With retained Gauge children a deleted
        volume keeps reporting its last value until the process restarts."""
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_lvol_stats.return_value = [LVolStatObject(data={
            'uuid': volume.get_id(), 'pool_id': pool.get_id(), 'size_used': 5,
        })]

        first = client.get(METRICS_URL).text
        assert _samples(first, 'simplyblock_lvol_size_used_bytes')

        db.get_lvols.return_value = []
        db.get_lvol_stats.return_value = []
        second = client.get(METRICS_URL).text

        assert not _samples(second, 'simplyblock_lvol_size_used_bytes')
        assert volume.get_id() not in second

    def test_repeated_scrapes_do_not_accumulate_series(self, client, db, cluster, pool, volume):
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_lvol_stats.return_value = [LVolStatObject(data={
            'uuid': volume.get_id(), 'pool_id': pool.get_id(), 'size_used': 5,
        })]

        first = client.get(METRICS_URL).text
        second = client.get(METRICS_URL).text

        assert _samples(first, 'simplyblock_lvol_size_used_bytes') == \
            _samples(second, 'simplyblock_lvol_size_used_bytes')

    def test_scrape_is_served_as_prometheus_text(self, client, db, cluster):
        _no_stats(db)
        db.get_storage_nodes_by_cluster_id.return_value = []
        db.get_pools.return_value = []
        db.get_lvols.return_value = []

        response = client.get(METRICS_URL)

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/plain')
