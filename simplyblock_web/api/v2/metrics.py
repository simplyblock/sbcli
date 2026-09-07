"""Prometheus exporter.

Implemented as a `prometheus_client` custom collector rather than a set of
long-lived `Gauge` objects. The values here are a snapshot of FoundationDB, not
state of this process, so the "set it and leave it" lifecycle of direct
instrumentation is wrong for them: a `Gauge.labels(...)` child is never
released, so a deleted volume keeps reporting its last value until the process
restarts. Since HAProxy round-robins scrapes across every API instance, each
instance accumulates a *different* set of those stale children, and Prometheus
sees one series written alternately by disagreeing sources.

A collector rebuilds every series inside `collect()`, so absence is meaningful:
an object that no longer exists stops being exported, and every instance
returns an identical response because the only input is shared FDB state.
"""

from collections.abc import Callable, Iterable, Iterator, Sequence

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.stats import CpuStats, ReactorStats, ThreadStats


api = APIRouter()
db = DBController()

NAMESPACE = 'simplyblock'

_CLUSTER_LABELS = ['cluster', 'cluster_name']
_NODE_LABELS = _CLUSTER_LABELS + ['snode', 'hostname']
_DEVICE_LABELS = _CLUSTER_LABELS + ['snode', 'device']
_POOL_LABELS = _CLUSTER_LABELS + ['pool', 'pool_name']
# `pool` is the uuid, so volume series join to pool series on it; `pool_name`
# rides along for human-facing filters and legends. It adds no series, being
# functionally dependent on the uuid.
_LVOL_LABELS = _CLUSTER_LABELS + ['pool', 'pool_name', 'lvol', 'lvol_name', 'pvc_name']

# Cumulative SPDK counters. Exported as counters so that PromQL `rate()` handles
# an SPDK restart natively; the collectors' `_ps` fields, which hand-derived
# rates from consecutive samples, are deliberately not exported.
#
# Latency is exported only as cumulative ticks alongside the operation count,
# never pre-divided. Mean latency is `rate(latency_ticks) / rate(operations)`,
# which stays correct when a node or cluster series is the sum over its
# devices, because summing numerator and denominator separately is the right
# aggregation. Dividing per-device first and summing the quotients — what the
# v1 exporter published — is not a latency at any level above the device.
_IO_COUNTERS: Sequence[tuple[str, str, str]] = (
    ('read_bytes', 'read_bytes', 'Cumulative bytes read'),
    ('read_io', 'read_operations', 'Cumulative read operations'),
    ('read_latency_ticks', 'read_latency_ticks', 'Cumulative read latency in SPDK ticks'),
    ('write_bytes', 'write_bytes', 'Cumulative bytes written'),
    ('write_io', 'write_operations', 'Cumulative write operations'),
    ('write_latency_ticks', 'write_latency_ticks', 'Cumulative write latency in SPDK ticks'),
    ('unmap_bytes', 'unmap_bytes', 'Cumulative bytes unmapped'),
    ('unmap_io', 'unmap_operations', 'Cumulative unmap operations'),
    ('unmap_latency_ticks', 'unmap_latency_ticks', 'Cumulative unmap latency in SPDK ticks'),
)

# Capacity is exported in bytes only. The collectors also persist integer
# percentages (`size_util`, `size_prov_util`), but those are truncated with
# `int(used / total * 100)`, so exporting them would publish a lossy copy of
# something PromQL can divide exactly.
#
# Every gauge below is EFFECTIVE (client-visible) capacity, so
# size_provisioned_bytes / size_total_bytes is a meaningful ratio -- it was not
# while the totals were raw, physical bytes. The raw figures the devices
# reported are exported separately as the `*_raw_bytes` gauges; the difference
# between a raw and an effective total is the erasure-coding parity overhead.
_SIZE_GAUGES: Sequence[tuple[str, str, str]] = (
    ('size_total', 'size_total_bytes', 'Total effective capacity in bytes'),
    ('size_used', 'size_used_bytes', 'Used effective capacity in bytes'),
    ('size_free', 'size_free_bytes', 'Free effective capacity in bytes'),
    ('size_prov', 'size_provisioned_bytes', 'Provisioned capacity in bytes'),
    ('size_total_raw', 'size_total_raw_bytes', 'Total raw (physical) capacity in bytes'),
    ('size_used_raw', 'size_used_raw_bytes', 'Used raw (physical) capacity in bytes'),
    ('size_free_raw', 'size_free_raw_bytes', 'Free raw (physical) capacity in bytes'),
)

# Only levels whose collector actually populates a key may export it, otherwise
# the field's zero default would be published as a real measurement. Device,
# volume and pool records never carry a provisioned size; volume and pool
# records carry no raw capacity either, being measured at the lvstore layer,
# which is effective by construction.
_SIZE_KEYS_BY_LEVEL = {
    'cluster': ('size_total', 'size_used', 'size_free', 'size_prov',
                'size_total_raw', 'size_used_raw', 'size_free_raw'),
    'snode': ('size_total', 'size_used', 'size_free', 'size_prov',
              'size_total_raw', 'size_used_raw', 'size_free_raw'),
    'device': ('size_total', 'size_used', 'size_free',
               'size_total_raw', 'size_used_raw', 'size_free_raw'),
    'lvol': ('size_total', 'size_used', 'size_free'),
    'pool': ('size_total', 'size_used', 'size_free'),
}

_CORE_LABELS = _NODE_LABELS + ['core_id']
_THREAD_LABELS = _CORE_LABELS + ['thread_name']

# Each entry pairs the metric with a literal-key accessor. A loop over variable
# keys would defeat the TypedDict: mypy widens `reactor.get(key)` to `object`
# when the key is not a literal, so the field names would go unchecked.
#
# The accessors return None for a key an older collector never wrote. That is
# not zero — for a counter a fabricated 0 reads as a reset — so the caller skips
# the sample instead, and mypy enforces that by refusing to pass Optional[int]
# to add_metric.
#
# mypy does not check the key of a TypedDict `.get()` (only of a subscript), so
# a typo here would read as permanently absent rather than failing to compile.
# test_every_reactor_and_thread_counter_reports pins all six names instead.
_REACTOR_COUNTERS: Sequence[tuple[str, str, Callable[[ReactorStats], int | None]]] = (
    ('reactor_busy_ticks', 'Cumulative reactor busy ticks', lambda r: r.get('busy')),
    ('reactor_idle_ticks', 'Cumulative reactor idle ticks', lambda r: r.get('idle')),
    ('reactor_irq_ticks', 'Cumulative reactor IRQ ticks', lambda r: r.get('irq')),
    ('reactor_sys_ticks', 'Cumulative reactor system ticks', lambda r: r.get('sys')),
)

_THREAD_COUNTERS: Sequence[tuple[str, str, Callable[[ThreadStats], int | None]]] = (
    ('thread_busy_ticks', 'Cumulative lightweight-thread busy ticks', lambda t: t.get('busy')),
    ('thread_idle_ticks', 'Cumulative lightweight-thread idle ticks', lambda t: t.get('idle')),
)


def _stat_families(
    level: str,
    labelnames: Sequence[str],
    entries: Iterable[tuple[list[str], dict]],
) -> Iterator[Metric]:
    """Yield one IO/capacity family per stat key, populated from `entries`.

    `entries` pairs a label-value list with the newest stat record for that
    object, as a plain dict.
    """
    counters = {
        key: CounterMetricFamily(f'{NAMESPACE}_{level}_{name}', help_text, labels=list(labelnames))
        for key, name, help_text in _IO_COUNTERS
    }
    size_keys = _SIZE_KEYS_BY_LEVEL[level]
    gauges = {
        key: GaugeMetricFamily(f'{NAMESPACE}_{level}_{name}', help_text, labels=list(labelnames))
        for key, name, help_text in _SIZE_GAUGES
        if key in size_keys
    }

    for labelvalues, record in entries:
        for key, counter in counters.items():
            if key in record:
                counter.add_metric(labelvalues, record[key])
        for key, gauge in gauges.items():
            if key in record:
                gauge.add_metric(labelvalues, record[key])

    yield from counters.values()
    yield from gauges.values()


def _status_family(level: str, labelnames: Sequence[str], entries: Iterable[tuple[list[str], object]]) -> Metric:
    """Status as a labelled indicator rather than the v1 numeric code.

    `<level>_status{status="online"} 1` lets an alert name the state it cares
    about instead of hardcoding a magic number from `_STATUS_CODE_MAP`, and
    keeps working when a new status is added to the map.
    """
    family = GaugeMetricFamily(
        f'{NAMESPACE}_{level}_status',
        f'Current {level} status; 1 on the series carrying the active status label',
        labels=list(labelnames) + ['status'],
    )
    for labelvalues, obj in entries:
        family.add_metric(labelvalues + [getattr(obj, 'status', '')], 1)
    return family


def _health_family(level: str, labelnames: Sequence[str], entries: Iterable[tuple[list[str], object]]) -> Metric:
    """Health as a plain 0/1 gauge, omitting objects with no verdict.

    The v1 exporter reported NaN when `health_check` was None so it would not
    read as unhealthy. Omitting the series says the same thing without asking
    every consumer to special-case a float.
    """
    family = GaugeMetricFamily(
        f'{NAMESPACE}_{level}_health_check',
        '1 when the most recent health check passed, 0 when it failed',
        labels=list(labelnames),
    )
    for labelvalues, obj in entries:
        health = getattr(obj, 'health_check', None)
        if health is not None:
            family.add_metric(labelvalues, 1 if health else 0)
    return family


def _threshold_families(entries: Iterable[tuple[list[str], object]]) -> Iterator[Metric]:
    """Configured capacity thresholds, as ratios to match the byte gauges.

    Persisted as integer percentages on the cluster; `prov_cap_*` exceeds 100
    by design because provisioning is allowed to overcommit.
    """
    families = {
        'cap_warn': GaugeMetricFamily(
            f'{NAMESPACE}_cluster_capacity_warning_threshold_ratio',
            'Configured used-capacity ratio at which a warning is raised',
            labels=_CLUSTER_LABELS),
        'cap_crit': GaugeMetricFamily(
            f'{NAMESPACE}_cluster_capacity_critical_threshold_ratio',
            'Configured used-capacity ratio at which a critical alert is raised',
            labels=_CLUSTER_LABELS),
        'prov_cap_warn': GaugeMetricFamily(
            f'{NAMESPACE}_cluster_provisioned_capacity_warning_threshold_ratio',
            'Configured provisioned-capacity ratio at which a warning is raised',
            labels=_CLUSTER_LABELS),
        'prov_cap_crit': GaugeMetricFamily(
            f'{NAMESPACE}_cluster_provisioned_capacity_critical_threshold_ratio',
            'Configured provisioned-capacity ratio at which a critical alert is raised',
            labels=_CLUSTER_LABELS),
    }
    for labelvalues, cluster in entries:
        for key, family in families.items():
            value = getattr(cluster, key, 0)
            if value:
                family.add_metric(labelvalues, value / 100)
    yield from families.values()


def _cpu_families(entries: Iterable[tuple[list[str], CpuStats]]) -> Iterator[Metric]:
    """Reactor and thread tick counters, from the node stat record's cpu_dict.

    Read from FDB like every other series — no RPC on the scrape path. Raw
    ticks, not percentages: SPDK counts them cumulatively since reactor start,
    so utilization is `rate(busy) / rate(busy + idle)` at query time.
    """
    reactor_families = [
        (CounterMetricFamily(f'{NAMESPACE}_snode_{name}', help_text, labels=_CORE_LABELS), read)
        for name, help_text, read in _REACTOR_COUNTERS
    ]
    thread_families = [
        (CounterMetricFamily(f'{NAMESPACE}_snode_{name}', help_text, labels=_THREAD_LABELS), read)
        for name, help_text, read in _THREAD_COUNTERS
    ]

    for labelvalues, cpu_dict in entries:
        # An absent collection legitimately means "none of these"; an absent
        # measurement does not mean zero. Only the former is defaulted.
        for reactor in cpu_dict.get('reactors') or []:
            lcore = reactor.get('lcore')
            if lcore is None:
                continue  # cannot identify which core these counters belong to
            core_labels = labelvalues + [str(lcore)]
            for family, read_reactor in reactor_families:
                reactor_ticks = read_reactor(reactor)
                if reactor_ticks is not None:
                    family.add_metric(core_labels, reactor_ticks)

            for thread in reactor.get('threads') or []:
                thread_name = thread.get('name')
                if thread_name is None:
                    continue  # cannot label the series without a thread name
                thread_labels = core_labels + [thread_name]
                for family, read_thread in thread_families:
                    thread_ticks = read_thread(thread)
                    if thread_ticks is not None:
                        family.add_metric(thread_labels, thread_ticks)

    for family, _ in reactor_families:
        yield family
    for family, _ in thread_families:
        yield family


class SimplyblockCollector(Collector):
    """Builds the full metric set from FoundationDB on every scrape."""

    def collect(self) -> Iterator[Metric]:
        cluster_stats: list[tuple[list[str], dict]] = []
        cluster_objects: list[tuple[list[str], object]] = []
        node_stats: list[tuple[list[str], dict]] = []
        node_objects: list[tuple[list[str], object]] = []
        cpu_entries: list[tuple[list[str], CpuStats]] = []
        device_stats: list[tuple[list[str], dict]] = []
        device_objects: list[tuple[list[str], object]] = []
        pool_stats: list[tuple[list[str], dict]] = []
        pool_objects: list[tuple[list[str], object]] = []
        lvol_stats: list[tuple[list[str], dict]] = []
        lvol_objects: list[tuple[list[str], object]] = []

        for cluster in db.get_clusters():
            cluster_labels = [cluster.get_id(), cluster.cluster_name]
            cluster_objects.append((cluster_labels, cluster))
            cluster_records = db.get_cluster_stats(cluster, 1)
            if cluster_records:
                cluster_stats.append((cluster_labels, cluster_records[0].get_clean_dict()))

            for node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
                # Status and health are emitted for every node regardless of
                # state. The v1 exporter skipped non-ONLINE nodes before setting
                # anything, so a node going down produced no sample at all —
                # its series simply went stale, which no alert can distinguish
                # from a scrape failure.
                node_labels = cluster_labels + [node.get_id(), node.hostname]
                node_objects.append((node_labels, node))
                node_records = db.get_node_stats(node, 1)
                if node_records:
                    record = node_records[0]
                    node_stats.append((node_labels, record.get_clean_dict()))
                    if record.cpu_dict:
                        cpu_entries.append((node_labels, record.cpu_dict))

                for device in node.nvme_devices:
                    device_labels = cluster_labels + [node.get_id(), device.get_id()]
                    device_objects.append((device_labels, device))
                    device_records = db.get_device_stats(device, 1)
                    if device_records:
                        device_stats.append((device_labels, device_records[0].get_clean_dict()))

            # Scoped to the cluster being iterated. The v1 exporter called an
            # unfiltered get_pools() inside its per-cluster loop, so every pool
            # was emitted once per cluster, each time labelled with a different
            # cluster.
            for pool in db.get_pools(cluster_id=cluster.get_id()):
                pool_labels = cluster_labels + [pool.get_id(), pool.pool_name]
                pool_objects.append((pool_labels, pool))
                pool_records = db.get_pool_stats(pool, 1)
                if pool_records:
                    pool_stats.append((pool_labels, pool_records[0].get_clean_dict()))

            for lvol in db.get_lvols(cluster.get_id()):
                # `pool` carries the pool uuid, matching the pool-level label.
                # v1 set it to the pool *name* here but to the pool id on pool
                # metrics, so the two levels could not be joined on it.
                lvol_labels = cluster_labels + [
                    lvol.pool_uuid, lvol.pool_name,
                    lvol.get_id(), lvol.lvol_name, lvol.pvc_name,
                ]
                lvol_objects.append((lvol_labels, lvol))
                lvol_records = db.get_lvol_stats(lvol, limit=1)
                if lvol_records:
                    lvol_stats.append((lvol_labels, lvol_records[0].get_clean_dict()))

        yield from _stat_families('cluster', _CLUSTER_LABELS, cluster_stats)
        yield _status_family('cluster', _CLUSTER_LABELS, cluster_objects)
        yield from _threshold_families(cluster_objects)

        yield from _stat_families('snode', _NODE_LABELS, node_stats)
        yield _status_family('snode', _NODE_LABELS, node_objects)
        yield _health_family('snode', _NODE_LABELS, node_objects)
        yield from _cpu_families(cpu_entries)

        yield from _stat_families('device', _DEVICE_LABELS, device_stats)
        yield _status_family('device', _DEVICE_LABELS, device_objects)
        yield _health_family('device', _DEVICE_LABELS, device_objects)

        yield from _stat_families('pool', _POOL_LABELS, pool_stats)
        yield _status_family('pool', _POOL_LABELS, pool_objects)

        yield from _stat_families('lvol', _LVOL_LABELS, lvol_stats)
        yield _status_family('lvol', _LVOL_LABELS, lvol_objects)
        yield _health_family('lvol', _LVOL_LABELS, lvol_objects)


@api.get('', response_class=Response, include_in_schema=False)
def metrics() -> Response:
    """Prometheus scrape endpoint.

    The registry is per-request, so nothing survives between scrapes.
    """
    registry = CollectorRegistry()
    registry.register(SimplyblockCollector())
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
