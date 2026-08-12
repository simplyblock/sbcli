import logging
import math
import re
from datetime import datetime, timedelta

from simplyblock_core import constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

from prometheus_api_client import PrometheusConnect

logger = logging.getLogger()

NAMESPACE = 'simplyblock'

#: Range-vector window for the derived per-second series. Must comfortably
#: exceed the scrape interval (60s, see scripts/prometheus.yml.j2) or `rate()`
#: has too few samples to interpolate from.
RATE_WINDOW = '5m'

#: Resolution requested from Prometheus. Matching the scrape interval avoids
#: asking for detail that was never recorded; callers downsample further via
#: `utils.process_records`.
STEP_SEC = 60

#: Legacy stat-record key -> the v2 counter it is derived from. The v1 exporter
#: published these cumulative counters directly under shorter names.
_COUNTERS = {
    'read_bytes': 'read_bytes_total',
    'read_io': 'read_operations_total',
    'read_latency_ticks': 'read_latency_ticks_total',
    'write_bytes': 'write_bytes_total',
    'write_io': 'write_operations_total',
    'write_latency_ticks': 'write_latency_ticks_total',
    'unmap_bytes': 'unmap_bytes_total',
    'unmap_io': 'unmap_operations_total',
    'unmap_latency_ticks': 'unmap_latency_ticks_total',
}

#: Legacy per-second key -> the v2 counter to take `rate()` of. The collectors
#: used to derive these from consecutive samples and store them; v2 exports only
#: the counters, so the derivation moves here.
_RATES = {
    'read_bytes_ps': 'read_bytes_total',
    'read_io_ps': 'read_operations_total',
    'read_latency_ps': 'read_latency_ticks_total',
    'write_bytes_ps': 'write_bytes_total',
    'write_io_ps': 'write_operations_total',
    'write_latency_ps': 'write_latency_ticks_total',
    'unmap_bytes_ps': 'unmap_bytes_total',
    'unmap_io_ps': 'unmap_operations_total',
    'unmap_latency_ps': 'unmap_latency_ticks_total',
}

_SIZES = {
    'size_total': 'size_total_bytes',
    'size_used': 'size_used_bytes',
    'size_free': 'size_free_bytes',
    'size_prov': 'size_provisioned_bytes',
}

#: Levels whose collector populates a provisioned size. For the others the v1
#: exporter published the field's zero default as if it were a measurement.
_PROV_LEVELS = {'cluster', 'snode'}

#: Percentages the collectors persist as truncated integers. Derived from the
#: byte gauges here so the result keeps full precision.
_RATIOS = {
    'size_util': ('size_used_bytes', 'size_total_bytes'),
    'size_prov_util': ('size_provisioned_bytes', 'size_total_bytes'),
}

#: Keys callers still ask for that have no v2 series. `date` is answered from
#: each sample's own timestamp; the `record_*` trio was never populated by any
#: collector, so v1 reported its defaults (2, 0, 0).
_SYNTHETIC = {'date'}
_UNAVAILABLE = {'record_duration', 'record_start_time', 'record_end_time'}


class PromClientException(Exception):
    def __init__(self, message):
        self.message = message


def _parse_history(history_string) -> timedelta:
    """Parse a history string such as `1d12h`, `2h` or `30m` into a timedelta."""
    results = re.search(r'^(\d+[hmd])(\d+[hmd])?$', history_string.lower())
    if not results:
        raise PromClientException(
            f"Error parsing history string: {history_string}. "
            "Expected e.g. 1d12h, 1d, 2h, 30m")

    unit_seconds = {'d': 86400, 'h': 3600, 'm': 60}
    seconds = sum(
        int(part[:-1]) * unit_seconds[part[-1]]
        for part in results.groups() if part
    )
    return timedelta(seconds=seconds)


class PromClient:

    def __init__(self, cluster_id):
        db_controller = DBController()
        cluster_ip = None
        prometheus_port = None
        cluster = db_controller.get_cluster_by_id(cluster_id)
        if cluster.mode == "docker":
            for node in db_controller.get_mgmt_nodes():
                if node.cluster_id == cluster_id and node.status == MgmtNode.STATUS_ONLINE:
                    cluster_ip = node.mgmt_ip
                    prometheus_port = "9090"
                    break
            if cluster_ip is None:
                raise PromClientException("Cluster has no online mgmt nodes")
        else:
            cluster_ip = constants.PROMETHEUS_STATEFULSET_NAME
            prometheus_port = constants.PROMETHEUS_STATEFULSET_PORT
        self.ip_address = f"{cluster_ip}:{prometheus_port}"
        self.url = 'http://%s/' % self.ip_address
        self.client = PrometheusConnect(url=self.url, disable_ssl=True)

    def _query(self, level, key, selector):
        """PromQL for one legacy stat-record key, or None if unavailable."""
        prefix = f'{NAMESPACE}_{level}'

        if key in _COUNTERS:
            return f'{prefix}_{_COUNTERS[key]}{{{selector}}}'

        if key in _RATES:
            return f'rate({prefix}_{_RATES[key]}{{{selector}}}[{RATE_WINDOW}])'

        if key in _SIZES:
            if key == 'size_prov' and level not in _PROV_LEVELS:
                return None
            return f'{prefix}_{_SIZES[key]}{{{selector}}}'

        if key in _RATIOS:
            numerator, denominator = _RATIOS[key]
            if key == 'size_prov_util' and level not in _PROV_LEVELS:
                return None
            return (f'100 * {prefix}_{numerator}{{{selector}}}'
                    f' / {prefix}_{denominator}{{{selector}}}')

        return None

    def get_metrics(self, key_prefix, metrics_lst, params, history=None):
        """Fetch the given legacy stat keys as a list of per-timestamp records.

        Accepts the v1 stat-record key names the controllers already use and
        translates each to a PromQL expression over the v2 exporter's series, so
        the JSON shape the v1 API returns is unchanged.

        Records are keyed by sample timestamp, not by list position. The previous
        implementation zipped each metric's samples together by index, which
        silently mixed values from different timestamps whenever two series had
        different sample counts or start offsets.
        """
        end_time = datetime.now()
        if history:
            start_time = end_time - _parse_history(history)
        else:
            start_time = end_time - timedelta(minutes=10)

        selector = ','.join(f'{label}="{value}"' for label, value in params.items())

        by_timestamp: dict[int, dict] = {}
        for key in metrics_lst:
            if key in _SYNTHETIC or key in _UNAVAILABLE:
                continue

            query = self._query(key_prefix, key, selector)
            if query is None:
                logger.debug("No v2 series backs %s_%s; omitting", key_prefix, key)
                continue

            try:
                result = self.client.custom_query_range(
                    query=query, start_time=start_time, end_time=end_time,
                    step=str(STEP_SEC))
            except Exception as e:
                raise PromClientException(f"Query failed for {query}: {e}") from e

            for series in result:
                for timestamp, raw in series.get('values', []):
                    record = by_timestamp.setdefault(int(timestamp), {})
                    if key in record:
                        continue
                    record[key] = _coerce(raw)

        records = []
        for timestamp in sorted(by_timestamp):
            record = by_timestamp[timestamp]
            if 'date' in metrics_lst:
                record['date'] = timestamp
            # Every requested key must be present: `utils.dict_agg` indexes each
            # record by key unconditionally, so a gap raises KeyError rather
            # than being skipped. Absent keys are the ones with no v2 series
            # (the never-populated `record_*` trio, or a provisioned size at a
            # level that has none) plus leading timestamps where `rate()` has
            # too little history — 0 is what v1 reported in all those cases.
            for key in metrics_lst:
                record.setdefault(key, 0)
            records.append(record)
        return records

    def get_cluster_metrics(self, cluster_uuid, metrics_lst, history=None):
        return self.get_metrics('cluster', metrics_lst, {'cluster': cluster_uuid}, history)

    def get_node_metrics(self, snode_uuid, metrics_lst, history=None):
        return self.get_metrics('snode', metrics_lst, {'snode': snode_uuid}, history)

    def get_device_metrics(self, device_uuid, metrics_lst, history=None):
        return self.get_metrics('device', metrics_lst, {'device': device_uuid}, history)

    def get_lvol_metrics(self, lvol_uuid, metrics_lst, history=None):
        return self.get_metrics('lvol', metrics_lst, {'lvol': lvol_uuid}, history)

    def get_pool_metrics(self, pool_uuid, metrics_lst, history=None):
        return self.get_metrics('pool', metrics_lst, {'pool': pool_uuid}, history)


def _coerce(raw):
    """Sample value as an int, mapping non-finite results to 0.

    A ratio over a zero denominator yields NaN or Inf, which is not JSON
    serializable. The collectors guarded the same case with `if size_total > 0`
    and stored 0, so 0 reproduces the v1 value.
    """
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return raw
    if not math.isfinite(value):
        return 0
    return int(value)
