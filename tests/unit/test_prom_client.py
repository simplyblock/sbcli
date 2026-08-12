# coding=utf-8
"""Unit tests for the PromClient translation layer.

PromClient is the compatibility shim between the v1 JSON API's stat-record key
names and the v2 exporter's metric names. The controllers still ask for
`read_bytes_ps` and `size_util`; neither exists as a series any more, so this
layer has to derive them. These tests pin the translation and the timestamp
alignment.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import prom_client as prom_client_module
from simplyblock_core.prom_client import PromClient, PromClientException, _parse_history


@pytest.fixture()
def client():
    """A PromClient with __init__ bypassed and a stubbed PrometheusConnect."""
    instance = PromClient.__new__(PromClient)
    instance.client = MagicMock()
    instance.client.custom_query_range.return_value = []
    return instance


def _queries(client):
    return [c.kwargs['query'] for c in client.client.custom_query_range.call_args_list]


def _series(*samples):
    return [{'metric': {}, 'values': list(samples)}]


class TestHistoryParsing:

    @pytest.mark.parametrize('text,expected', [
        ('1d', timedelta(days=1)),
        ('2h', timedelta(hours=2)),
        ('30m', timedelta(minutes=30)),
        ('1d12h', timedelta(days=1, hours=12)),
        ('90m', timedelta(minutes=90)),
    ])
    def test_valid(self, text, expected):
        assert _parse_history(text) == expected

    @pytest.mark.parametrize('text', ['', 'garbage', '5', '1w', '1d2d3d'])
    def test_invalid_raises(self, text):
        """v1 logged and returned False, so a typo silently became a 10m window."""
        with pytest.raises(PromClientException):
            _parse_history(text)


class TestQueryTranslation:

    def test_cumulative_counter_maps_to_total(self, client):
        client.get_cluster_metrics('c1', ['read_bytes'])
        assert _queries(client) == ['simplyblock_cluster_read_bytes_total{cluster="c1"}']

    def test_io_count_maps_to_operations(self, client):
        client.get_lvol_metrics('v1', ['write_io'])
        assert _queries(client) == ['simplyblock_lvol_write_operations_total{lvol="v1"}']

    def test_per_second_key_becomes_a_rate(self, client):
        client.get_cluster_metrics('c1', ['read_bytes_ps'])
        assert _queries(client) == [
            'rate(simplyblock_cluster_read_bytes_total{cluster="c1"}[5m])']

    def test_latency_ps_rates_the_tick_counter(self, client):
        """v1's `*_latency_ps` was ticks per second, not per-operation latency."""
        client.get_device_metrics('d1', ['write_latency_ps'])
        assert _queries(client) == [
            'rate(simplyblock_device_write_latency_ticks_total{device="d1"}[5m])']

    def test_size_keys_map_to_byte_gauges(self, client):
        client.get_pool_metrics('p1', ['size_total', 'size_used', 'size_free'])
        assert _queries(client) == [
            'simplyblock_pool_size_total_bytes{pool="p1"}',
            'simplyblock_pool_size_used_bytes{pool="p1"}',
            'simplyblock_pool_size_free_bytes{pool="p1"}',
        ]

    def test_util_is_derived_from_the_byte_gauges(self, client):
        """The stored percentage is int-truncated; dividing keeps precision."""
        client.get_snode_metrics = client.get_node_metrics
        client.get_node_metrics('n1', ['size_util'])
        assert _queries(client) == [
            '100 * simplyblock_snode_size_used_bytes{snode="n1"}'
            ' / simplyblock_snode_size_total_bytes{snode="n1"}'
        ]

    def test_provisioned_size_omitted_where_no_collector_sets_it(self, client):
        """Device, volume and pool records never carry a provisioned size."""
        client.get_device_metrics('d1', ['size_prov', 'size_prov_util'])
        assert _queries(client) == []

    def test_provisioned_size_queried_for_cluster_and_node(self, client):
        client.get_cluster_metrics('c1', ['size_prov'])
        assert _queries(client) == ['simplyblock_cluster_size_provisioned_bytes{cluster="c1"}']

    def test_unavailable_keys_are_not_queried(self, client):
        """No collector ever populated the record_* trio."""
        client.get_cluster_metrics(
            'c1', ['record_duration', 'record_start_time', 'record_end_time', 'date'])
        assert _queries(client) == []

    def test_level_label_scopes_the_query(self, client):
        for getter, uuid, label in [
            (client.get_cluster_metrics, 'c1', 'cluster'),
            (client.get_node_metrics, 'n1', 'snode'),
            (client.get_device_metrics, 'd1', 'device'),
            (client.get_lvol_metrics, 'v1', 'lvol'),
            (client.get_pool_metrics, 'p1', 'pool'),
        ]:
            client.client.custom_query_range.reset_mock()
            getter(uuid, ['read_bytes'])
            assert f'{{{label}="{uuid}"}}' in _queries(client)[0]


class TestRecordAssembly:

    def test_values_are_aligned_by_timestamp_not_position(self, client):
        """The v1 implementation zipped samples by list index, so two series
        with different start offsets had their values mixed across timestamps."""
        def result(query, **kwargs):
            if 'read_bytes_total' in query:
                return _series((100, '10'), (160, '20'), (220, '30'))
            return _series((160, '5'), (220, '6'))   # starts one step later
        client.client.custom_query_range.side_effect = result

        records = client.get_cluster_metrics('c1', ['read_bytes', 'write_bytes'])

        assert [r['read_bytes'] for r in records] == [10, 20, 30]
        # 5 belongs to t=160, not to the first record
        assert [r['write_bytes'] for r in records] == [0, 5, 6]

    def test_records_are_ordered_by_timestamp(self, client):
        client.client.custom_query_range.return_value = _series(
            (220, '3'), (100, '1'), (160, '2'))

        records = client.get_cluster_metrics('c1', ['read_bytes', 'date'])

        assert [r['date'] for r in records] == [100, 160, 220]
        assert [r['read_bytes'] for r in records] == [1, 2, 3]

    def test_date_comes_from_the_sample_timestamp(self, client):
        client.client.custom_query_range.return_value = _series((1700000000, '7'))

        records = client.get_cluster_metrics('c1', ['read_bytes', 'date'])

        assert records == [{'read_bytes': 7, 'date': 1700000000}]

    def test_every_requested_key_is_present(self, client):
        """utils.dict_agg indexes each record by key, so a gap is a KeyError."""
        client.client.custom_query_range.return_value = _series((100, '1'))
        keys = ['read_bytes', 'size_prov', 'record_duration', 'date', 'size_util']

        records = client.get_device_metrics('d1', keys)

        assert set(records[0]) == set(keys)

    def test_records_survive_dict_agg(self, client):
        """End-to-end guard against the KeyError the previous point describes."""
        from simplyblock_core import utils
        client.client.custom_query_range.return_value = _series((100, '1'), (160, '2'))
        keys = ['date', 'read_bytes', 'read_bytes_ps', 'size_util', 'record_duration']

        records = client.get_device_metrics('d1', keys)

        assert utils.process_records(records, 1, keys=keys)

    def test_non_finite_ratio_becomes_zero(self, client):
        """A util ratio over a zero total is NaN, which is not JSON serializable.
        The collectors guarded the same case and stored 0."""
        client.client.custom_query_range.return_value = _series((100, 'NaN'), (160, '+Inf'))

        records = client.get_cluster_metrics('c1', ['size_util'])

        assert [r['size_util'] for r in records] == [0, 0]

    def test_no_data_yields_no_records(self, client):
        assert client.get_cluster_metrics('c1', ['read_bytes']) == []

    def test_query_failure_raises(self, client):
        """v1 returned [] on error, so the API answered 200 with empty stats."""
        client.client.custom_query_range.side_effect = RuntimeError('boom')

        with pytest.raises(PromClientException):
            client.get_cluster_metrics('c1', ['read_bytes'])


class TestTimeWindow:

    def test_history_sets_the_range(self, client):
        client.get_cluster_metrics('c1', ['read_bytes'], history='2h')

        call = client.client.custom_query_range.call_args
        span = call.kwargs['end_time'] - call.kwargs['start_time']
        assert abs(span - timedelta(hours=2)) < timedelta(seconds=5)

    def test_default_window_is_ten_minutes(self, client):
        client.get_cluster_metrics('c1', ['read_bytes'])

        call = client.client.custom_query_range.call_args
        span = call.kwargs['end_time'] - call.kwargs['start_time']
        assert abs(span - timedelta(minutes=10)) < timedelta(seconds=5)

    def test_step_matches_the_scrape_interval(self, client):
        client.get_cluster_metrics('c1', ['read_bytes'])

        assert client.client.custom_query_range.call_args.kwargs['step'] == '60'

    def test_invalid_history_raises_instead_of_defaulting(self, client):
        with pytest.raises(PromClientException):
            client.get_cluster_metrics('c1', ['read_bytes'], history='nonsense')


class TestPrometheusEndpointResolution:

    def test_docker_mode_uses_an_online_mgmt_node(self):
        cluster = MagicMock(mode='docker')
        node = MagicMock(cluster_id='c1', status='online', mgmt_ip='10.0.0.5')
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_mgmt_nodes.return_value = [node]

        with patch.object(prom_client_module, 'DBController', return_value=db), \
                patch.object(prom_client_module, 'PrometheusConnect'):
            client = PromClient('c1')

        assert client.ip_address == '10.0.0.5:9090'

    def test_docker_mode_without_online_mgmt_node_raises(self):
        db = MagicMock()
        db.get_cluster_by_id.return_value = MagicMock(mode='docker')
        db.get_mgmt_nodes.return_value = []

        with patch.object(prom_client_module, 'DBController', return_value=db), \
                patch.object(prom_client_module, 'PrometheusConnect'):
            with pytest.raises(PromClientException):
                PromClient('c1')

    def test_kubernetes_mode_uses_the_statefulset(self):
        db = MagicMock()
        db.get_cluster_by_id.return_value = MagicMock(mode='kubernetes')

        with patch.object(prom_client_module, 'DBController', return_value=db), \
                patch.object(prom_client_module, 'PrometheusConnect'):
            client = PromClient('c1')

        assert 'simplyblock-prometheus' in client.ip_address
