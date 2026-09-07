"""Unit tests for CPU stat collection in the capacity-and-stats collector.

The counters are persisted raw. SPDK reports reactor and thread busy/idle
cumulatively since start, so a ratio taken here would be an average over the
process lifetime rather than current load — utilization is derived at query
time. These tests pin the shape that gets written and, importantly, that no
value is ever invented for a counter SPDK did not report.
"""

from unittest.mock import MagicMock

import pytest

from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import capacity_and_stats_collector as collector


def _rpc(reactors, threads):
    client = MagicMock()
    client.framework_get_reactors.return_value = {'reactors': reactors}
    client.thread_get_stats.return_value = {'threads': threads}
    return client


REACTOR = {'lcore': 3, 'busy': 700, 'idle': 300, 'irq': 5, 'sys': 10,
           'lw_threads': [{'id': 1, 'name': 'app_thread'}]}
THREAD_STATS = [{'id': 1, 'busy': 400, 'idle': 600}]


class TestCollectedShape:

    def test_reactor_counters_are_persisted_raw(self):
        result = collector.get_cpu_stats(_rpc([REACTOR], THREAD_STATS))

        reactor = result['reactors'][0]
        assert reactor['lcore'] == 3
        assert reactor['busy'] == 700
        assert reactor['idle'] == 300
        assert reactor['irq'] == 5
        assert reactor['sys'] == 10

    def test_thread_counters_are_matched_by_id(self):
        result = collector.get_cpu_stats(_rpc([REACTOR], THREAD_STATS))

        thread = result['reactors'][0]['threads'][0]
        assert thread == {'id': 1, 'name': 'app_thread', 'busy': 400, 'idle': 600}

    def test_no_percentage_is_computed(self):
        """A ratio over counters cumulative since reactor start would be a
        lifetime average, not current load."""
        result = collector.get_cpu_stats(_rpc([REACTOR], THREAD_STATS))

        assert set(result['reactors'][0]) == {
            'lcore', 'busy', 'idle', 'irq', 'sys', 'threads'}

    def test_multiple_reactors_and_threads(self):
        second = dict(REACTOR, lcore=4, lw_threads=[{'id': 2, 'name': 'poller'}])
        result = collector.get_cpu_stats(
            _rpc([REACTOR, second], THREAD_STATS + [{'id': 2, 'busy': 1, 'idle': 2}]))

        assert [r['lcore'] for r in result['reactors']] == [3, 4]
        assert result['reactors'][1]['threads'][0]['name'] == 'poller'


class TestAbsentValuesAreOmittedNotDefaulted:

    def test_thread_without_stats_omits_its_counters(self):
        """A thread present in the reactor listing but absent from
        thread_get_stats appeared between the two calls. Its counters are
        unknown — recording 0 would read as a genuine zero, and on a counter as
        a reset."""
        result = collector.get_cpu_stats(_rpc([REACTOR], []))

        thread = result['reactors'][0]['threads'][0]
        assert thread == {'id': 1, 'name': 'app_thread'}
        assert 'busy' not in thread
        assert 'idle' not in thread

    def test_reactor_with_no_threads_yields_an_empty_list(self):
        """An absent collection does mean 'none of these', unlike a measurement."""
        result = collector.get_cpu_stats(
            _rpc([dict(REACTOR, lw_threads=[])], THREAD_STATS))

        assert result['reactors'][0]['threads'] == []

    def test_missing_reactor_field_raises_rather_than_defaulting(self):
        """The caller turns this into 'no CPU data for this cycle'."""
        incomplete = {k: v for k, v in REACTOR.items() if k != 'irq'}

        with pytest.raises(KeyError):
            collector.get_cpu_stats(_rpc([incomplete], THREAD_STATS))

    def test_empty_reply_yields_no_reactors(self):
        client = MagicMock()
        client.framework_get_reactors.return_value = None
        client.thread_get_stats.return_value = None

        with pytest.raises(KeyError):
            collector.get_cpu_stats(client)

    def test_rpc_failure_propagates(self):
        client = MagicMock()
        client.framework_get_reactors.side_effect = RPCException('unreachable')

        with pytest.raises(RPCException):
            collector.get_cpu_stats(client)


class TestNodeRecord:

    def test_cpu_dict_is_stored_on_the_node_record(self, monkeypatch):
        monkeypatch.setattr(collector, 'db', MagicMock())
        cpu: dict = {'reactors': [{'lcore': 0, 'busy': 1, 'idle': 2}]}
        cluster, node = MagicMock(), MagicMock()
        cluster.get_id.return_value = 'c1'
        node.get_id.return_value = 'n1'

        record = collector.add_node_stats(cluster, node, [], [], cpu)

        assert record.cpu_dict == cpu

    def test_absent_cpu_data_is_stored_empty(self, monkeypatch):
        """Empty, not {'reactors': []} — the latter would assert the node has
        zero reactors, which a failed RPC does not tell us."""
        monkeypatch.setattr(collector, 'db', MagicMock())
        cluster, node = MagicMock(), MagicMock()
        cluster.get_id.return_value = 'c1'
        node.get_id.return_value = 'n1'

        record = collector.add_node_stats(cluster, node, [], [], None)

        assert record.cpu_dict == {}

    def test_cpu_dict_does_not_reach_the_cluster_roll_up(self):
        """__add__ sums int/float attributes; a scalar CPU field would become a
        meaningless cluster total. A dict is skipped."""
        from simplyblock_core.models.stats import NodeStatObject

        a = NodeStatObject(data={'uuid': 'n1', 'cluster_id': 'c1', 'read_bytes': 5,
                                 'cpu_dict': {'reactors': [{'lcore': 0}]}})
        b = NodeStatObject(data={'uuid': 'n2', 'cluster_id': 'c1', 'read_bytes': 7,
                                 'cpu_dict': {'reactors': [{'lcore': 1}]}})

        combined = a + b

        assert combined.read_bytes == 12
        assert not combined.to_dict().get('cpu_dict')
