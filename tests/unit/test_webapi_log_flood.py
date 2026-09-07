# coding=utf-8
"""test_webapi_log_flood.py — guards against the 2026-09-07 web-API log flood.

An 8-rule Grafana alert group polling `GET /clusters/{id}/logs?limit=1000` once
a minute produced 1.7GB of logs per hour from the web API. Three independent
defects combined:

1. `cluster_ops.get_logs()` called `logger.debug(record)` per event record.
   `EventObj.__repr__` is `pprint.pformat(to_dict())` over an `object_dict`
   holding a whole LVol, so each of ~780 records expanded to ~150 log lines.
2. `constants.LOG_WEB_LEVEL` was hardcoded to `logging.DEBUG`, so nothing could
   turn that output off -- and because `cluster_ops.deploy_stack` derives the
   stack's `$LOG_LEVEL` from it, every control-plane service was deployed
   verbose too.
3. The proxy in front of the API shipped no logs and set no `X-Forwarded-For`,
   so neither the proxy's view nor the real client's identity was recoverable
   while diagnosing it.

The flood overran the gelf driver's non-blocking buffer (dropping ~80% of all
control-plane logs) and, because `QueueHandler.prepare` formats on the *calling*
thread, the pformat work starved the event loop: lvol creates on the affected
worker stalled 11-38s and CSI retried them into 409 conflicts.
"""

import importlib
import logging
import os
from unittest.mock import patch

import pytest
import yaml

from simplyblock_core import cluster_ops, constants
from simplyblock_core.models.events import EventObj


SCRIPTS_DIR = os.path.join(os.path.dirname(constants.__file__), 'scripts')

#: Planted deep in the event payload. It can only reach a log record by way of
#: the whole object being formatted, which is exactly what must not happen.
CANARY = 'lvol-payload-canary-do-not-log'


def _make_event(index: int) -> EventObj:
    event = EventObj()
    event.uuid = f'0000000-0000-0000-0000-{index:012d}'
    event.cluster_uuid = 'cluster-1'
    event.date = 1788757412036
    event.event = 'OBJ_DELETED'
    event.event_level = EventObj.LEVEL_INFO
    event.message = f'Volume deleted {index}'
    event.node_id = 'node-1'
    event.status = 'in_deletion'
    event.storage_id = -1
    event.vuid = -1
    # Shaped like the real thing: a nested LVol dict is what made one record
    # worth ~150 log lines.
    event.object_dict = {
        'uuid': f'lvol-{index}',
        'lvol_name': f'pvc-{index}',
        'nqn': f'nqn.2023-02.io.simplyblock:cluster-1:lvol:{index}',
        'bdev_stack': [{'name': f'LVOL_{index}', 'type': 'bdev_lvol',
                        'params': {'size_in_mib': 40960}}],
        'pvc_name': CANARY,
    }
    return event


@pytest.fixture
def events():
    return [_make_event(i) for i in range(25)]


@pytest.fixture
def db(events):
    with patch.object(cluster_ops, 'db_controller') as mock_db:
        mock_db.get_events.return_value = list(events)
        yield mock_db


def _log_text(caplog) -> str:
    return '\n'.join(record.getMessage() for record in caplog.records)


class TestGetLogsDoesNotDumpRecords:
    """get_logs must not emit output proportional to a caller-supplied limit."""

    def test_event_payload_never_reaches_a_log_record(self, db, caplog):
        with caplog.at_level(logging.DEBUG):
            cluster_ops.get_logs('cluster-1', limit=25)

        assert CANARY not in _log_text(caplog)

    def test_output_is_not_proportional_to_record_count(self, db, events, caplog):
        with caplog.at_level(logging.DEBUG):
            cluster_ops.get_logs('cluster-1', limit=len(events))

        # One line per record (let alone ~150) is what made this unbounded.
        assert len(caplog.records) < len(events)

    def test_repr_of_an_event_is_still_the_expensive_multi_line_dump(self, events):
        """Why the debug call had to go, rather than being made conditional.

        If this ever stops holding, `logger.debug(<EventObj>)` is cheap again
        and this whole guard can be revisited.
        """
        rendered = repr(events[0])
        assert CANARY in rendered
        assert rendered.count('\n') > 5

    def test_records_are_still_returned_correctly(self, db, events):
        rows = cluster_ops.get_logs('cluster-1', limit=len(events))

        assert len(rows) == len(events)
        # get_logs reverses newest-first into chronological order.
        assert rows[0]['Message'] == 'Volume deleted 24'
        assert rows[0]['Event'] == 'OBJ_DELETED'
        assert rows[0]['Status'] == 'in_deletion'
        # A missing storage_id/vuid stays absent rather than leaking -1.
        assert rows[0]['Storage_ID'] == 'None'
        assert rows[0]['VUID'] == 'None'


class TestWebLogLevelIsOperatorSettable:
    """LOG_WEB_LEVEL drives both the web process and the deployed stack."""

    @staticmethod
    def _reload_with(value):
        env = dict(os.environ)
        env.pop('SIMPLYBLOCK_LOG_LEVEL', None)
        if value is not None:
            env['SIMPLYBLOCK_LOG_LEVEL'] = value
        with patch.dict(os.environ, env, clear=True):
            return importlib.reload(constants)

    def teardown_method(self):
        # Leave the module as the rest of the session expects it.
        importlib.reload(constants)

    def test_defaults_to_info(self):
        reloaded = self._reload_with(None)
        assert reloaded.LOG_WEB_LEVEL == logging.INFO
        assert reloaded.LOG_WEB_DEBUG is False

    def test_debug_is_opt_in(self):
        reloaded = self._reload_with('DEBUG')
        assert reloaded.LOG_WEB_LEVEL == logging.DEBUG
        assert reloaded.LOG_WEB_DEBUG is True

    def test_level_name_is_case_insensitive(self):
        assert self._reload_with('warning').LOG_WEB_LEVEL == logging.WARNING

    def test_a_typo_falls_back_to_info_rather_than_raising(self):
        # A bad deploy-time env var must not stop the control plane starting.
        assert self._reload_with('DEBGU').LOG_WEB_LEVEL == logging.INFO


class TestProxyObservability:
    """The proxy must ship its logs and name the real client."""

    def test_haproxy_forwards_the_client_address_to_the_api(self):
        with open(os.path.join(SCRIPTS_DIR, 'haproxy.cfg'), encoding='utf-8') as fh:
            backends = fh.read().split('\nbackend ')

        api = next(b for b in backends if b.startswith('wep_api_services'))
        assert 'option forwardfor' in api

    def test_every_service_ships_logs_to_the_collector(self):
        with open(os.path.join(SCRIPTS_DIR, 'docker-compose-swarm.yml'), encoding='utf-8') as fh:
            compose = yaml.safe_load(fh)

        # collect_logs.py asks for every one of these by name; a service with no
        # logging driver silently yields an empty file.
        missing = sorted(
            name for name, service in compose['services'].items()
            if 'logging' not in service
        )
        assert missing == []
