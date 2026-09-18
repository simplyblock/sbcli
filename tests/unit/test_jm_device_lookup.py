"""A JM lookup must not answer for an NVMe device.

NVMeDevice and JMDevice ids share one namespace — a JM built on a whole device
inherits that device's uuid (storage_node_ops._create_jm_stack_on_device) — and
both kinds live in the same StorageNode record, so a lookup keyed on a bare
device id cannot say which one it found. get_storage_node_by_jm_device is the
dangerous one: every caller acts on ``snode.jm_device``, so answering for an
NVMe id turns `sn remove-jm-device <mistyped id>` into a journal teardown on a
healthy node.

These tests pin that each accessor answers for its own kind of device only,
while the deliberately kind-agnostic owner lookup keeps answering for both.
"""
import pytest

from simplyblock_core.controllers import device_controller, health_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode


NVME_ID = 'dev-1'
JM_ID = 'jm-1'
NODE_ID = 'node-1'


class _FakeKV:
    """Minimal writable store: set/clear plus prefix scans.

    Holds no ``index_meta/`` records, so every index reads as still building
    and the query layer takes its scan fallback — which is the path that has to
    agree with the index, so it is the one worth pinning here.
    """

    def __init__(self):
        self._data = {}

    def set(self, key, value):
        self._data[key] = value

    def get(self, key):
        return self._data.get(key)

    def clear(self, key):
        self._data.pop(key, None)

    def add(self, key, value):
        self._data[key] = value

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        rows = sorted((k, v) for k, v in self._data.items() if k.startswith(prefix))
        if reverse:
            rows.reverse()
        if limit:
            rows = rows[:limit]
        return rows


@pytest.fixture
def db(monkeypatch):
    instance = DBController()
    instance.kv_store = _FakeKV()

    node = StorageNode({
        'uuid': NODE_ID,
        'cluster_id': 'cluster-1',
        'status': StorageNode.STATUS_ONLINE,
        'nvme_devices': [{'uuid': NVME_ID}],
        'jm_device': {'uuid': JM_ID},
    })
    node.write_to_db(instance.kv_store)

    for module in (device_controller, health_controller):
        monkeypatch.setattr(module, 'DBController', lambda inst=instance: inst)

    return instance


def test_jm_lookup_rejects_an_nvme_device_id(db):
    with pytest.raises(KeyError):
        device_controller.get_storage_node_by_jm_device(db, NVME_ID)


def test_jm_lookup_finds_the_journal_device(db):
    assert device_controller.get_storage_node_by_jm_device(db, JM_ID).get_id() == NODE_ID


def test_remove_jm_device_refuses_an_nvme_device_id(db):
    """The incident shape: a mistyped id must not delete the node's journal."""
    assert device_controller.remove_jm_device(NVME_ID) is False


def test_device_accessors_answer_for_their_own_kind_only(db):
    assert db.get_storage_device_by_id(NVME_ID).get_id() == NVME_ID
    assert db.get_jm_device_by_id(JM_ID).get_id() == JM_ID

    with pytest.raises(KeyError):
        db.get_storage_device_by_id(JM_ID)

    with pytest.raises(KeyError):
        db.get_jm_device_by_id(NVME_ID)


@pytest.mark.parametrize('device_id', [NVME_ID, JM_ID])
def test_owner_lookup_stays_kind_agnostic(db, device_id):
    """health_controller and health_check_service only want the owning node,
    which is the same answer for either kind."""
    assert db.get_storage_node_by_device_id(device_id).get_id() == NODE_ID


def test_check_device_routes_a_jm_id_to_the_jm_check(db, monkeypatch):
    checked = []
    monkeypatch.setattr(health_controller, 'check_jm_device', checked.append)

    health_controller.check_device(JM_ID)

    assert checked == [JM_ID]


def test_check_device_reports_an_unknown_id_as_missing(db, monkeypatch):
    monkeypatch.setattr(health_controller, 'check_jm_device',
                        lambda _id: pytest.fail('JM check ran for an unknown id'))

    assert health_controller.check_device('no-such-device') is False
