"""Batched bdev-presence probes (2026-07-21, revised 2026-07-27).

FD recovery issued +31,710 excess filtered ``bdev_get_bdevs`` — one RPC per
device per reconcile pass. The sweeps then fetched ONE unfiltered bdev dump
per pass — which run 20260725 showed scaling with lvol+snapshot count (~21k
bdevs, 18s+ JSON serialization on the SPDK app thread, KATO starvation, JC
aborts). The sweeps now fetch ONE ``bdev_nvme_get_controllers`` inventory per
pass (scales with attached controllers only) and answer remote_*n1 membership
locally, falling back to per-device filtered probes when the inventory fails.
"""
import types

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


class TestFetchBdevNameSet:
    def test_controller_and_namespace_names_included(self):
        rpc = types.SimpleNamespace(bdev_nvme_controller_list=lambda: [
            {"name": "remote_a1"},
            {"name": "remote_jm_x"},
        ])
        names = storage_node_ops._fetch_bdev_name_set(rpc)
        assert names == {"remote_a1", "remote_a1n1",
                         "remote_jm_x", "remote_jm_xn1"}

    def test_failure_returns_none(self):
        def boom():
            raise RuntimeError("rpc down")
        assert storage_node_ops._fetch_bdev_name_set(
            types.SimpleNamespace(bdev_nvme_controller_list=boom)) is None

    def test_empty_inventory_returns_none(self):
        # An empty/falsy inventory is indistinguishable from a failed RPC
        # layer — callers must fall back rather than treat every bdev as
        # absent.
        assert storage_node_ops._fetch_bdev_name_set(
            types.SimpleNamespace(bdev_nvme_controller_list=list)) is None

    def test_never_calls_unfiltered_get_bdevs(self):
        # The regression this guards: the batch probe must NEVER pay the
        # full bdev dump (app-thread serialization scales with object count).
        def dump_forbidden(name=None, all_bdevs=False):
            if name is None:
                raise AssertionError("unfiltered bdev dump on the batch path")
            return []
        rpc = types.SimpleNamespace(
            bdev_nvme_controller_list=lambda: [{"name": "remote_a1"}],
            get_bdevs=dump_forbidden)
        assert storage_node_ops._fetch_bdev_name_set(rpc) == {
            "remote_a1", "remote_a1n1"}


class _CountingRpc:
    """Inventory + filtered-probe stub sharing one truth set of namespace
    bdev names (``remote_<x>n1``); counts each kind of call."""

    def __init__(self, present):
        self.present = set(present)
        self.inventory_calls = 0
        self.filtered_calls = 0
        self.fail_inventory = False

    def bdev_nvme_controller_list(self, name=None):
        self.inventory_calls += 1
        if self.fail_inventory:
            raise RuntimeError("inventory failed")
        # Controller name = namespace bdev name minus the trailing "n1".
        return [{"name": n[:-2]} for n in sorted(self.present)]

    def get_bdevs(self, name=None, all_bdevs=False):
        assert name is not None, "unfiltered bdev dump on a sweep path"
        self.filtered_calls += 1
        return [{"name": name}] if name in self.present else []


def _coverage_env(monkeypatch, rpc):
    """Wire _verify_online_device_coverage's environment: one peer with two
    data devices; repair disabled."""
    dev1 = types.SimpleNamespace(status=NVMeDevice.STATUS_ONLINE,
                                 alceml_bdev="alc_1")
    dev2 = types.SimpleNamespace(status=NVMeDevice.STATUS_ONLINE,
                                 alceml_bdev="alc_2")
    peer = types.SimpleNamespace(
        get_id=lambda: "peer-1", status=StorageNode.STATUS_ONLINE,
        failure_domain="fd1", nvme_devices=[dev1, dev2])

    class _DBC:
        def get_storage_nodes_by_cluster_id(self, _cid):
            return [peer]

    monkeypatch.setattr(storage_node_ops, "DBController", lambda: _DBC())
    monkeypatch.setattr(storage_node_ops, "fd_dead_recovery_allowed",
                        lambda _db, _sn: False)
    return types.SimpleNamespace(
        get_id=lambda: "snode", cluster_id="c1", failure_domain="fd0",
        rpc_client=lambda timeout, retry: rpc)


class TestCoverageProbeBatching:
    def test_one_inventory_no_filtered_probes(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_1n1", "remote_alc_2n1"})
        snode = _coverage_env(monkeypatch, rpc)
        missing = storage_node_ops._verify_online_device_coverage(
            snode, repair=False)
        assert missing == []
        assert rpc.inventory_calls == 1
        assert rpc.filtered_calls == 0

    def test_missing_detected_via_batch(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_1n1"})  # alc_2 absent
        snode = _coverage_env(monkeypatch, rpc)
        missing = storage_node_ops._verify_online_device_coverage(
            snode, repair=False)
        assert missing == ["remote_alc_2n1"]
        assert rpc.filtered_calls == 0

    def test_fallback_to_filtered_on_inventory_failure(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_1n1"})
        rpc.fail_inventory = True
        snode = _coverage_env(monkeypatch, rpc)
        missing = storage_node_ops._verify_online_device_coverage(
            snode, repair=False)
        assert missing == ["remote_alc_2n1"]
        assert rpc.filtered_calls == 2  # per-device path used

    def test_batch_and_fallback_agree(self, monkeypatch):
        for present in ({"remote_alc_1n1", "remote_alc_2n1"},
                        {"remote_alc_1n1"}, set()):
            batched = _CountingRpc(present)
            snode = _coverage_env(monkeypatch, batched)
            m1 = storage_node_ops._verify_online_device_coverage(
                snode, repair=False)
            fb = _CountingRpc(present)
            fb.fail_inventory = True
            snode = _coverage_env(monkeypatch, fb)
            m2 = storage_node_ops._verify_online_device_coverage(
                snode, repair=False)
            assert m1 == m2, f'divergence for present={present}'


class TestSyncRemoteDevicesBatching:
    def test_sync_uses_single_inventory(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_pn1"})
        dev = types.SimpleNamespace(
            status=NVMeDevice.STATUS_ONLINE, alceml_bdev="alc_p",
            alceml_name="alceml_p", node_id="peer-1", uuid="u-1", size=10,
            nvmf_multipath=False, get_id=lambda: "u-1")
        peer = types.SimpleNamespace(
            get_id=lambda: "peer-1", status=StorageNode.STATUS_ONLINE,
            nvme_devices=[dev])
        fresh = types.SimpleNamespace(
            get_id=lambda: "snode", cluster_id="c1", remote_devices=[],
            write_to_db=lambda *a, **k: True)

        class _DBC:
            kv_store = None

            def get_storage_node_by_id(self, _id):
                return fresh

            def get_storage_nodes_by_cluster_id(self, _cid):
                return [peer]

        monkeypatch.setattr(storage_node_ops, "DBController", lambda: _DBC())
        this = types.SimpleNamespace(
            get_id=lambda: "snode",
            rpc_client=lambda timeout, retry: rpc)
        changed = storage_node_ops.sync_remote_devices_from_spdk(this)
        assert changed is True
        assert rpc.inventory_calls == 1
        assert rpc.filtered_calls == 0
