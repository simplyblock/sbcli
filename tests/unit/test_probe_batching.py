"""Batched bdev-presence probes (2026-07-21 follow-up).

FD recovery issued +31,710 excess filtered ``bdev_get_bdevs`` — one RPC per
device per reconcile pass. The sweeps now fetch ONE unfiltered dump per pass
and answer membership locally, falling back to per-device probes when the
dump fails. Filtered get_bdevs matches by name OR alias, so the batch set
must include aliases.
"""
import types

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


def _dump_entry(name, aliases=()):
    return {"name": name, "aliases": list(aliases)}


class TestFetchBdevNameSet:
    def test_names_and_aliases_included(self):
        rpc = types.SimpleNamespace(get_bdevs=lambda name=None: [
            _dump_entry("remote_a1n1"),
            _dump_entry("uuid-123", aliases=["LVS_1/lvol1", "alias2"]),
        ])
        names = storage_node_ops._fetch_bdev_name_set(rpc)
        assert names == {"remote_a1n1", "uuid-123", "LVS_1/lvol1", "alias2"}

    def test_failure_returns_none(self):
        def boom(name=None):
            raise RuntimeError("rpc down")
        assert storage_node_ops._fetch_bdev_name_set(
            types.SimpleNamespace(get_bdevs=boom)) is None

    def test_empty_dump_returns_none(self):
        # An empty/falsy dump is indistinguishable from a failed RPC layer —
        # callers must fall back rather than treat every bdev as absent.
        assert storage_node_ops._fetch_bdev_name_set(
            types.SimpleNamespace(get_bdevs=lambda name=None: [])) is None


class _CountingRpc:
    """get_bdevs stub: unfiltered call returns the dump, filtered call
    consults the same truth; counts each kind."""

    def __init__(self, present):
        self.present = set(present)
        self.dump_calls = 0
        self.filtered_calls = 0
        self.fail_dump = False

    def get_bdevs(self, name=None):
        if name is None:
            self.dump_calls += 1
            if self.fail_dump:
                raise RuntimeError("dump failed")
            return [_dump_entry(n) for n in sorted(self.present)]
        self.filtered_calls += 1
        return [_dump_entry(name)] if name in self.present else []


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
    def test_one_dump_no_filtered_probes(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_1n1", "remote_alc_2n1"})
        snode = _coverage_env(monkeypatch, rpc)
        missing = storage_node_ops._verify_online_device_coverage(
            snode, repair=False)
        assert missing == []
        assert rpc.dump_calls == 1
        assert rpc.filtered_calls == 0

    def test_missing_detected_via_batch(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_1n1"})  # alc_2 absent
        snode = _coverage_env(monkeypatch, rpc)
        missing = storage_node_ops._verify_online_device_coverage(
            snode, repair=False)
        assert missing == ["remote_alc_2n1"]
        assert rpc.filtered_calls == 0

    def test_fallback_to_filtered_on_dump_failure(self, monkeypatch):
        rpc = _CountingRpc({"remote_alc_1n1"})
        rpc.fail_dump = True
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
            fb.fail_dump = True
            snode = _coverage_env(monkeypatch, fb)
            m2 = storage_node_ops._verify_online_device_coverage(
                snode, repair=False)
            assert m1 == m2, f'divergence for present={present}'


class TestSyncRemoteDevicesBatching:
    def test_sync_uses_single_dump(self, monkeypatch):
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
        assert rpc.dump_calls == 1
        assert rpc.filtered_calls == 0
