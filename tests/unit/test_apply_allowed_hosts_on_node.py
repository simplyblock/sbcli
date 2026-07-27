# coding=utf-8
"""Tests for the shared node-side host-registration helpers in host_auth:
`add_host_to_subsystem` (the per-host primitive) and `apply_allowed_hosts_on_node`
(the per-node driver, which derives its own RPC client from the node).

Key selection must mirror what the subsystem enforces: a DHCHAP pool contributes
the shared pool key pair (no PSK); any other volume uses the per-host keys on the
allowed-hosts entry, or a plain add when it has none.
"""
from types import SimpleNamespace

from pydantic import SecretStr

from simplyblock_core import constants
from simplyblock_core.controllers import host_auth


class RecordingRpc:
    def __init__(self):
        self.calls = []

    def subsystem_add_host(self, nqn, host, **kwargs):
        self.calls.append((nqn, host, kwargs))
        return True


def _pool(dhchap, dhchap_key="", dhchap_ctrlr_key=""):
    return SimpleNamespace(dhchap=dhchap, dhchap_key=SecretStr(dhchap_key),
                           dhchap_ctrlr_key=SecretStr(dhchap_ctrlr_key))


def _patch_registers(monkeypatch):
    """Stub the keyring-registration helpers (which do node I/O). Returns a list
    recording each pool-key registration so the "register once" path is testable."""
    pool_reg = []
    monkeypatch.setattr(host_auth, "_register_pool_dhchap_keys_on_node",
                        lambda p, s, r: pool_reg.append((p, s)) or
                        {"dhchap_key": "kd_name", "dhchap_ctrlr_key": "kc_name"})
    monkeypatch.setattr(host_auth, "_register_dhchap_keys_on_node",
                        lambda s, h, e, r: {"psk": "psk_name",
                                            "dhchap_key": "hd_name",
                                            "dhchap_ctrlr_key": "hc_name"})
    return pool_reg


# ── add_host_to_subsystem (primitive) ────────────────────────────────────────

def test_primitive_dhchap_pool_registers_when_no_prereg(monkeypatch):
    pool_reg = _patch_registers(monkeypatch)
    rpc = RecordingRpc()
    host_auth.add_host_to_subsystem(rpc, SimpleNamespace(), "nqn:sub",
                                    {"nqn": "nqn:host"}, _pool(True), constants.DHCHAP_DHGROUP)
    assert len(pool_reg) == 1
    (_n, _h, kwargs), = rpc.calls
    assert kwargs["dhchap_key"] == "kd_name"
    assert kwargs["dhchap_ctrlr_key"] == "kc_name"
    assert kwargs["dhchap_group"] == constants.DHCHAP_DHGROUP
    assert "psk" not in kwargs


def test_primitive_dhchap_pool_reuses_prereg(monkeypatch):
    pool_reg = _patch_registers(monkeypatch)
    rpc = RecordingRpc()
    host_auth.add_host_to_subsystem(rpc, SimpleNamespace(), "nqn:sub",
                                    {"nqn": "nqn:host"}, _pool(True), constants.DHCHAP_DHGROUP,
                                    pool_key_names={"dhchap_key": "pre_kd", "dhchap_ctrlr_key": "pre_kc"})
    assert pool_reg == []
    (_n, _h, kwargs), = rpc.calls
    assert kwargs["dhchap_key"] == "pre_kd"


def test_primitive_per_host_keys(monkeypatch):
    _patch_registers(monkeypatch)
    rpc = RecordingRpc()
    host_auth.add_host_to_subsystem(rpc, SimpleNamespace(), "nqn:sub",
                                    {"nqn": "nqn:host", "psk": "raw"}, _pool(False), "null")
    (_n, _h, kwargs), = rpc.calls
    assert kwargs["psk"] == "psk_name"
    assert kwargs["dhchap_key"] == "hd_name"


def test_primitive_plain_when_no_keys(monkeypatch):
    _patch_registers(monkeypatch)
    rpc = RecordingRpc()
    host_auth.add_host_to_subsystem(rpc, SimpleNamespace(), "nqn:sub",
                                    {"nqn": "nqn:host"}, _pool(False), "null")
    assert rpc.calls == [("nqn:sub", "nqn:host", {})]


# ── apply_allowed_hosts_on_node (per-node driver) ────────────────────────────

def _install_db(monkeypatch, pool, cluster):
    db = SimpleNamespace(get_cluster_by_id=lambda _id: cluster,
                         get_pool_by_id=lambda _id: pool)
    monkeypatch.setattr(host_auth, "DBController", lambda: db)


def _snode(rpc):
    return SimpleNamespace(cluster_id="cl-1", rpc_client=lambda **kw: rpc)


def test_apply_dhchap_pool_registers_once_and_adds_all(monkeypatch):
    pool_reg = _patch_registers(monkeypatch)
    _install_db(monkeypatch, _pool(True, "kd", "kc"), SimpleNamespace(tls=False, tls_config=None))
    rpc = RecordingRpc()
    lvol = SimpleNamespace(nqn="nqn:sub", pool_uuid="pool-1",
                           allowed_hosts=[{"nqn": "nqn:host-a"}, {"nqn": "nqn:host-b"}])

    host_auth.apply_allowed_hosts_on_node(lvol, _snode(rpc))

    assert len(pool_reg) == 1  # registered once for the node, reused across hosts
    assert [c[1] for c in rpc.calls] == ["nqn:host-a", "nqn:host-b"]
    for _n, _h, kwargs in rpc.calls:
        assert kwargs["dhchap_key"] == "kd_name"
        assert kwargs["dhchap_group"] == constants.DHCHAP_DHGROUP


def test_apply_non_dhchap_without_keys_adds_plain(monkeypatch):
    _patch_registers(monkeypatch)
    _install_db(monkeypatch, _pool(False), SimpleNamespace(tls=False, tls_config=None))
    rpc = RecordingRpc()
    lvol = SimpleNamespace(nqn="nqn:sub", pool_uuid="pool-1",
                           allowed_hosts=[{"nqn": "nqn:host-a"}])

    host_auth.apply_allowed_hosts_on_node(lvol, _snode(rpc))

    assert rpc.calls == [("nqn:sub", "nqn:host-a", {})]
