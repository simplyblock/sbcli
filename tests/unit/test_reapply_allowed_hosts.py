# coding=utf-8
"""Tests for host_auth._reapply_allowed_hosts key selection.

Regression guard: a DHCHAP pool holds its key pair on the pool, so its
allowed-hosts entries carry no per-host keys. Reapply must still register the
pool keys and add each host with them — otherwise a subsystem recreate silently
drops the dhchap requirement.
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


def _install(monkeypatch, pool, cluster):
    db = SimpleNamespace(
        get_cluster_by_id=lambda _id: cluster,
        get_pool_by_id=lambda _id: pool,
    )
    monkeypatch.setattr(host_auth, "DBController", lambda: db)
    pool_reg = []
    monkeypatch.setattr(host_auth, "_register_pool_dhchap_keys_on_node",
                        lambda p, s, r: pool_reg.append((p, s)) or
                        {"dhchap_key": "kd_name", "dhchap_ctrlr_key": "kc_name"})
    monkeypatch.setattr(host_auth, "_register_dhchap_keys_on_node",
                        lambda s, h, e, r: {"psk": "psk_name",
                                            "dhchap_key": "hd_name",
                                            "dhchap_ctrlr_key": "hc_name"})
    return pool_reg


def test_dhchap_pool_adds_hosts_with_pool_keys(monkeypatch):
    pool = SimpleNamespace(dhchap=True, dhchap_key=SecretStr("kd"), dhchap_ctrlr_key=SecretStr("kc"))
    pool_reg = _install(monkeypatch, pool, SimpleNamespace(tls=False, tls_config=None))
    lvol = SimpleNamespace(nqn="nqn:sub", pool_uuid="pool-1",
                           allowed_hosts=[{"nqn": "nqn:host-a"}, {"nqn": "nqn:host-b"}])
    rpc = RecordingRpc()

    host_auth._reapply_allowed_hosts(lvol, SimpleNamespace(cluster_id="cl-1"), rpc)

    assert len(pool_reg) == 1  # registered once for the node, reused across hosts
    assert [c[1] for c in rpc.calls] == ["nqn:host-a", "nqn:host-b"]
    for _nqn, _host, kwargs in rpc.calls:
        assert kwargs["dhchap_key"] == "kd_name"
        assert kwargs["dhchap_ctrlr_key"] == "kc_name"
        assert kwargs["dhchap_group"] == constants.DHCHAP_DHGROUP
        assert "psk" not in kwargs


def test_non_dhchap_pool_uses_per_host_keys(monkeypatch):
    pool = SimpleNamespace(dhchap=False, dhchap_key=SecretStr(""), dhchap_ctrlr_key=SecretStr(""))
    _install(monkeypatch, pool, SimpleNamespace(tls=False, tls_config=None))
    lvol = SimpleNamespace(nqn="nqn:sub", pool_uuid="pool-1",
                           allowed_hosts=[{"nqn": "nqn:host-a", "psk": "raw"}])
    rpc = RecordingRpc()

    host_auth._reapply_allowed_hosts(lvol, SimpleNamespace(cluster_id="cl-1"), rpc)

    (_nqn, _host, kwargs), = rpc.calls
    assert kwargs["psk"] == "psk_name"
    assert kwargs["dhchap_key"] == "hd_name"


def test_non_dhchap_pool_without_keys_adds_plain(monkeypatch):
    pool = SimpleNamespace(dhchap=False, dhchap_key=SecretStr(""), dhchap_ctrlr_key=SecretStr(""))
    _install(monkeypatch, pool, SimpleNamespace(tls=False, tls_config=None))
    lvol = SimpleNamespace(nqn="nqn:sub", pool_uuid="pool-1",
                           allowed_hosts=[{"nqn": "nqn:host-a"}])
    rpc = RecordingRpc()

    host_auth._reapply_allowed_hosts(lvol, SimpleNamespace(cluster_id="cl-1"), rpc)

    assert rpc.calls == [("nqn:sub", "nqn:host-a", {})]
