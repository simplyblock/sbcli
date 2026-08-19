"""ANA state changes must be scoped to a namespace, not to the subsystem.

A subsystem can carry many namespaces, and the volumes behind them are migrated,
suspended and failed over independently — while the client reaches all of them
through ONE controller. A subsystem-wide ANA flip therefore moves the IO of
volumes that were never part of the operation.

In SPDK each namespace has its own ANA group whose id equals the namespace id, so
every per-volume flip passes ``anagrpid=lvol.ns_id``. Subsystems that hold a
single namespace (the hublvols) can still be flipped whole, which is why the RPC
parameter is optional.
"""
from simplyblock_core import rpc_client as rpc_module
from simplyblock_core import storage_node_ops
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


class _Recorder:
    """Captures the params dict each RPC would send."""

    def __init__(self):
        self.requests = []

    def __call__(self, method, params):
        self.requests.append((method, params))
        return ["ok"]


def _client(monkeypatch):
    client = rpc_client_instance()
    recorder = _Recorder()
    monkeypatch.setattr(client, "_request", recorder)
    return client, recorder


def rpc_client_instance():
    client = rpc_module.RPCClient.__new__(rpc_module.RPCClient)
    return client


def test_anagrpid_is_sent_when_given(monkeypatch):
    client, rec = _client(monkeypatch)
    client.nvmf_subsystem_listener_set_ana_state(
        "nqn.test:sub", "10.0.0.1", 4420, ana="inaccessible", anagrpid=7)
    method, params = rec.requests[0]
    assert method == "nvmf_subsystem_listener_set_ana_state"
    assert params["anagrpid"] == 7
    assert params["ana_state"] == "inaccessible"


def test_anagrpid_is_omitted_when_not_given(monkeypatch):
    """Single-namespace subsystems (hublvols) keep the subsystem-wide behaviour,
    so the parameter must stay absent rather than default to something."""
    client, rec = _client(monkeypatch)
    client.nvmf_subsystem_listener_set_ana_state("nqn.test:hub", "10.0.0.1", 4427, ana="optimized")
    _method, params = rec.requests[0]
    assert "anagrpid" not in params


def test_namespace_zero_is_still_sent(monkeypatch):
    """0 is a legitimate group id and must not be dropped by a truthiness test."""
    client, rec = _client(monkeypatch)
    client.nvmf_subsystem_listener_set_ana_state(
        "nqn.test:sub", "10.0.0.1", 4420, ana="optimized", anagrpid=0)
    _method, params = rec.requests[0]
    assert params["anagrpid"] == 0


# --------------------------------------------------------------------------- #
# Per-volume flips
# --------------------------------------------------------------------------- #

class _Nic:
    def __init__(self, ip="10.0.0.1", trtype="TCP"):
        self.ip4_address = ip
        self.trtype = trtype


class _NodeRPC:
    def __init__(self):
        self.calls = []

    def nvmf_subsystem_listener_set_ana_state(self, nqn, ip, port, trtype="TCP",
                                             is_optimized=True, ana=None, anagrpid=None):
        self.calls.append({"nqn": nqn, "ip": ip, "port": port, "ana": ana,
                           "anagrpid": anagrpid})
        return ["ok"]


class _Node:
    def __init__(self, uuid="N1", status=StorageNode.STATUS_ONLINE):
        self.uuid = uuid
        self.status = status
        self.data_nics = [_Nic()]
        self.active_tcp = True
        self.secondary_node_id = ""
        self._rpc = _NodeRPC()

    def get_id(self):
        return self.uuid

    def rpc_client(self, timeout=None, retry=None):
        return self._rpc

    def get_lvol_subsys_port(self, lvs_name):
        return 4420


def _lvol(uuid="LV1", ns_id=1, nqn="nqn.test:shared"):
    lv = LVol()
    lv.uuid = uuid
    lv.nqn = nqn
    lv.ns_id = ns_id
    lv.fabric = "tcp"
    lv.lvs_name = "LVS_1"
    return lv


def test_per_volume_flip_carries_its_namespace():
    node = _Node()
    storage_node_ops._set_lvol_ana_on_node(_lvol(ns_id=3), node, "optimized")  # type: ignore[arg-type]
    assert node._rpc.calls[0]["anagrpid"] == 3


def test_volumes_sharing_a_subsystem_flip_independently():
    """Two volumes, one NQN, different namespaces: each call must name its own
    group, or promoting one would promote the other."""
    node = _Node()
    for ns in (1, 2):
        lvol = _lvol(uuid=f"LV{ns}", ns_id=ns)
        storage_node_ops._set_lvol_ana_on_node(lvol, node, "optimized")  # type: ignore[arg-type]
    groups = [c["anagrpid"] for c in node._rpc.calls]
    nqns = {c["nqn"] for c in node._rpc.calls}
    assert groups == [1, 2]
    assert len(nqns) == 1, "the point of the test: same subsystem, separate groups"


# --------------------------------------------------------------------------- #
# Fail-over / fail-back dedupe
# --------------------------------------------------------------------------- #

class _DB:
    def __init__(self, lvols, nodes):
        self._lvols = lvols
        self._nodes = {n.get_id(): n for n in nodes}

    def get_lvols_by_node_id(self, node_id):
        return self._lvols

    def get_storage_node_by_id(self, node_id):
        return self._nodes[node_id]


def _install_db(monkeypatch, db):
    monkeypatch.setattr(storage_node_ops, "DBController", lambda: db)


def test_failover_promotes_every_namespace_of_a_shared_subsystem(monkeypatch):
    """The regression this change fixes.

    The old dedupe key was (nqn, lvs_name) because the flip was subsystem-wide.
    Once the flip is confined to one ANA group, that key skips every namespace but
    the first, so all the other volumes of a shared subsystem would stay
    unpromoted after a primary failure.
    """
    primary = _Node("N_PRIMARY")
    secondary = _Node("N_SEC")
    primary.secondary_node_id = "N_SEC"
    lvols = [_lvol("LV1", ns_id=1), _lvol("LV2", ns_id=2), _lvol("LV3", ns_id=3)]
    for lv in lvols:
        lv.status = LVol.STATUS_ONLINE
    _install_db(monkeypatch, _DB(lvols, [primary, secondary]))

    storage_node_ops._failover_primary_ana(primary)  # type: ignore[arg-type]

    promoted = sorted(c["anagrpid"] for c in secondary._rpc.calls)
    assert promoted == [1, 2, 3], "every namespace of the shared subsystem must be promoted"
    assert all(c["ana"] == "optimized" for c in secondary._rpc.calls)


def test_failover_still_dedupes_identical_namespace_records(monkeypatch):
    """Duplicate records for the same namespace must not multiply the RPCs."""
    primary = _Node("N_PRIMARY")
    secondary = _Node("N_SEC")
    primary.secondary_node_id = "N_SEC"
    duplicate = [_lvol("LV1", ns_id=1), _lvol("LV1", ns_id=1)]
    for lv in duplicate:
        lv.status = LVol.STATUS_ONLINE
    _install_db(monkeypatch, _DB(duplicate, [primary, secondary]))

    storage_node_ops._failover_primary_ana(primary)  # type: ignore[arg-type]
    assert len(secondary._rpc.calls) == 1


def test_failback_demotes_every_namespace(monkeypatch):
    primary = _Node("N_PRIMARY")
    secondary = _Node("N_SEC")
    primary.secondary_node_id = "N_SEC"
    lvols = [_lvol("LV1", ns_id=1), _lvol("LV2", ns_id=2)]
    for lv in lvols:
        lv.status = LVol.STATUS_ONLINE
    _install_db(monkeypatch, _DB(lvols, [primary, secondary]))

    storage_node_ops._failback_primary_ana(primary)  # type: ignore[arg-type]

    assert sorted(c["anagrpid"] for c in secondary._rpc.calls) == [1, 2]
    assert all(c["ana"] == "non_optimized" for c in secondary._rpc.calls)
