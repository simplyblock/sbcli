"""D6 unit tests for the shared cross-cluster replication cutover."""
from simplyblock_core.services import replication_final_step as rfs
from simplyblock_core.models.storage_node import StorageNode


class _Nic:
    def __init__(self, ip, trtype="TCP"):
        self.ip4_address = ip
        self.trtype = trtype


class _Hub:
    bdev_name = "transfer_hub"
    nqn = "nqn.hub"
    nvmf_port = 9100

    def get_remote_bdev_name(self):
        return "transfer_hub_remote"


class _RPC:
    def __init__(self, node_id, events, final_step_ret=True):
        self.node_id = node_id
        self.events = events
        self._final_step_ret = final_step_ret

    # hub attach fast-path: pretend the remote bdev already exists
    def get_bdevs(self, name=None):
        return [{"name": name or "x"}]

    def bdev_nvme_attach_controller(self, *a, **k):
        return ["ok"]

    def bdev_nvme_controller_list(self, *a, **k):
        return []

    def bdev_lvol_transfer_final_step(self, lvol_name, lvol_id, snapshot_name,
                                      batch, gateway, operation):
        self.events.append(("final_step", operation, lvol_name, snapshot_name, batch))
        return ["ok"] if self._final_step_ret else None

    def bdev_lvol_add_clone(self, lvol_name, parent):
        self.events.append(("add_clone", self.node_id, lvol_name, parent))
        return ["ok"]

    def nvmf_subsystem_listener_set_ana_state(self, nqn, ip, port, trtype="TCP", ana=None):
        self.events.append(("ana", self.node_id, ana))
        return ["ok"]


class _Node:
    def __init__(self, uuid, events, ip, lvstore, status=StorageNode.STATUS_ONLINE,
                 secondary="", tertiary="", final_step_ret=True):
        self.uuid = uuid
        self._events = events
        self._ip = ip
        self.lvstore = lvstore
        self.status = status
        self.secondary_node_id = secondary
        self.tertiary_node_id = tertiary
        self.active_rdma = False
        self.data_nics = [_Nic(ip)]
        self.mgmt_ip = ip
        self.transfer_hublvol = _Hub()
        self._rpc = _RPC(uuid, events, final_step_ret)

    def get_id(self):
        return self.uuid

    def rpc_client(self):
        return self._rpc

    def get_lvol_subsys_port(self, lvstore):
        return 4420

    def create_transfer_hublvol(self):
        self.transfer_hublvol = _Hub()


class _Lvol:
    uuid = "LV1"
    lvol_bdev = "LVOL_1"
    nqn = "nqn.orig:lvol:LV1"


def _install_nodes(monkeypatch, nodes_by_id):
    monkeypatch.setattr(rfs, "db", type("DB", (), {
        "get_storage_node_by_id": staticmethod(lambda nid: nodes_by_id[nid]),
    })())


def test_flip_ana_failover_ordering(monkeypatch):
    events: list = []
    tgt_sec = _Node("T2", events, "t2", "lvs_tgt")
    tgt = _Node("T1", events, "t1", "lvs_tgt", secondary="T2")
    src = _Node("S1", events, "s1", "lvs_src")
    _install_nodes(monkeypatch, {"T1": tgt, "T2": tgt_sec, "S1": src})

    rfs.flip_ana_failover(src, tgt, "lvs_src", "lvs_tgt", "nqn.orig:lvol:LV1")

    ana = [(nid, state) for (kind, nid, state) in events if kind == "ana"]
    assert ana == [
        ("T1", "optimized"),
        ("T2", "non_optimized"),
        ("S1", "inaccessible"),
    ]


def test_flip_ana_failover_skips_dead_source(monkeypatch):
    events: list = []
    tgt = _Node("T1", events, "t1", "lvs_tgt")
    src = _Node("S1", events, "s1", "lvs_src", status=StorageNode.STATUS_UNREACHABLE)
    _install_nodes(monkeypatch, {"T1": tgt, "S1": src})

    rfs.flip_ana_failover(src, tgt, "lvs_src", "lvs_tgt", "nqn.orig:lvol:LV1")

    ana = [(nid, state) for (kind, nid, state) in events if kind == "ana"]
    # Source unreachable (cluster failure) -> only target paths flipped.
    assert ana == [("T1", "optimized")]


def test_run_cutover_happy_path(monkeypatch):
    events: list = []
    tgt_sec = _Node("T2", events, "t2", "lvs_tgt")
    tgt = _Node("T1", events, "t1", "lvs_tgt", secondary="T2")
    src = _Node("S1", events, "s1", "lvs_src")
    _install_nodes(monkeypatch, {"T1": tgt, "T2": tgt_sec, "S1": src})

    ok, err = rfs.run_cutover(
        src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1", operation="replicate")

    assert ok is True and err is None
    kinds = [e[0] for e in events]
    # final_step happens before add_clone on the peer, before ANA flips.
    assert kinds.index("final_step") < kinds.index("add_clone")
    assert kinds.index("add_clone") < kinds.index("ana")

    final = [e for e in events if e[0] == "final_step"][0]
    assert final[1] == "replicate"
    assert final[3] == "lvs_tgt/SNAP1"
    # add_clone only on the online peer T2.
    clones = [e for e in events if e[0] == "add_clone"]
    assert clones == [("add_clone", "T2", "lvs_tgt/LVOL_1", "lvs_tgt/SNAP1")]


def test_run_cutover_final_step_failure_no_ana(monkeypatch):
    events: list = []
    tgt = _Node("T1", events, "t1", "lvs_tgt")
    # The final-step RPC is issued on the SOURCE (it pushes the delta to target).
    src = _Node("S1", events, "s1", "lvs_src", final_step_ret=False)
    _install_nodes(monkeypatch, {"T1": tgt, "S1": src})

    ok, err = rfs.run_cutover(
        src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1")

    assert ok is False
    assert "final_step" in err or "final" in err.lower()
    # No ANA flip and no add_clone when the final step fails.
    assert not [e for e in events if e[0] == "ana"]
    assert not [e for e in events if e[0] == "add_clone"]
