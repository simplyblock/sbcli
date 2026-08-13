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
        self.final_step_gateways = []

    # hub attach fast-path: pretend the remote bdev already exists
    def get_bdevs(self, name=None):
        return [{"name": name or "x"}]

    def bdev_nvme_attach_controller(self, *a, **k):
        return ["ok"]

    def bdev_nvme_controller_list(self, *a, **k):
        return []

    def bdev_lvol_transfer_final_step(self, lvol_name, lvol_id, snapshot_name,
                                      batch, gateway, operation):
        # `gateway` is recorded separately: it was omitted from the event tuple,
        # which is how the controller-name-instead-of-bdev bug went unnoticed.
        self.events.append(("final_step", operation, lvol_name, snapshot_name, batch))
        self.final_step_gateways.append(gateway)
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


def test_fence_source_paths_peers_first(monkeypatch):
    """All source paths inaccessible BEFORE the freeze, peers before primary:
    once the final delta is taken the source must not accept IO by any means —
    a write on a still-optimized source path after the delta of record is
    silently lost, and target-live-before-source-dark is dual-writable."""
    events: list = []
    src_sec = _Node("S2", events, "s2", "lvs_src")
    src = _Node("S1", events, "s1", "lvs_src", secondary="S2")
    _install_nodes(monkeypatch, {"S1": src, "S2": src_sec})

    rfs.fence_source_paths(src, "lvs_src", "nqn.orig:lvol:LV1")

    ana = [(nid, state) for (kind, nid, state) in events if kind == "ana"]
    assert ana == [
        ("S2", "inaccessible"),   # peers first
        ("S1", "inaccessible"),   # primary last
    ]


def test_fence_skips_dead_source(monkeypatch):
    events: list = []
    src = _Node("S1", events, "s1", "lvs_src", status=StorageNode.STATUS_UNREACHABLE)
    _install_nodes(monkeypatch, {"S1": src})
    rfs.fence_source_paths(src, "lvs_src", "nqn.orig:lvol:LV1")
    assert [e for e in events if e[0] == "ana"] == []


def test_enable_target_paths(monkeypatch):
    events: list = []
    tgt_sec = _Node("T2", events, "t2", "lvs_tgt")
    tgt = _Node("T1", events, "t1", "lvs_tgt", secondary="T2")
    _install_nodes(monkeypatch, {"T1": tgt, "T2": tgt_sec})

    rfs.enable_target_paths(tgt, "lvs_tgt", "nqn.orig:lvol:LV1")

    ana = [(nid, state) for (kind, nid, state) in events if kind == "ana"]
    assert ana == [("T1", "optimized"), ("T2", "non_optimized")]


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
    # Source fenced (first ana) BEFORE the freeze/final_step; target enabled
    # (last ana) only after the peer add_clone.
    first_ana = kinds.index("ana")
    last_ana = len(kinds) - 1 - kinds[::-1].index("ana")
    assert first_ana < kinds.index("final_step"), "source not fenced before freeze"
    assert events[first_ana][2] == "inaccessible"
    assert kinds.index("final_step") < kinds.index("add_clone")
    assert kinds.index("add_clone") < last_ana
    assert events[last_ana][2] in ("optimized", "non_optimized")

    final = [e for e in events if e[0] == "final_step"][0]
    assert final[1] == "replicate"
    assert final[3] == "lvs_tgt/SNAP1"
    # add_clone only on the online peer T2.
    clones = [e for e in events if e[0] == "add_clone"]
    assert clones == [("add_clone", "T2", "lvs_tgt/LVOL_1", "lvs_tgt/SNAP1")]


def test_run_cutover_gateway_is_the_attached_bdev_not_the_controller(monkeypatch):
    """The final-step gateway must be the namespace bdev, not the controller name.

    Regression: run_cutover passed ensure_hub_attached()'s FIRST return value
    (the controller name handed to bdev_nvme_attach_controller, e.g.
    "LVS_13/transferhub") instead of the SECOND (the attached bdev,
    "LVS_13/transferhubn1"). Only the latter exists as a bdev, so SPDK answered
    every call with ENODEV (-19): 80/80 failures in the lab, and every volume
    stayed in cutover_pending while snapshot replication kept working.
    """
    events: list = []
    tgt = _Node("T1", events, "t1", "lvs_tgt")
    src = _Node("S1", events, "s1", "lvs_src")
    _install_nodes(monkeypatch, {"T1": tgt, "S1": src})

    ok, err = rfs.run_cutover(
        src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1", operation="replicate")

    assert ok is True and err is None
    hub = tgt.transfer_hublvol
    assert src.rpc_client().final_step_gateways == [hub.get_remote_bdev_name()]
    # And specifically NOT the controller name.
    assert hub.bdev_name not in src.rpc_client().final_step_gateways


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
    # The source fence runs BEFORE the freeze (by design), but the target must
    # never be enabled and no add_clone may run when the final step fails.
    non_fence = [e for e in events if e[0] == "ana"
                 and e[2] not in ("inaccessible", "optimized", "non_optimized")]
    assert not non_fence
    # the TARGET must never be enabled on failure
    assert not [e for e in events if e[0] == "ana" and e[1].startswith("T")
                and e[2] in ("optimized", "non_optimized")]
    assert not [e for e in events if e[0] == "add_clone"]


def test_final_step_failure_unfences_the_source(monkeypatch):
    """If the freeze fails after the source was fenced, the source must be
    restored (it is still the authoritative copy) — never left dark."""
    events: list = []
    tgt = _Node("T1", events, "t1", "lvs_tgt")
    src = _Node("S1", events, "s1", "lvs_src", final_step_ret=False)
    _install_nodes(monkeypatch, {"T1": tgt, "S1": src})

    ok, _err = rfs.run_cutover(src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1")

    assert ok is False
    ana = [(e[1], e[2]) for e in events if e[0] == "ana"]
    assert ana[0] == ("S1", "inaccessible"), "fence before freeze"
    assert ana[-1] == ("S1", "optimized"), "source restored after failed freeze"
