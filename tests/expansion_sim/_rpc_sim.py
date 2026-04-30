# coding=utf-8
"""SPDK RPC server simulator framework.

Per-storage-node simulator (:class:`RpcServerSim`) that tracks bdev /
subsystem / nvme-controller / firewall state and responds to the RPC calls
that ``recreate_lvstore_on_sec``, ``create_lvstore``,
``teardown_non_leader_lvstore``, and ``reattach_sibling_failover`` make.

Phase 1 scope (current)
-----------------------
* All RPCs accept and return success.
* State is recorded but cross-server invariants are NOT enforced (e.g.,
  attaching to a peer's hublvol does not check whether the peer actually
  exposes that subsystem with a listener).
* No failure injection.

Phase 2 will encode preconditions and cross-server validation. Phase 3
will add deterministic failure injection. Keeping phase 1 permissive
unblocks the happy-path test that proves the FDB + RPC-patching wiring
works end-to-end.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State containers
# ---------------------------------------------------------------------------


class Lifecycle:
    OFFLINE = "offline"
    STARTING = "starting"
    FRAMEWORK_INIT = "framework_init"
    READY = "ready"
    STOPPING = "stopping"


@dataclass
class BdevSim:
    name: str
    type: str  # bdev_distr | bdev_raid | bdev_lvstore | bdev_alceml |
               # bdev_ptnonexcl | bdev_nvme | hublvol | jm_device
    params: dict = field(default_factory=dict)
    examined: bool = False


@dataclass
class SubsystemSim:
    nqn: str
    serial_number: str
    model_number: str
    min_cntlid: int
    listeners: List[Tuple[str, int, str]] = field(default_factory=list)
    # (traddr, trsvcid, ana_state)
    allowed_hosts: List[str] = field(default_factory=list)
    namespaces: List[str] = field(default_factory=list)


@dataclass
class NvmeControllerSim:
    name: str
    nqn: str
    paths: List[Tuple[str, int, str]] = field(default_factory=list)
    # (traddr, trsvcid, trtype)


@dataclass
class FirewallEntry:
    port: int
    proto: str
    blocked: bool


# ---------------------------------------------------------------------------
# Per-node simulator
# ---------------------------------------------------------------------------


class RpcServerSim:
    """One simulated SPDK process. Tracks state and answers RPC calls.

    Attached to a :class:`ClusterSim` for peer resolution.
    """

    def __init__(self, node_id: str, mgmt_ip: str, rpc_port: int):
        self.node_id = node_id
        self.mgmt_ip = mgmt_ip
        self.rpc_port = rpc_port
        self.lifecycle = Lifecycle.READY  # phase 1: nodes start ready
        self.bdevs: Dict[str, BdevSim] = {}
        self.subsystems: Dict[str, SubsystemSim] = {}
        self.controllers: Dict[str, NvmeControllerSim] = {}
        self.firewall: Dict[Tuple[int, str], bool] = {}
        # call_log captures every dispatched RPC for assertions
        self.call_log: List[Tuple[str, dict]] = []
        # JM membership view: which JMs (by remote_jm_<id>n1 key) the JC
        # currently sees. Phase 1 just defaults to all known peers True.
        self.jm_status: Dict[int, Dict[str, bool]] = defaultdict(dict)
        self.cluster_sim: Optional["ClusterSim"] = None

    # -- Bdev operations ----------------------------------------------------

    def bdev_distrib_create(self, **params):
        name = params.get("name") or params.get("distrib_name") or f"distr_{len(self.bdevs)}"
        self.bdevs[name] = BdevSim(name=name, type="bdev_distr", params=dict(params))
        return name

    def bdev_distrib_delete(self, name, **_):
        self.bdevs.pop(name, None)
        return True

    def bdev_distrib_check_inflight_io(self, jm_vuid, **_):
        return False  # never inflight in sim

    def bdev_distrib_force_to_non_leader(self, jm_vuid, **_):
        return True

    def bdev_raid_create(self, name, distribs_list=None, strip_size_kb=None, **_):
        self.bdevs[name] = BdevSim(name=name, type="bdev_raid",
                                    params={"distribs_list": distribs_list,
                                            "strip_size_kb": strip_size_kb})
        return True

    def bdev_raid_delete(self, name, **_):
        self.bdevs.pop(name, None)
        return True

    def bdev_alceml_create(self, name, nvme_bdev=None, uuid=None, **_):
        self.bdevs[name] = BdevSim(name=name, type="bdev_alceml",
                                    params={"nvme_bdev": nvme_bdev, "uuid": uuid})
        return name

    def bdev_PT_NoExcl_create(self, **params):
        name = params.get("name") or f"pt_{len(self.bdevs)}"
        self.bdevs[name] = BdevSim(name=name, type="bdev_ptnonexcl",
                                    params=dict(params))
        return True

    def bdev_PT_NoExcl_delete(self, name, **_):
        self.bdevs.pop(name, None)
        return True

    def create_lvstore(self, **params):
        name = params.get("lvs_name") or params.get("name") or "LVS"
        self.bdevs[name] = BdevSim(name=name, type="bdev_lvstore",
                                    params=dict(params))
        return True

    def bdev_lvol_delete_lvstore(self, name, **_):
        self.bdevs.pop(name, None)
        return True

    def bdev_lvol_get_lvstores(self, name=None, **_):
        if name is None:
            return [{"name": b.name, "lvs leadership": True}
                    for b in self.bdevs.values() if b.type == "bdev_lvstore"]
        if name in self.bdevs and self.bdevs[name].type == "bdev_lvstore":
            return [{"name": name, "lvs leadership": True}]
        return []

    def bdev_lvol_set_lvs_opts(self, *args, **_):
        return True

    def bdev_lvol_set_leader(self, *args, **_):
        return True

    def bdev_lvol_connect_hublvol(self, *args, **_):
        return True

    def bdev_examine(self, name=None, **_):
        if name and name in self.bdevs:
            self.bdevs[name].examined = True
        return True

    def bdev_wait_for_examine(self, **_):
        for b in self.bdevs.values():
            b.examined = True
        return True

    def get_bdevs(self, name=None, **_):
        if name is None:
            return [{"name": b.name, "aliases": []} for b in self.bdevs.values()]
        if name in self.bdevs:
            return [{"name": name, "aliases": []}]
        return []

    # -- Subsystem operations -----------------------------------------------

    def subsystem_create(self, nqn, serial_number, model_number,
                         min_cntlid=1, max_namespaces=32,
                         allow_any_host=True, **_):
        self.subsystems[nqn] = SubsystemSim(
            nqn=nqn, serial_number=serial_number,
            model_number=model_number, min_cntlid=min_cntlid)
        return True

    def subsystem_delete(self, nqn, **_):
        self.subsystems.pop(nqn, None)
        return True

    def listeners_create(self, nqn, trtype, traddr, trsvcid,
                         ana_state=None, **_):
        sub = self.subsystems.get(nqn)
        if sub is None:
            return False
        sub.listeners.append((traddr, int(trsvcid), ana_state or "optimized"))
        return True

    def nvmf_subsystem_add_listener(self, nqn, trtype, ip, port,
                                    ana_state=None, **_):
        return self.listeners_create(nqn, trtype, ip, port, ana_state=ana_state)

    # -- NVMe controller (cross-node) ---------------------------------------

    def bdev_nvme_attach_controller(self, name, nqn, traddr, trsvcid,
                                    trtype, multipath=False, **_):
        ctrl = self.controllers.get(name)
        if ctrl is None:
            ctrl = NvmeControllerSim(name=name, nqn=nqn)
            self.controllers[name] = ctrl
            # Register the bdev too — many tests assume nvme controllers
            # also register an associated bdev with a 'n1' suffix.
            self.bdevs[f"{name}n1"] = BdevSim(
                name=f"{name}n1", type="bdev_nvme",
                params={"nqn": nqn})
        ctrl.paths.append((traddr, int(trsvcid), trtype))
        return True

    def bdev_nvme_detach_controller(self, name, **_):
        ctrl = self.controllers.pop(name, None)
        if ctrl is not None:
            self.bdevs.pop(f"{name}n1", None)
        return True

    def bdev_nvme_remove_trid(self, name, traddr, trsvcid, trtype="TCP", **_):
        ctrl = self.controllers.get(name)
        if ctrl is None:
            return False
        before = len(ctrl.paths)
        ctrl.paths = [p for p in ctrl.paths
                      if not (p[0] == traddr and p[1] == int(trsvcid))]
        return len(ctrl.paths) < before

    # -- JC / journal -------------------------------------------------------

    def jc_get_jm_status(self, jm_vuid, **_):
        # Phase 1: report all known JMs as up. Cross-node truth wired in
        # phase 2 — for now the orchestrator doesn't gate on this anyway
        # (we removed the JC quorum poll in step 6 design).
        return {f"remote_jm_{nid}n1": True
                for nid in (self.cluster_sim.node_ids() if self.cluster_sim else [])
                if nid != self.node_id}

    def jc_compression_get_status(self, jm_vuid, **_):
        return False

    def jc_suspend_compression(self, jm_vuid=None, suspend=False, **_):
        return (True, None)

    def keyring_file_add_key(self, *_, **__):
        return True

    # -- Misc ---------------------------------------------------------------

    def bdev_lvol_create(self, *_, **__):
        return True

    def bdev_lvol_remove_from_group(self, *_, **__):
        return True

    def alceml_set_qos_weights(self, *_, **__):
        return True

    def __getattr__(self, attr):
        """Fallback for any RPC the simulator does not yet model. Logs the
        call, records it, and returns True. Phase 2 will tighten this to
        raise — but during phase 1 it lets us discover which RPCs the
        expansion path actually exercises without playing whack-a-mole."""
        if attr.startswith("_"):
            raise AttributeError(attr)

        def _fallback(*args, **kwargs):
            self.call_log.append((attr, {"args": args, "kwargs": kwargs}))
            logger.debug("RpcServerSim[%s] unmodelled RPC %s args=%s kwargs=%s",
                         self.node_id, attr, args, kwargs)
            return True

        return _fallback


# ---------------------------------------------------------------------------
# Cluster-level simulator + RPC router
# ---------------------------------------------------------------------------


class ClusterSim:
    """Owns all per-node :class:`RpcServerSim` instances. Used by
    :class:`RpcRouter` to resolve (mgmt_ip, rpc_port) → node sim, and by
    cross-node validation logic."""

    def __init__(self):
        self.servers: Dict[str, RpcServerSim] = {}  # node_id -> sim
        self._by_endpoint: Dict[Tuple[str, int], RpcServerSim] = {}

    def add_server(self, sim: RpcServerSim) -> None:
        self.servers[sim.node_id] = sim
        self._by_endpoint[(sim.mgmt_ip, sim.rpc_port)] = sim
        sim.cluster_sim = self

    def get_by_endpoint(self, mgmt_ip: str, rpc_port: int) -> Optional[RpcServerSim]:
        return self._by_endpoint.get((mgmt_ip, int(rpc_port)))

    def get_by_node_id(self, node_id: str) -> Optional[RpcServerSim]:
        return self.servers.get(node_id)

    def node_ids(self) -> List[str]:
        return list(self.servers.keys())


class RpcRouter:
    """Duck-typed replacement for :class:`simplyblock_core.rpc_client.RPCClient`.

    Constructed with ``(mgmt_ip, rpc_port, username, password, ...)``; routes
    every method call to the :class:`RpcServerSim` registered for that
    endpoint in the active :class:`ClusterSim`. The cluster sim is
    installed via :func:`set_active_cluster_sim` (per-test session).
    """

    _active_cluster: Optional[ClusterSim] = None

    @classmethod
    def set_active_cluster(cls, sim: Optional[ClusterSim]) -> None:
        cls._active_cluster = sim

    def __init__(self, mgmt_ip: str, rpc_port: int,
                 username: str = "", password: str = "",
                 timeout=None, retry=None, **_kwargs):
        self.mgmt_ip = mgmt_ip
        self.rpc_port = int(rpc_port)
        self.username = username
        self.password = password

    def _server(self) -> RpcServerSim:
        active = type(self)._active_cluster
        if active is None:
            raise RuntimeError(
                "RpcRouter: no active ClusterSim — install one via "
                "RpcRouter.set_active_cluster(sim) at test setup")
        srv = active.get_by_endpoint(self.mgmt_ip, self.rpc_port)
        if srv is None:
            raise RuntimeError(
                f"RpcRouter: no simulator registered for "
                f"{self.mgmt_ip}:{self.rpc_port}")
        return srv

    def __getattr__(self, attr):
        if attr.startswith("_"):
            raise AttributeError(attr)

        def _delegated(*args, **kwargs):
            srv = self._server()
            srv.call_log.append((attr, {"args": args, "kwargs": kwargs}))
            method = getattr(srv, attr)
            return method(*args, **kwargs)

        return _delegated


def install_rpc_router(modules: List[str]) -> List[Tuple[Any, str, Any]]:
    """Replace ``RPCClient`` with :class:`RpcRouter` in every named module.

    Returns the original (module, attr, value) tuples so callers can
    restore them at teardown.
    """
    import importlib
    saved = []
    for mod_name in modules:
        mod = importlib.import_module(mod_name)
        if hasattr(mod, "RPCClient"):
            saved.append((mod, "RPCClient", mod.RPCClient))
            mod.RPCClient = RpcRouter
    return saved


def restore_rpc_router(saved: List[Tuple[Any, str, Any]]) -> None:
    for mod, attr, value in saved:
        setattr(mod, attr, value)


# ---------------------------------------------------------------------------
# Firewall client stub
# ---------------------------------------------------------------------------


class FirewallClientSim:
    """In-process replacement for
    :class:`simplyblock_core.fw_api_client.FirewallClient`.

    The real client makes HTTP requests to a per-storage-node firewall API
    on port 5001. In simulation we route the same intent — set/unset a
    port block — into the target :class:`RpcServerSim`'s ``firewall``
    dict, so cross-node validation logic (phase 2) can read it back.

    Constructed with ``(snode, timeout=, retry=)`` to match the production
    signature.
    """

    _active_cluster: Optional[ClusterSim] = None

    @classmethod
    def set_active_cluster(cls, sim: Optional[ClusterSim]) -> None:
        cls._active_cluster = sim

    def __init__(self, snode, timeout=None, retry=None):
        self.snode = snode

    def _server(self) -> Optional[RpcServerSim]:
        active = type(self)._active_cluster
        if active is None:
            return None
        return active.get_by_endpoint(self.snode.mgmt_ip, self.snode.rpc_port)

    def firewall_set_port(self, port_id, port_type="tcp",
                          action="block", rpc_port=None,
                          is_reject=False, **_):
        srv = self._server()
        if srv is None:
            return True  # silently noop in tests not using cluster sim
        key = (int(port_id), port_type)
        srv.firewall[key] = (action == "block")
        srv.call_log.append(("firewall_set_port",
                              {"port": port_id, "proto": port_type,
                               "action": action}))
        return True

    def firewall_get_status(self, *_, **__):
        return {"status": "ok"}


def install_firewall_stub(modules: List[str]) -> List[Tuple[Any, str, Any]]:
    """Replace ``FirewallClient`` with :class:`FirewallClientSim` in every
    named module. Symmetric with :func:`install_rpc_router`."""
    import importlib
    saved = []
    for mod_name in modules:
        mod = importlib.import_module(mod_name)
        if hasattr(mod, "FirewallClient"):
            saved.append((mod, "FirewallClient", mod.FirewallClient))
            mod.FirewallClient = FirewallClientSim
    return saved


def restore_firewall_stub(saved: List[Tuple[Any, str, Any]]) -> None:
    for mod, attr, value in saved:
        setattr(mod, attr, value)
