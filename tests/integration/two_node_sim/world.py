"""The simulated two-node world: two ``SimNode``s, the network, the client, the
REAL arbiter and the control-plane flows around it, on one virtual clock and a
real FoundationDB.

What is real (imported, not modelled): the arbiter (``arbitration.arbiter``,
``decision``, ``events``), its default helpers (leadership restoration,
device check, rebuild-task bookkeeping, the leaderless
reconcile and its probe), the planned-loss marker, the port guards the flows
consult (``_single_peer_jm_is_complete``, ``_two_node_fence_holds_ports``,
``_may_unblock_peer_port``), the ANA helpers, every ``RPCClient`` wrapper
(only ``_request`` is routed), the task records (``tasks_controller``), and
all FDB reads and writes.

What is modelled: the nodes (``simnode``), the network, the client's
multipath IO, the Go event collector, and the outer shell of the flows the
arbiter does not own -- the monitor's status/restart trigger, the restart
runner, the migration runner, the port-allow runner and the monitor's
stale-port remediation. Each modelled flow keeps the real decision points
(the guards above) and stubs only the RPC-heavy work around them; see
``AGENTS.md`` in this directory.
"""

from __future__ import annotations

import errno
import logging
import random
import uuid as _uuid
from concurrent.futures import Future

from simplyblock_core import constants
from simplyblock_core.rpc_client import (
    RPCClient,
    RPCConnectionError,
    RPCHTTPError,
    RPCRemoteError,
)

from .simnode import FENCED, HOLDING, SOLO, NodeAbort, SimNode
from .vclock import VirtualTime

logger = logging.getLogger("two_node_sim")


# ---------------------------------------------------------------------------
# Futures and pools on virtual time
# ---------------------------------------------------------------------------

class VirtualPool:
    """A thread pool whose workers are ``CoopTask``s on the virtual clock: a
    call runs until it blocks on an RPC; its side effects land when the call
    returns, and its future completes then."""

    def __init__(self, world: World, name: str):
        self.world, self.name = world, name

    def submit(self, fn, *args, **kwargs) -> Future:
        fut: Future = Future()

        def done(task):
            if task.exc is not None:
                fut.set_exception(task.exc)
            else:
                fut.set_result(task.result)
        self.world.vt.spawn(lambda: fn(*args, **kwargs), self.name, on_done=done)
        return fut


def make_virtual_wait(world: World):
    """``concurrent.futures.wait`` for the arbiter: the decision thread waits
    on the virtual clock -- other events, the pool calls among them, run
    meanwhile -- until the futures are done or ``timeout`` has passed."""

    def vwait(fs, timeout=None, return_when=None):
        vt = world.vt
        fs = list(fs)
        vt.fold_context()
        end = vt.now + (timeout if timeout is not None else 3600.0)
        vt.run_while(lambda: not all(f.done() for f in fs), end)
        done = {f for f in fs if f.done()}
        return done, set(fs) - done

    return vwait


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

class Net:
    def __init__(self, world: World):
        self.world = world
        self.data_cut = False           # storage link between the two nodes
        self.data_lossy = False         # netem: delay above the JM timeout + loss
        self.cp_cut: set = set()        # node uuids the control plane cannot reach
        self.client_cut: set = set()    # node uuids the client cannot reach
        self.rng = random.Random(world.seed)

    def data_ok(self, a: SimNode, b: SimNode) -> bool:
        if self.data_cut:
            return False
        if self.data_lossy:
            # 1500 ms delay over a 1000 ms JM timeout: acks are always late;
            # an occasional packet still makes it through
            return self.rng.random() < 0.05
        return True

    def cp_ok(self, node: SimNode) -> bool:
        return node.uuid not in self.cp_cut


# ---------------------------------------------------------------------------
# RPC routing (only RPCClient._request is replaced)
# ---------------------------------------------------------------------------

class SimRPCClient(RPCClient):
    world: World = None             # set by World.install

    def __init__(self, host, port, username=None, password=None, timeout=180, retry=3, **_):
        self.host, self.port = host, port
        self.timeout = timeout if timeout is not None else 180
        self.retry = retry

    def _request(self, method: str, *, request_timeout=None, **kwargs):
        timeout = request_timeout if request_timeout is not None else self.timeout
        return type(self).world.call(self.host, int(self.port), method, kwargs, float(timeout))


# ---------------------------------------------------------------------------
# The world
# ---------------------------------------------------------------------------

LVS_PORTS = {"LVS_1": (4424, 4425), "LVS_6": (4426, 4427)}
VUIDS = {"LVS_1": 1, "LVS_6": 6}


class World:
    def __init__(self, db, seed: int = 1, bugs: set | None = None):
        self.db = db
        self.seed = seed
        self.bugs = set(bugs or ())
        self.vt = VirtualTime()
        self.t0 = self.vt.now
        self.net = Net(self)
        self.trace: list[str] = []
        self.violations: list[str] = []
        self.unknown_rpcs: set = set()
        self.reconnect_since: dict = {}
        self.resync_s = 3.0           # journal re-level after a reconnect
        self.nodes: list[SimNode] = []
        self.by_ep: dict = {}
        self.cluster = None
        self.arbiter = None
        self.client = None
        self.cp = None
        self.collector_seq: dict = {}
        self.collector_instance: dict = {}
        self.decision_blocked_ms: list[int] = []
        self.grants: list = []
        self.granted_until_heal: set = set()   # survivors since their grant (F17)
        self.fault_at: float | None = None     # when the first fault hit (timing base)
        self.fences: list = []
        self.ana_changed: dict = {}            # (node, port, group) -> last change
        self.conditions: set = set()          # environment (not bugs): jm_needs_reconnect
        self.jm_reconnected: dict = {}
        # data versions per LVS and node (F10): a write a node acks alone is
        # missing on its peer until the peer's rebuild completes
        self.version: dict = {}
        self.arbiter_down_until = 0.0
        self.takeover_s = 0.073

    # ---- logging -------------------------------------------------------------------

    def rel(self) -> float:
        return self.vt.time() - self.t0

    def log(self, msg: str):
        line = "%8.3f %s" % (self.rel(), msg)
        self.trace.append(line)
        logger.info(line)

    def violate(self, msg: str):
        line = "%8.3f VIOLATION %s" % (self.rel(), msg)
        if line not in self.violations:
            self.violations.append(line)
            self.trace.append(line)

    # ---- topology --------------------------------------------------------------------

    def peer_of(self, node: SimNode) -> SimNode:
        return self.nodes[1] if node is self.nodes[0] else self.nodes[0]

    def node(self, name: str) -> SimNode:
        return next(n for n in self.nodes if n.name == name)

    def sn(self, sim: SimNode):
        """The node's FDB record (fresh)."""
        return self.db.get_storage_node_by_id(sim.uuid)

    # ---- RPC --------------------------------------------------------------------------

    def call(self, host: str, port: int, method: str, params: dict, timeout: float):
        node = self.by_ep.get((host, port))
        if node is None:
            raise RPCConnectionError("no route to %s:%s" % (host, port))
        vt = self.vt
        if not self.net.cp_ok(node) or node.proc == "down":
            vt.charge(timeout)
            raise RPCConnectionError("timed out")
        if node.proc == "frozen":
            # The proxy is up, SPDK does not answer: the call waits its whole timeout.
            vt.charge(timeout)
            raise RPCConnectionError("timed out")
        if node.proc in ("dead", "stopped"):
            vt.charge(0.003)
            raise RPCHTTPError("HTTP 500 from remote", status_code=500)
        if node.proc == "restarting":
            # The proxy runs, the new SPDK is not initialised yet (campaign c2, F3).
            vt.charge(0.003)
            raise RPCRemoteError("Method may only be called after framework is initialized "
                                 "using framework_start_init RPC.", -1)
        vt.charge(0.002)
        if method in ("bdev_lvol_update_lvstore", "bdev_lvol_set_leader_all") and \
                node.uuid in self.granted_until_heal and \
                not self.peer_of(node).up:
            # campaign c7b, F17: nothing moves leadership onto a survivor
            # after its grant while its peer is gone (#1485)
            self.violate("%s sent to the survivor %s after its grant" % (method, node.name))
        try:
            return node.rpc(method, params)
        except NotImplementedError:
            self.unknown_rpcs.add(method)
            raise RPCRemoteError("Method not found: %s" % method, -32601)
        except NodeAbort:
            raise RPCHTTPError("HTTP 500 from remote", status_code=500)

    # ---- hooks from the nodes ------------------------------------------------------------

    def on_node_died(self, node: SimNode):
        if self.cp is not None:
            self.cp.note_death(node)

    def on_fence(self, node: SimNode, lvs):
        self.fences.append((self.vt.time(), node.name, lvs.name))
        self.log("%s: fence %s (port %s %s)" % (node.name, lvs.name, lvs.client_port,
                                                  "blocked" if lvs.client_port in node.blocked else "open"))

    def on_grant(self, node: SimNode, vuids):
        self.log("%s: granted solo %s" % (node.name, vuids))
        self.granted_until_heal.add(node.uuid)
        self.grants.append((self.vt.time(), node.name))
        self.client.note_grant(node)

    def on_leader(self, node: SimNode, lvs, how: str):
        self.log("%s: leader of %s (%s)" % (node.name, lvs.name, how))

    def namespace_groups(self, port: int) -> set:
        lvs = next(k for k, v in LVS_PORTS.items() if v[0] == port)
        return {int(v.ns_id) for v in self.vols.values() if v.lvs_name == lvs}

    def missing(self, node: SimNode, lvs_name: str) -> int:
        vers = self.version.get(lvs_name)
        return max(vers.values()) - vers[node.uuid] if vers else 0

    def note_ack(self, writer: SimNode, lvs_name: str, replicated: bool):
        """A write acked by ``writer``. Alone, it is missing on the peer until
        the peer's rebuild; replicated, both advance (an old gap stays)."""
        vers = self.version.setdefault(lvs_name, {n.uuid: 0 for n in self.nodes})
        peer = self.peer_of(writer)
        if self.missing(writer, lvs_name) and not peer.up:
            # campaign c3/03: the only up-to-date copy is gone and the stale
            # node serves the LVS (EIO / abort on the bed)
            self.violate("%s serves %s while missing writes; %s (the only up-to-date copy) is down"
                         % (writer.name, lvs_name, peer.name))
        vers[writer.uuid] += 1
        if replicated:
            vers[peer.uuid] += 1

    def rebuilt(self, node: SimNode):
        for vers in self.version.values():
            if vers:
                vers[node.uuid] = max(vers.values())

    # ---- install / seed -------------------------------------------------------------------

    def install(self, monkeypatch):
        from simplyblock_core.arbitration import arbiter as arb_mod

        from .vclock import install_virtual_time

        install_virtual_time(monkeypatch, self.vt)
        SimRPCClient.world = self
        for mod in ("simplyblock_core.rpc_client", "simplyblock_core.models.storage_node",
                    "simplyblock_core.storage_node_ops"):
            module = __import__(mod, fromlist=["RPCClient"])
            if hasattr(module, "RPCClient"):
                monkeypatch.setattr(module, "RPCClient", SimRPCClient)
        monkeypatch.setattr(arb_mod, "wait", make_virtual_wait(self))
        # The flag cache in storage_node_ops keys on real monotonic time.
        from simplyblock_core import storage_node_ops as sno
        monkeypatch.setattr(sno, "_ARBITRATION_FLAG", {})

    def seed_db(self):
        from pydantic import SecretStr

        from simplyblock_core.models.cluster import Cluster
        from simplyblock_core.models.iface import IFace
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.models.nvme_device import NVMeDevice
        from simplyblock_core.models.storage_node import StorageNode

        db = self.db
        cl = Cluster()
        cl.uuid = "sim-" + _uuid.uuid4().hex[:12]
        cl.status = Cluster.STATUS_ACTIVE
        cl.ha_type = "ha"
        cl.max_fault_tolerance = 1
        cl.distr_ndcs, cl.distr_npcs = 1, 1
        cl.blk_size, cl.page_size_in_blocks = 4096, 4096
        cl.nqn = "nqn.2023-02.io.simplyblock:" + cl.uuid[:8]
        cl.two_node_arbitration = True
        cl.write_to_db(db.kv_store)
        self.cluster = cl

        specs = [("sn1", "LVS_1", "10.0.0.1"), ("sn2", "LVS_6", "10.0.0.2")]
        recs = []
        for i, (name, lvs, ip) in enumerate(specs):
            n = StorageNode()
            n.uuid = str(_uuid.uuid4())
            n.cluster_id = cl.uuid
            n.status = StorageNode.STATUS_ONLINE
            n.hostname = name
            n.mgmt_ip = "127.0.1.%d" % (i + 1)
            n.rpc_port = 8080
            n.rpc_username, n.rpc_password = "u", SecretStr("p")
            n.api_endpoint = "%s:5000" % n.mgmt_ip
            nic = IFace()
            nic.uuid, nic.if_name, nic.ip4_address = str(_uuid.uuid4()), "eth0", ip
            nic.trtype, nic.net_type = "TCP", "data"
            n.data_nics = [nic]
            n.active_tcp = True
            n.enable_ha_jm = True
            n.jm_vuid = VUIDS[lvs]
            n.lvstore = lvs
            n.lvstore_status = "ready"
            n.lvstore_stack = [{"type": "bdev_distr", "name": "distr_%s_0" % lvs,
                                "params": {"name": "distr_%s_0" % lvs}}]
            n.health_check = True
            n.is_secondary_node = False
            n.lvstore_ports = {k: {"lvol_subsys_port": v[0], "hublvol_port": v[1]}
                               for k, v in LVS_PORTS.items()}
            devs = []
            for d in range(2):
                dev = NVMeDevice()
                dev.uuid = str(_uuid.uuid4())
                dev.cluster_id, dev.node_id = cl.uuid, n.uuid
                dev.status = NVMeDevice.STATUS_ONLINE
                dev.alceml_bdev = "alceml_%s_%d" % (name, d)
                dev.nvme_bdev, dev.pt_bdev = "nvme_%s_%d" % (name, d), "pt_%s_%d" % (name, d)
                dev.size = 100_000_000_000
                devs.append(dev)
            n.nvme_devices = devs
            recs.append(n)
        recs[0].secondary_node_id, recs[1].secondary_node_id = recs[1].uuid, recs[0].uuid
        recs[0].lvstore_stack_secondary, recs[1].lvstore_stack_secondary = recs[1].uuid, recs[0].uuid
        for n in recs:
            n.write_to_db(db.kv_store)

        # One volume per LVS; vol1 (the bed's fio target) lives on LVS_6 / sn2.
        # LVS_6's subsystem carries a second namespace (ANA group 2, F18).
        self.vols = {}
        for owner, lvs, vname, ns in ((recs[0], "LVS_1", "vol0", 1), (recs[1], "LVS_6", "vol1", 1),
                                      (recs[1], "LVS_6", "vol1b", 2)):
            lv = LVol()
            lv.uuid = str(_uuid.uuid4())
            lv.lvol_name = vname
            lv.node_id = owner.uuid
            lv.nodes = [owner.uuid, owner.secondary_node_id]
            lv.lvs_name = lvs
            lv.status = LVol.STATUS_ONLINE
            lv.ha_type = "ha"
            lv.nqn = cl.nqn + ":lvol:" + lvs
            lv.ns_id = ns
            lv.fabric = "tcp"
            lv.size = 1 << 30
            lv.top_bdev = "%s/%s" % (lvs, vname)
            lv.write_to_db(db.kv_store)
            self.vols[vname] = lv

        for i, n in enumerate(recs):
            sim = SimNode(self, n.hostname, n.uuid, n.mgmt_ip, n.rpc_port,
                          n.data_nics[0].ip4_address, int(n.jm_vuid))
            sim.bugs = self.bugs
            self.nodes.append(sim)
            self.by_ep[(n.mgmt_ip, n.rpc_port)] = sim
        for i, n in enumerate(recs):
            sim = self.nodes[i]
            for lvs, (cport, hport) in LVS_PORTS.items():
                from .simnode import Lvstore
                primary = recs[0].uuid if lvs == "LVS_1" else recs[1].uuid
                sim.lvstores[lvs] = Lvstore(lvs, VUIDS[lvs], primary, cport, hport,
                                            leader=(primary == sim.uuid))
                # ANA groups 1-2 carry namespaces (vol0/vol1, vol1b); group 3
                # is a listener group without a namespace (F18)
                for grp in (1, 2, 3):
                    sim.ana[(cport, grp)] = "optimized" if primary == sim.uuid else "non_optimized"
            for dev in n.nvme_devices:
                sim.devices[dev.alceml_bdev] = True
            for v in VUIDS.values():
                from .simnode import LvsHa
                sim.ha[v] = LvsHa(since_ms=self.vt.ms())

    # ---- the arbiter -------------------------------------------------------------------------

    def make_arbiter(self):
        from simplyblock_core.arbitration import arbiter as arb_mod

        world = self

        def run_async(fn):
            def body():
                try:
                    fn()
                except Exception:                   # noqa: BLE001 - a thread would log and die
                    logger.exception("arbiter recovery")
            world.vt.spawn(body, "recovery")

        def link_probe(node, peer):
            a, b = world.by_ep.get((node.mgmt_ip, node.rpc_port)), world.by_ep.get((peer.mgmt_ip, peer.rpc_port))
            if a is None or b is None or not a.up:
                return None
            return world.net.data_ok(a, b) and b.proc in ("up", "frozen")

        def reconnect_jms(node, peer):
            sim = world.by_ep.get((node.mgmt_ip, node.rpc_port))
            if sim is not None and sim.up and world.net.data_ok(sim, world.peer_of(sim)):
                world.jm_reconnected[sim.uuid] = True
                world.log("CP: reconnected remote JMs of %s" % sim.name)

        def readmit(node):
            world.cp.readmit(node)

        self.arbiter = arb_mod.Arbiter(
            self.db, clock=self.vt.ms, run_async=run_async,
            rpc_pool=VirtualPool(self, "rpc"), work_pool=VirtualPool(self, "work"),
            link_probe=link_probe, reconnect_jms=reconnect_jms, readmit=readmit)
        return self.arbiter

    def restart_arbiter(self, down_s: float):
        """The arbiter process restarts: down for ``down_s``, then a fresh
        ``Arbiter`` (its in-memory state is rebuilt from FDB and the nodes)."""
        self.arbiter_down_until = self.vt.time() + down_s
        self.log("CP: arbiter down for %.1f s" % down_s)

        def back():
            self.make_arbiter()
            self.log("CP: arbiter back (new process)")
        self.vt.at(self.arbiter_down_until, back)

    def arbiter_tick(self):
        if self.vt.time() < self.arbiter_down_until:
            self.vt.after(constants.TWO_NODE_DECISION_TICK_MS / 1000.0, self.arbiter_tick)
            return
        with self.vt.context("decision") as ctx:
            self.arbiter.tick()
        blocked = ctx.elapsed
        if blocked > 0.5:
            self.decision_blocked_ms.append(int(blocked * 1000))
            self.log("decision thread blocked %.0f ms" % (blocked * 1000))
        tick = constants.TWO_NODE_DECISION_TICK_MS / 1000.0
        self.vt.after(max(tick, blocked) , self.arbiter_tick)

    def lease_tick(self):
        if self.vt.time() < self.arbiter_down_until:
            self.vt.after(constants.TWO_NODE_LEASE_RENEW_MS / 1000.0, self.lease_tick)
            return
        with self.vt.context("lease") as ctx:
            self.arbiter.renew_all()
        interval = constants.TWO_NODE_LEASE_RENEW_MS / 1000.0
        self.vt.after(max(interval, ctx.elapsed), self.lease_tick)

    # ---- the Go collector ----------------------------------------------------------------------

    def collector_tick(self):
        from simplyblock_core.arbitration import events as ev
        for node in self.nodes:
            if not node.up or not self.net.cp_ok(node):
                continue
            last_inst = self.collector_instance.get(node.uuid)
            if last_inst != node.instance:
                self.collector_instance[node.uuid] = node.instance
                self.collector_seq[node.uuid] = node.seq
                ev.queue_event(self.db, self.cluster.uuid, node.uuid, node.instance, node.seq,
                               {"status": ev.ST_RESYNC, "ha_status": node.ha_status()}, self.vt.ms())
                continue
            after = self.collector_seq.get(node.uuid, 0)
            for e in node.ring:
                if e.seq <= after:
                    continue
                payload = {"status": e.status, "jm_vuid": e.jm_vuid, "reason": e.reason,
                           "ha_state": e.ha_state, "epoch": e.epoch}
                ev.queue_event(self.db, self.cluster.uuid, node.uuid, node.instance, e.seq,
                               payload, self.vt.ms())
                after = e.seq
            self.collector_seq[node.uuid] = after
            node.ring = [e for e in node.ring if e.seq > after]
        self.vt.after(0.02, self.collector_tick)

    # ---- node pollers ------------------------------------------------------------------------

    def node_tick(self):
        t = self.vt.time()
        self.end_grant_window()
        for node in self.nodes:
            if node.proc == "frozen" and t >= node.frozen_until:
                node.proc = "up"
                self.log("%s: wakes after freeze" % node.name)
            node.jc_tick()
        self.vt.after(0.05, self.node_tick)

    # ---- running ----------------------------------------------------------------------------------

    def start(self):
        from .actors import Client, ControlPlane
        self.cp = ControlPlane(self)
        self.client = Client(self)
        self.make_arbiter()
        # configure the nodes the way the arbiter does on its first ticks
        self.vt.after(0.0, self.collector_tick)
        self.vt.after(0.0, self.node_tick)
        self.vt.after(0.01, self.lease_tick)
        self.vt.after(0.02, self.arbiter_tick)
        self.cp.start()
        self.client.start()

    def run(self, seconds: float):
        self.vt.run_until(self.vt.now + seconds)

    def run_until(self, predicate, limit_s: float, step: float = 0.1) -> bool:
        end = self.vt.now + limit_s
        while self.vt.now < end:
            if predicate():
                return True
            self.vt.run_until(self.vt.now + step)
        return predicate()

    # ---- invariants and health ----------------------------------------------------------------------

    def check_invariants(self):
        """Called every 50 ms by the client loop."""
        for lvs in LVS_PORTS:
            writers = [n for n in self.nodes if n.up and n.lvstores[lvs].leader
                       and not n.lvstores[lvs].fenced]
            if len(writers) > 1:
                self.violate("two leaders for %s: %s" % (lvs, [n.name for n in writers]))

    def end_grant_window(self):
        """Both nodes up again: a later leadership move is the failback's."""
        if all(n.up for n in self.nodes):
            self.granted_until_heal.clear()

    def unhealthy_reasons(self, all_groups: bool = False) -> list:
        """Everything that keeps the pair from being healthy, without a manual step."""
        out = []
        rec = self.db.get_cluster_arbitration(self.cluster.uuid)
        if rec is None:
            return ["no arbitration record"]
        if rec.state != "steady":
            out.append("arbiter state %s" % rec.state)
        for n in self.nodes:
            if not n.up:
                out.append("%s not up (%s)" % (n.name, n.proc))
                continue
            for v, lv in n.ha.items():
                if lv.state != "normal":
                    out.append("%s LVS %s %s" % (n.name, v, lv.state))
                if not lv.remote_healthy:
                    out.append("%s remote JM %s unhealthy" % (n.name, v))
            if n.blocked:
                out.append("%s ports blocked %s" % (n.name, sorted(n.blocked)))
            if not all(n.devices.values()):
                out.append("%s devices missing" % n.name)
        for lvs in LVS_PORTS:
            leaders = [n.name for n in self.nodes if n.up and n.lvstores[lvs].leader]
            primary = next(n for n in self.nodes if n.lvstores[lvs].primary_node == n.uuid)
            if leaders != [primary.name]:
                out.append("%s leaders %s (primary %s)" % (lvs, leaders, primary.name))
            cport = LVS_PORTS[lvs][0]
            for n in self.nodes:
                for (port, grp), st in n.ana.items():
                    if port != cport:
                        continue
                    if not all_groups and grp not in self.namespace_groups(port):
                        continue
                    want = "optimized" if n is primary else "non_optimized"
                    if st != want:
                        out.append("%s ANA %s/%s %s" % (n.name, port, grp, st))
        for t in self.db.get_job_tasks(self.cluster.uuid):
            if t.status != "done" and not t.canceled:
                out.append("open task %s on %s: %s" % (t.function_name, t.node_id[:8],
                                                       str(t.function_result)[:60]))
        if rec.rebuild:
            out.append("rebuild pending %s" % sorted(rec.rebuild))
        return out

    def mark_fault(self):
        if self.fault_at is None:
            self.fault_at = self.vt.time()

    # used by scenario code ------------------------------------------------------

    def errno_name(self, code: int) -> str:
        return errno.errorcode.get(abs(code), str(code))


__all__ = ["FENCED", "HOLDING", "SOLO", "World"]
