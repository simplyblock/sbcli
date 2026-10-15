"""The control-plane flows around the arbiter, and the client.

Each flow keeps the decision points of the real code -- the same guard
functions, imported and called on the real FDB records -- and models only the
RPC-heavy work around them (device reconnects, hublvol checks, bdev stacks):

* restart runner  (``tasks_runner_restart`` + ``storage_node_ops.restart``):
  restart claim, planned-loss marker for a live node, SPDK restart, peer port
  unblock through ``_may_unblock_peer_port``, data rebuild queued
  (``tasks_controller.add_device_mig_task_for_node``), port-allow task;
* port-allow runner (``tasks_runner_port_allow.exec_port_allow_task``): the
  remote-JM gate with ``_single_peer_jm_is_complete`` (F14), the fence guard
  ``_two_node_fence_holds_ports`` (F15), the client-port unblock, the
  leadership failback to the primary;
* monitor (``storage_node_monitor``): offline detection and auto-restart
  (``tasks_controller.add_node_to_auto_restart``), ANA failover
  (``_failover_primary_ana``), stale-port remediation through
  ``_may_unblock_peer_port``;
* migration runner: completes the data rebuild once its destination devices
  exist and the LVS has a leader; otherwise reports the bed's ``mig error``
  bits (DST_NOT_AVAILABLE = 1<<3) or "not the leader";
* shutdown flow (``sn shutdown``): planned-loss marker, ANA failover, SPDK stop.
"""

from __future__ import annotations

import logging

from simplyblock_core import constants

from .simnode import FENCED, HOLDING, SOLO, SimNode

logger = logging.getLogger("two_node_sim")

DST_NOT_AVAILABLE = 1 << 3
RESTART_S = 7.0
FAST_IO_FAIL_S = 8.0
#: Outstanding writes per volume. fio on the bed ran several jobs, so new
#: writes kept being submitted while some sat on a frozen path; with a small
#: depth every slot pins to the frozen node and no write reaches the newly
#: optimized path (the bed measured 2-3 s, not KATO, in 02b).
DEPTH = 256
CLIENT_KATO_S = float(constants.TWO_NODE_CLIENT_KEEP_ALIVE_TO)
#: keep-alive of the node-to-node NVMe controllers (hublvol, remote JM)
HUBLVOL_KATO_S = float(constants.KATO) / 1000.0


class ControlPlane:
    def __init__(self, world):
        self.world = world
        self.dead_since: dict = {}
        self.restarting: dict = {}          # node uuid -> restart finish time
        self.block_since: dict = {}         # (node uuid, port) -> first seen blocked
        self.mig_ready_since: dict = {}
        self.last_data_cut = False
        self.port_allow_runs: list = []
        self.mig_s = 5.0                        # rebuild time once it can run

    @property
    def db(self):
        return self.world.db

    def start(self):
        vt = self.world.vt
        vt.after(1.0, self.monitor_tick)
        vt.after(2.0, self.port_allow_tick)
        vt.after(3.0, self.migration_tick)
        vt.after(0.5, self.restart_tick)

    def note_death(self, node: SimNode):
        self.dead_since.setdefault(node.uuid, self.world.vt.time())

    def _tasks(self, kinds):
        from simplyblock_core.models.job_schedule import JobSchedule
        return [t for t in self.db.get_job_tasks(self.world.cluster.uuid)
                if t.function_name in kinds and t.status != JobSchedule.STATUS_DONE
                and not t.canceled]

    def _finish(self, task, result="done"):
        from simplyblock_core.models.job_schedule import JobSchedule

        def put(t):
            t.status = JobSchedule.STATUS_DONE
            t.function_result = result
        self.db.atomic_update(task, put) if hasattr(self.db, "atomic_update") else None

    def _result(self, task, result):
        def put(t):
            t.function_result = result
            t.retry = int(getattr(t, "retry", 0) or 0) + 1
        self.db.atomic_update(task, put)

    def _set_status(self, node: SimNode, status: str):
        from simplyblock_core import storage_node_ops as sno
        if self.world.sn(node).status != status:
            sno.set_node_status(node.uuid, status, caused_by="sim")
            self.world.log("CP: %s -> %s" % (node.name, status))

    # ---- monitor ------------------------------------------------------------------

    def monitor_tick(self):
        from simplyblock_core import storage_node_ops as sno
        from simplyblock_core.controllers import tasks_controller
        from simplyblock_core.models.storage_node import StorageNode
        w = self.world
        now = w.vt.time()
        for node in w.nodes:
            rec = w.sn(node)
            if node.proc in ("dead", "down", "stopped") and node.uuid not in self.restarting:
                since = self.dead_since.setdefault(node.uuid, now)
                if now - since >= 2.0 and rec.status == StorageNode.STATUS_ONLINE:
                    self._set_status(node, StorageNode.STATUS_OFFLINE)
                    sno.trigger_ana_failover_for_node(w.sn(node))
                if now - since >= 2.0 and node.proc == "dead" and \
                        w.sn(node).status == StorageNode.STATUS_OFFLINE:
                    if not self._tasks({"node_restart"}):
                        tasks_controller.add_node_to_auto_restart(w.sn(node))
            elif node.up:
                self.dead_since.pop(node.uuid, None)
        # partition / both links healed: the node checks create port-allow tasks
        cut = w.net.data_cut or w.net.data_lossy
        if self.last_data_cut and not cut:
            for node in w.nodes:
                if node.up:
                    for port in sorted({lvs.client_port for lvs in node.lvstores.values()}):
                        tasks_controller.add_port_allow_task(w.cluster.uuid, node.uuid, port)
        self.last_data_cut = cut
        self.remediate_stale_ports()
        w.vt.after(1.0, self.monitor_tick)

    def remediate_stale_ports(self):
        """storage_node_monitor._remediate_stale_port_blocks: a client port
        blocked for more than 12 s on an ONLINE node is lifted, unless the
        two-node guard keeps it."""
        from simplyblock_core import storage_node_ops as sno
        from simplyblock_core.models.storage_node import StorageNode
        w = self.world
        now = w.vt.time()
        for node in w.nodes:
            if not node.up:
                continue
            for port in list(node.blocked):
                first = self.block_since.setdefault((node.uuid, port), now)
                if now - first < 12.0:
                    continue
                rec = w.sn(node)
                if rec.status != StorageNode.STATUS_ONLINE:
                    continue
                if self._tasks({"node_restart"}):
                    continue
                lvs = next((s for s in node.lvstores.values() if s.client_port == port), None)
                vuid = lvs.vuid if lvs else 0
                if sno._may_unblock_peer_port(rec, port, vuid, self.db, "stale port block"):
                    rec.rpc_client(timeout=3, retry=0).nvmf_port_unblock(port)
                    w.log("CP monitor: lifted stale block %s on %s" % (port, node.name))
                    if lvs is not None and lvs.fenced:
                        w.violate("monitor lifted fenced port %s on %s" % (port, node.name))
            for key in [k for k in self.block_since if k[0] == node.uuid and k[1] not in node.blocked]:
                self.block_since.pop(key, None)

    # ---- restart -------------------------------------------------------------------

    def readmit(self, node_rec):
        """arbiter default_readmit: OFFLINE + auto-restart."""
        from simplyblock_core import storage_node_ops as sno
        from simplyblock_core.controllers import tasks_controller
        from simplyblock_core.models.storage_node import StorageNode
        sno.set_node_status(node_rec.get_id(), StorageNode.STATUS_OFFLINE, caused_by="two-node-arbiter")
        tasks_controller.add_node_to_auto_restart(self.db.get_storage_node_by_id(node_rec.get_id()))

    def request_restart(self, node: SimNode):
        """``sbctl sn restart``."""
        from simplyblock_core.controllers import tasks_controller
        from simplyblock_core.models.job_schedule import JobSchedule
        tasks_controller._add_task(JobSchedule.FN_NODE_RESTART, self.world.cluster.uuid, node.uuid, "")

    def restart_tick(self):
        from simplyblock_core import storage_node_ops as sno
        from simplyblock_core.arbitration.planned import note_planned_loss
        from simplyblock_core.controllers import tasks_controller
        from simplyblock_core.models.storage_node import StorageNode
        w = self.world
        now = w.vt.time()
        for task in self._tasks({"node_restart"}):
            node = next(n for n in w.nodes if n.uuid == task.node_id)
            if node.proc == "down":                 # host still rebooting
                continue
            fin = self.restarting.get(node.uuid)
            if fin is None:
                if node.up:
                    # restart of a live node: announce, then stop SPDK (F3);
                    # the proxy stays up and answers "framework not initialized"
                    note_planned_loss(w.sn(node), "restart", db=self.db)
                    node.proc = "restarting"
                    node.shutting_down = True
                    w.peer_of(node).detach_until = now + 2.0
                self._set_status(node, StorageNode.STATUS_RESTARTING)
                self.restarting[node.uuid] = now + RESTART_S
                w.log("CP: restarting %s" % node.name)
                continue
            if now < fin:
                continue
            node.start()
            peer = w.peer_of(node)
            # the leader re-levels the returning journal at once (B1 auto-resync)
            peer.relevel_until = now + 3.0
            # restart precheck on the LVS leader (campaign c2, F1)
            own = next(v for v in node.lvstores.values() if v.primary_node == node.uuid)
            leader = next((n for n in w.nodes if n.up and n.lvstores[own.name].leader), None)
            if leader is not None:
                try:
                    with w.vt.context("restart-precheck"):
                        sno._jc_compression_status_with_retry(w.sn(leader), own.vuid)
                except Exception as e:              # noqa: BLE001 - the restart aborts
                    w.log("CP: restart of %s aborted: %s" % (node.name, e))
                    node.abort("restart aborted: %s" % e)
                    self.restarting.pop(node.uuid, None)
                    self._finish(task, "aborted")
                    continue
            self.restarting.pop(node.uuid, None)
            self.dead_since.pop(node.uuid, None)
            self._set_status(node, StorageNode.STATUS_ONLINE)
            w.log("CP: %s restarted (instance %s)" % (node.name, node.instance))
            tasks_controller.add_device_mig_task_for_node(node.uuid)
            peer = w.peer_of(node)
            prec = w.sn(peer)
            for lvs in peer.lvstores.values():
                port = lvs.client_port
                if port in peer.blocked and peer.blocked[port] == "cp":
                    if sno._may_unblock_peer_port(prec, port, lvs.vuid, self.db, "restart done",
                                                  rebuild_too=False):
                        prec.rpc_client(timeout=3, retry=0).nvmf_port_unblock(port)
            for port in sorted({lvs.client_port for lvs in node.lvstores.values()}):
                tasks_controller.add_port_allow_task(w.cluster.uuid, node.uuid, port)
            self._finish(task)
        w.vt.after(0.5, self.restart_tick)

    # ---- shutdown --------------------------------------------------------------------

    def shutdown(self, node: SimNode):
        """``sbctl sn shutdown``: announce, ANA failover, stop SPDK."""
        from simplyblock_core import storage_node_ops as sno
        from simplyblock_core.arbitration.planned import note_planned_loss
        from simplyblock_core.models.storage_node import StorageNode
        w = self.world
        note_planned_loss(w.sn(node), "shutdown", db=self.db)
        self._set_status(node, StorageNode.STATUS_IN_SHUTDOWN)
        node.shutting_down = True
        w.peer_of(node).detach_until = w.vt.time() + 2.0
        node.proc = "stopped"
        w.log("CP: %s SPDK stopped (shutdown)" % node.name)
        self._set_status(node, StorageNode.STATUS_OFFLINE)
        sno._failover_primary_ana(w.sn(node))

    # ---- port allow --------------------------------------------------------------------

    def port_allow_tick(self):
        from simplyblock_core.services import tasks_runner_port_allow as pa
        w = self.world
        for task in self._tasks({"port_allow"}):
            node = next(n for n in w.nodes if n.uuid == task.node_id)
            rec = w.sn(node)
            if not node.up or rec.status != "online":
                self._result(task, "Node is %s, retry task" % rec.status)
                continue
            peer = w.peer_of(node)
            remote = ["remote_jm_" + peer.uuid] if (peer.up and w.net.data_ok(node, peer)) else []
            if not remote or (len(remote) < 2 and not pa._single_peer_jm_is_complete(self.db, rec, remote)):
                self._result(task, "Node unable to connect to remote JMs, retry task")
                continue
            keep = pa._two_node_fence_holds_ports(rec)
            if keep:
                self._result(task, "Client ports stay blocked: %s, retry task" % keep)
                continue
            for lvs in node.lvstores.values():
                if lvs.client_port in node.blocked:
                    if lvs.fenced:
                        w.violate("port_allow lifted fenced port %s on %s" % (lvs.client_port, node.name))
                    rec.rpc_client(timeout=3, retry=0).nvmf_port_unblock(lvs.client_port)
                    w.log("CP port_allow: %s port %s allowed" % (node.name, lvs.client_port))
            if not self._failback(node):
                self._result(task, "Leadership failback incomplete, retry task")
                continue
            self._finish(task)
        w.vt.after(2.0, self.port_allow_tick)

    def _failback(self, node: SimNode) -> bool:
        """The primary takes its own LVS back from the acting leader."""
        from simplyblock_core import storage_node_ops as sno
        w = self.world
        own = node.lvstores[next(k for k, v in node.lvstores.items() if v.primary_node == node.uuid)]
        peer = w.peer_of(node)
        if own.leader:
            return True
        acting = peer.lvstores[own.name]
        if not (peer.up and acting.leader):
            return True                             # leaderless: the arbiter restores it
        lv = node.ha.get(own.vuid)
        if own.fenced or lv is None or not lv.remote_healthy or lv.state in (FENCED, HOLDING):
            return False
        prec, rec = w.sn(peer), w.sn(node)
        prec.rpc_client(timeout=5, retry=0).bdev_lvol_set_leader(own.name, leader=False)
        rpc = rec.rpc_client(timeout=10, retry=0)
        rpc.bdev_lvol_update_lvstore(own.name)
        rpc.bdev_lvol_set_leader(own.name, leader=True)
        for vol in w.vols.values():
            if vol.lvs_name == own.name:
                sno._set_lvol_ana_on_node(vol, rec, "optimized")
        sno._failback_primary_ana(rec)
        w.log("CP port_allow: failback of %s to %s" % (own.name, node.name))
        return True

    # ---- migration -------------------------------------------------------------------------

    def migration_tick(self):
        w = self.world
        now = w.vt.time()
        kinds = {"device_migration", "failed_device_migration", "new_device_migration",
                 "balancing_on_restart"}
        open_ids = {t.uuid for t in self._tasks(kinds)}
        for task in self._tasks(kinds):
            if task.function_name == "balancing_on_restart":
                if not (set(task.sub_tasks or []) & open_ids):
                    self._finish(task)
                continue
            node = next((n for n in w.nodes if n.uuid == task.node_id), None)
            if node is None:
                self._finish(task)
                continue
            if not node.up or not all(node.devices.values()):
                self._result(task, "mig error: %d, retrying" % DST_NOT_AVAILABLE)
                self.mig_ready_since.pop(task.uuid, None)
                continue
            own = next(v for v in node.lvstores.values() if v.primary_node == node.uuid)
            if not any(n.up and n.lvstores[own.name].leader for n in w.nodes):
                self._result(task, "Node %s is not the leader " % node.uuid)
                self.mig_ready_since.pop(task.uuid, None)
                continue
            since = self.mig_ready_since.setdefault(task.uuid, now)
            if now - since >= self.mig_s:
                w.rebuilt(node)
                self._finish(task)
                self.mig_ready_since.pop(task.uuid, None)
        w.vt.after(3.0, self.migration_tick)


def zero_run(per_sec: dict, first: int, last: int) -> int:
    """Longest run of consecutive seconds without a completion in
    ``[first, last]`` -- the bed's ``max_stall_after_inject_s`` (tn2
    it/analyze.py over fio's 1 s IOPS log)."""
    longest = cur = 0
    for sec in range(first, last + 1):
        cur = cur + 1 if per_sec.get(sec, 0) == 0 else 0
        longest = max(longest, cur)
    return longest


class Client:
    """NVMe/TCP multipath client writing to every volume (KATO 4, fast_io_fail 8)."""

    def __init__(self, world):
        self.world = world
        self.pending: dict = {}          # vol -> outstanding writes
        self.last_ack: dict = {}
        self.max_stall: dict = {}      # longest continuous gap (diagnostic)
        self.completions: dict = {}    # vol -> {second since fio start: completions}
        self.fio_start = 0.0
        self.measure_from = 0.0
        self.stalls: list = []
        self.io_errors: dict = {}
        self.acks_by: dict = {}          # (lvs, node) -> last ack time
        self.failed_path: dict = {}      # (vol, node uuid) -> path down until (KATO)
        self.measuring = False
        self.after_fence_acks = 0

    def start(self):
        now = self.world.vt.time()
        self.fio_start = now
        for v in self.world.vols:
            self.last_ack[v] = now
            self.max_stall[v] = 0.0
            self.io_errors[v] = 0
        self.world.vt.after(0.05, self.tick)

    def note_grant(self, node):
        pass

    def stall_s(self, inject_at: float) -> dict:
        """Per volume, the bed's stall metric from ``inject_at`` to now."""
        now = self.world.vt.time()
        first = max(0, int(inject_at - self.fio_start))
        last = int(now - self.fio_start) - 1          # the current second is not complete
        return {v: zero_run(self.completions.get(v, {}), first, last) for v in self.world.vols}

    def reset_metrics(self):
        now = self.world.vt.time()
        self.measure_from = now
        for v in self.world.vols:
            self.last_ack[v] = now
            self.max_stall[v] = 0.0
            self.io_errors[v] = 0
        self.measuring = True

    def _paths(self, vol):
        w = self.world
        lvs = vol.lvs_name
        port = next(s.client_port for s in w.nodes[0].lvstores.values() if s.name == lvs)
        now = w.vt.time()
        out = []
        for n in w.nodes:
            if n.uuid in w.net.client_cut or n.proc in ("dead", "down", "stopped"):
                continue
            if port in n.blocked:
                continue
            if self.failed_path.get((vol.lvol_name, n.uuid), 0) > now:
                continue
            st = n.ana.get((port, vol.ns_id), "inaccessible")
            if st not in ("optimized", "non_optimized"):
                continue
            # An ANA change notice re-selects the path: of two optimized
            # paths the host moves to the one that became optimized last
            # (the grant's failover), as the bed's 02b showed (2-3 s stall).
            changed = w.ana_changed.get((n.uuid, port, vol.ns_id), 0.0)
            out.append((0 if st == "optimized" else 1, -changed, n.name, n))
        return [x[3] for x in sorted(out, key=lambda x: x[:3])]

    def _serve(self, node: SimNode, lvs_name: str, depth=0):
        """'ack', 'wait' or 'error' for one write arriving at ``node``."""
        w = self.world
        if node.proc == "frozen":
            return "hang"
        lvs = node.lvstores[lvs_name]
        lv = node.ha[lvs.vuid]
        if lvs.fenced or lv.state == FENCED:
            return "error"
        if lvs.held or lv.state == HOLDING:
            return "wait"
        peer = w.peer_of(node)
        if lvs.takeover_until:
            if w.vt.time() < lvs.takeover_until:
                return "wait"                        # lvstore update after the takeover
            lvs.takeover_until = 0.0
            lvs.leader = True
            w.on_leader(node, lvs, "IO takeover")
        if lvs.leader:
            if lv.state == SOLO:
                w.note_ack(node, lvs_name, replicated=False)
                return "ack"
            if peer.up and w.net.data_ok(node, peer):
                w.note_ack(node, lvs_name, replicated=True)
                return "ack"
            return "wait"                            # remote JM not acking yet
        # non-leader: redirect over the hublvol to the leader
        plvs = peer.lvstores[lvs_name]
        if peer.proc in ("up", "frozen") and w.net.data_ok(node, peer) and depth == 0:
            if peer.proc == "frozen":
                # the hublvol controller to a frozen peer stays connected until
                # its keep-alive expires (SPDK-to-SPDK KATO); then the redirect
                # fails and the next IO takes the LVS over
                if w.vt.time() - peer.frozen_since < HUBLVOL_KATO_S:
                    return "wait"
                plvs = None
            if plvs is not None and plvs.leader:
                return self._serve(peer, lvs_name, depth=1)
            if plvs is not None and plvs.fenced:
                return "wait"                        # F13: redirect to a fenced primary
        if lvs.takeover_blocked:
            return "error"
        # IO-triggered takeover: lvstore update, 73 ms on new_md_journal
        # (4.05 s on master's md-journal, campaign c3 F7)
        delay = 4.05 if "slow_takeover" in node.bugs else w.takeover_s
        if delay > 1.0:
            # campaign c3, F7: the takeover held client IO for 4 s
            w.violate("%s: lvstore takeover of %s takes %.2f s (> 1 s)" % (node.name, lvs_name, delay))
        lvs.takeover_until = w.vt.time() + delay
        w.log("%s: takes over %s on new IO (%.0f ms)" % (node.name, lvs_name, delay * 1000))
        return "wait"

    def tick(self):
        """Every 50 ms: one new write per volume (queue depth ``DEPTH``), and
        every pending write tries its path. A write on a path whose SPDK is
        frozen stays there until the keep-alive declares the path dead (KATO),
        then it is retried elsewhere; a write that cannot complete within
        fast_io_fail is an IO error. The stall is the longest gap without any
        completion, as fio's IOPS log shows it on the bed."""
        w = self.world
        now = w.vt.time()
        w.check_invariants()
        for vname, vol in w.vols.items():
            queue = self.pending.setdefault(vname, [])
            if len(queue) < DEPTH:
                queue.append({"issued": now, "path": None})
            done_any = False
            for io in list(queue):
                if io["path"] is not None:
                    n = io["path"]
                    if n.proc == "frozen" and now - io["issued"] < CLIENT_KATO_S:
                        continue                     # stuck on the frozen controller
                    if n.proc == "frozen":
                        self.failed_path[(vname, n.uuid)] = now + 1.0
                    io["path"] = None
                result = None
                for n in self._paths(vol):
                    res = self._serve(n, vol.lvs_name)
                    if res == "hang":
                        io["path"] = n
                        result = "hang"
                        break
                    if res in ("ack", "wait"):
                        result = res
                        if res == "ack":
                            self.acks_by[(vol.lvs_name, n.uuid)] = now
                        break
                    # path error: next path
                if result == "ack":
                    queue.remove(io)
                    done_any = True
                elif now - io["issued"] >= FAST_IO_FAIL_S:
                    queue.remove(io)
                    self.io_errors[vname] += 1
            if done_any:
                sec = int(now - self.fio_start)
                per = self.completions.setdefault(vname, {})
                per[sec] = per.get(sec, 0) + 1
                gap = now - self.last_ack[vname]
                if gap > self.max_stall[vname]:
                    self.max_stall[vname] = gap
                self.last_ack[vname] = now
        w.vt.after(0.05, self.tick)
