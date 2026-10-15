"""Scenarios and the bug switches of the regression proofs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

#: Bug switches. Node-side ones are read by ``SimNode`` (``node.bugs``);
#: control-plane ones re-introduce the pre-fix code by patching the real
#: function back to its old behaviour (``apply_bugs``).
BUGS = {
    # node side (ultra / SPDK behaviour before the fix)
    "slow_frozen_detect": "F5: a frozen peer is seen only on the NVMe keep-alive (5 s)",
    "slow_takeover": "F7: lvstore takeover takes 4.05 s (master's md-journal)",
    "unfence_keeps_ports": "F8: spdk_lvs_ha_unfence does not lift its fence's port block",
    "stall_fence": "F8/F12/F13: a node waking from a freeze fences itself instead of aborting",
    "alceml_stall": "F12: the alceml stuck-IO watchdog counts the freeze and drops the devices",
    # control plane (sbcli before the fix)
    "no_compression_retry": "F1: the restart aborts on the first jc_compression failure",
    "no_regrant_degraded": "F2: no new verdict while DEGRADED",
    "no_lease_expiry_grant": "F5 (CP side): the survivor is granted only on its own report",
    "no_planned_loss": "F3: the restart/shutdown flows do not announce the planned loss",
    "no_self_fence_readmit": "F4: a self-fenced node is never re-admitted while STEADY",
    "no_partition_recovery": "F6/F11: no JM reconnect after a partition heals",
    "no_rebuild_mark": "F10: a re-admitted node is not marked for a data rebuild",
    "no_device_check": "F12: re-admission does not check the node's data devices",
    "no_stuck_rebuild_recovery": "F12: a rebuild failing on unavailable devices retries forever",
    "promote_at_grant": "F17: #1476's promote-at-grant (bdev_lvol_update_lvstore + set_leader on "
                        "the survivor right after the grant), removed by #1485",
    "port_allow_two_jms": "F14: port_allow requires 2 remote JMs",
    "port_allow_ignores_fence": "F15: port_allow lifts a fenced LVS's client port",
    "blocking_leader_probe": "F16: the leadership probe runs on the decision thread (5 s + retry)",
    # hosts (client connect parameters)
    "client_kato4": "F20: hosts connect with keep-alive 4 s, so a frozen node's path is "
                    "detected only about 7 s after the freeze",
}


def apply_bugs(world, monkeypatch, bugs: set) -> None:
    from simplyblock_core import constants
    from simplyblock_core import storage_node_ops as sno
    from simplyblock_core.arbitration import arbiter as arb
    from simplyblock_core.arbitration import decision as dec
    from simplyblock_core.arbitration import planned
    from simplyblock_core.services import tasks_runner_port_allow as pa

    unknown = bugs - set(BUGS)
    assert not unknown, unknown
    if "client_kato4" in bugs:
        world.client_kato_s = 4.0
    if "no_compression_retry" in bugs:
        monkeypatch.setattr(constants, "RESTART_JC_COMPRESSION_RETRY_S", 0)
    if "no_regrant_degraded" in bugs:
        real = dec.decide

        def decide(snap):
            if snap.state == dec.ARB_DEGRADED:
                return dec.Verdict(dec.V_NONE, snap.state, reason="verdict in force")
            return real(snap)
        monkeypatch.setattr(dec, "decide", decide)
    if "no_planned_loss" in bugs:
        monkeypatch.setattr(planned, "note_planned_loss", lambda *a, **k: False)
        from . import actors
        monkeypatch.setattr(actors, "_note_planned_loss", lambda *a, **k: False, raising=False)
    if "no_lease_expiry_grant" in bugs:
        monkeypatch.setattr(dec, "_unreported_loss", lambda snap: None)
    if "no_self_fence_readmit" in bugs:
        monkeypatch.setattr(arb.Arbiter, "readmit_self_fenced", lambda self, rec, nodes: None)
    if "no_partition_recovery" in bugs:
        monkeypatch.setattr(arb.Arbiter, "recover_partition", lambda self, rec, nodes: None)
    if "no_rebuild_mark" in bugs:
        monkeypatch.setattr(arb.Arbiter, "mark_rebuild", lambda self, rec, node, reason: None)
    if "no_device_check" in bugs:
        monkeypatch.setattr(arb, "default_devices_ok", lambda db, node: True)
    if "no_stuck_rebuild_recovery" in bugs:
        monkeypatch.setattr(arb.Arbiter, "_check_rebuild_stuck", lambda self, *a, **k: None)
    if "promote_at_grant" in bugs:
        real_send = arb.Arbiter.send

        def send(self, rec, verdict, nodes):
            ok = real_send(self, rec, verdict, nodes)
            if ok and verdict.kind == dec.V_DEGRADED and verdict.winner and verdict.loser:
                # pre-#1485: the survivor takes the lost primary's LVS at once
                taker = next(n for n in nodes if n.get_id() == verdict.winner)
                lost = next(n for n in nodes if n.get_id() == verdict.loser)
                if lost.lvstore:
                    rpc = taker.rpc_client(timeout=10, retry=0)
                    try:
                        rpc.bdev_lvol_update_lvstore(lost.lvstore)
                        rpc.bdev_lvol_set_leader(lost.lvstore, leader=True)
                    except Exception:               # noqa: BLE001 - logged by the old code
                        pass
            return ok
        monkeypatch.setattr(arb.Arbiter, "send", send)
    if "port_allow_two_jms" in bugs:
        monkeypatch.setattr(pa, "_single_peer_jm_is_complete", lambda db, node, jms: False)
    if "port_allow_ignores_fence" in bugs:
        monkeypatch.setattr(pa, "_two_node_fence_holds_ports", lambda node: "")
    if "blocking_leader_probe" in bugs:
        real_work = arb.Arbiter._work

        def work(self, key, ctx, fn, *args):
            if key[0] != "leadership":
                return real_work(self, key, ctx, fn, *args)
            # pre-F16: inline on the decision thread, 5 s timeout, one retry
            from concurrent.futures import Future
            fut = Future()
            try:
                fut.set_result(fn(*args))
            except Exception as e:                  # noqa: BLE001
                fut.set_exception(e)
            self.work_ctx[key] = ctx
            self.inflight[key] = fut
            return True
        monkeypatch.setattr(arb.Arbiter, "_work", work)
        real_probe = sno._lvs_leader_probe
        monkeypatch.setattr(sno, "_lvs_leader_probe",
                            lambda nodes, lvs, timeout=5, retry=1: real_probe(nodes, lvs, 5, 1))


@dataclass
class Scenario:
    name: str
    covers: tuple
    inject: Callable
    stall_limit_s: float = 3.0
    io_errors_ok: bool = False
    aborts_ok: tuple = ()               # node names whose abort is expected
    heal_s: float = 240.0
    observe_s: float = 30.0
    ana_all_groups: bool = False
    conditions: tuple = ()
    grant_within_s: float | None = None
    # node capabilities on top of DEPLOYED_CAPS: isolates a finding's proof
    # from another open finding the same fault also hits
    caps: frozenset = frozenset()
    notes: str = ""
    extra: dict = field(default_factory=dict)
    # kubernetes-fencing contract (mode witness): host fencing on the cluster,
    # and the nodes whose host must be fenced in the run (() = none; an int: that
    # many, whichever they are). None: not checked.
    host_fencing: bool = False
    host_fenced: tuple | int | None = None
    # review on #1527: subsystems allow any host and list both hosts
    stale_host_lists: bool = False


# ---------------------------------------------------------------------------
# Faults (the bed's injections)
# ---------------------------------------------------------------------------

def crash(name):
    def inject(w):
        w.mark_fault()
        w.node(name).abort("killed by the scenario")
    return inject


def hang(name, seconds=4.0):
    def inject(w):
        w.mark_fault()
        n = w.node(name)
        n.proc, n.frozen_until = "frozen", w.vt.time() + seconds
        n.frozen_since = w.vt.time()
        w.log("%s: frozen for %.1f s" % (name, seconds))
    return inject


def reboot(name, down_s=30.0):
    def inject(w):
        w.mark_fault()
        n = w.node(name)
        n.proc = "down"
        for lvs in n.lvstores.values():
            lvs.leader = False
        w.log("%s: host reboots" % name)

        def boot():
            n.proc = "dead"                     # host up, SPDK not running
            w.cp.note_death(n)
            w.log("%s: host is back, SPDK not started" % name)
        w.vt.after(down_s, boot)
    return inject


def shutdown_then_restart(name, down_s=20.0):
    def inject(w):
        w.mark_fault()
        w.cp.shutdown(w.node(name))
        w.vt.after(down_s, lambda: w.cp.request_restart(w.node(name)))
    return inject


def restart_live(name):
    def inject(w):
        w.mark_fault()
        w.cp.request_restart(w.node(name))
    return inject


def client_path(name, seconds=20.0):
    def inject(w):
        w.mark_fault()
        n = w.node(name)
        w.net.client_cut.add(n.uuid)
        w.log("client path to %s cut" % name)

        def back():
            w.net.client_cut.discard(n.uuid)
            w.log("client path to %s back" % name)
        w.vt.after(seconds, back)
    return inject


def partition(seconds=30.0):
    def inject(w):
        w.mark_fault()
        w.net.data_cut = True
        w.log("storage link cut")

        def back():
            w.net.data_cut = False
            w.log("storage link back")
        w.vt.after(seconds, back)
    return inject


def both_links(name="sn1", seconds=25.0):
    def inject(w):
        w.mark_fault()
        n = w.node(name)
        w.net.data_cut = True
        w.net.cp_cut.add(n.uuid)
        w.log("storage and management links of %s cut" % name)

        def back():
            w.net.data_cut = False
            w.net.cp_cut.discard(n.uuid)
            w.log("links back")
        w.vt.after(seconds, back)
    return inject


def lossy(seconds=40.0):
    def inject(w):
        w.mark_fault()
        w.net.data_lossy = True
        w.log("storage link lossy (1500 ms, 30%)")

        def back():
            w.net.data_lossy = False
            w.log("storage link clean")
        w.vt.after(seconds, back)
    return inject


def freeze_at_leader_probe(name, seconds=4.0):
    """Freeze ``name`` right when the arbiter's leadership pass starts (F16)."""
    def inject(w):
        from simplyblock_core import constants
        cid = w.cluster.uuid
        last = w.arbiter.leadership_checked_at.get(cid, 0)
        due = last / 1000.0 + constants.TWO_NODE_LEADERSHIP_RECONCILE_S
        at = max(w.vt.now + 0.01, due - 0.05)
        w.vt.at(at, lambda: hang(name, seconds)(w))
    return inject


def cp_down_during_crash(name="sn2", arbiter_down_s=6.0):
    """F4: the arbiter is down (CP restart) when ``name`` crashes. Its peer
    holds, its lease has run out, so it fences itself; the arbiter comes back
    to a STEADY record with one node lost and the other self-fenced. Only the
    re-admission of a self-fenced node (once the lost one is back) heals it."""
    def inject(w):
        w.restart_arbiter(arbiter_down_s)
        w.vt.after(0.5, lambda: crash(name)(w))
    return inject


def double_loss(name):
    """F2: ``name`` crashes and comes back; while the arbiter is still DEGRADED
    and its peer runs normally again (remote JM resynced), ``name`` crashes a
    second time: the peer holds and needs a fresh verdict."""
    def inject(w):
        w.mark_fault()
        n = w.node(name)
        peer = w.peer_of(n)
        n.abort("killed by the scenario")

        def again():
            rec = w.db.get_cluster_arbitration(w.cluster.uuid)
            if (n.up and rec is not None and rec.state == "degraded"
                    and all(lv.state == "normal" for lv in peer.ha.values())):
                n.abort("killed again by the scenario")
                return
            w.vt.after(0.25, again)
        w.vt.after(8.0, again)
    return inject


def stale_takeover(primary="sn2"):
    """F10 (campaign c3, 02b then 03): the primary crashes and the survivor
    runs alone; the primary comes back (restart flow, configured by the
    arbiter, serving its LVS again) but still misses the survivor's writes;
    then the survivor is lost before the rebuild finished. The stale node must
    not serve."""
    def inject(w):
        p = w.node(primary)
        s = w.peer_of(p)
        w.cp.mig_s = 40.0                       # a rebuild slower than the heal
        p.abort("killed by the scenario")

        def readmitted():
            # back from the restart, configured for arbitration by the arbiter,
            # serving its LVS normally -- but still missing the survivor's writes
            rec = w.db.get_cluster_arbitration(w.cluster.uuid)
            settled = rec is not None and (rec.state == "steady" or
                                           (rec.state == "healing" and bool(rec.rebuild)))
            return (settled and p.up and p.arbitration and w.vt.time() - p.started_at >= 1.0
                    and all(lv.state == "normal" for lv in p.ha.values())
                    and not any(lvs.fenced for lvs in p.lvstores.values())
                    and any(w.missing(p, lvs) for lvs in w.version))

        def kill_survivor():
            if readmitted():
                s.abort("survivor killed before the rebuild finished")
                return
            w.vt.after(0.25, kill_survivor)
        w.vt.after(5.0, kill_survivor)
    return inject


# ---------------------------------------------------------------------------
# The catalogue
# ---------------------------------------------------------------------------

BED = [
    Scenario("01a-crash-sn1", ("bed", "B1"), crash("sn1"), aborts_ok=("sn1",), grant_within_s=2.2),
    Scenario("01b-crash-sn2", ("bed", "F7"), crash("sn2"), aborts_ok=("sn2",), grant_within_s=2.2),
    Scenario("02a-hang-sn1", ("bed", "F5", "F12", "F16"), hang("sn1"), aborts_ok=("sn1",), grant_within_s=2.6),
    Scenario("02b-hang-sn2", ("bed", "F5", "F13", "F16"), hang("sn2"), aborts_ok=("sn2",), grant_within_s=2.6),
    Scenario("03-reboot-sn1", ("bed", "F10"), reboot("sn1"), grant_within_s=2.6, heal_s=300),
    Scenario("04-shutdown-sn2", ("bed", "F3", "F17"), shutdown_then_restart("sn2"), grant_within_s=2.2),
    Scenario("05-client-path-sn2", ("bed",), client_path("sn2"), stall_limit_s=5.0),
    Scenario("05b-client-path-sn1", ("bed",), client_path("sn1"), stall_limit_s=5.0),
    Scenario("07-partition", ("bed", "F6", "F8", "F14", "F15"), partition(), io_errors_ok=True,
             stall_limit_s=None, heal_s=300),
    Scenario("08-both-links", ("bed", "F11", "F14", "F15"), both_links(), io_errors_ok=True,
             stall_limit_s=None, heal_s=300),
    Scenario("10b-netem", ("bed", "F14", "F15"), lossy(), io_errors_ok=True, stall_limit_s=None,
             heal_s=300),
]

#: docs/design/two-node-kubernetes-fencing.md, mode witness. A host is fenced
#: (revoked on the winner, then StorageFenced and the out-of-service taint) only
#: when its state is unknown; never for an SPDK-only or planned loss. A partition
#: has a winner (contract 3.3): the other node's host is fenced. Every fence is released and every
#: subsystem restored without a manual step (the operator actor releases a
#: releasable fence once the node runs again).
KUBE = [
    Scenario("K1-host-fence-reboot-sn1", ("k8s",), reboot("sn1"), host_fencing=True,
             host_fenced=("sn1",), grant_within_s=2.6, heal_s=300),
    Scenario("K2-no-host-fence-crash-sn2", ("k8s",), crash("sn2"), aborts_ok=("sn2",),
             host_fencing=True, host_fenced=(), grant_within_s=2.2),
    Scenario("K3-no-host-fence-shutdown-sn2", ("k8s",), shutdown_then_restart("sn2"),
             host_fencing=True, host_fenced=(), grant_within_s=2.2),
    Scenario("K4-host-fence-partition-winner", ("k8s",), partition(), io_errors_ok=True,
             stall_limit_s=None, heal_s=300, host_fencing=True, host_fenced=1),
    Scenario("K5-host-fence-hang-sn2", ("k8s",), hang("sn2"), aborts_ok=("sn2",),
             host_fencing=True, host_fenced=("sn2",), grant_within_s=2.6),
    # review on #1527: the loser is listed on an allow-any subsystem; the fence
    # must not stick in revoking (SPDK refuses a duplicate add), and the
    # release gives the listed loser back because the volumes authorize it.
    Scenario("K6-host-fence-stale-host-lists", ("k8s",), reboot("sn1"), host_fencing=True,
             host_fenced=("sn1",), grant_within_s=2.6, heal_s=300, stale_host_lists=True),
]

#: finding -> (scenario, bugs that re-introduce it, signature): the scenario
#: must pass on main and fail with the bugs applied -- with a failure that
#: contains the signature, so it is caught for that reason and no other.
REGRESSIONS = {
    "F1": (Scenario("F1-restart-during-relevel", ("F1",), crash("sn2"), aborts_ok=("sn2",)),
           {"no_compression_retry"}, 'restart aborted'),
    "F2": (Scenario("F2-second-loss-while-degraded", ("F2",), double_loss("sn2"), aborts_ok=("sn2",),
                    heal_s=300), {"no_regrant_degraded"}, 'IO errors'),
    "F3": (Scenario("F3-restart-live-node", ("F3",), restart_live("sn1"), grant_within_s=2.2),
           {"no_planned_loss"}, 'grant'),
    "F4": (Scenario("F4-self-fence-readmit", ("F4",), cp_down_during_crash("sn2"), io_errors_ok=True,
                    stall_limit_s=None, aborts_ok=("sn2",)), {"no_self_fence_readmit"}, 'fenced'),
    "F5": (Scenario("F5-frozen-peer-detection", ("F5",), hang("sn2"), aborts_ok=("sn2",),
                    grant_within_s=2.6), {"slow_frozen_detect", "no_lease_expiry_grant"}, "grant"),
    "F6": (Scenario("F6-partition-recovery", ("F6",), partition(), io_errors_ok=True, stall_limit_s=None,
                    heal_s=300, conditions=("jm_needs_reconnect",)), {"no_partition_recovery"}, 'remote JM'),
    "F7": (Scenario("F7-takeover-time", ("F7",), crash("sn2"), aborts_ok=("sn2",)), {"slow_takeover"},
           "lvstore takeover"),
    "F8": (Scenario("F8-unfence-lifts-ports", ("F8",), partition(), io_errors_ok=True, stall_limit_s=None,
                    heal_s=300), {"unfence_keeps_ports"}, 'unfence of'),
    "F10": (Scenario("F10-stale-node-takeover", ("F10",), stale_takeover("sn2"), io_errors_ok=True,
                     stall_limit_s=None, aborts_ok=("sn1", "sn2"), heal_s=0, observe_s=60.0),
            {"no_rebuild_mark"}, 'only up-to-date copy'),
    # judged on the devices and the heal; the stall of a frozen secondary and
    # the survivor's writes waiting on its devices are F23's (its fix's cap)
    "F12": (Scenario("F12-freeze-devices", ("F12",), hang("sn1"), aborts_ok=("sn1",), stall_limit_s=None,
                     caps=frozenset({"stoplist_peer_devices_on_grant"})),
            {"stall_fence", "alceml_stall", "no_device_check", "no_stuck_rebuild_recovery"}, 'not healthy'),
    "F13": (Scenario("F13-frozen-primary-takeover", ("F13",), hang("sn2"), aborts_ok=("sn2",)),
            {"stall_fence"}, ("client stall", "not healthy")),
    "F14": (Scenario("F14-port-allow-two-node", ("F14",), partition(), io_errors_ok=True,
                     stall_limit_s=None, heal_s=200), {"port_allow_two_jms"}, 'port_allow'),
    "F15": (Scenario("F15-port-allow-respects-fence", ("F15",), partition(60.0), io_errors_ok=True,
                     stall_limit_s=None, heal_s=300), {"port_allow_ignores_fence"}, 'port_allow lifted fenced port'),
    "F17": (Scenario("F17-no-leadership-move-after-grant", ("F17",), shutdown_then_restart("sn2"),
                     grant_within_s=2.2), {"promote_at_grant"}, "after its grant"),
    # F16 is judged on its own property: the grant arrives although the
    # leadership probe ran against the frozen node (the stall of a frozen
    # primary is 02b's and F19's)
    "F16": (Scenario("F16-blocking-leader-probe", ("F16",), freeze_at_leader_probe("sn2"),
                     aborts_ok=("sn2",), grant_within_s=2.6, stall_limit_s=None),
            {"blocking_leader_probe"}, 'grant'),
}

#: finding -> check that the scenario on main really went through the step
#: the finding is about (not a vacuous pass).
EXERCISED = {
    "F2": lambda out: any("killed again" in why for _, _, why in out.aborts),
    "F3": lambda out: any("restarting sn1" in t for t in out.trace),
    "F4": lambda out: any("sn1: fence" in t for t in out.trace),
    "F10": lambda out: any("survivor killed" in why for _, _, why in out.aborts),
    "F1": lambda out: any("jc_compression" in t or "restarted" in t for t in out.trace),
    "F16": lambda out: any("frozen" in t for t in out.trace),
}

#: The fast tier (required on every PR): one scenario per failure class --
#: crash, freeze of the vol1 primary (abort-on-wake, F5/F13), partition (fence
#: both, leadership after the heal, F14/F15) -- and the cheapest proofs that
#: the decision loop never blocks (F16) and port_allow respects the fence (F15).
FAST = {"01b-crash-sn2", "02b-hang-sn2", "07-partition", "detect:F16", "detect:F3",
        "K1-host-fence-reboot-sn1", "K2-no-host-fence-crash-sn2"}

#: Known failing on main: scenario -> (reason with the fix that flips it, a
#: text the failure must contain, so it fails for that reason and no other).
KNOWN_FAILING = {
    "02a-hang-sn1": (("F23: after its solo grant sn2 still writes vol1's stripes to the frozen "
                     "sn1's data devices and waits until sn1 aborts on wake and its connections "
                     "close (bed run c9/02a: 4 s stall; fix: a solo grant isolates the lost "
                     "peer's devices, ultra, stacked on #217)"),
                     "F23"),
    "F23-frozen-secondary-data-io": (("F23: the survivor's writes wait on the frozen peer's data "
                                     "devices after the grant (fix: a solo grant isolates the "
                                     "lost peer's devices)"),
                                     "F23"),
    "F19-long-freeze-primary": (("F19: after #1485 (no promote-at-grant) the survivor does not take "
                                "a frozen primary's LVS over; its volume stalls until the hublvol "
                                "keep-alive (5 s) expires or the frozen node aborts on wake "
                                "(fix: on a solo grant stop redirecting to the lost peer)"),
                                "client stall"),
    "F20-frozen-leader-client-kato": (("F19 on what main deploys: the survivor keeps redirecting "
                                      "to the frozen primary (fix: SPDK #81 + ultra #215)"),
                                      "client stall"),
    "F18-ana-groups-restored": (("F18: the fence restore puts back only the namespaces' ANA "
                                "groups (fix: restore every group the fence changed)"),
                                "ANA 44"),
    "F24-frozen-primary-takeover-read": (("F23 on what main deploys: the follower survivor's takeover "
                                         "metadata read waits on the frozen primary's data devices "
                                         "until it aborts on wake (bed c9/c10 02c: 9 s; fix: ultra "
                                         "#219 with F24, isolate by marking the devices unavailable)"),
                                         "F23"),
    "F25-frozen-primary-takeover-append-lock": (("F23 on what main deploys: the follower survivor's "
                                                "takeover waits on the frozen primary's data devices "
                                                "(fix: ultra #219 with F24 and F25)"),
                                                "F23"),
    "F26-takeover-read-inflight-on-peer-jm": (("F23 on what main deploys: the follower survivor's "
                                              "takeover waits on the frozen primary's data devices "
                                              "(fix: ultra #219 with F24, F25 and F26)"),
                                              "F23"),
    "F21-takeover-reader-frozen-peer-jm": (("F21: the survivor's takeover reader picks the frozen "
                                           "peer's JM and waits for the 8 s NVMe IO timeout "
                                           "(fix: a solo LVS skips the peer JM the HA layer knows "
                                           "is dead)"),
                                           "F21"),
}

#: Open findings whose fix exists as a pending PR stack: scenario -> the node
#: capabilities (``simnode``) that model the fix. On DEPLOYED_CAPS the scenario
#: stays in KNOWN_FAILING; with these capabilities it must pass (bed run c8:
#: SPDK #80 + #81 on new_md_journal, ultra #214 + #215).
PENDING_FIX = {
    # F19 alone is not enough: with the redirect stopped at the grant, the
    # takeover's reader still waits 8 s on the frozen peer's JM (c8 02c, F21),
    # and the survivor's writes of its own LVS wait on the frozen peer's data
    # devices (c9 02a, F23)
    "F19-long-freeze-primary": frozenset({"grant_stops_redirect", "reader_skips_dead_peer_jm",
                                          "stoplist_peer_devices_on_grant", "isolate_marks_devices_unavailable",
                                                   "appender_lock_skips_peer_jm", "reader_resets_peer_jm_read"}),
    "F21-takeover-reader-frozen-peer-jm": frozenset({"grant_stops_redirect", "reader_skips_dead_peer_jm",
                                                     "stoplist_peer_devices_on_grant", "isolate_marks_devices_unavailable",
                                                   "appender_lock_skips_peer_jm", "reader_resets_peer_jm_read"}),
    "F18-ana-groups-restored": frozenset({"ana_restore_all_groups"}),
    "F20-frozen-leader-client-kato": frozenset({"grant_stops_redirect", "reader_skips_dead_peer_jm",
                                                "stoplist_peer_devices_on_grant", "isolate_marks_devices_unavailable",
                                                   "appender_lock_skips_peer_jm", "reader_resets_peer_jm_read"}),
    # the survivor also takes the frozen primary's other LVS (vol0's LVS_1) over;
    # its takeover read needs F24's capability (the bed's fio wrote vol1 only)
    "F23-frozen-secondary-data-io": frozenset({"stoplist_peer_devices_on_grant",
                                               "isolate_marks_devices_unavailable",
                                                   "appender_lock_skips_peer_jm", "reader_resets_peer_jm_read"}),
    # c11 02c: the frozen primary's LVS is taken over by the follower survivor;
    # its metadata read must go degraded around the isolated peer devices
    # c12 02c: with F24 the takeover read goes degraded, but the new leader's first
    # write waited for its append lock on the frozen peer's JM (F25); judged on
    # the client stall the bed measures (default limit, KATO 2)
    "F25-frozen-primary-takeover-append-lock": frozenset({"grant_stops_redirect", "reader_skips_dead_peer_jm",
                                                          "stoplist_peer_devices_on_grant",
                                                          "isolate_marks_devices_unavailable",
                                                          "appender_lock_skips_peer_jm", "reader_resets_peer_jm_read"}),
    # c12b 02c-r1: the survivor's reader had a history read in flight on the frozen
    # peer's JM; the takeover's reader restart waited for it (F26)
    "F26-takeover-read-inflight-on-peer-jm": frozenset({"grant_stops_redirect", "reader_skips_dead_peer_jm",
                                                        "stoplist_peer_devices_on_grant",
                                                        "isolate_marks_devices_unavailable",
                                                        "appender_lock_skips_peer_jm",
                                                        "reader_resets_peer_jm_read"}),
    "F24-frozen-primary-takeover-read": frozenset({"grant_stops_redirect", "reader_skips_dead_peer_jm",
                                                   "stoplist_peer_devices_on_grant",
                                                   "isolate_marks_devices_unavailable",
                                                   "appender_lock_skips_peer_jm", "reader_resets_peer_jm_read"}),
}

EXTRA = [
    # F20 (bed c8/02c): the vol1 primary frozen 8 s. With F19 fixed, the stall
    # is the host's keep-alive detection of the frozen path: within the limit
    # with KATO 2, about 7 s with KATO 4 (test_f20_client_kato4_detected).
    Scenario("F20-frozen-leader-client-kato", ("F20",), hang("sn2", 8.0), aborts_ok=("sn2",),
             grant_within_s=2.6),
    Scenario("F19-long-freeze-primary", ("F19",), hang("sn2", 6.0), aborts_ok=("sn2",),
             grant_within_s=2.6),
    Scenario("F18-ana-groups-restored", ("F18",), partition(), io_errors_ok=True, stall_limit_s=None,
             heal_s=300, ana_all_groups=True),
    # c8 02c: the vol1 primary frozen 8 s. Judged on its own property (no takeover
    # waits on the frozen peer's JM); the client-side detection (F20) is not part of it.
    Scenario("F21-takeover-reader-frozen-peer-jm", ("F21",), hang("sn2", 8.0), aborts_ok=("sn2",),
             grant_within_s=2.6, stall_limit_s=None),
    # c9 02a: the secondary sn1 frozen 4 s. vol1's leader sn2 is granted at about
    # +2 s, but its writes (each stripe has a chunk on sn1) wait on sn1's data
    # devices until sn1 aborts on wake and its connections close (+5.1 s).
    # A frozen primary (02c) is F24-frozen-primary-takeover-read below.
    Scenario("F23-frozen-secondary-data-io", ("F23",), hang("sn1"), aborts_ok=("sn1",),
             grant_within_s=2.6),
    # c9/c10/c11 02c: the vol1 primary sn2 frozen 8 s; the follower sn1 takes LVS_6
    # over after its grant. Its lvstore metadata read waits on sn2's data devices
    # until sn2 aborts on wake and its connections close (F23, c9/c10: 9 s), or,
    # with #219 as run in c11 (devices closed but still online), fails and aborts
    # the survivor (F24: 160 IO errors). Judged on the takeover itself; the
    # client-side detection (F20) is not part of it.
    Scenario("F24-frozen-primary-takeover-read", ("F23", "F24"), hang("sn2", 8.0), aborts_ok=("sn2",),
             grant_within_s=2.6, stall_limit_s=None),
    # c12 02c: the same freeze with F24 fixed. The takeover's first write waits for
    # the append lock on the frozen sn2's JM until sn2 aborts on wake (F25: 6-8 s
    # client stall, 0 IO errors). Judged on the client stall, as the bed's 02c.
    Scenario("F25-frozen-primary-takeover-append-lock", ("F23", "F25"), hang("sn2", 8.0), aborts_ok=("sn2",),
             grant_within_s=2.6),
    # c12b 02c-r1 (intermittent): the same freeze while the survivor's JC reader has
    # a history read in flight on sn2's JM. With F21 and F25 fixed the takeover's
    # reader restart still waits for that read (F26: 7 s client stall, 0 IO errors).
    Scenario("F26-takeover-read-inflight-on-peer-jm", ("F23", "F26"), hang("sn2", 8.0), aborts_ok=("sn2",),
             grant_within_s=2.6, conditions=("reader_read_inflight_on_peer_jm",)),
]

#: Findings the simulator cannot express, and why.
NOT_MODELLED = {
    "F11": "remote JMs not reconnecting after both links healed (c3/08): its root cause was "
           "never identified and it did not recur in c5/c6; the node model reconnects. "
           "08-both-links covers the scenario",
    "F9": "distribs rejecting the cluster map after a reboot: inside ultra's distrib map "
          "handling, below the RPC level the simulator models",
}
