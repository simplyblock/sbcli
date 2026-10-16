"""One simulated two-node storage node: the JC HA state machine and the SPDK
side the control plane drives.

Ported from the node code, rule by rule (references are to ultra main
``bdev_distrib/parts/alg_two_node_ha.cpp`` and the SPDK fork ``lib/lvol/lvol.c``
on ``new_md_journal``):

* per-LVS HA state normal / holding / solo / fenced, hold deadline, lease,
  epoch gate (ESTALE), EBUSY on a grant to a fenced LVS, EAGAIN on an unfence
  while the remote JM is unhealthy, the event ring with seq/instance;
* ``ha_tick``: a holding LVS self-fences on lease expiry or at its hold
  deadline; a lease that merely expires never fences a normal LVS; a gap
  between two ticks longer than the lease TTL is a stall of this node: the
  node aborts (ultra #211, abort-on-wake);
* the remote-JM watchdog: a peer whose SPDK died is seen at once (TCP reset),
  a frozen or cut-off peer after ``remote_jm_timeout_ms``;
* ``spdk_lvs_ha_fence`` / ``spdk_lvs_ha_unfence``: demote, the takeover latch
  (only ``bdev_lvol_set_leader`` clears it), the client-port block with
  ownership (SPDK #76, F8), ANA of the client-port listeners to inaccessible
  and back (#76 point 4).

``bugs`` re-introduces a fixed behaviour for the regression proofs (see
``scenarios.BUGS``); empty on main.
"""

from __future__ import annotations

import errno
import uuid as _uuid
from dataclasses import dataclass, field

NORMAL, HOLDING, SOLO, FENCED = "normal", "holding", "solo", "fenced"

#: Node capabilities: behaviour of SPDK/ultra changes that exist as PRs but are
#: not yet in the node image main deploys. ``DEPLOYED_CAPS`` must match what is
#: merged (SPDK new_md_journal, ultra main), so main's CI stays truthful; a
#: scenario can opt in to a capability to test a pending stack (bed run c8).
#:
#: * ``ana_restore_all_groups`` -- F18, SPDK #80: the unfence restores every ANA
#:   group the fence changed (else only the groups that carry a namespace).
#: * ``grant_stops_redirect`` -- F19, SPDK #81 + ultra #215: every solo grant
#:   releases the LVS, and the release stops a non-leader's hublvol redirect to
#:   the lost peer, so the next IO takes the LVS over at once.
#: * ``reader_skips_dead_peer_jm`` -- F21, ultra (stacked on #215): on a solo LVS
#:   the takeover's JC reader does not use the peer's JM that the HA layer knows
#:   to be dead, instead of waiting for the 8 s NVMe IO timeout on a frozen peer.
CAP_ANA_RESTORE_ALL = "ana_restore_all_groups"
CAP_GRANT_STOPS_REDIRECT = "grant_stops_redirect"
CAP_READER_SKIPS_DEAD_PEER_JM = "reader_skips_dead_peer_jm"
ALL_CAPS = frozenset({CAP_ANA_RESTORE_ALL, CAP_GRANT_STOPS_REDIRECT, CAP_READER_SKIPS_DEAD_PEER_JM})
DEPLOYED_CAPS: frozenset = frozenset()


class NodeAbort(Exception):
    """Raised inside an RPC when the node's SPDK aborts while serving it."""


@dataclass
class LvsHa:
    state: str = NORMAL
    since_ms: int = 0
    hold_deadline_ms: int = 0
    remote_healthy: bool = True
    reason: str = "healthy"


@dataclass
class Lvstore:
    name: str
    vuid: int
    primary_node: str           # uuid of the LVS's primary
    client_port: int
    hub_port: int
    leader: bool = False
    takeover_blocked: bool = False
    fenced: bool = False
    held: bool = False
    fence_port_owned: bool = False
    takeover_until: float = 0.0
    redirect_stopped: bool = False   # F19: hublvol redirect stopped by a grant
    ana_restore_pending: bool = False
    ana_saved: dict = field(default_factory=dict)


@dataclass
class Event:
    seq: int
    jm_vuid: int
    status: str
    reason: str
    ha_state: str
    epoch: int


class SimNode:
    def __init__(self, world, name: str, uuid: str, mgmt_ip: str, rpc_port: int,
                 data_ip: str, own_vuid: int):
        self.world = world
        self.name, self.uuid = name, uuid
        self.mgmt_ip, self.rpc_port, self.data_ip = mgmt_ip, rpc_port, data_ip
        self.own_vuid = own_vuid
        self.bugs: set = set()
        self.caps: frozenset = DEPLOYED_CAPS
        # process: up | frozen | dead (SPDK gone, proxy answers 500) | down (host off)
        self.proc = "up"
        self.frozen_until = 0.0
        self.frozen_since = 0.0
        self.started_at = 0.0
        self.aborts: list[tuple[float, str]] = []
        self.shutting_down = False       # planned stop in progress (CP flow)
        self.detach_until = 0.0          # remote JM of the peer detaching (F17)
        self.relevel_until = 0.0         # re-levelling a returning journal (F1)
        self.lvstores: dict[str, Lvstore] = {}
        self.devices: dict[str, bool] = {}    # alceml bdev -> present
        self.blocked: dict[int, str] = {}     # port -> owner tag
        self.ana: dict[tuple, str] = {}       # (port, grp) -> state
        self._reset_ha()

    # ---- lifecycle ------------------------------------------------------------

    def _reset_ha(self):
        self.instance = _uuid.uuid4().hex[:12]
        self.seq = 0
        self.ring: list[Event] = []
        self.dual_node = False
        self.arbitration = False
        self.hold_ms, self.lease_ttl_ms, self.remote_jm_timeout_ms = 2500, 1500, 1000
        self.epoch_seen = 0
        self.lease_expiry_ms = 0
        self.last_tick_ms = 0
        self.ha: dict[int, LvsHa] = {}
        self.peer_lost_at: float | None = None   # when the remote JM stopped acking

    def peer(self) -> SimNode:
        return self.world.peer_of(self)

    def now_ms(self) -> int:
        return self.world.vt.ms()

    @property
    def up(self) -> bool:
        return self.proc == "up"

    def abort(self, why: str):
        if self.proc in ("dead", "down"):
            return
        self.world.log(f"{self.name}: SPDK abort ({why})")
        self.aborts.append((self.world.vt.time(), why))
        self.proc = "dead"
        for lvs in self.lvstores.values():
            lvs.leader = False
        self.world.on_node_died(self)

    def start(self):
        """SPDK (re)started by the restart flow: new instance, fresh HA state,
        stores recreated as non-leaders, devices back, no port blocks."""
        self.proc = "up"
        self.shutting_down = False
        self.started_at = self.world.vt.time()
        self._reset_ha()
        for v in (self.own_vuid, self.peer().own_vuid):
            self.ha[v] = LvsHa(since_ms=self.now_ms())
        for lvs in self.lvstores.values():
            lvs.leader = False
            lvs.takeover_blocked = False
            lvs.fenced = lvs.held = lvs.fence_port_owned = False
            lvs.ana_saved = {}
            lvs.ana_restore_pending = False
            lvs.redirect_stopped = False
        for d in self.devices:
            self.devices[d] = True
        self.blocked = {}

    # ---- events -----------------------------------------------------------------

    def push_event(self, vuid: int, status: str, reason: str, state: str):
        self.seq += 1
        self.ring.append(Event(self.seq, vuid, status, reason, state, self.epoch_seen))

    # ---- JC: remote-JM health and ha_tick ---------------------------------------

    def on_remote_jm(self, vuid: int, healthy: bool, reason: str):
        lv = self.ha.setdefault(vuid, LvsHa(since_ms=self.now_ms()))
        if lv.remote_healthy == healthy:
            return
        lv.remote_healthy = healthy
        lv.reason = "healthy" if healthy else reason
        if not healthy:
            started = False
            if self.arbitration and lv.state == NORMAL:
                lv.state, lv.since_ms = HOLDING, self.now_ms()
                lv.hold_deadline_ms = lv.since_ms + self.hold_ms
                self._hold(vuid)
                started = True
            self.push_event(vuid, "remote_jm_unhealthy", reason, lv.state)
            if started:
                self.push_event(vuid, "ha_hold_started", reason, lv.state)
        else:
            if lv.state == SOLO:
                lv.state, lv.since_ms = NORMAL, self.now_ms()
            self.push_event(vuid, "remote_jm_healthy", "", lv.state)

    def jc_tick(self):
        """The JC poller: remote-JM health (watchdog) then ha_tick."""
        if not self.up:
            return
        now = self.now_ms()
        # Stall detection (ultra #211): the poller did not run for longer than
        # the lease TTL -- this node was frozen.
        gap = now - self.last_tick_ms if self.last_tick_ms else 0
        stall_limit = self.lease_ttl_ms if self.arbitration else 4000
        if gap > stall_limit:
            if self.arbitration and "stall_fence" in self.bugs:
                # c3/c4 behaviour (F8/F12/F13): fence every LVS, stay alive
                self.last_tick_ms = now
                self.lease_expiry_ms = now
                for v in list(self.ha):
                    lv = self.ha[v]
                    lv.state, lv.since_ms = FENCED, now
                    self._fence(v)
                    self.push_event(v, "ha_self_fenced", "stalled", FENCED)
                # the alceml stuck-IO watchdog counts the freeze as IO age (F12)
                if "alceml_stall" in self.bugs:
                    for d in self.devices:
                        self.devices[d] = False
                return
            self.abort("HA node stalled for %d ms" % gap if self.arbitration
                       else "JC thread was hanging")
            return
        self.last_tick_ms = now
        self._watchdog()
        if not self.arbitration:
            return
        expired = self.lease_expiry_ms != 0 and now >= self.lease_expiry_ms
        for v, lv in self.ha.items():
            if lv.state != HOLDING:
                continue
            if expired:
                lv.state, lv.since_ms = FENCED, now
                self._fence(v)
                self.push_event(v, "ha_lease_expired", lv.reason, FENCED)
                self.push_event(v, "ha_self_fenced", "lease_expired", FENCED)
            elif now >= lv.hold_deadline_ms:
                lv.state, lv.since_ms = FENCED, now
                self._fence(v)
                self.push_event(v, "ha_hold_expired", "no_verdict", FENCED)
                self.push_event(v, "ha_self_fenced", "deadline", FENCED)

    def _watchdog(self):
        """helper_check_jm_health: the peer's JM on the data network."""
        peer = self.peer()
        link = self.world.net.data_ok(self, peer)
        t = self.world.vt.time()
        if peer.proc == "up" and link:
            self.peer_lost_at = None
            # reconnected: in sync after the resync delay. With
            # "jm_needs_reconnect" (the bed's F6/F11 condition) the remote JM
            # comes back only after the control plane reconnects it.
            if "jm_needs_reconnect" in self.world.conditions and                     not self.world.jm_reconnected.get(self.uuid):
                return
            for v in list(self.ha):
                if not self.ha[v].remote_healthy:
                    since = self.world.reconnect_since.get((self.uuid, peer.uuid))
                    if since is None:
                        self.world.reconnect_since[(self.uuid, peer.uuid)] = t
                    elif t - since >= self.world.resync_s:
                        self.on_remote_jm(v, True, "")
            return
        self.world.reconnect_since.pop((self.uuid, peer.uuid), None)
        if not link:
            self.world.jm_reconnected[self.uuid] = False
        if self.peer_lost_at is None:
            self.peer_lost_at = t
        if peer.proc in ("dead", "stopped", "restarting") and link:
            delay, reason = 0.05, "unavailable"     # connection reset
        else:
            # frozen peer (writes not acked) or cut link
            delay = self.remote_jm_timeout_ms / 1000.0
            if "slow_frozen_detect" in self.bugs:
                delay = 5.0                         # c2 F5: KATO only
            reason = "unavailable"
        if t - self.peer_lost_at >= delay:
            for v in list(self.ha):
                if self.ha[v].remote_healthy:
                    self.on_remote_jm(v, False, reason)

    # ---- SPDK lvol side ------------------------------------------------------------

    def lvs_by_vuid(self, vuid: int) -> Lvstore | None:
        return next((s for s in self.lvstores.values() if s.vuid == vuid), None)

    def _hold(self, vuid: int):
        lvs = self.lvs_by_vuid(vuid)
        if lvs is not None and not lvs.fenced:
            lvs.held = True

    def _release(self, vuid: int):
        lvs = self.lvs_by_vuid(vuid)
        if lvs is not None:
            lvs.held = False
            # SPDK #81 (F19): a non-leader, non-primary LVS stops redirecting to the
            # lost peer (skip_redirecting + drain_in_action)
            if (CAP_GRANT_STOPS_REDIRECT in self.caps and not lvs.leader
                    and lvs.primary_node != self.uuid):
                lvs.redirect_stopped = True

    def _fence(self, vuid: int):
        """spdk_lvs_ha_fence."""
        lvs = self.lvs_by_vuid(vuid)
        if lvs is None:
            return
        if not lvs.fenced:
            port = lvs.client_port
            other = any(o is not lvs and o.fenced and o.fence_port_owned and o.client_port == port
                        for o in self.lvstores.values())
            lvs.fence_port_owned = port not in self.blocked or other
            if port not in self.blocked:
                self.blocked[port] = "fence"
            if lvs.fence_port_owned and not other:
                for key in [k for k in self.ana if k[0] == port]:
                    lvs.ana_saved[key] = self.ana[key]
                    self.ana[key] = "inaccessible"
        lvs.fenced, lvs.held = True, False
        lvs.leader = False
        lvs.takeover_blocked = True
        self.world.on_fence(self, lvs)

    def _unfence(self, vuid: int):
        """spdk_lvs_ha_unfence."""
        lvs = self.lvs_by_vuid(vuid)
        if lvs is None or not lvs.fenced:
            return
        lvs.fenced = False
        port = lvs.client_port
        still = any(o is not lvs and o.fenced and o.client_port == port for o in self.lvstores.values())
        if lvs.fence_port_owned and not still and "unfence_keeps_ports" not in self.bugs:
            self.blocked.pop(port, None)
        if lvs.fence_port_owned and not still and self.blocked.get(port) == "fence":
            # SPDK #76 contract (campaign c3, F8): the unfence lifts its fence's block
            self.world.violate("%s: unfence of %s left its fence's block on port %s"
                               % (self.name, lvs.name, port))
        lvs.fence_port_owned = False
        if lvs.ana_saved:
            if lvs.primary_node == self.uuid:
                lvs.ana_restore_pending = True      # waits for the CP's set_leader
            else:
                self._restore_ana(lvs)

    def _restore_ana(self, lvs: Lvstore):
        """Put back the ANA state the fence saved. As observed on the bed (c7b,
        F18), only the groups that carry a namespace come back; an ANA group
        without a namespace stays inaccessible. With the F18 fix (capability
        ``ana_restore_all_groups``) every group comes back."""
        for key, state in lvs.ana_saved.items():
            if (CAP_ANA_RESTORE_ALL in self.caps
                    or key[1] in self.world.namespace_groups(key[0])):
                self.ana[key] = state
        lvs.ana_saved = {}

    # ---- RPC dispatch -------------------------------------------------------------

    def rpc(self, method: str, params: dict):
        handler = getattr(self, "rpc_" + method, None)
        if handler is None:
            raise NotImplementedError(method)
        return handler(**params)

    @staticmethod
    def _err(code: int, msg: str = ""):
        from simplyblock_core.rpc_client import RPCRemoteError
        raise RPCRemoteError(msg or errno.errorcode.get(code, str(code)), -code)

    def rpc_rpc_get_methods(self, **_):
        from simplyblock_core import constants
        return list(constants.TWO_NODE_ARBITRATION_RPCS) + ["bdev_lvol_get_lvstores"]

    def rpc_jc_set_dual_node(self, enable, hold_ms=None, lease_ttl_ms=None, arbitration=None,
                             remote_jm_timeout_ms=None, **_):
        self.dual_node = bool(enable)
        self.arbitration = bool(enable) and bool(arbitration)
        if hold_ms:
            self.hold_ms = int(hold_ms)
        if lease_ttl_ms:
            self.lease_ttl_ms = int(lease_ttl_ms)
        if remote_jm_timeout_ms:
            self.remote_jm_timeout_ms = int(remote_jm_timeout_ms)
        if self.remote_jm_timeout_ms >= self.hold_ms:
            self.remote_jm_timeout_ms = max(1, self.hold_ms // 2)
        self.last_tick_ms = 0
        return True

    def _verdict(self, epoch, lvs=None, need_lvs=False):
        if need_lvs and not lvs:
            self._err(errno.EINVAL)
        if not self.arbitration:
            self._err(errno.EINVAL)
        if int(epoch) < self.epoch_seen:
            self._err(errno.ESTALE)
        self.epoch_seen = int(epoch)
        for v in lvs or []:
            if int(v) not in self.ha:
                self._err(errno.ENOENT)

    def _most_severe(self) -> str:
        order = {NORMAL: 0, SOLO: 1, HOLDING: 2, FENCED: 3}
        return max((lv.state for lv in self.ha.values()), key=lambda s: order[s], default=NORMAL)

    def rpc_jc_lease_renew(self, epoch, ttl_ms=0, **_):
        self._verdict(epoch)
        now = self.now_ms()
        self.lease_expiry_ms = now + int(ttl_ms or self.lease_ttl_ms)
        return {"epoch_seen": self.epoch_seen, "state": self._most_severe(),
                "lease_left_ms": self.lease_expiry_ms - now}

    def rpc_jc_grant_solo(self, epoch, lvs, ttl_ms=0, **_):
        self._verdict(epoch, lvs, True)
        if any(self.ha[int(v)].state == FENCED for v in lvs):
            self._err(errno.EBUSY)
        now = self.now_ms()
        self.lease_expiry_ms = now + int(ttl_ms or self.lease_ttl_ms)
        for v in lvs:
            lv = self.ha[int(v)]
            if lv.state == SOLO:
                continue
            was_holding = lv.state == HOLDING
            lv.state, lv.since_ms = SOLO, now
            # ultra #215 (F19): release on every grant, not only after a hold
            if was_holding or CAP_GRANT_STOPS_REDIRECT in self.caps:
                self._release(int(v))
            self.push_event(int(v), "ha_solo", "grant", SOLO)
        self.world.on_grant(self, [int(v) for v in lvs])
        return {"granted": [int(v) for v in lvs]}

    def rpc_jc_fence(self, epoch, lvs, **_):
        self._verdict(epoch, lvs, True)
        for v in lvs:
            lv = self.ha[int(v)]
            if lv.state != FENCED:
                self.push_event(int(v), "ha_fenced", "verdict", FENCED)
            lv.state, lv.since_ms = FENCED, self.now_ms()
            self._fence(int(v))
        return {"fenced": [int(v) for v in lvs]}

    def rpc_jc_unfence(self, epoch, lvs, **_):
        self._verdict(epoch, lvs, True)
        if any(not self.ha[int(v)].remote_healthy for v in lvs):
            self._err(errno.EAGAIN)
        for v in lvs:
            lv = self.ha[int(v)]
            if lv.state == FENCED:
                self.push_event(int(v), "ha_unfenced", "verdict", NORMAL)
            lv.state, lv.since_ms = NORMAL, self.now_ms()
        for v in lvs:
            self._unfence(int(v))
        return {"unfenced": [int(v) for v in lvs]}

    def ha_status(self) -> dict:
        now = self.now_ms()
        return {
            "instance": self.instance, "seq": self.seq, "dual_node": self.dual_node,
            "arbitration": self.arbitration, "remote_jm_timeout_ms": self.remote_jm_timeout_ms,
            "epoch": self.epoch_seen,
            "lease_left_ms": max(0, self.lease_expiry_ms - now),
            "lvs": [{"jm_vuid": v, "state": lv.state,
                     "since_ms": max(0, now - lv.since_ms),
                     "hold_left_ms": max(0, lv.hold_deadline_ms - now) if lv.state == HOLDING else 0,
                     "remote_jm": {"name": "remote_jm_" + self.peer().uuid,
                                   "health": "healthy" if lv.remote_healthy else "unhealthy",
                                   "reason": lv.reason}}
                    for v, lv in sorted(self.ha.items())],
        }

    def rpc_jc_ha_status(self, **_):
        return self.ha_status()

    def rpc_jc_explicit_synchronization(self, jm_vuid, **_):
        return {"res": 1}

    def rpc_jc_compression(self, jm_vuid=None, get_status=False, **_):
        # The leader re-levels a returning journal on its own (B1 auto-resync);
        # while it does, jc_compression answers "Compression request failed".
        if self.world.vt.time() < self.relevel_until:
            self._err(errno.EIO, "Compression request failed")
        return False

    def rpc_bdev_lvol_get_lvstores(self, lvs_name=None, **_):
        lvs = self.lvstores.get(lvs_name)
        if lvs is None:
            self._err(errno.ENODEV)
        return [{"name": lvs.name, "lvs leadership": lvs.leader, "lvs_primary": lvs.primary_node == self.uuid}]

    def rpc_bdev_lvol_update_lvstore(self, lvs_name=None, uuid=None, **_):
        lvs = self.lvstores.get(lvs_name)
        if lvs is None:
            self._err(errno.ENODEV)
        if lvs.leader:
            self._err(errno.EINVAL, "update on a leader")
        # F17 (c7b 04): on a planned stop of the peer, the survivor's forced
        # takeover reads the journal while the peer's remote JM is detaching;
        # the reader hits the detached JM while the LVS is solo, and
        # ha_jc_error_expected covers only holding/fenced: JCERR -> abort.
        if self.world.vt.time() < self.detach_until and \
                any(lv.state == SOLO for lv in self.ha.values()):
            self.abort("JCERR: takeover reader hit a detached remote JM while solo")
            raise NodeAbort("node aborted")
        return True

    def rpc_bdev_lvol_set_leader_all(self, lvs_name=None, lvs_leadership=False, uuid=None, **_):
        lvs = self.lvstores.get(lvs_name)
        if lvs is None:
            self._err(errno.ENODEV)
        if lvs_leadership:
            lvs.leader = True
            lvs.takeover_blocked = False
            lvs.redirect_stopped = False
            if lvs.ana_restore_pending:
                self._restore_ana(lvs)
                lvs.ana_restore_pending = False
            self.world.on_leader(self, lvs, "set_leader")
        else:
            lvs.leader = False
            lvs.redirect_stopped = False     # set_leader resets the redirect state
            lv = self.ha.get(lvs.vuid)
            if lv is not None and lv.state == SOLO:    # ha_on_demoted
                lv.state, lv.since_ms = NORMAL, self.now_ms()
                self.push_event(lvs.vuid, "ha_normal", "demoted", NORMAL)
        return True

    def rpc_nvmf_subsystem_listener_set_ana_state(self, nqn=None, listen_address=None,
                                                  ana_state=None, anagrpid=None, **_):
        port = int((listen_address or {}).get("trsvcid", 0))
        groups = [int(anagrpid)] if anagrpid else sorted({g for (p, g) in self.ana if p == port}) or [1]
        for g in groups:
            if self.ana.get((port, g)) != ana_state:
                self.world.ana_changed[(self.uuid, port, g)] = self.world.vt.time()
            self.ana[(port, g)] = ana_state
        return True

    def rpc_nvmf_port_block(self, port, reject=False, **_):
        self.blocked.setdefault(int(port), "cp")
        return True

    def rpc_nvmf_port_unblock(self, port, **_):
        self.blocked.pop(int(port), None)
        for lvs in self.lvstores.values():
            if lvs.client_port == int(port):
                lvs.fence_port_owned = False
        return True

    def rpc_nvmf_get_blocked_ports(self, **_):
        return {"total_blocked_ports": len(self.blocked),
                "blocked_ports": [{"port": p, "is_reject": False} for p in sorted(self.blocked)]}

    def rpc_distr_status_events_update(self, **_):
        return True

    def rpc_bdev_get_bdevs(self, name=None, **_):
        if name in self.devices:
            if not self.devices[name]:
                self._err(errno.ENODEV)
            return [{"name": name}]
        self._err(errno.ENODEV)
