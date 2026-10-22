"""Host fencing: revoke a lost node's client host on the winner.

docs/design/two-node-kubernetes-fencing.md, sections 2 and 3.2. A fence of an
LVS blocks only the loser's own client port; the loser's host keeps its NVMe
paths to the winner, which accepts any host while it runs alone. Before
Kubernetes may start a second instance of the loser's VMs (out-of-service
taint), every kernel host NQN of the loser is taken off every volume subsystem
on the winner, and its live controllers there are disconnected.

A Kubernetes node presents more than one host NQN: the kernel default
(``/etc/nvme/hostnqn``) for plain volumes, and the simplyblock form derived
from the node's UID for access-controlled volumes. All of them are revoked.

``plan_revoke`` is pure (table-tested); ``revoke`` and ``release`` run the plan
against one node's SPDK and return what they changed, so a release undoes
exactly that. The arbiter calls them on its work pool, never on the decision
thread.
"""

from dataclasses import dataclass, field

from simplyblock_core import utils

logger = utils.get_logger(__name__)

#: Subsystems that are never touched: the returning peer's SPDK needs its
#: hublvol subsystem to resync (campaign c1, B2).
HUBLVOL_MARK = ":hublvol:"

MODE_ANY = "any"    # was allow_any_host: converted to an allow-list
MODE_LIST = "list"  # had an allow-list: the losers listed in it were removed


@dataclass
class Step:
    nqn: str
    mode: str
    #: Hosts added to the allow-list before allow_any_host is switched off.
    add: list = field(default_factory=list)
    #: Loser hosts taken off the subsystem.
    remove: list = field(default_factory=list)


def _hosts(subsystem: dict) -> set:
    return {h.get("nqn", "") for h in subsystem.get("hosts") or [] if h.get("nqn")}


def fenceable(nqn: str, volume_nqns: set, volume_prefix: str = "") -> bool:
    """A volume subsystem: listed in the DB, or named like a volume of the
    cluster (``<cluster nqn>:lvol:``). Both, so that neither a DB read that
    misses a volume nor a subsystem named otherwise (namespaced subsystems)
    leaves the loser admitted while the fence reports ``revoked``."""
    if not nqn or HUBLVOL_MARK in nqn:
        return False
    return nqn in volume_nqns or bool(volume_prefix and nqn.startswith(volume_prefix))


def plan_revoke(subsystems: list, volume_nqns: set, losers, keep: set,
                connected: dict, volume_prefix: str = "") -> list:
    """What to change so that no host in ``losers`` reaches a volume subsystem.

    ``subsystems``: ``nvmf_get_subsystems`` of the winner. ``keep``: hosts that
    must keep access (the winner's own client hosts). ``connected``: nqn -> host
    NQNs connected right now (only needed for allow-any subsystems). Idempotent:
    a subsystem that already excludes every loser yields no step."""
    losers = {h for h in losers if h}
    steps = []
    for sub in subsystems:
        nqn = sub.get("nqn", "")
        if not fenceable(nqn, volume_nqns, volume_prefix):
            continue
        hosts = _hosts(sub)
        if sub.get("allow_any_host"):
            add = sorted((set(keep) | set(connected.get(nqn) or ())) - losers - {""} - hosts)
            steps.append(Step(nqn, MODE_ANY, add, sorted(losers)))
        elif hosts & losers:
            steps.append(Step(nqn, MODE_LIST, [], sorted(hosts & losers)))
    return steps


def revoke(rpc, volume_nqns: set, losers, keep: set, volume_prefix: str = "") -> dict:
    """Apply ``plan_revoke`` on one node. Returns {nqn: {"mode", "added",
    "removed"}} for every subsystem changed. Raises on the first failed RPC:
    the caller keeps the fence in ``revoking`` and tries again (every step is
    idempotent)."""
    subsystems = rpc.subsystem_list() or []
    connected: dict = {}
    for sub in subsystems:
        nqn = sub.get("nqn", "")
        if sub.get("allow_any_host") and fenceable(nqn, volume_nqns, volume_prefix):
            ctrls = rpc.nvmf_subsystem_get_controllers(nqn) or []
            connected[nqn] = {c.get("hostnqn", "") for c in ctrls if c.get("hostnqn")}
    changed = {}
    for step in plan_revoke(subsystems, volume_nqns, losers, keep, connected, volume_prefix):
        if step.mode == MODE_ANY:
            for host in step.add:
                rpc.subsystem_add_host(step.nqn, host)
            rpc.subsystem_allow_any_host(step.nqn, False)
            # SPDK answers remove_host for a host that is not listed with an
            # error and disconnects nothing; listed first, the remove cuts the
            # loser's live controllers.
            for host in step.remove:
                rpc.subsystem_add_host(step.nqn, host)
        for host in step.remove:
            rpc.subsystem_remove_host(step.nqn, host)
        changed[step.nqn] = {"mode": step.mode, "added": list(step.add),
                             "removed": list(step.remove) if step.mode == MODE_LIST else []}
        logger.warning("Host fence: %s revoked on %s (%s)", ",".join(step.remove), step.nqn,
                       step.mode)
    return changed


def merge(old: dict, new: dict) -> dict:
    """Fold a later pass's changes into the record of earlier ones, so the
    release undoes every pass: a subsystem converted earlier stays ``any``,
    and the losers removed from an allow-list accumulate."""
    out = {k: dict(v) for k, v in (old or {}).items()}
    for nqn, change in (new or {}).items():
        cur = out.get(nqn)
        if cur is None or change.get("mode") == MODE_ANY:
            out[nqn] = dict(change)
            continue
        if cur.get("mode") == MODE_LIST:
            cur["removed"] = sorted(set(cur.get("removed") or []) | set(change.get("removed") or []))
    return out


def release(rpc, subsystems: dict, readd) -> list:
    """Undo a revocation on one node. ``subsystems`` is the merged record of
    every ``revoke`` pass; ``readd(nqn, host)`` gives a removed loser back to
    an allow-list subsystem with its keys (``host_auth``). Hosts added to a
    converted subsystem stay listed: harmless with allow_any_host on, and
    removing them would disconnect them. Subsystems that are gone are skipped.
    Returns the nqns done; raises on the first failed RPC."""
    present = {s.get("nqn", ""): s for s in rpc.subsystem_list() or []}
    done = []
    for nqn, change in sorted(subsystems.items()):
        sub = present.get(nqn)
        if sub is None:
            done.append(nqn)
            continue
        if change.get("mode") == MODE_ANY:
            if not sub.get("allow_any_host"):
                rpc.subsystem_allow_any_host(nqn, True)
        else:
            listed = _hosts(sub)
            for host in change.get("removed") or []:
                if host not in listed:
                    readd(nqn, host)
        done.append(nqn)
    return done
