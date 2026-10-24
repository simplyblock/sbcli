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

The live state of a subsystem is not trusted to be consistent: it can allow
any host and list hosts at the same time (``add_host_to_lvol`` lists a host
without switching ``allow_any_host`` off). The plan therefore never adds a
host that is listed already, records every loser it takes off a list in either
mode, and the release gives a loser back only where the database still
authorizes it, with its keys (``controllers.subsystem_access``).

``plan_revoke`` is pure (table-tested); ``revoke`` and ``release`` run against
one node's SPDK and return what they changed, so a release undoes exactly
that. The arbiter calls them on its work pool, never on the decision thread.
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
    #: Hosts that keep access, added before allow_any_host is switched off.
    add: list = field(default_factory=list)
    #: Losers not listed yet, listed only so their removal disconnects them
    #: (SPDK answers remove_host for an unlisted host with an error and
    #: disconnects nothing).
    prelist: list = field(default_factory=list)
    #: Losers listed before the fence; the release gives them back if the
    #: database still authorizes them.
    listed: list = field(default_factory=list)


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


def plan_revoke(subsystems: list, volume_nqns: set, losers, keep: set, connected: dict,
                volume_prefix: str = "", authorized: dict | None = None) -> list:
    """What to change so that no host in ``losers`` reaches a volume subsystem.

    ``subsystems``: ``nvmf_get_subsystems`` of the winner. ``keep``: hosts that
    must keep access (the winner's own client hosts). ``connected``: nqn -> host
    NQNs connected right now; ``authorized``: nqn -> host NQNs the database
    authorizes on it (both only needed for allow-any subsystems, which become
    an allow-list). Idempotent: a subsystem that already excludes every loser
    yields no step."""
    losers = {h for h in losers if h}
    authorized = authorized or {}
    steps = []
    for sub in subsystems:
        nqn = sub.get("nqn", "")
        if not fenceable(nqn, volume_nqns, volume_prefix):
            continue
        hosts = _hosts(sub)
        listed = sorted(hosts & losers)
        if sub.get("allow_any_host"):
            stay = set(keep) | set(connected.get(nqn) or ()) | set(authorized.get(nqn) or ())
            steps.append(Step(nqn, MODE_ANY, add=sorted(stay - losers - {""} - hosts),
                              prelist=sorted(losers - hosts), listed=listed))
        elif listed:
            steps.append(Step(nqn, MODE_LIST, listed=listed))
    return steps


def revoke(rpc, volume_nqns: set, losers, keep: set, volume_prefix: str = "",
           authorized: dict | None = None) -> dict:
    """Apply ``plan_revoke`` on one node. Returns {nqn: {"mode", "added",
    "removed"}} for every subsystem changed; "removed" are the losers that were
    listed before. Raises on the first failed RPC: the caller keeps the fence
    in ``revoking`` and tries again (every step is idempotent)."""
    subsystems = rpc.subsystem_list() or []
    connected: dict = {}
    for sub in subsystems:
        nqn = sub.get("nqn", "")
        if sub.get("allow_any_host") and fenceable(nqn, volume_nqns, volume_prefix):
            ctrls = rpc.nvmf_subsystem_get_controllers(nqn) or []
            connected[nqn] = {c.get("hostnqn", "") for c in ctrls if c.get("hostnqn")}
    changed = {}
    for step in plan_revoke(subsystems, volume_nqns, losers, keep, connected, volume_prefix,
                            authorized):
        if step.mode == MODE_ANY:
            for host in step.add:
                rpc.subsystem_add_host(step.nqn, host)
            rpc.subsystem_allow_any_host(step.nqn, False)
            for host in step.prelist:
                rpc.subsystem_add_host(step.nqn, host)
        for host in sorted(set(step.prelist) | set(step.listed)):
            rpc.subsystem_remove_host(step.nqn, host)
        changed[step.nqn] = {"mode": step.mode, "added": list(step.add),
                             "removed": list(step.listed)}
        logger.warning("Host fence: %s revoked on %s (%s)",
                       ",".join(sorted(set(step.prelist) | set(step.listed))), step.nqn, step.mode)
    return changed


def merge(old: dict, new: dict) -> dict:
    """Fold a later pass's changes into the record of earlier ones, so the
    release undoes every pass: a subsystem converted earlier stays ``any``, and
    the listed losers removed accumulate."""
    out = {k: dict(v) for k, v in (old or {}).items()}
    for nqn, change in (new or {}).items():
        cur = out.get(nqn)
        if cur is None:
            out[nqn] = dict(change)
            continue
        if change.get("mode") == MODE_ANY:
            cur["mode"] = MODE_ANY
            cur["added"] = sorted(set(cur.get("added") or []) | set(change.get("added") or []))
        cur["removed"] = sorted(set(cur.get("removed") or []) | set(change.get("removed") or []))
    return out


def release(rpc, subsystems: dict, readd) -> list:
    """Undo a revocation on one node. ``subsystems`` is the merged record of
    every ``revoke`` pass. ``readd(nqn, host)`` gives a loser that was listed
    back to the subsystem; it decides from the database whether the host is
    still authorized and with which keys, and returns False when it is not
    (removed from the pool meanwhile: it stays out). Converted subsystems get
    allow_any_host back afterward; the hosts added for the fence stay listed,
    which is harmless while any host is allowed (removing them would disconnect
    them). Subsystems that are gone are skipped. Returns the hosts left out as
    "nqn host" strings; raises on the first failed RPC."""
    present = {s.get("nqn", ""): s for s in rpc.subsystem_list() or []}
    left_out = []
    for nqn, change in sorted(subsystems.items()):
        sub = present.get(nqn)
        if sub is None:
            continue
        listed = _hosts(sub)
        for host in change.get("removed") or []:
            if host not in listed and not readd(nqn, host):
                left_out.append("%s %s" % (nqn, host))
        if change.get("mode") == MODE_ANY and not sub.get("allow_any_host"):
            rpc.subsystem_allow_any_host(nqn, True)
    return left_out
