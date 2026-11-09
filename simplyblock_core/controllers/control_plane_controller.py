"""The stretched control plane: one control plane on each of a cluster's two
sites over one FoundationDB, one of them leading (docs/stretched-control-plane.md).

Everything here revolves around one record, the :class:`ControlPlaneLease` of
a cluster, and one question every service and the API ask of it: *does this
control plane lead?*

- :func:`local_site` is the site this process runs in.
- :func:`services_enabled` answers the question for the background services;
  :func:`wait_until_services_enabled` is the one call every service loop makes
  at the top of each cycle, and :func:`api_verdict` the one the web app's
  middleware makes per request.
- :func:`lease_agent_forever` is the agent (a thread of the mgmt-node monitor)
  that renews, drains, releases and acquires the lease so that it follows
  ``Cluster.leader_site``: the site an operator names leads, and nothing moves
  the lease on its own. :func:`plan_step` is its decision, pure and tested.
- :func:`move_leader_site` is the planned move with its readiness gates,
  :func:`takeover` the unplanned one after an operator confirmed the leader's
  site down, :func:`control_plane_status` what both read and what the API shows.

A cluster that is not stretched has no lease: every answer here is "lead",
and the services behave exactly as they always did.
"""
import datetime
import json
import logging
import os
import socket
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Literal, NamedTuple

from simplyblock_core import constants, utils
from simplyblock_core.controllers import cluster_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import (
    ControlPlaneError,
    ControlPlaneNotReadyError,
    ControlPlaneTakeoverRefusedError,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.control_plane_lease import ControlPlaneLease
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

Mode = Literal["single", "leader", "standby", "draining", "disconnected"]

#: Lease ``state`` values a running service set is allowed under.
_SERVING_STATES = (ControlPlaneLease.STATE_HELD,)


# ---------------------------------------------------------------- identity --

def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _iso(dt: datetime.datetime) -> str:
    return dt.isoformat()


def _parse(ts: str) -> datetime.datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.datetime.fromisoformat(ts)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.UTC)
    return dt


_local_site_cache: dict[str, str] = {}


def local_host() -> str:
    return socket.gethostname()


def _host_candidates() -> list[str]:
    """Names this process's host may be registered under as a MgmtNode: the
    Swarm node's hostname handed in through the service template
    (``NODE_HOSTNAME``, see docker-compose-swarm.yml), then the process's
    own hostname; each also as its short form, which is what ``hostname -s``
    stored at ``control-plane add``."""
    names: list[str] = []
    for raw in (os.environ.get(constants.CONTROL_PLANE_NODE_HOSTNAME_ENV, ""), local_host()):
        raw = (raw or "").strip()
        if not raw:
            continue
        for name in (raw, raw.split(".")[0]):
            if name and name not in names:
                names.append(name)
    return names


def local_site() -> str:
    """The site this control plane runs in: the ``SB_CONTROL_PLANE_SITE``
    environment variable (the operator sets it on every control-plane pod),
    or the ``site`` of this host's MgmtNode record (a Docker management
    node, found by the Swarm node hostname the service template passes in).
    "" when neither knows, which a stretched cluster treats as "no site":
    such a control plane never leads."""
    env = os.environ.get(constants.CONTROL_PLANE_SITE_ENV, "").strip()
    if env:
        return env
    if "site" in _local_site_cache:
        return _local_site_cache["site"]
    site = ""
    db = DBController()
    for name in _host_candidates():
        try:
            site = db.get_mgmt_node_by_hostname(name).site or ""
        except Exception as e:  # noqa: BLE001 - identity lookup, not a flow
            logger.debug(f"no MgmtNode record for host {name}: {e}")
            continue
        if site:
            break
    if site:
        _local_site_cache["site"] = site
    return site


def reset_identity_cache() -> None:
    """Tests only."""
    _local_site_cache.clear()
    _mode_cache.clear()


# ------------------------------------------------------------- the lease --

def lease_ttl_sec(cluster: Cluster) -> int:
    return int(cluster.control_plane_lease_ttl_sec or constants.CONTROL_PLANE_LEASE_TTL_SEC)


def lease_expired(lease: ControlPlaneLease | None, now: datetime.datetime | None = None) -> bool:
    if lease is None:
        return True
    expires = _parse(lease.expires_at)
    if expires is None:
        return True
    return expires <= (now or _now())


class AgentStep(NamedTuple):
    """What the lease agent of ``local`` should do this cycle."""
    action: Literal["acquire", "renew", "drain", "release", "wait", "none"]
    reason: str


def plan_step(*, local: str, desired: str, lease: ControlPlaneLease | None,
              now: datetime.datetime, drain_sec: int) -> AgentStep:
    """The agent's decision, pure.

    ``local`` is the agent's site, ``desired`` the cluster's ``leader_site``.
    The rules, in order:

    - no lease, an expired one or a released one: the desired site acquires,
      everyone else waits;
    - the lease is ours and we are desired: renew (a drain that was reverted
      by moving ``leader_site`` back is renewed into ``held`` the same way);
    - the lease is ours and another site is desired: drain first, release
      once the drain period has passed (the services have observed the drain
      and stopped by then);
    - another site holds an unexpired lease: wait, whatever ``desired`` says.
      The holder drains and releases on its own when it reads the new
      ``leader_site``; if it cannot (it is gone), the lease expires and the
      first rule applies - and until then nobody else may serve.
    """
    if not local:
        return AgentStep("none", "this control plane has no site")
    if lease is None:
        return (AgentStep("acquire", "no lease yet") if local == desired
                else AgentStep("wait", f"no lease yet; {desired or '-'} should lead"))
    if lease.state == ControlPlaneLease.STATE_RELEASED:
        return (AgentStep("acquire", f"lease released ({lease.released_by or 'handover'})")
                if local == desired
                else AgentStep("wait", f"lease released; {desired or '-'} should lead"))
    if lease_expired(lease, now):
        return (AgentStep("acquire", f"lease of {lease.holder_site} expired")
                if local == desired
                else AgentStep("wait", f"lease of {lease.holder_site} expired; {desired or '-'} should lead"))
    if lease.holder_site == local:
        if desired == local:
            return AgentStep("renew", "held")
        if lease.state == ControlPlaneLease.STATE_HELD:
            return AgentStep("drain", f"leader_site moved to {desired}")
        since = _parse(lease.state_since)
        if since is None or (now - since).total_seconds() >= drain_sec:
            return AgentStep("release", f"drained; handing over to {desired}")
        return AgentStep("wait", f"draining for {desired}")
    return AgentStep("wait", f"{lease.holder_site} holds the lease")


def _write_lease(db: DBController, cluster: Cluster, lease: ControlPlaneLease | None,
                 step: AgentStep, *, site: str, host: str) -> ControlPlaneLease | None:
    """Apply ``step`` to the lease record with a compare-and-set on its epoch
    and state, so a concurrent agent (the other site, a second management
    node of this one) never overwrites a change it did not see."""
    now = _now()
    ttl = lease_ttl_sec(cluster)
    expected_epoch = lease.epoch if lease is not None else 0
    expected_state = lease.state if lease is not None else ""

    if lease is None:
        fresh = ControlPlaneLease()
        fresh.uuid = cluster.get_id()
        fresh.cluster_id = cluster.get_id()
        fresh.holder_site = site
        fresh.holder_host = host
        fresh.epoch = 1
        fresh.state = ControlPlaneLease.STATE_HELD
        fresh.expires_at = _iso(now + datetime.timedelta(seconds=ttl))
        fresh.state_since = _iso(now)
        fresh.updated_at = _iso(now)
        # No record to compare against: the first write of a cluster's lease.
        # A second site racing here loses on the Unique key of the record
        # itself (same uuid), and reads the winner next cycle.
        fresh.write_to_db(db.kv_store)
        return fresh

    def mutate(current: ControlPlaneLease):
        if current.epoch != expected_epoch or current.state != expected_state:
            return False  # someone else moved it; re-plan next cycle
        if step.action == "acquire":
            current.holder_site = site
            current.holder_host = host
            current.epoch = current.epoch + 1
            current.state = ControlPlaneLease.STATE_HELD
            current.state_since = _iso(now)
            current.released_by = ""
        elif step.action == "renew":
            if current.holder_site != site:
                return False
            current.holder_host = host
            if current.state != ControlPlaneLease.STATE_HELD:
                current.state = ControlPlaneLease.STATE_HELD
                current.state_since = _iso(now)
        elif step.action == "drain":
            if current.holder_site != site:
                return False
            current.state = ControlPlaneLease.STATE_DRAINING
            current.state_since = _iso(now)
        elif step.action == "release":
            if current.holder_site != site:
                return False
            current.state = ControlPlaneLease.STATE_RELEASED
            current.state_since = _iso(now)
            current.released_by = "handover"
        else:
            return False
        if step.action in ("acquire", "renew", "drain"):
            current.expires_at = _iso(now + datetime.timedelta(seconds=ttl))
        current.updated_at = _iso(now)
        return None

    return db.atomic_update(lease, mutate)


def release_lease(cluster_id: str, *, released_by: str) -> ControlPlaneLease | None:
    """Release the lease on the holder's behalf (a takeover) with a new epoch,
    so the holder - if it is still alive somewhere - fails its next renewal
    and stops, and the desired site's agent acquires."""
    db = DBController()
    lease = db.get_control_plane_lease(cluster_id)
    now = _now()
    if lease is None:
        return None

    def mutate(current: ControlPlaneLease):
        current.epoch = current.epoch + 1
        current.state = ControlPlaneLease.STATE_RELEASED
        current.state_since = _iso(now)
        current.released_by = released_by
        current.updated_at = _iso(now)

    return db.atomic_update(lease, mutate)


# ------------------------------------------------------------ the agent --

class _AgentState:
    def __init__(self):
        self.last_step: dict[str, str] = {}
        self.failures = 0
        self.disconnected = False


def _agent_cycle(db: DBController, state: _AgentState, site: str, host: str) -> None:
    clusters = [c for c in db.get_clusters() if c.stretched_control_plane]
    now = _now()
    for cluster in clusters:
        cid = cluster.get_id()
        lease = db.get_control_plane_lease(cid)
        step = plan_step(local=site, desired=cluster.leader_site, lease=lease, now=now,
                         drain_sec=constants.CONTROL_PLANE_DRAIN_SEC)
        key = f"{step.action}:{step.reason}"
        if state.last_step.get(cid) != key:
            level = logging.INFO if step.action != "wait" else logging.DEBUG
            logger.log(level, f"cluster {cid}: lease agent of site {site!r}: {step.action} ({step.reason})")
            state.last_step[cid] = key
        if step.action in ("wait", "none"):
            continue
        written = _write_lease(db, cluster, lease, step, site=site, host=host)
        if written is None:
            logger.debug(f"cluster {cid}: lease changed under the agent; re-planning next cycle")
            continue
        if step.action == "acquire":
            cluster_events.control_plane_leader_change(
                cluster, site, lease.holder_site if lease else "", how=step.reason)
        elif step.action == "release":
            cluster_events.control_plane_leader_change(
                cluster, "", site, how="handover")


def lease_agent_forever(interval: float | None = None) -> None:
    """The lease agent: runs in a thread of the mgmt-node monitor on every
    management node. Harmless on a deployment without stretched clusters
    (nothing to do each cycle)."""
    db = DBController()
    state = _AgentState()
    host = local_host()
    interval = interval or constants.CONTROL_PLANE_LEASE_RENEW_SEC
    logger.info(f"Starting control-plane lease agent on {host}")
    while True:
        site = local_site()
        try:
            _agent_cycle(db, state, site, host)
            if state.disconnected:
                logger.info("control-plane lease agent: FoundationDB answers again")
            state.failures = 0
            state.disconnected = False
        except Exception as e:  # noqa: BLE001 - the agent must outlive FDB outages
            state.failures += 1
            if state.failures >= constants.CONTROL_PLANE_DISCONNECT_FAILURES and not state.disconnected:
                state.disconnected = True
                logger.error(f"control-plane lease agent: FoundationDB unavailable "
                             f"({state.failures} failures): this site is DISCONNECTED; "
                             f"last error: {e}")
            else:
                logger.warning(f"control-plane lease agent cycle failed: {e}")
        time.sleep(interval)


# ----------------------------------------------------- mode for services --

@dataclass
class ModeVerdict:
    mode: Mode
    reason: str
    #: The stretched clusters this control plane is not leading, with the
    #: leader's API endpoint where known ("" otherwise).
    redirects: dict[str, str] = field(default_factory=dict)


_mode_cache: dict[str, Any] = {}
_mode_lock = threading.Lock()


def _compute_mode() -> ModeVerdict:
    db = DBController()
    if db.kv_store is None:
        # No database configured (the unit tier's stub, a tool run outside a
        # control plane): nothing is stretched as far as this process knows.
        return ModeVerdict("single", "no database configured")
    site = local_site()
    now = _now()
    clusters = [c for c in db.get_clusters() if c.stretched_control_plane]
    if not clusters:
        return ModeVerdict("single", "no stretched cluster")
    if not site:
        return ModeVerdict("standby", "this control plane has no site",
                           redirects={c.get_id(): "" for c in clusters})
    redirects: dict[str, str] = {}
    draining = False
    for cluster in clusters:
        lease = db.get_control_plane_lease(cluster.get_id())
        if (lease is not None and lease.holder_site == site
                and lease.state == ControlPlaneLease.STATE_HELD and not lease_expired(lease, now)):
            continue
        if lease is not None and lease.holder_site == site and lease.state == ControlPlaneLease.STATE_DRAINING:
            draining = True
        leader = lease.holder_site if (lease is not None and lease.state != ControlPlaneLease.STATE_RELEASED
                                       and not lease_expired(lease, now)) else ""
        redirects[cluster.get_id()] = cluster.site_api_endpoints.get(leader, "") if leader else ""
    if not redirects:
        return ModeVerdict("leader", f"site {site} holds the lease")
    if draining:
        return ModeVerdict("draining", f"site {site} is handing the lease over", redirects=redirects)
    return ModeVerdict("standby", f"site {site} does not hold the lease", redirects=redirects)


def current_mode() -> ModeVerdict:
    """This control plane's mode, cached for CONTROL_PLANE_MODE_CACHE_SEC.
    Raises when FoundationDB does not answer (the caller decides what
    "disconnected" means for it)."""
    with _mode_lock:
        cached = _mode_cache.get("verdict")
        at = _mode_cache.get("at", 0.0)
        if cached is not None and time.monotonic() - at < constants.CONTROL_PLANE_MODE_CACHE_SEC:
            return cached
    verdict = _compute_mode()
    with _mode_lock:
        _mode_cache["verdict"] = verdict
        _mode_cache["at"] = time.monotonic()
    return verdict


def services_enabled() -> tuple[bool, str]:
    """Whether this control plane's background services may run: on a
    deployment without stretched clusters always; on one with them only
    while this site holds every stretched cluster's lease. A FoundationDB
    failure is "disconnected", and services stay paused: a site that lost
    the database's majority must not act."""
    try:
        verdict = current_mode()
    except Exception as e:  # noqa: BLE001 - a paused service, not a crash
        return False, f"disconnected: {e}"
    return verdict.mode in ("single", "leader"), f"{verdict.mode}: {verdict.reason}"


def wait_until_services_enabled(who: str, poll: float = 3.0) -> None:
    """Block while this control plane must not run its services. Every
    service loop calls this at the top of each cycle; a leader passes
    through at the cost of one cached lease read.

    A FoundationDB error propagates, as the services' own database calls
    do: the process exits and the orchestrator restarts it with a fresh
    client, which on a site that lost the database's majority means it keeps
    restarting - and never acts - until FoundationDB answers again."""
    paused_for = ""
    while True:
        verdict = current_mode()
        if verdict.mode in ("single", "leader"):
            if paused_for:
                logger.info(f"{who}: control plane leads again ({verdict.reason}); resuming")
            return
        reason = f"{verdict.mode}: {verdict.reason}"
        if reason != paused_for:
            logger.info(f"{who}: paused, this control plane does not lead ({reason})")
            paused_for = reason
        time.sleep(poll)


class ApiVerdict(NamedTuple):
    #: "serve", "redirect" or "unavailable".
    action: str
    location: str = ""
    message: str = ""


_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})
#: Paths a standby or disconnected control plane always answers itself.
_ALWAYS_LOCAL = ("/_meta", "/api/v2/_meta", "/api/v2/metrics", "/api/v1/metrics")


def api_verdict(method: str, path: str, cluster_id: str = "") -> ApiVerdict:
    """What the web app does with a request: serve it, redirect it (307) to
    the leader's API, or answer 503. Reads are served locally everywhere -
    FoundationDB never serves stale data - except when the database does not
    answer at all. ``cluster_id`` (parsed from the path by the caller) picks
    the stretched cluster when there are several."""
    if path.startswith(_ALWAYS_LOCAL) or "/control-plane" in path:
        return ApiVerdict("serve")
    try:
        verdict = current_mode()
    except Exception as e:  # noqa: BLE001 - the API answers 503, not 500
        return ApiVerdict("unavailable", message=f"control plane disconnected from FoundationDB: {e}")
    if verdict.mode in ("single", "leader") or method.upper() not in _MUTATING:
        return ApiVerdict("serve")
    endpoint = ""
    if cluster_id and cluster_id in verdict.redirects:
        endpoint = verdict.redirects[cluster_id]
    elif len(verdict.redirects) == 1:
        endpoint = next(iter(verdict.redirects.values()))
    if endpoint:
        return ApiVerdict("redirect", location=endpoint.rstrip("/") + path)
    return ApiVerdict("unavailable",
                      message=f"this control plane ({verdict.mode}) does not lead the stretched cluster "
                              f"and the leader's API endpoint is not configured "
                              f"(Cluster.site_api_endpoints); {verdict.reason}")


# ------------------------------------------------------------- readiness --

def _fdb_status(db: DBController) -> dict | None:
    """``fdbcli status json`` through the client's special key; None when the
    store has no transactions (unit stub) or the read fails."""
    kv = db.kv_store
    if kv is None or not hasattr(kv, "create_transaction"):
        return None
    try:
        tr = kv.create_transaction()
        tr.options.set_access_system_keys()
        raw = tr[b'\xff\xff/status/json'].wait()
        if not raw.present():
            return None
        return json.loads(bytes(raw).decode())
    except Exception as e:  # noqa: BLE001 - a gate input, reported as a problem
        logger.debug(f"fdb status read failed: {e}")
        return None


def fdb_region_lag(status: dict | None) -> tuple[float | None, str]:
    """The remote region's lag behind the primary in seconds and the data
    distribution state name, from a status document; (None, "") when the
    document has neither (a single-region cluster, or no document)."""
    if not status:
        return None, ""
    cluster = status.get("cluster", {}) if isinstance(status, dict) else {}
    lag = cluster.get("datacenter_lag", {})
    seconds = lag.get("seconds") if isinstance(lag, dict) else None
    data_state = cluster.get("data", {}).get("state", {})
    name = data_state.get("name", "") if isinstance(data_state, dict) else ""
    try:
        return (float(seconds) if seconds is not None else None), str(name)
    except (TypeError, ValueError):
        return None, str(name)


@dataclass
class SiteReadiness:
    site: str
    role: str                      # "control_plane" | "storage"
    mgmt_nodes_online: int = 0
    mgmt_nodes_total: int = 0
    storage_nodes_online: int = 0
    storage_nodes_total: int = 0
    holds_lease: bool = False
    fdb_lag_seconds: float | None = None
    fdb_data_state: str = ""
    problems: list[str] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return not self.problems


def _cluster_sites(db: DBController, cluster: Cluster) -> tuple[list[MgmtNode], list[StorageNode]]:
    mgmt = [n for n in db.get_mgmt_nodes(cluster.get_id())
            if n.status != MgmtNode.STATUS_REMOVED]
    storage = [n for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())
               if n.status != StorageNode.STATUS_REMOVED]
    return mgmt, storage


def control_plane_sites(cluster: Cluster) -> list[str]:
    """The sites with a management node (the ones that may lead)."""
    db = DBController()
    mgmt, _ = _cluster_sites(db, cluster)
    return sorted({n.site for n in mgmt if n.site})


def evaluate_site(db: DBController, cluster: Cluster, site: str, *, as_leader_target: bool,
                  fdb_status: dict | None, lease: ControlPlaneLease | None,
                  running_tasks: Iterable[JobSchedule] = ()) -> SiteReadiness:
    """The readiness of ``site``: what ``control-plane status`` shows and what
    a planned move gates on (``as_leader_target``)."""
    mgmt, storage = _cluster_sites(db, cluster)
    my_mgmt = [n for n in mgmt if n.site == site]
    my_storage = [n for n in storage if n.site == site]
    r = SiteReadiness(site=site, role="control_plane" if my_mgmt else "storage")
    r.mgmt_nodes_total = len(my_mgmt)
    r.mgmt_nodes_online = sum(1 for n in my_mgmt if n.status == MgmtNode.STATUS_ONLINE)
    r.storage_nodes_total = len(my_storage)
    r.storage_nodes_online = sum(1 for n in my_storage if n.status == StorageNode.STATUS_ONLINE)
    r.holds_lease = bool(lease is not None and lease.holder_site == site
                         and lease.state != ControlPlaneLease.STATE_RELEASED and not lease_expired(lease))
    r.fdb_lag_seconds, r.fdb_data_state = fdb_region_lag(fdb_status)
    if not as_leader_target:
        return r

    if not my_mgmt:
        r.problems.append(f"site {site} has no management node")
    elif r.mgmt_nodes_online == 0:
        r.problems.append(f"no management node of site {site} is online")
    not_online = [n for n in my_storage if n.status != StorageNode.STATUS_ONLINE]
    if not_online:
        r.problems.append(
            f"storage nodes of site {site} not online: "
            + ", ".join(f"{n.get_id()[:8]}={n.status}" for n in not_online))
    if cluster.lost_site == site:
        r.problems.append(f"site {site} is recorded lost ({cluster.lost_site_state})")
    if cluster.site_activation_site:
        r.problems.append(f"a site activation of {cluster.site_activation_site} is running")
    if cluster.status != Cluster.STATUS_ACTIVE:
        r.problems.append(f"cluster status is {cluster.status}, not {Cluster.STATUS_ACTIVE}")
    if cluster.sync_replication:
        sync_problem = _sync_problem(cluster)
        if sync_problem:
            r.problems.append(sync_problem)
    if r.fdb_lag_seconds is not None and r.fdb_lag_seconds > constants.CONTROL_PLANE_MAX_FDB_LAG_SEC:
        r.problems.append(f"FoundationDB region lag {r.fdb_lag_seconds:.1f}s exceeds "
                          f"{constants.CONTROL_PLANE_MAX_FDB_LAG_SEC:.0f}s")
    if r.fdb_data_state and r.fdb_data_state not in ("healthy", "healthy_repartitioning"):
        r.problems.append(f"FoundationDB data distribution state is {r.fdb_data_state}")
    running = [t for t in running_tasks if t.status == JobSchedule.STATUS_RUNNING]
    if running:
        r.problems.append(
            f"{len(running)} task(s) running on the current leader: "
            + ", ".join(f"{t.function_name}:{t.uuid[:8]}" for t in running[:5]))
    return r


def _sync_problem(cluster: Cluster) -> str:
    """A sync-replication cluster is ready for a move only when every LVS is
    in sync; imported lazily because the sync controller imports a great deal."""
    from simplyblock_core.controllers import sync_replication_controller
    try:
        status = sync_replication_controller.cluster_sync_status(cluster.get_id(), max_age=30.0)
    except Exception as e:  # noqa: BLE001 - a gate input
        return f"sync replication status unavailable: {e}"
    if status.degraded or status.resyncing or status.diverged or not status.peer_ready:
        return (f"sync replication not settled: state={status.state} degraded={status.degraded} "
                f"resyncing={status.resyncing} diverged={status.diverged} peer_ready={status.peer_ready}")
    return ""


@dataclass
class ControlPlaneStatus:
    cluster_id: str
    stretched: bool
    local_site: str
    mode: Mode
    mode_reason: str
    leader_site: str
    lease: ControlPlaneLease | None
    sites: list[SiteReadiness]
    fdb_lag_seconds: float | None
    fdb_data_state: str


def control_plane_status(cluster_id: str) -> ControlPlaneStatus:
    db = DBController()
    cluster = db.get_cluster_by_id(cluster_id)
    try:
        verdict = current_mode()
        mode: Mode = verdict.mode
        reason = verdict.reason
    except Exception as e:  # noqa: BLE001 - status must answer while disconnected
        mode, reason = "disconnected", str(e)
    lease = db.get_control_plane_lease(cluster_id) if cluster.stretched_control_plane else None
    status = _fdb_status(db) if cluster.stretched_control_plane else None
    mgmt, storage = _cluster_sites(db, cluster)
    sites = sorted({n.site for n in mgmt if n.site} | {n.site for n in storage if n.site})
    tasks = db.get_job_tasks(cluster_id) if cluster.stretched_control_plane else []
    lag, data_state = fdb_region_lag(status)
    return ControlPlaneStatus(
        cluster_id=cluster_id,
        stretched=cluster.stretched_control_plane,
        local_site=local_site(),
        mode=mode,
        mode_reason=reason,
        leader_site=cluster.leader_site,
        lease=lease,
        sites=[evaluate_site(db, cluster, s, as_leader_target=(s != (lease.holder_site if lease else "")),
                             fdb_status=status, lease=lease, running_tasks=tasks)
               for s in sites],
        fdb_lag_seconds=lag,
        fdb_data_state=data_state,
    )


# ------------------------------------------------------------ the moves --

def _require_stretched(cluster: Cluster) -> None:
    if not cluster.stretched_control_plane:
        raise ControlPlaneError(
            f"cluster {cluster.get_id()} has no stretched control plane")


def _require_control_plane_site(cluster: Cluster, site: str) -> None:
    if not site:
        raise ControlPlaneError("site must not be empty")
    sites = control_plane_sites(cluster)
    if site not in sites:
        raise ControlPlaneError(
            f"site {site!r} has no control plane in cluster {cluster.get_id()} "
            f"(control-plane sites: {sites})")


def move_leader_site(cluster_id: str, site: str, *, force: bool = False) -> SiteReadiness:
    """The planned move: ``leader_site`` becomes ``site`` once every readiness
    gate of ``site`` is green (or ``force``); the holder's agent then drains
    and releases, and ``site``'s agent acquires. Returns the gate result.

    Raises ControlPlaneError for a cluster that is not stretched or a site
    without a control plane, ControlPlaneNotReadyError (with ``problems``)
    when a gate is red and ``force`` is not given; the sync-replication
    checks of ``leader_site`` (the lost site is refused) apply as well."""
    from simplyblock_core.controllers import sync_replication_controller
    db = DBController()
    cluster = db.get_cluster_by_id(cluster_id)
    _require_stretched(cluster)
    _require_control_plane_site(cluster, site)
    lease = db.get_control_plane_lease(cluster_id)
    readiness = evaluate_site(
        db, cluster, site, as_leader_target=True, fdb_status=_fdb_status(db), lease=lease,
        running_tasks=db.get_job_tasks(cluster_id))
    if readiness.problems and not force:
        raise ControlPlaneNotReadyError(
            f"site {site} is not ready to lead cluster {cluster_id}: " + "; ".join(readiness.problems),
            readiness.problems)
    sync_replication_controller.check_leader_site(cluster, site)
    sync_replication_controller.set_leader_site(cluster_id, site)
    if force and readiness.problems:
        cluster_events.control_plane_leader_change(
            cluster, site, lease.holder_site if lease else "",
            how="planned move forced past: " + "; ".join(readiness.problems))
    return readiness


def probe_site(cluster: Cluster, site: str, *, timeout: float = 3.0) -> list[str]:
    """What still answers on ``site``: every management node that answers a
    ping, every storage node whose host answers a ping or whose node API
    answers. Empty means nothing answered."""
    from simplyblock_core.snode_client import SNodeClient
    db = DBController()
    mgmt, storage = _cluster_sites(db, cluster)
    alive: list[str] = []
    for m in mgmt:
        if m.site == site and m.mgmt_ip and utils.ping_host(m.mgmt_ip):
            alive.append(f"management node {m.hostname or m.get_id()[:8]} ({m.mgmt_ip}) answers ping")
    for sn in storage:
        if sn.site != site:
            continue
        if sn.mgmt_ip and utils.ping_host(sn.mgmt_ip):
            alive.append(f"storage node {sn.get_id()[:8]} host {sn.mgmt_ip} answers ping")
            continue
        try:
            if sn.api_endpoint and SNodeClient(sn.api_endpoint, timeout=timeout, retry=1).is_live():
                alive.append(f"storage node {sn.get_id()[:8]} API {sn.api_endpoint} answers")
        except Exception:  # noqa: BLE001 - not answering is the expected outcome
            pass
    return alive


def takeover(cluster_id: str, site: str, confirm_site_down: str, *, operator: str = "") -> ControlPlaneStatus:
    """The unplanned move after the leader's site is down: the operator names
    the surviving site and confirms the other one down; the command probes
    the confirmed site and refuses if anything there answers, then moves
    ``leader_site`` and releases the lease on the holder's behalf so that
    ``site``'s agent acquires it.

    FoundationDB must answer on ``site`` for this to run at all: when the
    coordinator majority was lost with the leader's site, the FDB recovery
    (the FDB operator's multi-region recovery) comes first
    (docs/stretched-control-plane.md).

    Raises ControlPlaneError for a cluster that is not stretched, a site
    without a control plane, or a confirmation that does not name the
    leading site; ControlPlaneTakeoverRefusedError (with ``alive``) when the
    confirmed site still answers."""
    from simplyblock_core.controllers import sync_replication_controller
    db = DBController()
    cluster = db.get_cluster_by_id(cluster_id)
    _require_stretched(cluster)
    _require_control_plane_site(cluster, site)
    if not confirm_site_down or confirm_site_down == site:
        raise ControlPlaneError("confirm_site_down must name the other site, the one that is down")
    lease = db.get_control_plane_lease(cluster_id)
    leading = (lease.holder_site if lease is not None and lease.state != ControlPlaneLease.STATE_RELEASED
               else cluster.leader_site)
    if leading and confirm_site_down != leading:
        raise ControlPlaneError(
            f"confirm_site_down={confirm_site_down!r} but {leading!r} leads cluster {cluster_id}; "
            f"a takeover is only for the leading site being down")
    alive = probe_site(cluster, confirm_site_down)
    if alive:
        raise ControlPlaneTakeoverRefusedError(
            f"site {confirm_site_down} still answers; a takeover while it is alive would make two "
            f"control planes drive the same storage nodes: " + "; ".join(alive), alive)
    who = operator or f"{local_host()}"
    sync_replication_controller.set_leader_site(cluster_id, site)
    release_lease(cluster_id, released_by=f"takeover:{who}")
    cluster_events.control_plane_leader_change(
        cluster, site, confirm_site_down,
        how=f"takeover by {who}; site {confirm_site_down} confirmed down, nothing there answered")
    return control_plane_status(cluster_id)


# ------------------------------------------------------- scheme and sites --

STRETCH_SCHEMES = {
    "sync_replication": "any n+k with two-side synchronous replication over two sites",
    "fd_1p1": "1+1 with failure domains across two sites",
    "fd_1p2": "1+2 with failure domains across three storage sites",
}


def stretch_scheme(*, sync_replication: bool, enable_failure_domain: bool,
                   distr_ndcs: int, distr_npcs: int) -> str:
    """The data-plane scheme a stretched cluster runs, or "" for a
    combination that does not replicate properly across sites."""
    if sync_replication:
        return "sync_replication"
    if enable_failure_domain and (distr_ndcs, distr_npcs) == (1, 1):
        return "fd_1p1"
    if enable_failure_domain and (distr_ndcs, distr_npcs) == (1, 2):
        return "fd_1p2"
    return ""


def storage_sites_allowed(cluster: Cluster) -> int:
    """How many storage sites the cluster's scheme spans."""
    scheme = stretch_scheme(sync_replication=cluster.sync_replication,
                            enable_failure_domain=cluster.enable_failure_domain,
                            distr_ndcs=cluster.distr_ndcs, distr_npcs=cluster.distr_npcs)
    return 3 if scheme == "fd_1p2" else 2


def validate_stretched_control_plane(*, stretched_control_plane: bool, sync_replication: bool,
                                     enable_failure_domain: bool, distr_ndcs: int, distr_npcs: int,
                                     ha_type: str, is_single_node: bool) -> None:
    """The deploy-time rule: a stretched control plane needs ``ha_type='ha'``
    and one of the three supported schemes.

    Raises:
        ValueError: with the three schemes named.
    """
    if not stretched_control_plane:
        return
    if ha_type != "ha" or is_single_node:
        raise ValueError("a stretched control plane requires ha_type='ha' and more than one node")
    if not stretch_scheme(sync_replication=sync_replication, enable_failure_domain=enable_failure_domain,
                          distr_ndcs=distr_ndcs, distr_npcs=distr_npcs):
        raise ValueError(
            f"a stretched control plane supports exactly three data-plane schemes: "
            f"{'; '.join(STRETCH_SCHEMES.values())}. Got ndcs={distr_ndcs}, npcs={distr_npcs}, "
            f"sync_replication={sync_replication}, enable_failure_domain={enable_failure_domain}")


def site_failure_domain(cluster_id: str, site: str) -> int:
    """The failure-domain id of ``site`` on a stretched cluster whose scheme
    binds failure domain to site, assigned on first use (the next free id,
    never reassigned) in a compare-and-set on the cluster record."""
    db = DBController()
    cluster = db.get_cluster_by_id(cluster_id)
    known = cluster.site_failure_domains.get(site)
    if known is not None:
        return int(known)
    assigned: dict[str, int] = {}

    def mutate(fresh: Cluster):
        if site in fresh.site_failure_domains:
            assigned["fd"] = int(fresh.site_failure_domains[site])
            return False
        limit = storage_sites_allowed(fresh)
        if len(fresh.site_failure_domains) >= limit:
            raise ValueError(
                f"site {site!r} would be storage site {len(fresh.site_failure_domains) + 1} of cluster "
                f"{cluster_id}, whose scheme spans {limit} (sites: {sorted(fresh.site_failure_domains)})")
        used = {int(v) for v in fresh.site_failure_domains.values()}
        fd = 0
        while fd in used:
            fd += 1
        fresh.site_failure_domains[site] = fd
        assigned["fd"] = fd
        return None

    db.atomic_update(cluster, mutate)
    return assigned["fd"]
