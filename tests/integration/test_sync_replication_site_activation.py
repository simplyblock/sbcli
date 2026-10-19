"""The site activation of a sync-replication cluster - main's cluster
activation narrowed to the recorded site - against the real FoundationDB.

3 + 3 nodes, FTT 1, site A recorded, the cluster DEGRADED, A's nodes ONLINE
without lvstores (what their restart leaves). Four LVS shapes: a0 owns LVS_1
(homed and led on A), b0 owns LVS_2 (homed on B, led from A: its remote
triplet a0 a1 a2), b1 owns LVS_3 (homed and led on B, remote a1 a2 a0), a1
owns LVS_4 (homed on A, led from B: its remote triplet b2 b0 b1).

The activation runs through the real wrapper (``cluster_activate(...,
site=A)``: the flag's begin, heartbeat and end). Its storage-node primitives
are captured at the ``storage_node_ops`` boundary - the leader rebuild, the
two non-leader routes (the activation path behind a leader of A, the restart
path next to a serving leader of B), the hublvol wiring, the ANA pass - and
the database never is. Where a primitive's own cleanup is the point, it runs
for real behind the RPC mocks.
"""
import threading
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import fdb
import pytest

from simplyblock_core import cluster_ops
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import activation_gate, storage_events, tasks_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import storage_node_monitor as monitor
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DOWN, FENCING, DONE = Cluster.LOST_SITE_DOWN, Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE
DRAINING = Cluster.LOST_SITE_DRAINING
ONLINE, OFFLINE = StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE
RECORD_STATES = [DOWN, FENCING, DONE]

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
adopt = stack.adopt
env = stack.env
_fresh, _update = stack._fresh, stack._update


@pytest.fixture(autouse=True)
def _clean_caches():
    ttl_cache.invalidate_all()
    yield
    ttl_cache.invalidate_all()


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _shapes(db, state=DOWN, *, b0_status="failed"):
    """The four LVS shapes, site A recorded ``state`` ("" = no record). b0's
    ``lvstore_status`` is ``b0_status``, b1's ``ready``: an owner of B keeps
    whatever it had, which a test tells apart from a ``ready`` or ``failed``
    write by the choice of ``b0_status``."""
    cluster = stack._seed_cluster(db, ftt=1, lost_site=SITE_A if state else "")
    cluster.lost_site_state = state
    cluster.status = Cluster.STATUS_DEGRADED
    cluster.cluster_vip = "10.0.0.1"
    cluster.write_to_db(db.kv_store)
    a, b = stack._sites(db, cluster)
    stack._make_owner(db, a[0], a, b, lvs_id=1, ftt=1)
    stack._make_owner(db, b[0], b, a, lvs_id=2, ftt=1)
    stack._make_owner(db, b[1], [b[1], b[2], b[0]], [a[1], a[2], a[0]], lvs_id=3, ftt=1)
    stack._make_owner(db, a[1], [a[1], a[2], a[0]], [b[2], b[0], b[1]], lvs_id=4, ftt=1)
    _update(db, b[0], lvs_active_site=SITE_A, lvstore_status=b0_status)
    _update(db, a[1], lvs_active_site=SITE_B)
    for node in a:
        _update(db, node, lvstore_status="")
    for node in (*a, *b):
        _update(db, node, lvstore_ports=ops._derive_lvstore_ports(_fresh(db, node), None, db))
    return SimpleNamespace(cid=cluster.get_id(), a=[_fresh(db, n) for n in a],
                           b=[_fresh(db, n) for n in b])


def _ids(nodes):
    return [n.get_id() for n in nodes]


def _cluster(db, s):
    return db.get_cluster_by_id(s.cid)


def _set_cluster(db, s, **fields):
    def _mutate(c):
        for key, value in fields.items():
            setattr(c, key, value)
    db.atomic_update(db.get_cluster_by_id(s.cid), _mutate)


def _status(db, node):
    return _fresh(db, node).lvstore_status


def _update_status(db, node, value):
    db.atomic_update(_fresh(db, node), lambda n: setattr(n, "lvstore_status", value))


def _resync_lvs(db, s):
    return sorted(t.function_params["lvs_name"] for t in db.get_job_tasks(s.cid)
                  if t.function_name == JobSchedule.FN_SYNC_RESYNC)


def _wait_for(predicate, timeout=10.0):
    """Poll ``predicate`` until it holds (time.sleep is patched out)."""
    tick = threading.Event()
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return True
        tick.wait(0.01)
    return predicate()


# ---------------------------------------------------------------------------
# the boundary
# ---------------------------------------------------------------------------

#: Pass 2's restart-path routes: next to a leader that serves.
RESTART_ROUTES = ("remote", "local")


class _Boundary:
    """The primitives of an activation, each captured, with one ordered log
    of ``(route, node id, owner id)``: ``p1`` the leader rebuild, ``act`` the
    activation-path non-leader rebuild, ``remote`` / ``local`` the restart-path
    ones, ``ana`` Pass 4, ``event`` the site's return event.

    ``fail[(route, node id, owner id)]`` makes that primitive return False or
    raise the exception given; ``hook(route, node id, owner id)`` runs at the
    start of every Pass 1 / Pass 2 primitive."""

    def __init__(self, db, es, s, *, real_wait=False, set_port=True):
        self.db, self.s = db, s
        self.log: list = []
        self.fail: dict = {}
        self.hook = None
        self.real_non_leader = None
        self._guard = threading.Lock()
        self._restart_running = 0
        self.overlaps = 0
        self.writes: list = []
        self.ana_view: list = []
        self.leader_kwargs: list = []
        self.holders: list = []
        self.b_at_pass2: tuple | None = None
        real_non_leader = ops.recreate_lvstore_on_non_leader
        real_write = StorageNode.write_to_db

        if not real_wait:
            self.wait = es.enter_context(patch.object(cluster_ops, "_wait_for_full_device_connectivity"))
        es.enter_context(patch.object(cluster_ops.time, "sleep"))
        es.enter_context(patch.object(cluster_ops, "tcp_ports_events"))
        if set_port:
            self.set_port = es.enter_context(patch.object(cluster_ops.port_block, "set_port"))
        es.enter_context(patch.object(cluster_ops.utils, "set_storage_mcp_max_unavailable"))
        self.jm_mesh = es.enter_context(patch.object(ops, "verify_jm_mesh_coverage", return_value=[]))
        self.create = es.enter_context(patch.object(ops, "create_lvstore", side_effect=self._create))
        self.leader = es.enter_context(patch.object(ops, "recreate_lvstore", side_effect=self._leader))
        self.non_leader = es.enter_context(
            patch.object(ops, "recreate_lvstore_on_non_leader", side_effect=self._non_leader))
        self.remote = es.enter_context(
            patch.object(ops, "_rebuild_remote_instance", side_effect=self._remote))
        self.anchor = es.enter_context(patch.object(ops, "_rebuild_anchor", side_effect=self._anchor))
        self.ana = es.enter_context(
            patch.object(cluster_ops, "_activation_open_node_ana", side_effect=self._ana))
        self.event = es.enter_context(
            patch.object(storage_events, "sync_site_returned", side_effect=self._event))
        self.set_status = es.enter_context(
            patch.object(cluster_ops, "set_cluster_status", wraps=cluster_ops.set_cluster_status))
        self.labels = es.enter_context(
            patch.object(ops, "get_next_physical_device_order", return_value=0))
        self.recorded = es.enter_context(
            patch.object(cluster_ops, "_record_activated_nodes", wraps=cluster_ops._record_activated_nodes))
        es.enter_context(patch.object(StorageNode, "write_to_db", autospec=True,
                                      side_effect=lambda node, *a, **k: self._write(real_write, node, a, k)))
        self._real_non_leader = real_non_leader

    def _write(self, real_write, node, args, kwargs):
        self.writes.append(node.get_id())
        return real_write(node, *args, **kwargs)

    def _primitive(self, route, node_id, owner_id):
        with self._guard:
            self.log.append((route, node_id, owner_id))
            if route != "p1" and self.b_at_pass2 is None:
                self.b_at_pass2 = tuple(_status(self.db, n) for n in self.s.b[:2])
        if self.hook is not None:
            self.hook(route, node_id, owner_id)
        result = self.fail.get((route, node_id, owner_id), True)
        if isinstance(result, BaseException):
            raise result
        return result

    def _restart_path(self, route, node_id, owner_id):
        with self._guard:
            self._restart_running += 1
            if self._restart_running > 1:
                self.overlaps += 1
        try:
            threading.Event().wait(0.05)
            return self._primitive(route, node_id, owner_id)
        finally:
            with self._guard:
                self._restart_running -= 1

    def _create(self, snode, *args):
        self.log.append(("create", snode.get_id(), snode.get_id()))
        _update(self.db, snode, lvstore=f"LVS_{snode.get_id()[:6]}")
        return True

    def _leader(self, snode, force=False, lvs_primary=None, activation_mode=False, **kwargs):
        """The leader rebuild, with its own status writes: the owner ``ready``
        on success (storage_node_ops.py:14819, kept by _rebuild_leader_off_owner),
        nothing left behind on a failure."""
        owner = lvs_primary if lvs_primary is not None else snode
        holder = kwargs.get("site_activation")
        with self._guard:
            self.leader_kwargs.append(dict(kwargs))
            if holder is not None:
                cluster = self.db.get_cluster_by_id(self.s.cid)
                self.holders.append((holder.site, holder.token == cluster.site_activation_since != ""))
        ok = self._primitive("p1", snode.get_id(), owner.get_id())
        if ok:
            self.db.atomic_update(self.db.get_storage_node_by_id(owner.get_id()),
                                  lambda n: setattr(n, "lvstore_status", "ready"))
        return ok

    def _non_leader(self, snode, leader, owner, activation_mode=False, force=False, **kwargs):
        if activation_mode:
            return self._primitive("act", snode.get_id(), owner.get_id())
        if self.real_non_leader is not None and self.real_non_leader(snode, owner):
            self.log.append(("local", snode.get_id(), owner.get_id()))
            return self._real_non_leader(snode, leader, owner, activation_mode=False, force=force)
        return self._restart_path("local", snode.get_id(), owner.get_id())

    def _remote(self, member, owner, force=False):
        return self._restart_path("remote", member.get_id(), owner.get_id())

    def _anchor(self, owner, snode, db_controller):
        return self.db.get_storage_node_by_id(owner.remote_primary_node_id)

    def _ana(self, owner_id):
        cluster = self.db.get_cluster_by_id(self.s.cid)
        self.ana_view.append((owner_id, cluster.lost_site, cluster.site_activation_site))
        self.log.append(("ana", owner_id, owner_id))

    def _event(self, cluster, site, **kwargs):
        self.log.append(("event", site, site))

    def calls(self, *routes):
        return [entry for entry in self.log if entry[0] in routes]


def _activate(db, s, *, expect=None, configure=None, real_wait=False, set_port=True, site=SITE_A,
              **extra):
    """Run the site activation of ``site`` through the wrapper; ``expect``
    the exception type it must raise. Returns the boundary."""
    scope = {"site": site, **extra} if site else dict(extra)
    with ExitStack() as es:
        bnd = _Boundary(db, es, s, real_wait=real_wait, set_port=set_port)
        if configure is not None:
            configure(bnd)
        if expect is None:
            cluster_ops.cluster_activate(s.cid, force=True, **scope)
        else:
            with pytest.raises(expect):
                cluster_ops.cluster_activate(s.cid, force=True, **scope)
    return bnd


def _b0_at_each_rebuild(db, s, seen):
    """A boundary hook: ``(route, node id, owner id, b0's lvstore_status)``
    appended to ``seen`` at the start of every Pass 2 primitive."""
    def _hook(route, node_id, owner_id):
        if route != "p1":
            seen.append((route, node_id, owner_id, _status(db, s.b[0])))
    return _hook


def _nothing_returned(db, s, bnd, state=DOWN):
    """The record kept, the flag gone, the cluster status untouched, no Pass 4
    and no event."""
    cluster = _cluster(db, s)
    assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, state)
    assert cluster.site_activation_site == ""
    assert cluster.status == Cluster.STATUS_DEGRADED
    bnd.set_status.assert_not_called()
    assert bnd.calls("ana", "event") == []


# ---------------------------------------------------------------------------
# what the site activation touches
# ---------------------------------------------------------------------------

class TestSiteScope:

    @pytest.mark.parametrize("state", RECORD_STATES)
    def test_pass1_builds_the_leaders_led_from_site(self, db, rpcs, hub, adopt, state):
        s = _shapes(db, state)
        a0, b0 = s.a[0].get_id(), s.b[0].get_id()
        bnd = _activate(db, s)
        assert sorted(bnd.calls("p1")) == sorted([("p1", a0, a0), ("p1", a0, b0)])
        # each carries the flag's own token, live at the call
        assert bnd.holders == [(SITE_A, True), (SITE_A, True)]
        bnd.create.assert_not_called()

    @pytest.mark.parametrize("state", RECORD_STATES)
    def test_pass2_runs_only_on_the_site_nodes(self, db, rpcs, hub, adopt, state):
        s = _shapes(db, state)
        (a0, a1, a2), (b0, b1, b2) = _ids(s.a), _ids(s.b)
        bnd = _activate(db, s)
        # led from A: the activation path behind the leader Pass 1 built on a0
        assert sorted(bnd.calls("act")) == sorted([("act", a1, a0), ("act", a1, b0), ("act", a2, b0)])
        assert {c.args[1].get_id() for c in bnd.non_leader.call_args_list
                if c.kwargs.get("activation_mode")} == {a0}
        # led from B, whose leader serves: main's restart path
        assert sorted(bnd.calls("remote")) == sorted([("remote", a0, b1), ("remote", a1, b1),
                                                      ("remote", a2, b1)])
        assert sorted(bnd.calls("local")) == sorted([("local", a1, a1), ("local", a2, a1)])
        assert {c.args[1].get_id() for c in bnd.non_leader.call_args_list
                if not c.kwargs.get("activation_mode")} == {b2}
        # no activation-mode port-block of a leader of B
        assert {c.args[0].get_id() for c in bnd.set_port.call_args_list} == {a0}

    def test_restart_path_rebuilds_never_overlap(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        bnd = _activate(db, s)
        assert len(bnd.calls(*RESTART_ROUTES)) == 5
        assert bnd.overlaps == 0

    def test_owner_statuses_after_a_site_activation(self, db, rpcs, hub, adopt):
        s = _shapes(db, b0_status="failed")
        bnd = _activate(db, s)
        # Pass 1 wrote b0 ``ready`` (its leader rebuilt on a0): restored before Pass 2
        assert bnd.b_at_pass2 == ("failed", "ready")
        assert [_status(db, n) for n in s.a] == ["ready", "ready", "ready"]
        assert (_status(db, s.b[0]), _status(db, s.b[1])) == ("failed", "ready")

    def test_a_late_marked_owner_ends_ready(self, db, rpcs, hub, adopt):
        # One worker at a time: a0's own Pass 2 (LVS_3) ends before a1's
        # rebuild of LVS_1 marks a0 ``in_creation``.
        s = _shapes(db)
        a0, a1 = s.a[0].get_id(), s.a[1].get_id()
        with patch.object(cluster_ops.constants, "CLUSTER_ACTIVATION_MAX_PARALLEL_NODES", 1):
            bnd = _activate(db, s)
        assert bnd.log.index(("remote", a0, s.b[1].get_id())) < bnd.log.index(("act", a1, a0))
        assert _status(db, s.a[0]) == "ready"

    def test_a_held_owner_is_marked_per_rebuild(self, db, rpcs, hub, adopt):
        """b0 serves from B while its LVS_2, led from A, is rebuilt on a1 and
        a2: its ``in_creation`` marker - the storage-node monitor skips such a
        node - spans each of those rebuilds, and b0 has its own status back at
        every other one."""
        s = _shapes(db, b0_status="failed")
        b0 = s.b[0].get_id()
        seen: list = []

        def _configure(bnd):
            bnd.hook = _b0_at_each_rebuild(db, s, seen)
        with patch.object(cluster_ops.constants, "CLUSTER_ACTIVATION_MAX_PARALLEL_NODES", 1):
            _activate(db, s, configure=_configure)
        assert [e[3] for e in seen if e[2] == b0] == ["in_creation", "in_creation"]
        others = {e[3] for e in seen if e[2] != b0}
        assert others == {"failed"}
        assert _status(db, s.b[0]) == "failed"

    def test_pass3_wires_only_the_lvs_led_from_site(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        a_ids = set(_ids(s.a))
        _activate(db, s)
        assert stack._hub_callers(hub, "recreate_hublvol") == [s.a[0].get_id()]
        assert [(c.args[0].get_id(), c.args[1].get_id()) for c in adopt.call_args_list] == [
            (s.a[0].get_id(), s.b[0].get_id())]
        for name in hub:
            assert set(stack._hub_callers(hub, name)) <= a_ids, name

    def test_pass4_opens_after_the_record_is_cleared(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        bnd = _activate(db, s)
        assert sorted(bnd.ana_view) == sorted([(s.a[0].get_id(), "", SITE_A),
                                               (s.b[0].get_id(), "", SITE_A)])

    @pytest.mark.parametrize("state", RECORD_STATES)
    def test_every_lvs_gets_its_catch_up_queued(self, db, rpcs, hub, adopt, state):
        s = _shapes(db, state)
        _activate(db, s)
        assert _resync_lvs(db, s) == ["LVS_1", "LVS_2", "LVS_3", "LVS_4"]
        cluster = _cluster(db, s)
        assert (cluster.lost_site, cluster.lost_site_state) == ("", "")

    def test_the_event_follows_pass4_and_flag_ends(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        bnd = _activate(db, s)
        assert bnd.log[-1] == ("event", SITE_A, SITE_A)
        assert [e[0] for e in bnd.log[-3:]] == ["ana", "ana", "event"]
        cluster = _cluster(db, s)
        assert (cluster.site_activation_site, cluster.site_activation_since,
                cluster.site_activation_heartbeat) == ("", "", "")

    def test_no_cluster_status_or_b_record_written(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        bnd = _activate(db, s)
        bnd.set_status.assert_not_called()
        assert _cluster(db, s).status == Cluster.STATUS_DEGRADED
        bnd.labels.assert_not_called()
        bnd.recorded.assert_not_called()
        assert set(bnd.writes) & set(_ids(s.b)) == set()


# ---------------------------------------------------------------------------
# failures
# ---------------------------------------------------------------------------

#: route -> (the failing primitive as (route, node index on A, owner), the
#: node of A that ends ``failed`` or None).
FAILURES = {
    "pass1-leader": (("p1", 0, "a0"), 0),
    "pass1-remote-led": (("p1", 0, "b0"), None),
    "pass2-activation": (("act", 1, "a0"), 1),
    "pass2-remote": (("remote", 0, "b1"), 0),
    "pass2-local": (("local", 2, "a1"), 2),
}


def _owner_id(s, label):
    return {"a0": s.a[0], "a1": s.a[1], "b0": s.b[0], "b1": s.b[1]}[label].get_id()


class TestSiteFailures:

    @pytest.mark.parametrize("result", [False, RPCException("spdk gone")], ids=["false", "raised"])
    @pytest.mark.parametrize("route", list(FAILURES))
    def test_a_failure_keeps_the_record_and_ends(self, db, rpcs, hub, adopt, route, result):
        s = _shapes(db, b0_status="ready")
        (kind, node_index, owner), failed_index = FAILURES[route]
        key = (kind, s.a[node_index].get_id(), _owner_id(s, owner))

        def _configure(bnd):
            bnd.fail[key] = result
        bnd = _activate(db, s, expect=ValueError, configure=_configure)
        assert key in bnd.log
        _nothing_returned(db, s, bnd)
        assert _resync_lvs(db, s) == []
        assert (_status(db, s.b[0]), _status(db, s.b[1])) == ("ready", "ready")
        if failed_index is not None:
            assert _status(db, s.a[failed_index]) == "failed"

    def test_a_backup_setup_failure_keeps_b_status(self, db, rpcs, hub, adopt):
        # Both leaders built (b0 written ``ready`` by its rebuild), then the S3
        # bdev of a0's backup raises past every pass.
        s = _shapes(db, b0_status="failed")
        _set_cluster(db, s, backup_config={"bucket_name": "b"})
        with patch.object(cluster_ops.backup_device, "create_s3_bdev",
                          side_effect=RuntimeError("s3 unreachable")) as s3:
            bnd = _activate(db, s, expect=RuntimeError)
        s3.assert_called_once()
        assert len(bnd.calls("p1")) == 2
        _nothing_returned(db, s, bnd)
        assert (_status(db, s.b[0]), _status(db, s.b[1])) == ("failed", "ready")

    def test_a_failure_before_pass1_holds_no_owner(self, db, rpcs, hub, adopt):
        # b0 carries a leaked ``in_creation`` the serving site's monitor reclaims
        # to ``ready`` while the connectivity wait runs; the wait then fails.
        s = _shapes(db, b0_status="in_creation")

        def _wait(*args, **kwargs):
            _update_status(db, s.b[0], "ready")
            raise ValueError("cross-node device connection(s) still missing")

        def _configure(bnd):
            bnd.wait.side_effect = _wait
        bnd = _activate(db, s, expect=ValueError, configure=_configure)
        assert bnd.log == []
        _nothing_returned(db, s, bnd)
        assert _status(db, s.b[0]) == "ready"

    @pytest.mark.parametrize("result", [False, RPCException("spdk gone")], ids=["false", "raised"])
    def test_a_failed_rebuild_gives_back_held_status(self, db, rpcs, hub, adopt, result):
        """a1's rebuild of LVS_2 (owner b0, serving from B) fails: b0 has its
        own status back at every later rebuild of another LVS, before the pool
        of Pass 2 has joined."""
        s = _shapes(db, b0_status="failed")
        a1, b0 = s.a[1].get_id(), s.b[0].get_id()
        seen: list = []

        def _configure(bnd):
            bnd.fail[("act", a1, b0)] = result
            bnd.hook = _b0_at_each_rebuild(db, s, seen)
        with patch.object(cluster_ops.constants, "CLUSTER_ACTIVATION_MAX_PARALLEL_NODES", 1):
            _activate(db, s, expect=ValueError, configure=_configure)
        failed_at = seen.index(("act", a1, b0, "in_creation"))
        later = {e[3] for e in seen[failed_at + 1:] if e[2] != b0}
        assert later == {"failed"}
        assert _status(db, s.b[0]) == "failed"

    def test_a_later_ready_write_keeps_its_failure(self, db, rpcs, hub, adopt):
        # a1's own LVS_4 rebuild fails; a2's LVS_4 rebuild after it writes the
        # owner a1 ``ready`` (the non-leader helper's own status write).
        s = _shapes(db)
        a1, a2 = s.a[1].get_id(), s.a[2].get_id()

        def _hook(route, node_id, owner_id):
            if (route, node_id, owner_id) == ("local", a2, a1):
                _update_status(db, s.a[1], "ready")

        def _configure(bnd):
            bnd.fail[("local", a1, a1)] = RPCException("spdk gone")
            bnd.hook = _hook
        with patch.object(cluster_ops.constants, "CLUSTER_ACTIVATION_MAX_PARALLEL_NODES", 1):
            bnd = _activate(db, s, expect=ValueError, configure=_configure)
        order = [e[1:] for e in bnd.calls("local")]
        assert order.index((a1, a1)) < order.index((a2, a1))
        assert _status(db, s.a[1]) == "failed"


class TestRecordRefusal:

    def test_a_node_failed_since_keeps_the_record(self, db, rpcs, hub, adopt):
        s = _shapes(db)

        def _configure(bnd):
            bnd.jm_mesh.side_effect = lambda *a, **k: (_update(db, s.a[2], status=OFFLINE), [])[1]
        bnd = _activate(db, s, expect=ValueError, configure=_configure)
        _nothing_returned(db, s, bnd)

    def test_a_node_not_built_keeps_the_record(self, db, rpcs, hub, adopt):
        # a3 was shut down by an operator and comes ONLINE after Pass 2 took
        # its node set: no instance was built on it.
        s = _shapes(db)
        a3 = stack._seed_node(db, _cluster(db, s), "a3", SITE_A, status=OFFLINE,
                              auto_restart_disabled=True)

        def _configure(bnd):
            bnd.jm_mesh.side_effect = lambda *a, **k: (_update(db, a3, status=ONLINE), [])[1]
        bnd = _activate(db, s, expect=ValueError, configure=_configure)
        assert a3.get_id() not in {e[1] for e in bnd.log}
        _nothing_returned(db, s, bnd)

    def test_a_suspension_during_enqueue_keeps_record(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        real_enqueue = tasks_controller.add_sync_resync_task

        def _enqueue(*args, **kwargs):
            _set_cluster(db, s, status=Cluster.STATUS_SUSPENDED)
            return real_enqueue(*args, **kwargs)

        with patch.object(tasks_controller, "add_sync_resync_task", side_effect=_enqueue):
            bnd = _activate(db, s, expect=ValueError)
        cluster = _cluster(db, s)
        assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, DOWN)
        assert cluster.site_activation_site == ""
        assert bnd.calls("ana", "event") == []


class TestMovingLvs:
    """An LVS whose leadership move was left in flight (``moving:``) by a
    promote that ended: no leader may be built for it before the move is
    settled (_refuse_leader_rebuild), and the move is settled only once every
    member answers (reconcile_lvs_move). Its instances on the site come back
    as non-leaders by main's restart path."""

    @pytest.mark.parametrize("state", [DOWN, FENCING])
    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    def test_a_site_owned_moving_lvs_takes_restart_path(self, db, rpcs, hub, adopt, target, state):
        s = _shapes(db, state)
        _update(db, s.a[0], lvs_active_site=ops.lvs_moving_value(target))
        a0, a1 = s.a[0].get_id(), s.a[1].get_id()

        def _configure(bnd):
            # what _refuse_leader_rebuild answers for an LVS being moved
            bnd.fail[("p1", a0, a0)] = ops.LVSLeadershipElsewhereError("LVS_1 is being moved")
        bnd = _activate(db, s, configure=_configure)
        assert [e for e in bnd.calls("p1", "act", "ana") if e[2] == a0] == []
        assert sorted(e for e in bnd.calls("local") if e[2] == a0) == sorted(
            [("local", a0, a0), ("local", a1, a0)])
        assert stack._hub_callers(hub, "recreate_hublvol") == []
        assert [_status(db, n) for n in s.a] == ["ready", "ready", "ready"]
        cluster = _cluster(db, s)
        assert (cluster.lost_site, cluster.lost_site_state) == ("", "")

    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    def test_a_b_owned_moving_lvs_takes_restart_path(self, db, rpcs, hub, adopt, target):
        """b0 serves from B and is never held: no leader of LVS_2 is built,
        and b0 keeps its status throughout."""
        s = _shapes(db, b0_status="failed")
        _update(db, s.b[0], lvs_active_site=ops.lvs_moving_value(target))
        b0 = s.b[0].get_id()
        bnd = _activate(db, s)
        assert [e for e in bnd.calls("p1", "act", "ana") if e[2] == b0] == []
        assert sorted(e for e in bnd.calls("remote") if e[2] == b0) == sorted(
            ("remote", n, b0) for n in _ids(s.a))
        assert bnd.b_at_pass2[0] == "failed"
        assert _status(db, s.b[0]) == "failed"
        adopt.assert_not_called()


class TestBeginRefused:

    def test_force_lvstore_create_is_refused_first(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        bnd = _activate(db, s, expect=ValueError, force_lvstore_create=True)
        assert bnd.log == []
        bnd.wait.assert_not_called()
        cluster = _cluster(db, s)
        assert (cluster.site_activation_site, cluster.site_activation_since) == ("", "")

    @pytest.mark.parametrize("case", ["flag set", "draining", "no record"])
    def test_a_refused_begin_runs_nothing(self, db, rpcs, hub, adopt, case):
        s = _shapes(db, {"draining": DRAINING, "no record": ""}.get(case, DOWN))
        other = src.begin_site_activation(s.cid, SITE_A) if case == "flag set" else ""
        bnd = _activate(db, s, expect=ValueError)
        assert bnd.log == []
        bnd.wait.assert_not_called()
        cluster = _cluster(db, s)
        assert (cluster.site_activation_site, cluster.site_activation_since) == (
            (SITE_A, other) if other else ("", ""))


# ---------------------------------------------------------------------------
# the heartbeat
# ---------------------------------------------------------------------------

class _Held:
    """Run an activation in a thread, held inside its first Pass 2 primitive
    until ``release()``."""

    def __init__(self, db, s, **activate_kwargs):
        self.entered, self.released = threading.Event(), threading.Event()
        self.errors: list = []

        def _hook(route, node_id, owner_id):
            if route in ("act", *RESTART_ROUTES) and not self.entered.is_set():
                self.entered.set()
                self.released.wait(30)

        def _configure(bnd):
            bnd.hook = _hook

        def _run():
            try:
                _activate(db, s, configure=_configure, **activate_kwargs)
            except BaseException as e:   # the test thread reports it
                self.errors.append(e)
        self.thread = threading.Thread(target=_run, name="held-activation")

    def __enter__(self):
        self.thread.start()
        assert self.entered.wait(30)
        return self

    def __exit__(self, *exc):
        self.released.set()
        self.thread.join(30)
        return False


def _distinct(db, s, field, count, timeout=10.0):
    seen = []

    def _collect():
        value = getattr(_cluster(db, s), field)
        if value and value not in seen:
            seen.append(value)
        return len(seen) >= count
    _wait_for(_collect, timeout)
    return seen


class TestHeartbeat:

    def test_the_site_heartbeat_advances_while_held(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        with patch.object(cluster_ops, "ACTIVATION_HEARTBEAT_INTERVAL_SEC", 0.05), \
                patch.object(monitor, "ACTIVATION_HEARTBEAT_STALE_SEC", 1), \
                _Held(db, s) as held:
            stamps = _distinct(db, s, "site_activation_heartbeat", 30)
            assert len(stamps) >= 30                  # >= 1.5 s held, a stamp every 0.05 s
            monitor._watchdog_stuck_site_activation(_cluster(db, s))
            assert _cluster(db, s).site_activation_site == SITE_A
        assert held.errors == []

    def test_a_failed_stamp_is_followed_by_a_later_one(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        real_stamp = src.stamp_site_activation
        failures = [fdb.FDBError(1020)]

        def _stamp(*args, **kwargs):
            if failures:
                raise failures.pop()
            return real_stamp(*args, **kwargs)

        with patch.object(cluster_ops, "ACTIVATION_HEARTBEAT_INTERVAL_SEC", 0.05), \
                patch.object(src, "stamp_site_activation", side_effect=_stamp) as stamp, \
                _Held(db, s) as held:
            begun = _cluster(db, s).site_activation_heartbeat
            assert _wait_for(lambda: _cluster(db, s).site_activation_heartbeat != begun)
            assert failures == [] and stamp.call_count >= 2
        assert held.errors == []

    def test_a_full_activation_stamps_only_its_own(self, db, rpcs, hub, adopt):
        s = _shapes(db, "")
        _set_cluster(db, s, status=Cluster.STATUS_SUSPENDED,
                     activated_node_ids=sorted(_ids(s.a) + _ids(s.b)))
        with patch.object(cluster_ops, "ACTIVATION_HEARTBEAT_INTERVAL_SEC", 0.05), \
                _Held(db, s, site="") as held:
            assert _cluster(db, s).status == Cluster.STATUS_IN_ACTIVATION
            assert len(_distinct(db, s, "activation_heartbeat", 3)) >= 3
            assert _cluster(db, s).site_activation_heartbeat == ""
        assert held.errors == []


# ---------------------------------------------------------------------------
# cancellation
# ---------------------------------------------------------------------------

def _cancel(db, s, how):
    """Take the activation's ground away: the watchdog clears the token, a
    suspension, or the watchdog's clear and a newer activation begun.
    Returns the newer token ("" otherwise)."""
    if how == "suspended":
        _set_cluster(db, s, status=Cluster.STATUS_SUSPENDED)
        return ""
    src.clear_site_activation(s.cid)
    return src.begin_site_activation(s.cid, SITE_A) if how == "newer" else ""


class TestCancellation:

    @pytest.mark.parametrize("how", ["token", "suspended", "newer"])
    def test_pass2_stops_at_the_next_checkpoint(self, db, rpcs, hub, adopt, how):
        s = _shapes(db)
        newer = []

        def _hook(route, node_id, owner_id):
            if route != "p1" and not newer:
                newer.append(_cancel(db, s, how))

        def _configure(bnd):
            bnd.hook = _hook
        with patch.object(cluster_ops.constants, "CLUSTER_ACTIVATION_MAX_PARALLEL_NODES", 1):
            bnd = _activate(db, s, expect=src.SiteActivationCancelled, configure=_configure)
        assert len(bnd.calls("act", *RESTART_ROUTES)) == 1
        assert bnd.calls("ana", "event") == []
        assert _resync_lvs(db, s) == []
        cluster = _cluster(db, s)
        assert (cluster.lost_site, cluster.lost_site_state) == (SITE_A, DOWN)
        assert (cluster.site_activation_site, cluster.site_activation_since) == (
            (SITE_A, newer[0]) if how == "newer" else ("", ""))

    @pytest.mark.parametrize("how", ["token", "suspended"])
    def test_a_worker_behind_the_lock_never_starts(self, db, rpcs, hub, adopt, how):
        # Every worker's first LVS takes the restart path, behind one lock.
        # The first one holds it until another worker has passed its own
        # checkpoint, then the ground goes; that worker gets the lock next.
        s = _shapes(db)
        real_owned = src.site_activation_owned
        passed: list = []
        changed = threading.Condition()

        def _owned(*args, **kwargs):
            real_owned(*args, **kwargs)
            if threading.current_thread().name.startswith("activate-p2"):
                with changed:
                    passed.append(threading.get_ident())
                    changed.notify_all()

        def _hook(route, node_id, owner_id):
            if route not in RESTART_ROUTES:
                return
            me = threading.get_ident()
            with changed:
                assert changed.wait_for(lambda: any(t != me for t in passed), 10)
            _cancel(db, s, how)

        def _configure(bnd):
            bnd.hook = _hook
        with patch.object(src, "site_activation_owned", side_effect=_owned):
            bnd = _activate(db, s, expect=src.SiteActivationCancelled, configure=_configure)
        assert len(bnd.calls("act", *RESTART_ROUTES)) == 1
        assert _cluster(db, s).site_activation_site == ""

    def test_the_connectivity_wait_stops_on_suspend(self, db, rpcs, hub, adopt):
        s = _shapes(db)
        suspend = lambda node, **kwargs: (_set_cluster(db, s, status=Cluster.STATUS_SUSPENDED), [])[1]  # noqa: E731
        with patch.object(ops, "_connect_to_remote_devs", side_effect=suspend) as repair, \
                patch.object(ops, "_connect_to_remote_jm_devs", return_value=[]):
            bnd = _activate(db, s, expect=src.SiteActivationCancelled, real_wait=True)
        assert repair.call_count == len(s.a) + len(s.b)          # one repair round
        assert bnd.log == []
        cluster = _cluster(db, s)
        assert (cluster.lost_site, cluster.site_activation_site) == (SITE_A, "")

    def test_the_wait_without_checkpoint_ignores_status(self, db):
        s = _shapes(db)
        suspend = lambda node, **kwargs: (_set_cluster(db, s, status=Cluster.STATUS_SUSPENDED), [])[1]  # noqa: E731
        with patch.object(ops, "_connect_to_remote_devs", side_effect=suspend) as repair, \
                patch.object(ops, "_connect_to_remote_jm_devs", return_value=[]), \
                patch.object(cluster_ops.time, "sleep"):
            with pytest.raises(ValueError, match="cross-node device"):
                cluster_ops._wait_for_full_device_connectivity(s.cid, timeout_sec=20, poll_sec=10)
        assert repair.call_count == 3 * (len(s.a) + len(s.b))    # until the stalled limit


# ---------------------------------------------------------------------------
# the restart path's own cleanup, for real
# ---------------------------------------------------------------------------

class TestServingLeaderRelease:

    def test_a_failed_local_rebuild_unblocks_the_leader(self, db, rpcs, hub, adopt, env):
        # a2 rebuilds its LVS_4 role behind b2 for real: b2's port is blocked,
        # then the examine on a2 raises.
        s = _shapes(db)
        a2, b2 = s.a[2].get_id(), s.b[2].get_id()
        rpcs[a2].bdev_examine.side_effect = RPCException("spdk gone")

        def _configure(bnd):
            bnd.real_non_leader = lambda snode, owner: snode.get_id() == a2
        bnd = _activate(db, s, expect=ValueError, configure=_configure, set_port=False)
        ports = [(c.args[0].get_id(), bool(c.kwargs.get("block"))) for c in env.set_port.call_args_list]
        assert (b2, True) in ports
        assert (b2, False) in ports[ports.index((b2, True)):]
        _nothing_returned(db, s, bnd)


# ---------------------------------------------------------------------------
# the leader rebuild on the recorded site
# ---------------------------------------------------------------------------

class TestLeaderRebuildRefusal:

    @pytest.fixture()
    def built(self):
        with patch.object(ops, "_recreate_lvstore_impl", return_value=True) as own, \
                patch.object(ops, "_rebuild_leader_off_owner", return_value=True) as off_owner:
            yield own, off_owner

    def test_the_flag_token_admits_a_site_leader(self, db, built):
        s = _shapes(db)
        token = src.begin_site_activation(s.cid, SITE_A)
        holder = activation_gate.SiteActivation(SITE_A, token)
        assert ops.recreate_lvstore(_fresh(db, s.a[0]), activation_mode=True, site_activation=holder)
        assert ops.recreate_lvstore(_fresh(db, s.a[0]), lvs_primary=_fresh(db, s.b[0]),
                                    activation_mode=True, site_activation=holder)
        assert [m.call_count for m in built] == [1, 1]

    def test_activation_mode_alone_is_refused(self, db, built):
        s = _shapes(db)
        assert src.begin_site_activation(s.cid, SITE_A)
        with pytest.raises(ops.LVSLeadershipElsewhereError, match="lost site"):
            ops.recreate_lvstore(_fresh(db, s.a[0]), activation_mode=True)
        assert [m.call_count for m in built] == [0, 0]

    @pytest.mark.parametrize("case", ["stale token", "other site", "flag ended"])
    def test_a_token_not_the_flags_is_refused(self, db, built, case):
        s = _shapes(db)
        token = src.begin_site_activation(s.cid, SITE_A)
        holder = {"stale token": activation_gate.SiteActivation(SITE_A, "2026-01-01T00:00:00+00:00"),
                  "other site": activation_gate.SiteActivation(SITE_B, token),
                  "flag ended": activation_gate.SiteActivation(SITE_A, token)}[case]
        if case == "flag ended":
            src.end_site_activation(s.cid, token)
        with pytest.raises(ops.LVSLeadershipElsewhereError, match="lost site"):
            ops.recreate_lvstore(_fresh(db, s.a[0]), activation_mode=True, site_activation=holder)
        assert [m.call_count for m in built] == [0, 0]

    def test_the_grant_guard_refuses_the_site(self, db):
        s = _shapes(db)
        assert src.begin_site_activation(s.cid, SITE_A)
        with ops._sync_grant_guard(s.cid, "LVS_1", s.a[0].get_id(), s.a[0].get_id()) as ok:
            assert ok is False


# ---------------------------------------------------------------------------
# a full activation
# ---------------------------------------------------------------------------

class TestFullActivation:

    def test_a_suspended_sync_cluster_reactivates(self, db, rpcs, hub, adopt):
        s = _shapes(db, "")
        _set_cluster(db, s, status=Cluster.STATUS_SUSPENDED,
                     activated_node_ids=sorted(_ids(s.a) + _ids(s.b)))
        bnd = _activate(db, s, site="")
        assert [c.args[1] for c in bnd.set_status.call_args_list] == [
            Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_ACTIVE]
        routes = [e[0] for e in bnd.log]
        assert routes.index("act") > max(i for i, r in enumerate(routes) if r in ("p1", "create"))
        assert routes[-1] == "ana" and "ana" not in routes[:routes.index("act")]
        assert bnd.calls(*RESTART_ROUTES) == [] and "event" not in routes
        assert all("site_activation" not in kw for kw in bnd.leader_kwargs)
        assert bnd.labels.called and bnd.recorded.called
        cluster = _cluster(db, s)
        assert (cluster.status, cluster.site_activation_site, cluster.site_activation_heartbeat) == (
            Cluster.STATUS_ACTIVE, "", "")

    def test_a_fresh_activation_creates_every_lvstore(self, db, rpcs, hub, adopt):
        cluster = stack._seed_cluster(db, ftt=1)
        cluster.status = Cluster.STATUS_UNREADY
        cluster.write_to_db(db.kv_store)
        a, b = stack._sites(db, cluster)
        s = SimpleNamespace(cid=cluster.get_id(), a=a, b=b)
        bnd = _activate(db, s, site="")
        assert [c.args[1] for c in bnd.set_status.call_args_list] == [
            Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_ACTIVE]
        assert sorted(e[1] for e in bnd.calls("create")) == sorted(_ids(a) + _ids(b))
        assert bnd.calls("p1", *RESTART_ROUTES, "event") == []
        assert bnd.labels.called and bnd.recorded.called
        assert _cluster(db, s).site_activation_site == ""

    def test_a_fresh_non_sync_activation_creates_all(self, db, rpcs, hub, adopt):
        cluster = stack._seed_cluster(db, ftt=1)
        cluster.status = Cluster.STATUS_UNREADY
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        nodes = [stack._seed_node(db, cluster, f"n{i}", "") for i in range(3)]
        s = SimpleNamespace(cid=cluster.get_id(), a=nodes, b=[])
        bnd = _activate(db, s, site="")
        assert _cluster(db, s).sync_replication is False
        assert [c.args[1] for c in bnd.set_status.call_args_list] == [
            Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_ACTIVE]
        assert sorted(e[1] for e in bnd.calls("create")) == sorted(_ids(nodes))
        routes = [e[0] for e in bnd.log]
        assert "act" in routes and routes.index("act") > max(
            i for i, r in enumerate(routes) if r == "create")
        assert routes[-1] == "ana"
        assert bnd.calls("p1", *RESTART_ROUTES, "event") == []
        assert bnd.labels.called and bnd.recorded.called
        assert _cluster(db, s).site_activation_site == ""
