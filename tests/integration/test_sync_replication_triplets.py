"""Site-aware role assignment and remote triplets against the real FoundationDB.

On a sync-replication cluster the local secondary / tertiary of an LVS stay on
its home site and every LVS gets a remote triplet on the other site. Covers
activation (failure domains off and on), the splice repairs, expansion,
removal and the per-site failure-domain admission. Storage nodes and every
other external side effect are mocked; the database never is.
"""
import uuid
from unittest.mock import patch

import pytest

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.controllers.cluster_expansion import preconditions
from simplyblock_core.controllers.cluster_expansion.executor import integrate_new_node_into_cluster
from simplyblock_core.controllers.cluster_expansion.orchestrator import MoveExecutor, NoopMoveExecutor
from simplyblock_core.controllers.cluster_expansion.planner import (
    EXPAND_PHASE_ABORTED,
    EXPAND_PHASE_COMPLETED,
    JC_MAX_CONTEXTS_PER_NODE,
    RemoteTripletPlacementError,
    expand_state_rearm,
    jc_contexts_per_node,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

SITE_A = "site-a"
SITE_B = "site-b"


@pytest.fixture()
def db():
    return DBController()


def _seed_cluster(db, *, sync=True, ftt=1, fd=False, status=Cluster.STATUS_UNREADY, npcs=1):
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.cluster_name = f"cl-{cluster.uuid[:8]}"
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{cluster.uuid}"
    cluster.status = status
    cluster.ha_type = "ha"
    cluster.sync_replication = sync
    cluster.enable_failure_domain = fd
    cluster.distr_ndcs = 1
    cluster.distr_npcs = npcs
    cluster.max_fault_tolerance = ftt
    cluster.mode = "docker"
    cluster.cluster_vip = "10.0.0.1"
    cluster.write_to_db(db.kv_store)
    return cluster


def _seed_node(db, cluster, site, host, *, fd=-1, status=StorageNode.STATUS_ONLINE, name=None,
               lvstore=""):
    node = StorageNode()
    node.uuid = name or str(uuid.uuid4())
    node.cluster_id = cluster.get_id()
    node.site = site
    node.mgmt_ip = host
    node.status = status
    node.failure_domain = fd
    node.lvstore = lvstore
    device = NVMeDevice()
    device.uuid = str(uuid.uuid4())
    device.status = NVMeDevice.STATUS_ONLINE
    device.size = 10 * 1024 ** 3
    node.nvme_devices = [device]
    node.write_to_db(db.kv_store)
    return node


def _seed_site(db, cluster, site, count, *, fds=None, status=StorageNode.STATUS_ONLINE, lvstore=False):
    prefix = "a" if site == SITE_A else "b"
    net = 3 if site == SITE_A else 4
    return [_seed_node(db, cluster, site, f"10.{net}.0.{i + 1}",
                       fd=fds[i] if fds else -1, status=status,
                       name=f"{prefix}{i}-{uuid.uuid4().hex[:6]}",
                       lvstore=f"LVS_{prefix}{i}" if lvstore else "")
            for i in range(count)]


def _update(db, node, **fields):
    fresh = db.get_storage_node_by_id(node.get_id())
    for key, value in fields.items():
        setattr(fresh, key, value)
    fresh.write_to_db(db.kv_store)
    return fresh


def _wire_rotation(db, nodes, ftt):
    """Local rotation inside ``nodes`` (one site), pointers and back-refs."""
    n = len(nodes)
    for i, node in enumerate(nodes):
        _update(db, node, secondary_node_id=nodes[(i + 1) % n].get_id(),
                tertiary_node_id=nodes[(i + 2) % n].get_id() if ftt >= 2 else "",
                lvstore_stack_secondary=nodes[(i - 1) % n].get_id(),
                lvstore_stack_tertiary=nodes[(i - 2) % n].get_id() if ftt >= 2 else "")


def _nodes(db, cluster):
    return {n.get_id(): n for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())}


def _activate(cluster, force=False):
    """Run activation through the role assignment; Pass 1 is refused by the
    patched create_lvstore, which ends the activation with a plain error."""
    with patch.object(cluster_ops, "_wait_for_full_device_connectivity"), \
            patch.object(storage_node_ops, "create_lvstore", return_value=False) as create:
        with pytest.raises(ValueError, match="^Failed to activate cluster$"):
            cluster_ops._cluster_activate(cluster.get_id(), force=force)
    return create


def _assert_site_layout(db, cluster, ftt):
    """Local roles on the home site, remote triplet host-disjoint on the other."""
    nodes = _nodes(db, cluster)
    for owner in nodes.values():
        local = [owner.secondary_node_id] + ([owner.tertiary_node_id] if ftt >= 2 else [])
        assert all(local), owner.get_id()
        assert {nodes[m].site for m in local} == {owner.site}
        remote = storage_node_ops.remote_triplet_refs(owner)
        assert all(remote), owner.get_id()
        assert {nodes[m].site for m in remote} == ({SITE_A, SITE_B} - {owner.site})
        assert len({nodes[m].mgmt_ip for m in remote}) == 3
        assert len({nodes[m].mgmt_ip for m in [owner.get_id(), *local]}) == len(local) + 1
    return nodes


def _remote_role_counts(nodes, site):
    """Per remote slot, the sorted counts of that role over ``site``'s nodes."""
    ids = [n.get_id() for n in nodes.values() if n.site == site]
    counts = {i: [0, 0, 0] for i in ids}
    for owner in nodes.values():
        for slot, ref in enumerate(storage_node_ops.remote_triplet_refs(owner)):
            if ref in counts:
                counts[ref][slot] += 1
    return [sorted(c[slot] for c in counts.values()) for slot in range(3)]


def _contexts(nodes):
    instances = {}
    for owner in nodes.values():
        if owner.status == StorageNode.STATUS_REMOVED or not owner.secondary_node_id:
            continue
        instances[owner.get_id()] = [owner.secondary_node_id, owner.tertiary_node_id,
                                     *storage_node_ops.remote_triplet_refs(owner)]
    return jc_contexts_per_node(instances)


# ---------------------------------------------------------------------------
# activation
# ---------------------------------------------------------------------------

class TestActivation:

    @pytest.mark.parametrize("ftt,per_site", [(1, 3), (2, 4)])
    def test_local_roles_home_remote_triplet_other_site(self, db, ftt, per_site):
        cluster = _seed_cluster(db, ftt=ftt)
        _seed_site(db, cluster, SITE_A, per_site)
        _seed_site(db, cluster, SITE_B, per_site)
        create = _activate(cluster)
        assert create.call_count == 2 * per_site
        nodes = _assert_site_layout(db, cluster, ftt)
        for site in (SITE_A, SITE_B):
            assert _remote_role_counts(nodes, site) == [[1] * per_site] * 3
        # Local HA follows the FTT (no local tertiary on FTT1); the remote
        # triplet always has three members: 5 instances per LVS on FTT1, 6 on FTT2.
        for owner in nodes.values():
            assert bool(owner.tertiary_node_id) is (ftt >= 2)
        assert set(_contexts(nodes).values()) == {3 + ftt + 1}

    def test_failure_domains_rotate_per_site(self, db):
        cluster = _seed_cluster(db, ftt=2, fd=True)
        _seed_site(db, cluster, SITE_A, 6, fds=[0, 1, 2, 0, 1, 2])
        _seed_site(db, cluster, SITE_B, 6, fds=[3, 4, 5, 3, 4, 5])
        _activate(cluster)
        nodes = _assert_site_layout(db, cluster, 2)
        for owner in nodes.values():
            members = [owner.get_id(), owner.secondary_node_id, owner.tertiary_node_id]
            assert len({nodes[m].failure_domain for m in members}) == 3
            remote = storage_node_ops.remote_triplet_refs(owner)
            assert len({nodes[m].failure_domain for m in remote}) == 3

    def test_failure_domain_balance_is_judged_per_site(self, db):
        """Globally (3, 3, 3) hosts per domain, per site (2, 1, 1) and (1, 2, 2)."""
        cluster = _seed_cluster(db, fd=True)
        _seed_site(db, cluster, SITE_A, 4, fds=[0, 0, 1, 2])
        _seed_site(db, cluster, SITE_B, 5, fds=[0, 1, 1, 2, 2])
        with patch.object(cluster_ops, "_wait_for_full_device_connectivity"), \
                pytest.raises(ValueError, match="site site-a: failure domains must hold an EQUAL"):
            cluster_ops._cluster_activate(cluster.get_id())
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_UNREADY

    def test_failure_domain_count_is_judged_per_site(self, db):
        """Five domains overall, but site B has only two (a journal on failure
        domains needs four hosts per site)."""
        cluster = _seed_cluster(db, fd=True)
        _seed_site(db, cluster, SITE_A, 6, fds=[0, 1, 2, 0, 1, 2])
        _seed_site(db, cluster, SITE_B, 4, fds=[3, 4, 3, 4])
        with patch.object(cluster_ops, "_wait_for_full_device_connectivity"), \
                pytest.raises(ValueError, match="site site-b: failure domains are enabled"):
            cluster_ops._cluster_activate(cluster.get_id())

    def test_non_sync_failure_domain_balance_stays_cluster_wide(self, db):
        cluster = _seed_cluster(db, sync=False, fd=True)
        _seed_site(db, cluster, "", 4, fds=[0, 0, 1, 2])
        for i, fd in enumerate([0, 1, 1, 2, 2]):
            _seed_node(db, cluster, "", f"10.9.0.{i + 1}", fd=fd)
        _activate(cluster)
        assert all(storage_node_ops.remote_triplet_refs(n) == ("", "", "")
                   for n in _nodes(db, cluster).values())

    def test_non_sync_cluster_gets_no_remote_triplet(self, db):
        cluster = _seed_cluster(db, sync=False, ftt=2)
        for i in range(4):
            _seed_node(db, cluster, "", f"10.8.0.{i + 1}")
        _activate(cluster)
        nodes = _nodes(db, cluster)
        assert all(n.secondary_node_id and n.tertiary_node_id for n in nodes.values())
        assert all(storage_node_ops.remote_triplet_refs(n) == ("", "", "") for n in nodes.values())

    def test_jc_context_cap_refuses_activation(self, db):
        """20 owners on site A, 4 nodes on site B (FTT2): 15 remote roles plus
        3 local ones per site-B node."""
        cluster = _seed_cluster(db, ftt=2)
        _seed_site(db, cluster, SITE_A, 20)
        _seed_site(db, cluster, SITE_B, 4)
        with patch.object(cluster_ops, "_wait_for_full_device_connectivity"), \
                patch.object(storage_node_ops, "create_lvstore") as create, \
                pytest.raises(ValueError, match=r"Failed to activate cluster: .*more LVS instances"):
            cluster_ops._cluster_activate(cluster.get_id())
        create.assert_not_called()
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_UNREADY
        assert all(storage_node_ops.remote_triplet_refs(n) == ("", "", "")
                   for n in _nodes(db, cluster).values())


class TestReactivation:

    def _activated(self, db, *, b_status=StorageNode.STATUS_ONLINE, ftt=1):
        cluster = _seed_cluster(db, ftt=ftt, status=Cluster.STATUS_SUSPENDED)
        a = _seed_site(db, cluster, SITE_A, 3)
        b = _seed_site(db, cluster, SITE_B, 3, status=b_status)
        _wire_rotation(db, a, ftt)
        _wire_rotation(db, b, ftt)
        for i, owner in enumerate(a):
            _update(db, owner, remote_primary_node_id=b[i].get_id(),
                    remote_secondary_node_id=b[(i + 1) % 3].get_id(),
                    remote_tertiary_node_id=b[(i + 2) % 3].get_id())
        for i, owner in enumerate(b):
            _update(db, owner, remote_primary_node_id=a[i].get_id(),
                    remote_secondary_node_id=a[(i + 1) % 3].get_id(),
                    remote_tertiary_node_id=a[(i + 2) % 3].get_id())
        cluster.activated_node_ids = [n.get_id() for n in a + b]
        cluster.write_to_db(db.kv_store)
        return cluster, a, b

    def test_offline_site_keeps_every_remote_ref(self, db):
        cluster, a, b = self._activated(db, b_status=StorageNode.STATUS_OFFLINE)
        before = {n.get_id(): storage_node_ops.remote_triplet_refs(n) for n in _nodes(db, cluster).values()}
        _activate(cluster)
        after = {n.get_id(): storage_node_ops.remote_triplet_refs(n) for n in _nodes(db, cluster).values()}
        assert after == before

    def test_force_reactivation_keeps_refs_to_an_offline_site(self, db):
        cluster, a, b = self._activated(db, b_status=StorageNode.STATUS_OFFLINE)
        cluster.status = Cluster.STATUS_ACTIVE
        cluster.write_to_db(db.kv_store)
        before = {n.get_id(): storage_node_ops.remote_triplet_refs(n) for n in _nodes(db, cluster).values()}
        _activate(cluster, force=True)
        after = {n.get_id(): storage_node_ops.remote_triplet_refs(n) for n in _nodes(db, cluster).values()}
        assert after == before

    def test_a_missing_ref_cannot_be_filled_while_the_other_site_is_down(self, db):
        cluster, a, b = self._activated(db, b_status=StorageNode.STATUS_OFFLINE)
        _update(db, a[0], remote_secondary_node_id="")
        with patch.object(cluster_ops, "_wait_for_full_device_connectivity"), \
                pytest.raises(ValueError, match=f"remote triplet of node {a[0].get_id()}.*remote secondary"):
            cluster_ops._cluster_activate(cluster.get_id())
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_SUSPENDED

    def test_a_missing_ref_is_filled_on_the_other_site(self, db):
        cluster, a, b = self._activated(db)
        _update(db, a[0], remote_secondary_node_id="")
        _activate(cluster)
        refs = storage_node_ops.remote_triplet_refs(db.get_storage_node_by_id(a[0].get_id()))
        assert refs[0] == b[0].get_id() and refs[2] == b[2].get_id()
        assert refs[1] == b[1].get_id()

    @pytest.mark.parametrize("field", ["secondary_node_id", "tertiary_node_id"])
    def test_cross_site_local_pointer_is_refused(self, db, field):
        cluster, a, b = self._activated(db, ftt=2)
        _update(db, a[0], **{field: b[0].get_id()})
        with patch.object(cluster_ops, "_wait_for_full_device_connectivity") as wait, \
                pytest.raises(ValueError, match=f"local roles must stay on the home site: "
                                                f"{field.split('_')[0]} of node {a[0].get_id()}"):
            cluster_ops._cluster_activate(cluster.get_id())
        wait.assert_not_called()
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_SUSPENDED


# ---------------------------------------------------------------------------
# candidate lists and splice repairs
# ---------------------------------------------------------------------------

class TestLocalCandidates:

    @pytest.mark.parametrize("sync", [True, False])
    def test_get_secondary_nodes_stay_on_the_site(self, db, sync):
        """Every other site-A node already hosts a secondary and a tertiary:
        only site B has free candidates."""
        cluster = _seed_cluster(db, sync=sync, ftt=2)
        a = _seed_site(db, cluster, SITE_A, 3)
        b = _seed_site(db, cluster, SITE_B, 3)
        for node in a[1:]:
            _update(db, node, lvstore_stack_secondary="x", lvstore_stack_tertiary="x")
        b_ids = {n.get_id() for n in b}
        secs = storage_node_ops.get_secondary_nodes(a[0])
        terts = storage_node_ops.get_secondary_nodes_2(a[0])
        if sync:
            assert secs == [] and terts == []
        else:
            assert secs and set(secs) <= b_ids
            assert terts and set(terts) <= b_ids

    def test_get_secondary_nodes_pick_on_the_site(self, db):
        cluster = _seed_cluster(db, ftt=2)
        a = _seed_site(db, cluster, SITE_A, 3)
        _seed_site(db, cluster, SITE_B, 3)
        a_ids = {n.get_id() for n in a[1:]}
        assert set(storage_node_ops.get_secondary_nodes(a[0])) <= a_ids
        assert storage_node_ops.get_secondary_nodes_2(
            a[0], exclude_ids=[a[1].get_id()], exclude_mgmt_ips=[a[1].mgmt_ip]) == [a[2].get_id()]

    @pytest.mark.parametrize("sync", [True, False])
    def test_splice_secondary_never_uses_another_site(self, db, sync):
        """Site A's only edge points across sites; site B has a clean edge."""
        cluster = _seed_cluster(db, sync=sync)
        a = _seed_site(db, cluster, SITE_A, 3)
        b = _seed_site(db, cluster, SITE_B, 3)
        _update(db, a[0], secondary_node_id=b[2].get_id())
        _update(db, b[0], secondary_node_id=b[1].get_id())
        assert storage_node_ops.splice_stranded_secondary(
            db.get_storage_node_by_id(a[2].get_id())) is (not sync)

    def test_splice_secondary_uses_an_edge_of_its_site(self, db):
        cluster = _seed_cluster(db)
        a = _seed_site(db, cluster, SITE_A, 3)
        b = _seed_site(db, cluster, SITE_B, 3)
        _update(db, a[0], secondary_node_id=a[1].get_id())
        _update(db, b[0], secondary_node_id=b[1].get_id())
        assert storage_node_ops.splice_stranded_secondary(db.get_storage_node_by_id(a[2].get_id()))
        nodes = _nodes(db, cluster)
        assert nodes[a[0].get_id()].secondary_node_id == a[2].get_id()
        assert nodes[a[2].get_id()].secondary_node_id == a[1].get_id()
        assert nodes[b[0].get_id()].secondary_node_id == b[1].get_id()

    @pytest.mark.parametrize("sync", [True, False])
    def test_splice_tertiary_never_uses_another_site(self, db, sync):
        cluster = _seed_cluster(db, sync=sync, ftt=2)
        a = _seed_site(db, cluster, SITE_A, 4)
        b = _seed_site(db, cluster, SITE_B, 4)
        _update(db, a[3], secondary_node_id=a[0].get_id())
        _update(db, a[1], tertiary_node_id=b[3].get_id())
        _update(db, b[0], tertiary_node_id=b[2].get_id())
        assert storage_node_ops.splice_stranded_tertiary(
            db.get_storage_node_by_id(a[3].get_id())) is (not sync)

    @pytest.mark.parametrize("sync", [True, False])
    def test_relocation_splice_target_stays_on_the_site(self, db, sync):
        cluster = _seed_cluster(db, sync=sync)
        a = _seed_site(db, cluster, SITE_A, 3)
        b = _seed_site(db, cluster, SITE_B, 3)
        _update(db, b[0], secondary_node_id=b[1].get_id())
        target = storage_node_ops._find_splice_target_for_relocation(
            db.get_storage_node_by_id(a[0].get_id()), "secondary", db)
        assert (target is None) is sync

    def test_removal_planner_sees_only_the_removed_nodes_site(self, db):
        cluster = _seed_cluster(db)
        a = _seed_site(db, cluster, SITE_A, 4, lvstore=True)
        b = _seed_site(db, cluster, SITE_B, 4, lvstore=True)
        _wire_rotation(db, a, 1)
        _wire_rotation(db, b, 1)
        _update(db, a[3], status=StorageNode.STATUS_OFFLINE)
        inputs = storage_node_ops._relocation_planner_inputs(b[0], db, allow_without_fd=True)
        assert inputs is not None
        assert sorted(inputs[0]) == sorted(n.get_id() for n in b[1:])

    def test_relocation_pick_stays_on_the_site(self, db):
        cluster = _seed_cluster(db)
        a = _seed_site(db, cluster, SITE_A, 4, lvstore=True)
        b = _seed_site(db, cluster, SITE_B, 4, lvstore=True)
        _wire_rotation(db, a, 1)
        _wire_rotation(db, b, 1)
        primary = db.get_storage_node_by_id(a[0].get_id())
        picked = storage_node_ops._pick_replica_relocation_node(
            primary, db.get_storage_node_by_id(a[1].get_id()), "secondary", db)
        assert picked in {n.get_id() for n in a}


# ---------------------------------------------------------------------------
# expansion
# ---------------------------------------------------------------------------

class _FailAfter(MoveExecutor):
    """Executes ``fail_at`` moves -- re-pointing the primary's role in FDB like
    the real executor -- then fails."""

    def __init__(self, db, fail_at):
        self.db = db
        self.fail_at = fail_at
        self.executed = []

    def execute(self, move):
        if len(self.executed) == self.fail_at:
            raise RuntimeError("move failed")
        if move.role in ("secondary", "tertiary"):
            _update(self.db, self.db.get_storage_node_by_id(move.lvs_primary_node_id),
                    **{f"{move.role}_node_id": move.to_node_id})
        self.executed.append(move)


def _active_cluster(db, *, ftt=1, per_site=3, fd=False, fds_a=None, fds_b=None):
    cluster = _seed_cluster(db, ftt=ftt, fd=fd, status=Cluster.STATUS_ACTIVE)
    a = _seed_site(db, cluster, SITE_A, per_site, lvstore=True, fds=fds_a)
    b = _seed_site(db, cluster, SITE_B, per_site, lvstore=True, fds=fds_b)
    _wire_rotation(db, a, ftt)
    _wire_rotation(db, b, ftt)
    storage_node_ops.assign_remote_triplets(cluster, [n.get_id() for n in a + b])
    return cluster, a, b


class TestExpansion:

    @pytest.mark.parametrize("fd", [False, True])
    def test_newcomer_joins_its_site_and_gets_a_remote_triplet(self, db, fd):
        cluster, a, b = _active_cluster(db, fd=fd, fds_a=[0, 1, 2], fds_b=[3, 4, 5])
        before = {n.get_id(): storage_node_ops.remote_triplet_refs(n)
                  for n in _nodes(db, cluster).values()}
        newcomer = _seed_node(db, cluster, SITE_A, "10.3.0.9", fd=0 if fd else -1)
        executor = NoopMoveExecutor()
        integrate_new_node_into_cluster(db.get_cluster_by_id(cluster.get_id()), newcomer,
                                        executor=executor, db_controller=db)
        a_ids = {n.get_id() for n in a} | {newcomer.get_id()}
        assert executor.executed
        for move in executor.executed:
            assert {move.lvs_primary_node_id, move.to_node_id} <= a_ids
            assert move.from_node_id in a_ids | {""}
        nodes = _nodes(db, cluster)
        refs = storage_node_ops.remote_triplet_refs(nodes[newcomer.get_id()])
        assert {nodes[r].site for r in refs} == {SITE_B}
        assert len({nodes[r].mgmt_ip for r in refs}) == 3
        assert {n: storage_node_ops.remote_triplet_refs(nodes[n]) for n in before} == before
        assert db.get_cluster_by_id(cluster.get_id()).expand_state["phase"] == EXPAND_PHASE_COMPLETED

    def test_remote_triplet_survives_a_failed_move_and_the_resume(self, db):
        """The first move lands in FDB, the second fails. The resume judges the
        JC cap on the same final layout as the fresh attempt did, although
        the DB pointers now reflect a partly executed plan."""
        cluster, a, b = _active_cluster(db)
        newcomer = _seed_node(db, cluster, SITE_A, "10.3.0.9")
        plan = storage_node_ops.plan_remote_triplets
        with patch.object(storage_node_ops, "plan_remote_triplets", wraps=plan) as fresh, \
                pytest.raises(RuntimeError, match="move failed"):
            integrate_new_node_into_cluster(db.get_cluster_by_id(cluster.get_id()), newcomer,
                                            executor=_FailAfter(db, 1), db_controller=db)
        stored = db.get_cluster_by_id(cluster.get_id())
        assert stored.expand_state["phase"] == EXPAND_PHASE_ABORTED
        assert stored.expand_state["cursor"] == 1
        first = stored.expand_state["moves"][0]
        assert getattr(db.get_storage_node_by_id(first["lvs_primary_node_id"]),
                       f"{first['role']}_node_id") == first["to_node_id"]
        refs = storage_node_ops.remote_triplet_refs(db.get_storage_node_by_id(newcomer.get_id()))
        assert all(refs)

        stored.expand_state = expand_state_rearm(stored.expand_state)
        stored.write_to_db(db.kv_store)
        executor = NoopMoveExecutor()
        with patch.object(storage_node_ops, "plan_remote_triplets", wraps=plan) as resumed, \
                patch.object(storage_node_ops.role_planner, "pick_remote_triplet",
                             wraps=storage_node_ops.role_planner.pick_remote_triplet) as pick:
            integrate_new_node_into_cluster(db.get_cluster_by_id(cluster.get_id()), newcomer,
                                            executor=executor, db_controller=db)
        final_layout = fresh.call_args.kwargs["local_layout"]
        assert final_layout[newcomer.get_id()][0]
        assert resumed.call_args.kwargs["local_layout"] == final_layout
        assert len(executor.executed) == len(stored.expand_state["moves"]) - 1
        assert pick.call_args.kwargs["keep"] == tuple(
            storage_node_ops._site_node(db.get_storage_node_by_id(r)) for r in refs)
        assert storage_node_ops.remote_triplet_refs(db.get_storage_node_by_id(newcomer.get_id())) == refs
        assert db.get_cluster_by_id(cluster.get_id()).expand_state["phase"] == EXPAND_PHASE_COMPLETED

    def test_jc_context_cap_refuses_before_any_move(self, db):
        """Site B's three nodes already carry 14 remote roles plus 2 local ones;
        a 15th owner would put 17 contexts on each."""
        cluster = _seed_cluster(db, status=Cluster.STATUS_ACTIVE)
        a = _seed_site(db, cluster, SITE_A, 14, lvstore=True)
        b = _seed_site(db, cluster, SITE_B, 3, lvstore=True)
        _wire_rotation(db, a, 1)
        _wire_rotation(db, b, 1)
        storage_node_ops.assign_remote_triplets(cluster, [n.get_id() for n in a + b])
        assert max(_contexts(_nodes(db, cluster)).values()) == JC_MAX_CONTEXTS_PER_NODE
        newcomer = _seed_node(db, cluster, SITE_A, "10.3.0.99")
        executor = NoopMoveExecutor()
        with pytest.raises(RemoteTripletPlacementError, match=r"\(17\)"):
            integrate_new_node_into_cluster(db.get_cluster_by_id(cluster.get_id()), newcomer,
                                            executor=executor, db_controller=db)
        assert executor.executed == []
        assert db.get_cluster_by_id(cluster.get_id()).expand_state == {}
        assert storage_node_ops.remote_triplet_refs(
            db.get_storage_node_by_id(newcomer.get_id())) == ("", "", "")


# ---------------------------------------------------------------------------
# removal
# ---------------------------------------------------------------------------

class TestRemoval:

    def test_remote_roles_of_a_removed_node_move_to_its_site(self, db):
        cluster, a, b = _active_cluster(db, per_site=4)
        removed = b[0]
        holders = storage_node_ops.owners_with_remote_role_on(
            removed.get_id(), list(_nodes(db, cluster).values()))
        assert holders
        before = {n.get_id(): storage_node_ops.remote_triplet_refs(n) for n in _nodes(db, cluster).values()}

        assert storage_node_ops._check_replica_relocation_feasible(removed, db) == (True, "")
        assert storage_node_ops._reselect_remote_roles_held_by(removed) is True

        nodes = _nodes(db, cluster)
        for owner_id in holders:
            refs = storage_node_ops.remote_triplet_refs(nodes[owner_id])
            assert removed.get_id() not in refs
            assert {nodes[r].site for r in refs} == {SITE_B}
            assert len({nodes[r].mgmt_ip for r in refs}) == 3
            for slot, old in enumerate(before[owner_id]):
                if old != removed.get_id():
                    assert refs[slot] == old
        # Everyone else, the removed node's own refs included, is untouched.
        for node_id in set(before) - set(holders):
            assert storage_node_ops.remote_triplet_refs(nodes[node_id]) == before[node_id]

        with patch.object(DBController, "atomic_update") as write:
            assert storage_node_ops._reselect_remote_roles_held_by(removed) is True
        write.assert_not_called()

    def test_removal_is_refused_when_the_site_has_no_replacement(self, db):
        cluster, a, b = _active_cluster(db)
        ok, reason = storage_node_ops._check_replica_relocation_feasible(b[0], db)
        assert ok is False
        assert "no replacement for the remote-triplet roles it holds" in reason
        assert "no node left for the remote" in reason
        with patch.object(storage_node_ops.logger, "error") as error:
            assert storage_node_ops._reselect_remote_roles_held_by(b[0]) is False
        assert "cannot re-select" in error.call_args.args[0]

    def test_removing_a_node_with_no_remote_role_needs_no_reselection(self, db):
        cluster, a, b = _active_cluster(db)
        idle = _seed_node(db, cluster, SITE_B, "10.4.0.99", lvstore="LVS_idle")
        assert storage_node_ops.owners_with_remote_role_on(
            idle.get_id(), list(_nodes(db, cluster).values())) == []
        with patch.object(DBController, "atomic_update") as write:
            assert storage_node_ops._reselect_remote_roles_held_by(idle) is True
        write.assert_not_called()

    def test_admission_and_phase_3d_agree_across_a_local_move(self, db):
        """Admission plans the re-selection before phase 3b moves the local
        roles the removed node hosted; phase 3d, after that move, lands on the
        same triplets (remote picks do not depend on local roles) and within
        the JC-context cap."""
        cluster, a, b = _active_cluster(db, ftt=2, per_site=5)
        removed = b[0]
        nodes = _nodes(db, cluster)
        holders = storage_node_ops.owners_with_remote_role_on(removed.get_id(), list(nodes.values()))
        planned = storage_node_ops.plan_remote_triplets(
            db.get_cluster_by_id(cluster.get_id()), list(nodes.values()), holders,
            exclude_ids=[removed.get_id()])
        assert storage_node_ops._check_replica_relocation_feasible(removed, db)[0] is True

        # Phase 3a / 3b, as far as the pointers go: the removed node's own LVS
        # is gone, the roles it hosted move to another node of its site.
        for owner in nodes.values():
            for field in ("secondary_node_id", "tertiary_node_id"):
                if getattr(owner, field) == removed.get_id():
                    free = [n.get_id() for n in b[1:]
                            if n.get_id() not in (owner.get_id(), owner.secondary_node_id,
                                                  owner.tertiary_node_id)]
                    _update(db, owner, **{field: free[0]})
        _update(db, removed, status=StorageNode.STATUS_REMOVED, secondary_node_id="",
                tertiary_node_id="")

        assert storage_node_ops._reselect_remote_roles_held_by(removed) is True
        nodes = _nodes(db, cluster)
        assert {o: storage_node_ops.remote_triplet_refs(nodes[o]) for o in holders} == planned
        counts = _contexts(nodes)
        assert removed.get_id() not in counts
        assert max(counts.values()) <= JC_MAX_CONTEXTS_PER_NODE


# ---------------------------------------------------------------------------
# failure-domain admission per site
# ---------------------------------------------------------------------------

class TestFailureDomainAdmission:
    """Site A hosts per domain (3, 1, 1), site B (1, 3, 3): (4, 4, 4) overall."""

    def _layout(self, db, sync):
        cluster = _seed_cluster(db, sync=sync, fd=True, status=Cluster.STATUS_ACTIVE)
        a = _seed_site(db, cluster, SITE_A, 5, fds=[0, 0, 0, 1, 2], lvstore=True)
        b = _seed_site(db, cluster, SITE_B, 7, fds=[0, 1, 1, 1, 2, 2, 2], lvstore=True)
        return cluster, a, b

    def test_current_balance(self, db):
        cluster, _, _ = self._layout(db, sync=True)
        ok, reason = preconditions.check_fd_balance_current(cluster, db)
        assert ok is False and reason.startswith("site site-a: failure domains would be unbalanced")
        cluster, _, _ = self._layout(db, sync=False)
        assert preconditions.check_fd_balance_current(cluster, db) == (True, "")

    def test_add_is_judged_on_the_new_nodes_site(self, db):
        cluster, _, _ = self._layout(db, sync=True)
        ok, reason = preconditions.check_fd_admission_for_add(
            cluster, db, 0, new_mgmt_ip="10.3.0.50", new_site=SITE_A)
        assert ok is False and "site site-a" in reason
        assert preconditions.check_fd_admission_for_add(
            cluster, db, 0, new_mgmt_ip="10.4.0.50", new_site=SITE_B) == (True, "")

    def test_remove_is_judged_on_the_nodes_site(self, db):
        """Removing a domain-1 host of site B: (1, 2, 3) there, (4, 3, 4) overall."""
        cluster, a, b = self._layout(db, sync=True)
        ok, reason = preconditions.check_fd_admission_for_remove(cluster, db, b[1])
        assert ok is False and "site site-b" in reason
        cluster, a, b = self._layout(db, sync=False)
        assert preconditions.check_fd_admission_for_remove(cluster, db, b[1]) == (True, "")
