"""Tests for the node that gets a device-migration task on a sync-replication
cluster: the restart re-balance, the failed-device migration and the
new-device migration. The database is real FoundationDB, and no storage
node is called.

The task goes to the primary of the LVS's active triplet
(tasks_controller.migration_role_node):
- the owner, when the home site leads the LVS;
- the primary of the remote triplet, when the other site leads the LVS;
- the owner, while the LVS is being moved.
A failed-device migration of a primary that is being removed goes to another
member of the active triplet. There is at most one open task per distrib, on
any node. A cluster without sync replication queues the tasks on the owner.
"""
import pytest

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db

LVS_1_DISTRIBS = ["distrib_11", "distrib_12"]
LVS_2_DISTRIBS = ["distrib_21", "distrib_22"]
SHUT_DOWN = list(StorageNode.REMOVAL_SHUT_DOWN_STATUSES)


def _layout(db, *, active_site=""):
    """a0 owns LVS_1, with the remote triplet b0, b1, b2. b0 owns LVS_2, with
    the remote triplet a0, a1, a2. ``active_site`` is the site that leads
    LVS_1; when it is empty, the home site leads."""
    cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
    if active_site:
        owner_a = stack._update(db, owner_a, lvs_active_site=active_site)
    return cluster, a, b, owner_a, owner_b


def _status(db, node, status):
    return stack._update(db, node, status=status)


def _device(db, cluster, node):
    """``node``'s data device, registered with the cluster."""
    fresh = stack._fresh(db, node)
    fresh.nvme_devices[0].cluster_id = cluster.get_id()
    fresh.write_to_db(db.kv_store)
    return fresh.nvme_devices[0].get_id()


def _tasks(db, cluster, function_name):
    return [t for t in db.get_job_tasks(cluster.get_id())
            if t.function_name == function_name and t.status != JobSchedule.STATUS_DONE and not t.canceled]


def _placed(db, cluster, function_name):
    """{distr_name: node_id} of the open ``function_name`` tasks."""
    tasks = _tasks(db, cluster, function_name)
    placed = {t.function_params["distr_name"]: t.node_id for t in tasks}
    assert len(placed) == len(tasks), "two open tasks for one distrib"
    return placed


def _role(db, owner, function_name):
    return tasks_controller.migration_role_node(stack._fresh(db, owner), function_name)


ALL_FAMILIES = [JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG]
#: A peer slot of LVS_1's remote triplet whose node does not exist.
DANGLING_PEERS = ["remote_secondary_node_id", "remote_tertiary_node_id"]


class TestMigrationRoleNode:

    @pytest.mark.parametrize("fn", ALL_FAMILIES)
    def test_home_led_is_the_owner(self, db, fn):
        _, _, _, owner_a, _ = _layout(db)
        placement = _role(db, owner_a, fn)
        assert placement.node.get_id() == owner_a.get_id()
        assert placement.reason == ""

    @pytest.mark.parametrize("fn", ALL_FAMILIES)
    def test_led_from_the_other_site_is_the_remote_primary(self, db, fn):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        assert _role(db, owner_a, fn).node.get_id() == b[0].get_id()

    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    @pytest.mark.parametrize("fn", ALL_FAMILIES)
    def test_moving_has_no_node(self, db, fn, target):
        _, _, _, owner_a, _ = _layout(db, active_site=f"moving:{target}")
        assert _role(db, owner_a, fn) == (None, tasks_controller.PLACEMENT_MOVING)

    @pytest.mark.parametrize("status", SHUT_DOWN)
    def test_failed_device_of_a_shut_down_home_primary_goes_to_its_secondary(self, db, status):
        _, a, _, owner_a, _ = _layout(db)
        _status(db, owner_a, status)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == a[1].get_id()

    def test_failed_device_of_a_shut_down_owner_led_remotely_goes_to_the_remote_primary(self, db):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        _status(db, owner_a, StorageNode.STATUS_MIGRATING_DEVICES)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == b[0].get_id()

    @pytest.mark.parametrize("down, expected", [
        ([0], 1),
        ([0, 1], 2),
        ([0, 1, 2], None),
    ])
    def test_failed_device_walks_the_active_triplet(self, db, down, expected):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        for i in down:
            _status(db, b[i], StorageNode.STATUS_IN_REMOVAL)
        placement = _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG)
        if expected is None:
            assert placement == (None, tasks_controller.PLACEMENT_DEPARTING)
        else:
            assert placement.node.get_id() == b[expected].get_id()

    def test_failed_device_skips_a_pending_removal_secondary(self, db):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        _status(db, b[0], StorageNode.STATUS_IN_REMOVAL)
        _status(db, b[1], StorageNode.STATUS_PENDING_REMOVAL)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == b[2].get_id()

    def test_failed_device_with_no_tertiary_slot(self, db):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        owner_a = stack._update(db, owner_a, remote_tertiary_node_id="")
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == b[0].get_id()
        _status(db, b[0], StorageNode.STATUS_REMOVED)
        _status(db, b[1], StorageNode.STATUS_REMOVED)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG) == (None, tasks_controller.PLACEMENT_DEPARTING)

    def test_pending_removal_primary_keeps_failed_device(self, db):
        _, _, _, owner_a, _ = _layout(db)
        _status(db, owner_a, StorageNode.STATUS_PENDING_REMOVAL)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == owner_a.get_id()

    @pytest.mark.parametrize("status", list(StorageNode.DEPARTING_STATUSES))
    @pytest.mark.parametrize("fn", [JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG])
    def test_departing_primary_runs_no_rebalance(self, db, fn, status):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        _status(db, b[0], status)
        assert _role(db, owner_a, fn) == (None, tasks_controller.PLACEMENT_DEPARTING)

    def test_no_remote_primary_is_an_error(self, db):
        _, _, _, owner_a, _ = _layout(db, active_site=SITE_B)
        owner_a = stack._update(db, owner_a, remote_primary_node_id="")
        with pytest.raises(KeyError):
            tasks_controller.migration_role_node(owner_a, JobSchedule.FN_FAILED_DEV_MIG)

    def test_a_missing_remote_primary_is_an_error(self, db):
        _, _, _, owner_a, _ = _layout(db, active_site=SITE_B)
        owner_a = stack._update(db, owner_a, remote_primary_node_id="no-such-node")
        with pytest.raises(KeyError):
            tasks_controller.migration_role_node(owner_a, JobSchedule.FN_FAILED_DEV_MIG)

    @pytest.mark.parametrize("slot", DANGLING_PEERS)
    @pytest.mark.parametrize("fn", ALL_FAMILIES)
    def test_a_missing_peer_does_not_hide_the_primary(self, db, fn, slot):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        stack._update(db, owner_a, **{slot: "no-such-node"})
        assert _role(db, owner_a, fn).node.get_id() == b[0].get_id()

    @pytest.mark.parametrize("slot", DANGLING_PEERS)
    def test_a_pending_removal_primary_keeps_failed_device_with_a_missing_peer(self, db, slot):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        stack._update(db, owner_a, **{slot: "no-such-node"})
        _status(db, b[0], StorageNode.STATUS_PENDING_REMOVAL)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == b[0].get_id()

    def test_a_missing_secondary_read_by_the_removal_exception_is_an_error(self, db):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        stack._update(db, owner_a, remote_secondary_node_id="no-such-node")
        _status(db, b[0], StorageNode.STATUS_IN_REMOVAL)
        with pytest.raises(KeyError, match="no-such-node"):
            _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG)

    def test_the_removal_exception_reads_the_tertiary_only_after_the_secondary(self, db):
        _, _, b, owner_a, _ = _layout(db, active_site=SITE_B)
        stack._update(db, owner_a, remote_tertiary_node_id="no-such-node")
        _status(db, b[0], StorageNode.STATUS_IN_REMOVAL)
        assert _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG).node.get_id() == b[1].get_id()
        _status(db, b[1], StorageNode.STATUS_IN_REMOVAL)
        with pytest.raises(KeyError, match="no-such-node"):
            _role(db, owner_a, JobSchedule.FN_FAILED_DEV_MIG)


class TestRestartRebalance:

    def test_led_from_the_other_site_queues_on_the_remote_primary(self, db):
        cluster, a, b, owner_a, owner_b = _layout(db, active_site=SITE_B)
        assert tasks_controller.add_device_mig_task_for_node(a[1].get_id())

        placed = _placed(db, cluster, JobSchedule.FN_DEV_MIG)
        assert placed == {d: b[0].get_id() for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS}
        by_distr = {t.function_params["distr_name"]: t for t in _tasks(db, cluster, JobSchedule.FN_DEV_MIG)}
        assert by_distr["distrib_11"].function_params == {
            "distr_name": "distrib_11", "lvs_name": "LVS_1", "lvs_owner": owner_a.get_id()}
        assert by_distr["distrib_21"].function_params == {
            "distr_name": "distrib_21", "lvs_name": "LVS_2", "lvs_owner": owner_b.get_id()}

        masters = _tasks(db, cluster, JobSchedule.FN_BALANCING_AFTER_NODE_RESTART)
        assert len(masters) == 1
        assert sorted(masters[0].sub_tasks) == sorted(t.uuid for t in by_distr.values())

    def test_home_led_queues_on_the_owner(self, db):
        cluster, _, b, owner_a, _ = _layout(db)
        tasks_controller.add_device_mig_task_for_node(owner_a.get_id())
        assert _placed(db, cluster, JobSchedule.FN_DEV_MIG) == {
            **{d: owner_a.get_id() for d in LVS_1_DISTRIBS}, **{d: b[0].get_id() for d in LVS_2_DISTRIBS}}

    def test_moving_queues_on_the_owner(self, db):
        cluster, _, _, owner_a, _ = _layout(db, active_site=f"moving:{SITE_B}")
        tasks_controller.add_device_mig_task_for_node(owner_a.get_id())
        tasks = [t for t in _tasks(db, cluster, JobSchedule.FN_DEV_MIG)
                 if t.function_params["distr_name"] in LVS_1_DISTRIBS]
        assert {t.node_id for t in tasks} == {owner_a.get_id()}
        assert all(t.function_params["lvs_owner"] == owner_a.get_id() for t in tasks)

    def test_a_second_trigger_after_a_promote_adds_no_duplicate(self, db):
        cluster, a, b, owner_a, _ = _layout(db)
        tasks_controller.add_device_mig_task_for_node(owner_a.get_id())
        stack._update(db, owner_a, lvs_active_site=SITE_B)
        assert not tasks_controller.add_device_mig_task_for_node(a[1].get_id())
        assert _placed(db, cluster, JobSchedule.FN_DEV_MIG)["distrib_11"] == owner_a.get_id()
        assert len(_tasks(db, cluster, JobSchedule.FN_BALANCING_AFTER_NODE_RESTART)) == 1


def _close(db, task, how):
    fresh = db.get_task_by_id(task.uuid)
    if how == "done":
        fresh.status = JobSchedule.STATUS_DONE
    else:
        fresh.canceled = True
    fresh.write_to_db(db.kv_store)


@pytest.mark.parametrize("how", ["done", "canceled"])
@pytest.mark.parametrize("fn", ALL_FAMILIES)
def test_a_closed_task_is_replaced_on_the_new_primary(db, fn, how):
    """The duplicate check covers all nodes. After a promote, the open
    distrib_11 task on the owner stops a second distrib_11 task on the remote
    primary. The closed distrib_12 task does not stop a new one."""
    cluster, a, b, owner_a, _ = _layout(db)
    dev_id = _device(db, cluster, a[1])
    create = {
        JobSchedule.FN_DEV_MIG: lambda: tasks_controller.add_device_mig_task_for_node(a[1].get_id()),
        JobSchedule.FN_FAILED_DEV_MIG: lambda: tasks_controller.add_device_failed_mig_task(dev_id),
        JobSchedule.FN_NEW_DEV_MIG: lambda: tasks_controller.add_new_device_mig_task(dev_id),
    }[fn]
    create()
    first = {t.function_params["distr_name"]: t for t in _tasks(db, cluster, fn)}
    _close(db, first["distrib_12"], how)
    stack._update(db, owner_a, lvs_active_site=SITE_B)

    create()

    placed = _placed(db, cluster, fn)
    assert placed["distrib_11"] == owner_a.get_id()
    assert placed["distrib_12"] == b[0].get_id()


class TestFailedDevice:

    def test_led_from_the_other_site_queues_on_the_remote_primary(self, db):
        cluster, a, b, owner_a, _ = _layout(db, active_site=SITE_B)
        tasks_controller.add_device_failed_mig_task(_device(db, cluster, a[1]))
        placed = _placed(db, cluster, JobSchedule.FN_FAILED_DEV_MIG)
        assert placed == {d: b[0].get_id() for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS}
        task = next(t for t in _tasks(db, cluster, JobSchedule.FN_FAILED_DEV_MIG)
                    if t.function_params["distr_name"] == "distrib_11")
        assert task.function_params["lvs_owner"] == owner_a.get_id()
        assert task.function_params["lvs_name"] == "LVS_1"

    def test_removal_of_a_home_led_owner_queues_on_its_secondary(self, db):
        cluster, a, b, owner_a, _ = _layout(db)
        dev_id = _device(db, cluster, owner_a)
        _status(db, owner_a, StorageNode.STATUS_MIGRATING_DEVICES)
        tasks_controller.add_device_failed_mig_task(dev_id)
        assert _placed(db, cluster, JobSchedule.FN_FAILED_DEV_MIG) == {
            **{d: a[1].get_id() for d in LVS_1_DISTRIBS}, **{d: b[0].get_id() for d in LVS_2_DISTRIBS}}

    def test_removal_of_an_owner_led_remotely_queues_on_the_remote_primary(self, db):
        cluster, a, b, owner_a, _ = _layout(db, active_site=SITE_B)
        dev_id = _device(db, cluster, owner_a)
        _status(db, owner_a, StorageNode.STATUS_MIGRATING_DEVICES)
        tasks_controller.add_device_failed_mig_task(dev_id)
        placed = _placed(db, cluster, JobSchedule.FN_FAILED_DEV_MIG)
        assert {placed[d] for d in LVS_1_DISTRIBS} == {b[0].get_id()}
        assert not {a[1].get_id(), a[2].get_id()} & set(placed.values())

    def test_removal_of_the_remote_primary_queues_on_the_next_member(self, db):
        cluster, _, b, _, _ = _layout(db, active_site=SITE_B)
        dev_id = _device(db, cluster, b[0])
        _status(db, b[0], StorageNode.STATUS_MIGRATING_DEVICES)
        tasks_controller.add_device_failed_mig_task(dev_id)
        assert _placed(db, cluster, JobSchedule.FN_FAILED_DEV_MIG) == {
            d: b[1].get_id() for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS}

    def test_pending_removal_is_not_remapped(self, db):
        cluster, _, b, owner_a, _ = _layout(db)
        dev_id = _device(db, cluster, owner_a)
        _status(db, owner_a, StorageNode.STATUS_PENDING_REMOVAL)
        tasks_controller.add_device_failed_mig_task(dev_id)
        assert _placed(db, cluster, JobSchedule.FN_FAILED_DEV_MIG) == {
            **{d: owner_a.get_id() for d in LVS_1_DISTRIBS}, **{d: b[0].get_id() for d in LVS_2_DISTRIBS}}

    def test_another_device_of_the_same_distrib_gets_its_own_task(self, db):
        cluster, a, _, _, _ = _layout(db)
        first, second = _device(db, cluster, a[1]), _device(db, cluster, a[2])
        tasks_controller.add_device_failed_mig_task(first)
        tasks_controller.add_device_failed_mig_task(second)
        tasks = _tasks(db, cluster, JobSchedule.FN_FAILED_DEV_MIG)
        assert sorted((t.device_id, t.function_params["distr_name"]) for t in tasks) == sorted(
            (dev, d) for dev in (first, second) for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS)


class TestNewDevice:

    def test_led_from_the_other_site_queues_on_the_remote_primary(self, db):
        cluster, a, b, _, _ = _layout(db, active_site=SITE_B)
        tasks_controller.add_new_device_mig_task(_device(db, cluster, a[1]))
        assert _placed(db, cluster, JobSchedule.FN_NEW_DEV_MIG) == {
            d: b[0].get_id() for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS}

    def test_departing_owner_led_remotely_still_queues_on_the_remote_primary(self, db):
        cluster, a, b, owner_a, _ = _layout(db, active_site=SITE_B)
        _status(db, owner_a, StorageNode.STATUS_MIGRATING_DEVICES)
        tasks_controller.add_new_device_mig_task(_device(db, cluster, a[1]))
        placed = _placed(db, cluster, JobSchedule.FN_NEW_DEV_MIG)
        assert {placed[d] for d in LVS_1_DISTRIBS} == {b[0].get_id()}

    @pytest.mark.parametrize("status", [StorageNode.STATUS_PENDING_REMOVAL, StorageNode.STATUS_IN_REMOVAL])
    def test_departing_primary_gets_no_task(self, db, status):
        cluster, a, b, _, _ = _layout(db, active_site=SITE_B)
        _status(db, b[0], status)
        tasks_controller.add_new_device_mig_task(_device(db, cluster, a[1]))
        assert _placed(db, cluster, JobSchedule.FN_NEW_DEV_MIG) == {}


BROKEN_TRIPLETS = {
    "no remote primary": {"remote_primary_node_id": ""},
    "missing remote primary": {"remote_primary_node_id": "no-such-node"},
}


class TestBrokenActiveTriplet:
    """SITE_B leads LVS_1 of a0, and the remote primary id of LVS_1 is empty or
    names a node that does not exist. Then a0 is skipped with a warning. The
    tasks of b0 (LVS_2) are still queued, in either order of the owners. A
    secondary or tertiary id that names no node does not skip the owner: its
    online primary runs the migrations."""

    @staticmethod
    def _broken_layout(db, broken, broken_first, **owner_fields):
        cluster, a, b, owner_a, owner_b = _layout(db, active_site=SITE_B)
        stack._update(db, owner_a, create_dt="1" if broken_first else "2", **broken, **owner_fields)
        stack._update(db, owner_b, create_dt="2" if broken_first else "1")
        return cluster, a, b, owner_a

    @staticmethod
    def _assert_skipped(caplog, fn, owner_a):
        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert any(fn in m and "LVS_1" in m and owner_a.get_id() in m for m in warnings), warnings

    @pytest.mark.parametrize("broken_first", [True, False])
    @pytest.mark.parametrize("broken", BROKEN_TRIPLETS.values(), ids=BROKEN_TRIPLETS.keys())
    @pytest.mark.parametrize("fn", [JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG])
    def test_device_tasks_queue_the_other_owners(self, db, caplog, fn, broken, broken_first):
        cluster, a, b, owner_a = self._broken_layout(db, broken, broken_first)
        dev_id = _device(db, cluster, a[1])
        {
            JobSchedule.FN_FAILED_DEV_MIG: tasks_controller.add_device_failed_mig_task,
            JobSchedule.FN_NEW_DEV_MIG: tasks_controller.add_new_device_mig_task,
        }[fn](dev_id)
        assert _placed(db, cluster, fn) == {d: b[0].get_id() for d in LVS_2_DISTRIBS}
        self._assert_skipped(caplog, fn, owner_a)

    @pytest.mark.parametrize("broken_first", [True, False])
    @pytest.mark.parametrize("broken", BROKEN_TRIPLETS.values(), ids=BROKEN_TRIPLETS.keys())
    def test_rebalance_queues_the_other_owners_under_a_master(self, db, caplog, broken, broken_first):
        cluster, a, b, owner_a = self._broken_layout(db, broken, broken_first)
        assert tasks_controller.add_device_mig_task_for_node(a[1].get_id())
        assert _placed(db, cluster, JobSchedule.FN_DEV_MIG) == {d: b[0].get_id() for d in LVS_2_DISTRIBS}
        masters = _tasks(db, cluster, JobSchedule.FN_BALANCING_AFTER_NODE_RESTART)
        assert len(masters) == 1
        assert sorted(masters[0].sub_tasks) == sorted(t.uuid for t in _tasks(db, cluster, JobSchedule.FN_DEV_MIG))
        self._assert_skipped(caplog, JobSchedule.FN_DEV_MIG, owner_a)

    @pytest.mark.parametrize("slot", DANGLING_PEERS)
    @pytest.mark.parametrize("fn", [JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG])
    def test_a_missing_peer_queues_on_the_primary(self, db, caplog, fn, slot):
        """The secondary or tertiary id of LVS_1's remote triplet names no node.
        Its primary b0 is online. The tasks of LVS_1 and of LVS_2 are both
        queued on b0, and no owner is skipped."""
        cluster, a, b, owner_a = self._broken_layout(db, {slot: "no-such-node"}, True)
        dev_id = _device(db, cluster, a[1])
        {
            JobSchedule.FN_FAILED_DEV_MIG: tasks_controller.add_device_failed_mig_task,
            JobSchedule.FN_NEW_DEV_MIG: tasks_controller.add_new_device_mig_task,
        }[fn](dev_id)
        assert _placed(db, cluster, fn) == {d: b[0].get_id() for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS}
        assert not [r for r in caplog.records if "cannot be resolved" in r.getMessage()]

    @pytest.mark.parametrize("slot", DANGLING_PEERS)
    def test_a_missing_peer_rebalances_on_the_primary(self, db, caplog, slot):
        cluster, a, b, owner_a = self._broken_layout(db, {slot: "no-such-node"}, True)
        assert tasks_controller.add_device_mig_task_for_node(a[1].get_id())
        assert _placed(db, cluster, JobSchedule.FN_DEV_MIG) == {
            d: b[0].get_id() for d in LVS_1_DISTRIBS + LVS_2_DISTRIBS}
        [master] = _tasks(db, cluster, JobSchedule.FN_BALANCING_AFTER_NODE_RESTART)
        assert sorted(master.sub_tasks) == sorted(t.uuid for t in _tasks(db, cluster, JobSchedule.FN_DEV_MIG))
        assert not [r for r in caplog.records if "cannot be resolved" in r.getMessage()]

    @pytest.mark.parametrize("fn", [JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG])
    def test_a_removed_owner_led_remotely(self, db, caplog, fn):
        """The removal has cleared the secondary and tertiary ids of the
        removed owner, while the other site leads its LVS. The owner still has
        its lvstore_stack."""
        cluster, a, b, owner_a = self._broken_layout(
            db, {}, True, status=StorageNode.STATUS_REMOVED, secondary_node_id="", tertiary_node_id="",
            remote_primary_node_id="", remote_secondary_node_id="", remote_tertiary_node_id="")
        dev_id = _device(db, cluster, a[1])
        {
            JobSchedule.FN_FAILED_DEV_MIG: tasks_controller.add_device_failed_mig_task,
            JobSchedule.FN_NEW_DEV_MIG: tasks_controller.add_new_device_mig_task,
        }[fn](dev_id)
        assert _placed(db, cluster, fn) == {d: b[0].get_id() for d in LVS_2_DISTRIBS}
        self._assert_skipped(caplog, fn, owner_a)


def _non_sync_layout(db):
    """A cluster without sync replication: n0 owns LVS_1 (secondary n1,
    tertiary n2)."""
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    n = [stack._seed_node(db, cluster, f"n{i}", "") for i in range(3)]
    owner = stack._update(db, n[0], secondary_node_id=n[1].get_id(), tertiary_node_id=n[2].get_id(),
                          lvstore="LVS_1", lvstore_stack=stack._stack(1))
    return cluster, n, owner


class TestWithoutSyncReplication:

    @pytest.mark.parametrize("fn", ALL_FAMILIES)
    def test_queued_on_the_owner_with_the_distrib_only(self, db, fn):
        cluster, n, owner = _non_sync_layout(db)
        dev_id = _device(db, cluster, n[1])
        {
            JobSchedule.FN_DEV_MIG: lambda: tasks_controller.add_device_mig_task_for_node(n[1].get_id()),
            JobSchedule.FN_FAILED_DEV_MIG: lambda: tasks_controller.add_device_failed_mig_task(dev_id),
            JobSchedule.FN_NEW_DEV_MIG: lambda: tasks_controller.add_new_device_mig_task(dev_id),
        }[fn]()
        tasks = _tasks(db, cluster, fn)
        assert {t.node_id for t in tasks} == {owner.get_id()}
        assert sorted((t.function_params for t in tasks), key=lambda p: p["distr_name"]) == [
            {"distr_name": d} for d in LVS_1_DISTRIBS]

    def test_removal_queues_on_the_departing_device_nodes_secondary(self, db):
        cluster, n, owner = _non_sync_layout(db)
        dev_id = _device(db, cluster, owner)
        _status(db, owner, StorageNode.STATUS_MIGRATING_DEVICES)
        tasks_controller.add_device_failed_mig_task(dev_id)
        assert _placed(db, cluster, JobSchedule.FN_FAILED_DEV_MIG) == {d: n[1].get_id() for d in LVS_1_DISTRIBS}

    def test_device_migration_lookups_stay_per_node(self, db):
        cluster, n, owner = _non_sync_layout(db)
        tasks_controller.add_device_mig_task_for_node(owner.get_id())
        cid = cluster.get_id()
        assert tasks_controller.get_device_mig_task(cid, owner.get_id(), "", "distrib_11")
        assert not tasks_controller.get_device_mig_task(cid, n[1].get_id(), "", "distrib_11")
        assert tasks_controller.get_device_mig_task(cid, None, "", "distrib_11")
