"""A sync-replication LVS that is open on a site with nobody leading it - the
state between a promote and the first client IO - under the health check
service and the storage-node monitor, against the real FoundationDB: neither
pass grants the leadership, queues a task, or changes a node's status or the
state of an LVS port because of it. The taker is stamped primary and reports
no leadership: (a) the owner after a fail-back promote, (b) the remote
primary after a promote to the other site.
No product code of its own implements this. It holds because the leadership a hublvol check lists as expected is never
compared (health_controller._check_node_hublvol / _check_sec_node_hublvol)
and because both services check an LVS on its home triplet only - the owner's
own lvstore and its local secondaries - never an instance of the remote one.
Storage nodes (RPC, node API), ping and the port block are mocked; the
database, the task queue and the status writes never are.
"""
import threading
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core import utils
from simplyblock_core.controllers import health_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import (
    JMDevice,
    NVMeDevice,
    RemoteDevice,
    RemoteJMDevice,
)
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.services import health_check_service, storage_node_monitor
from simplyblock_core.utils import port_block
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_update, _fresh = stack._update, stack._fresh

db = stack.db

#: The RPCs that hand an LVS its leadership (the reload and the grant) or take it away.
_LEADERSHIP_RPCS = ("bdev_lvol_set_leader", "bdev_lvol_update_lvstore")


class _Spdk(dict):
    """One fake SPDK per node id, healthy in everything a health or a monitor
    pass asks for: every bdev, subsystem and controller exists (a controller
    with two paths), the cluster map agrees with the database, no port is
    blocked. A test varies two things: what a node reports for an lvstore
    (``reports``) and which of its ports are blocked (``block``).

    The mocks are made up front (``add``), never on first use: both passes
    call a node from several threads at once."""

    _PATHS = ({"state": "enabled", "trid": {"traddr": "10.9.0.1", "trsvcid": "4420"}},
              {"state": "enabled", "trid": {"traddr": "10.9.0.2", "trsvcid": "4420"}})

    def __init__(self, db):
        super().__init__()
        self.db = db
        self.set_port = MagicMock(name="set_port")
        self._lvs: dict = {}
        self._blocked: dict = {}

    def add(self, node):
        node_id = node.get_id()
        lvs = self._lvs[node_id] = {}
        blocked = self._blocked[node_id] = set()
        rpc = create_autospec(RPCClient, instance=True, name=f"rpc-{node_id}")
        rpc.get_version.return_value = {"version": "test"}
        rpc.bdev_get.side_effect = lambda name: {"name": name}
        rpc.subsystem_get.side_effect = lambda nqn: {
            "nqn": nqn, "namespaces": [{"uuid": "ns"}],
            "listen_addresses": [{"traddr": "10.9.0.1", "trsvcid": "4420"}]}
        rpc.bdev_nvme_controller_list.side_effect = lambda name=None: self._controllers(node_id, name)
        rpc.distr_get_cluster_map.side_effect = lambda distr: self._cluster_map()
        rpc.nvmf_get_blocked_ports.side_effect = lambda: {
            "blocked_ports": [{"port": port} for port in sorted(blocked)]}
        rpc.jc_get_jm_status.return_value = {"jm": True}
        rpc.get_lvstore.side_effect = lambda name: dict(lvs[name]) if name in lvs else None
        self[node_id] = rpc

    def reports(self, node, info):
        """``node`` answers get_lvstore for ``info["name"]`` with ``info``."""
        self._lvs[node.get_id()][info["name"]] = info

    def block(self, node, port):
        self._blocked[node.get_id()].add(port)

    def lvstore_reads(self, node):
        """The lvstore names ``node`` was asked about, in order."""
        return [c.args[0] for c in self[node.get_id()].get_lvstore.call_args_list]

    def _controllers(self, node_id, name):
        """The named controller, or - asked for all - the ones the node's
        records say it has attached (the remote device sweep reads that)."""
        if name:
            return [{"name": name, "ctrlrs": list(self._PATHS)}]
        node = self.db.get_storage_node_by_id(node_id)
        return [{"name": dev.remote_bdev.removesuffix("n1"), "ctrlrs": list(self._PATHS)}
                for dev in (*node.remote_devices, *node.remote_jm_devices)]

    def _cluster_map(self):
        lines = []
        for node in self.db.get_storage_nodes():
            lines.append(f"uuid_node={node.get_id()}  status={node.status}")
            lines.extend(
                f"storage_ID={dev.cluster_device_order}  status={dev.status}  "
                f"uuid_device={dev.get_id()}  storage_bdev_name={dev.alceml_bdev}"
                for dev in node.nvme_devices)
        return "\n".join(lines)


@pytest.fixture()
def spdk(db):
    spdk = _Spdk(db)
    api = MagicMock(name="node-api")
    api.is_live.return_value = (True, None)
    api.spdk_process_is_up.return_value = (True, None)
    api.ping_ip.return_value = (True, None)
    with patch.object(StorageNode, "rpc_client", autospec=True,
                      side_effect=lambda self, *args, **kwargs: spdk[self.get_id()]), \
            patch.object(StorageNode, "client", autospec=True, return_value=api), \
            patch.object(utils, "ping_host", return_value=True), \
            patch.object(port_block, "set_port", spdk.set_port), \
            patch.object(ops, "ensure_nvme_options", return_value=(True, {})):
        yield spdk


class _Checks:
    """The verdicts of the two hublvol checks, the real ones called through."""

    def __init__(self):
        self.own: list = []   # (node id, verdict)
        self.sec: list = []   # (node id, primary id, verdict)
        self._lock = threading.Lock()

    def record(self, into, entry):
        with self._lock:
            into.append(entry)


@pytest.fixture()
def checks():
    """Spies on the two checks that list the leadership as expected. The
    health service runs them from worker threads."""
    seen = _Checks()
    real_own, real_sec = health_controller._check_node_hublvol, health_controller._check_sec_node_hublvol

    def _own(node):
        verdict = real_own(node)
        seen.record(seen.own, (node.get_id(), verdict))
        return verdict

    def _sec(node, auto_fix=False, primary_node_id=None, repair_paths=False):
        verdict = real_sec(node, auto_fix=auto_fix, primary_node_id=primary_node_id, repair_paths=repair_paths)
        seen.record(seen.sec, (node.get_id(), primary_node_id, verdict))
        return verdict

    with patch.object(health_controller, "_check_node_hublvol", side_effect=_own), \
            patch.object(health_controller, "_check_sec_node_hublvol", side_effect=_sec):
        yield seen


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _mesh(db, nodes):
    """Every node holds its remote data device and remote journal of every
    peer, as after a clean start: the passes find nothing to reconnect."""
    for node in nodes:
        devices, journals = [], []
        for peer in nodes:
            if peer.get_id() == node.get_id():
                continue
            for dev in peer.nvme_devices:
                remote = RemoteDevice()
                remote.uuid = dev.uuid
                remote.node_id = peer.get_id()
                remote.size = dev.size
                remote.status = NVMeDevice.STATUS_ONLINE
                remote.remote_bdev = f"remote_{dev.alceml_bdev}n1"
                devices.append(remote)
            journal = RemoteJMDevice()
            journal.uuid = peer.jm_device.uuid
            journal.node_id = peer.get_id()
            journal.jm_bdev = peer.jm_device.jm_bdev
            journal.status = JMDevice.STATUS_ONLINE
            journal.remote_bdev = ops.remote_jm_controller_name(
                node.site, peer.site, peer.jm_device.jm_bdev) + "n1"
            journals.append(journal)
        _update(db, node, remote_devices=devices, remote_jm_devices=journals)


def _lvs_info(owner, cluster, *, primary, leading):
    """What an instance of ``owner``'s LVS reports: stamped ``primary`` or a
    non-leader behind the hublvol, ``leading`` or not."""
    info = {"name": owner.lvstore, "base_bdev": owner.raid, "block_size": cluster.blk_size,
            "cluster_size": cluster.page_size_in_blocks, "lvs_read_only": False,
            "lvs_primary": primary, "lvs_secondary": not primary, "lvs leadership": leading}
    if not primary:
        info.update(lvs_redirect=True, connect_state=True,
                    remote_bdev=owner.hublvol.get_remote_bdev_name())
    return info


class _Layout:
    """a0 owns LVS_1 (home a0 a1 a2, remote b0 b1 b2), b0 owns LVS_2 (home
    b0 b1 b2, remote a0 a1 a2). Every node hosts an instance of both and has
    the ports of both recorded. LVS_1 is open on ``site`` and its taker - the
    primary of that site's triplet - is stamped primary; LVS_2 is led by b0."""

    def __init__(self, db, spdk, site, *, leading):
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
        # settled: the monitor's one-shot placement / write-protection switches have run
        cluster.shared_placement = True
        cluster.write_protection_v2 = True
        cluster.write_to_db(db.kv_store)
        owner_a = _update(db, owner_a, lvs_active_site=site)
        for node in (*a, *b):
            # the layout records an LVS's ports on its owner only
            _update(db, node, lvstore_ports=ops._derive_lvstore_ports(_fresh(db, node), None, db))
        _mesh(db, [_fresh(db, n) for n in (*a, *b)])
        self.cluster = cluster
        self.a = [_fresh(db, n) for n in a]
        self.b = [_fresh(db, n) for n in b]
        self.nodes = [*self.a, *self.b]
        taker_id = (a if site == SITE_A else b)[0].get_id()
        for node in self.nodes:
            spdk.add(node)
            taker = node.get_id() == taker_id
            spdk.reports(node, _lvs_info(owner_a, cluster, primary=taker, leading=taker and leading))
            leads = node.get_id() == owner_b.get_id()
            spdk.reports(node, _lvs_info(owner_b, cluster, primary=leads, leading=leads))


def _nothing_done(db, spdk, layout):
    """The pass left the LVS and its nodes as it found them: no leadership
    RPC in either direction, no task queued, every node and the cluster in
    their status, no LVS port blocked, opened or re-recorded."""
    for node in layout.nodes:
        rpc = spdk[node.get_id()]
        for method in (*_LEADERSHIP_RPCS, "nvmf_port_block", "nvmf_port_unblock"):
            getattr(rpc, method).assert_not_called()
        fresh = _fresh(db, node)
        assert fresh.status == StorageNode.STATUS_ONLINE
        assert set(node.lvstore_ports) == {"LVS_1", "LVS_2"}
        assert fresh.lvstore_ports == node.lvstore_ports
        assert fresh.lvol_subsys_port == node.lvol_subsys_port
    spdk.set_port.assert_not_called()
    assert db.get_job_tasks(layout.cluster.get_id()) == []
    assert db.get_cluster_by_id(layout.cluster.get_id()).status == Cluster.STATUS_ACTIVE


#: (a) the owner as the fail-back taker, (b) the remote primary - each with nobody leading
#: and, as the twin a missing leadership must not differ from, with the taker leading.
_states = pytest.mark.parametrize("site, leading", [
    pytest.param(SITE_A, False, id="fail-back-taker-nobody-leads"),
    pytest.param(SITE_A, True, id="fail-back-taker-leads"),
    pytest.param(SITE_B, False, id="remote-primary-nobody-leads"),
    pytest.param(SITE_B, True, id="remote-primary-leads"),
])


# ---------------------------------------------------------------------------
# the health check service
# ---------------------------------------------------------------------------

def _health_pass(db, layout):
    for node in layout.nodes:
        health_check_service.check_node(_fresh(db, node))


class TestHealthPass:

    @_states
    def test_nothing_is_granted_queued_or_changed(self, db, spdk, checks, site, leading):
        layout = _Layout(db, spdk, site, leading=leading)
        a, b = layout.a, layout.b

        _health_pass(db, layout)

        _nothing_done(db, spdk, layout)
        # The checks that list the leadership ran on each owner and its local
        # secondaries, and passed whatever the instance reports.
        assert sorted(checks.own) == [(a[0].get_id(), True), (b[0].get_id(), True)]
        assert sorted(checks.sec) == sorted([
            (a[1].get_id(), a[0].get_id(), True), (a[2].get_id(), a[0].get_id(), True),
            (b[1].get_id(), b[0].get_id(), True), (b[2].get_id(), b[0].get_id(), True)])
        # An LVS is read on its HOME triplet only, wherever it is led from: the
        # owner's instance (as the distrib stack check and as the hublvol
        # check), never the remote triplet's - the taker's in state (b).
        for node in a:
            assert set(spdk.lvstore_reads(node)) == {"LVS_1"}
        for node in b:
            assert set(spdk.lvstore_reads(node)) == {"LVS_2"}
        assert spdk.lvstore_reads(a[0]).count("LVS_1") >= 2
        # healthy all the same, and nothing was dialled to get there
        for node in layout.nodes:
            assert _fresh(db, node).health_check is True
            spdk[node.get_id()].bdev_nvme_attach_controller.assert_not_called()

    def test_a_blocked_lvs_port_queues_its_port_allow_task(self, db, spdk):
        # the control of the "no task" assertion: the pass does queue one for a real finding
        layout = _Layout(db, spdk, SITE_A, leading=False)
        owner = layout.a[0]
        port = owner.get_lvol_subsys_port(owner.lvstore)
        spdk.block(owner, port)

        health_check_service.check_node(_fresh(db, owner))

        tasks = db.get_job_tasks(layout.cluster.get_id())
        assert [(t.function_name, t.node_id, t.function_params) for t in tasks] == [
            (JobSchedule.FN_PORT_ALLOW, owner.get_id(), {"port_number": port})]
        for method in _LEADERSHIP_RPCS:
            getattr(spdk[owner.get_id()], method).assert_not_called()


# ---------------------------------------------------------------------------
# the storage-node monitor
# ---------------------------------------------------------------------------

def _monitor_pass(db, layout):
    """One round of the monitor's loop: every node, then the cluster."""
    for node in layout.nodes:
        storage_node_monitor.check_node(_fresh(db, node))
    storage_node_monitor.update_cluster_status(layout.cluster.get_id())


class TestMonitorPass:

    @_states
    def test_nothing_is_granted_queued_or_changed(self, db, spdk, checks, site, leading):
        layout = _Layout(db, spdk, site, leading=leading)
        a, b = layout.a, layout.b

        _monitor_pass(db, layout)

        _nothing_done(db, spdk, layout)
        # It looked: the ports of each owner, and its OWN lvstore once, for the
        # cluster status - never the instance it hosts for the other owner.
        for owner in (a[0], b[0]):
            spdk[owner.get_id()].nvmf_get_blocked_ports.assert_called()
        assert spdk.lvstore_reads(a[0]) == ["LVS_1"]
        assert spdk.lvstore_reads(b[0]) == ["LVS_2"]
        # With every port open the monitor runs no check that lists the leadership.
        assert checks.own == [] and checks.sec == []

    def test_a_blocked_lvs_port_takes_the_node_down(self, db, spdk):
        # the control of the "no status change" assertion: a real finding does change it
        layout = _Layout(db, spdk, SITE_A, leading=False)
        owner = layout.a[0]
        spdk.block(owner, owner.get_lvol_subsys_port(owner.lvstore))

        storage_node_monitor.check_node(_fresh(db, owner))

        assert _fresh(db, owner).status == StorageNode.STATUS_DOWN
        for method in _LEADERSHIP_RPCS:
            getattr(spdk[owner.get_id()], method).assert_not_called()
