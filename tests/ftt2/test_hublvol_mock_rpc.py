# coding=utf-8
"""
test_hublvol_mock_rpc.py – Mock-RPC-server tests for hublvol NVMe multipath.

These tests run the actual StorageNode methods (create_hublvol,
create_secondary_hublvol, connect_to_hublvol) against real FTT2MockRpcServer
HTTP instances.  No FoundationDB required — DB writes are patched out.

This lets the tests run in any environment where a network loopback is
available, including CI without a running FDB cluster.

Verifies the same invariants as TestHublvolActivate in test_hublvol_paths.py
but without the FDB dependency, so they always execute (not skipped).

Topology used:
  LVS_0: primary=n0 (srv[0]), secondary=n1 (srv[1]), tertiary=n2 (srv[2])
"""

import uuid as _uuid_mod
from unittest.mock import patch

import pytest

from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import rpc_client as _rpc_client_mod

from tests.ftt2.conftest import _worker_port_offset, _BASE_PORT
from tests.ftt2.mock_cluster import FTT2MockRpcServer


# ---------------------------------------------------------------------------
# Helpers — in-memory node construction
# ---------------------------------------------------------------------------

_CLUSTER_NQN = "nqn.2023-02.io.simplyblock:mocktestcluster"
_LVS = "LVS_0"


def _make_nic(ip: str) -> IFace:
    nic = IFace()
    nic.uuid = str(_uuid_mod.uuid4())
    nic.if_name = "eth0"
    nic.ip4_address = ip
    nic.trtype = "TCP"
    nic.net_type = "data"
    return nic


def _make_node(ip: str, lvstore: str, port: int, jm_vuid: int) -> StorageNode:
    """Build a StorageNode pointing at a mock RPC server — no FDB writes."""
    n = StorageNode()
    n.uuid = str(_uuid_mod.uuid4())
    n.cluster_id = "mock-cluster"
    n.status = StorageNode.STATUS_ONLINE
    n.hostname = f"mock-host-{ip}"
    n.mgmt_ip = "127.0.0.1"
    n.rpc_port = port
    n.rpc_username = "spdkuser"
    n.rpc_password = "spdkpass"
    n.active_tcp = True
    n.active_rdma = False
    n.data_nics = [_make_nic(ip)]
    n.lvstore = lvstore
    n.jm_vuid = jm_vuid
    n.lvstore_ports = {lvstore: {"lvol_subsys_port": 4420, "hublvol_port": 4430}}
    n.hublvol = None
    return n


def _clear_rpc_cache():
    """Clear the module-level RPC response cache between tests."""
    with _rpc_client_mod._rpc_cache_lock:
        _rpc_client_mod._rpc_cache.clear()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def mock_servers():
    """Start 3 mock RPC servers for the duration of this module."""
    offset = _worker_port_offset()
    # Use a different port range (offset +10) to avoid conflicts with
    # the session-scoped mock_rpc_servers fixture used by other test files.
    base = _BASE_PORT + offset + 10
    servers = []
    for i in range(3):
        srv = FTT2MockRpcServer(host="127.0.0.1", port=base + i, node_id=f"mrpc-n{i}")
        srv.start()
        servers.append(srv)
    yield servers
    for srv in servers:
        srv.stop()


@pytest.fixture()
def env(mock_servers):
    """
    Per-test environment: reset servers, create fresh in-memory nodes,
    patch out write_to_db so no FDB is needed.
    """
    for srv in mock_servers:
        srv.reset()
        srv.state.lvstores[_LVS] = {
            'name': _LVS, 'base_bdev': '',
            'block_size': 4096, 'cluster_size': 4096,
            'lvs leadership': True, 'lvs_primary': True, 'lvs_read_only': False,
            'lvs_secondary': False, 'lvs_redirect': False,
            'remote_bdev': '', 'connect_state': False,
        }
    _clear_rpc_cache()

    base_port = mock_servers[0].port
    n0 = _make_node("10.0.0.1", _LVS, base_port,     jm_vuid=100)
    n1 = _make_node("10.0.0.2", _LVS, base_port + 1, jm_vuid=200)
    n2 = _make_node("10.0.0.3", _LVS, base_port + 2, jm_vuid=300)

    write_db_patcher = patch(
        'simplyblock_core.models.base_model.BaseModel.write_to_db',
        return_value=True,
    )
    write_db_patcher.start()

    yield {
        'servers': mock_servers,
        'nodes': [n0, n1, n2],
        'cluster_nqn': _CLUSTER_NQN,
    }

    write_db_patcher.stop()


# ---------------------------------------------------------------------------
# Helper accessors
# ---------------------------------------------------------------------------

def _hublvol_nqn():
    return f"{_CLUSTER_NQN}:hublvol:{_LVS}"


def _ana_states(server, nqn):
    """Return list of ana_state values from all listeners on the given NQN."""
    sub = server.state.subsystems.get(nqn)
    if sub is None:
        return []
    return [la.get('ana_state') for la in sub.get('listen_addresses', [])]


def _attach_calls(server, nqn_fragment=None):
    """bdev_nvme_attach_controller calls, optionally filtered by NQN fragment."""
    calls = server.get_rpc_calls('bdev_nvme_attach_controller')
    if nqn_fragment:
        calls = [(ts, m, p) for ts, m, p in calls
                 if nqn_fragment in p.get('subnqn', '')]
    return calls


def _call_order(server, method):
    """Return 0-based positions of method in the full RPC log."""
    return [i for i, (_, m, _) in enumerate(server.get_rpc_calls()) if m == method]


# ---------------------------------------------------------------------------
# Primary — create_hublvol
# ---------------------------------------------------------------------------

class TestPrimaryCreateHublvol:
    """create_hublvol sends correct RPC sequence to the primary mock server."""

    @pytest.fixture(autouse=True)
    def _run(self, env):
        self.env = env
        n0 = env['nodes'][0]
        n0.create_hublvol(cluster_nqn=_CLUSTER_NQN)

    def test_bdev_created_on_server(self):
        """bdev_lvol_create_hublvol must land on the primary mock server."""
        assert self.env['servers'][0].was_called('bdev_lvol_create_hublvol'), \
            "bdev_lvol_create_hublvol not received by primary mock server"

    def test_hublvol_nqn_uses_shared_scheme(self):
        """NQN set on the node must use the cluster-wide shared scheme."""
        n0 = self.env['nodes'][0]
        assert n0.hublvol is not None, "hublvol attribute not set after create"
        assert n0.hublvol.nqn == _hublvol_nqn(), \
            f"Expected shared NQN {_hublvol_nqn()!r}; got {n0.hublvol.nqn!r}"

    def test_subsystem_created_on_server(self):
        """nvmf_create_subsystem must land on primary mock server with shared NQN."""
        nqn = _hublvol_nqn()
        assert nqn in self.env['servers'][0].state.subsystems, \
            f"Subsystem {nqn} not found in primary mock server state"

    def test_listener_created_with_optimized_ana(self):
        """Primary hublvol NVMe-oF listener must be created with ana_state=optimized."""
        states = _ana_states(self.env['servers'][0], _hublvol_nqn())
        assert states, "No listener found on primary hublvol subsystem"
        assert 'optimized' in states, \
            f"Primary must expose optimized ANA state; got {states}"

    def test_no_calls_land_on_other_servers(self):
        """create_hublvol must only contact the primary node's server."""
        # Secondary and tertiary servers should see no hublvol-related calls
        for srv_idx in (1, 2):
            assert not self.env['servers'][srv_idx].was_called('bdev_lvol_create_hublvol'), \
                f"server[{srv_idx}] must not receive hublvol create calls"


# ---------------------------------------------------------------------------
# Secondary — create_secondary_hublvol
# ---------------------------------------------------------------------------

class TestSecondaryCreateHublvol:
    """create_secondary_hublvol sends correct RPC sequence to secondary server."""

    @pytest.fixture(autouse=True)
    def _run(self, env):
        self.env = env
        n0, n1 = env['nodes'][0], env['nodes'][1]
        # Give n0 a hublvol (as if create_hublvol already ran)
        n0.hublvol = HubLVol({
            'uuid': str(_uuid_mod.uuid4()),
            'nqn': _hublvol_nqn(),
            'bdev_name': f'{_LVS}/hublvol',
            'model_number': str(_uuid_mod.uuid4()),
            'nguid': 'ab' * 16,
            'nvmf_port': 4430,
        })
        n1.create_secondary_hublvol(n0, _CLUSTER_NQN)

    def test_bdev_created_on_secondary_server(self):
        """bdev_lvol_create_hublvol must land on secondary mock server."""
        assert self.env['servers'][1].was_called('bdev_lvol_create_hublvol'), \
            "bdev_lvol_create_hublvol not received by secondary mock server"

    def test_subsystem_created_with_shared_nqn(self):
        """Secondary server must have a subsystem for the shared hublvol NQN."""
        nqn = _hublvol_nqn()
        assert nqn in self.env['servers'][1].state.subsystems, \
            f"Secondary server must have subsystem {nqn}"

    def test_listener_created_with_non_optimized_ana(self):
        """Secondary hublvol listener must use ana_state=non_optimized."""
        states = _ana_states(self.env['servers'][1], _hublvol_nqn())
        assert states, "No listener on secondary hublvol subsystem"
        assert 'non_optimized' in states, \
            f"Secondary must expose non_optimized ANA state; got {states}"

    def test_primary_server_unaffected(self):
        """create_secondary_hublvol must not contact primary's mock server."""
        assert not self.env['servers'][0].was_called('bdev_lvol_create_hublvol'), \
            "Primary server must not be contacted during create_secondary_hublvol"


# ---------------------------------------------------------------------------
# Secondary — connect_to_hublvol (secondary role, no failover)
# ---------------------------------------------------------------------------

class TestSecondaryConnect:
    """Secondary connects to primary hublvol: 1 path, full SPDK 3-step sequence."""

    @pytest.fixture(autouse=True)
    def _run(self, env):
        self.env = env
        n0, n1 = env['nodes'][0], env['nodes'][1]
        n0.hublvol = HubLVol({
            'uuid': str(_uuid_mod.uuid4()),
            'nqn': _hublvol_nqn(),
            'bdev_name': f'{_LVS}/hublvol',
            'model_number': str(_uuid_mod.uuid4()),
            'nguid': 'ab' * 16,
            'nvmf_port': 4430,
        })
        n1.connect_to_hublvol(n0, failover_node=None, role="secondary")

    def test_attach_controller_called(self):
        """Step 1: bdev_nvme_attach_controller must land on secondary server."""
        assert self.env['servers'][1].was_called('bdev_nvme_attach_controller'), \
            "bdev_nvme_attach_controller not received by secondary server"

    def test_exactly_one_path_attached(self):
        """Secondary must attach exactly 1 NVMe path (primary IP only)."""
        calls = _attach_calls(self.env['servers'][1], 'hublvol')
        assert len(calls) == 1, \
            f"Secondary must attach 1 path; got {len(calls)}"

    def test_attached_path_targets_primary_ip(self):
        """The attached path must point to the primary node's data IP."""
        calls = _attach_calls(self.env['servers'][1], 'hublvol')
        assert calls, "No attach_controller calls found"
        _, _, params = calls[0]
        assert params.get('traddr') == '10.0.0.1', \
            f"Attached path must target primary IP 10.0.0.1; got {params.get('traddr')!r}"

    def test_no_multipath_on_secondary(self):
        """Secondary with no failover must not request multipath mode."""
        calls = _attach_calls(self.env['servers'][1], 'hublvol')
        assert calls
        _, _, params = calls[0]
        assert params.get('multipath') != 'multipath', \
            f"Secondary must not use multipath mode; got multipath={params.get('multipath')!r}"

    def test_set_lvs_opts_role_secondary(self):
        """Step 2: bdev_lvol_set_lvs_opts must set role=secondary on secondary server."""
        calls = self.env['servers'][1].get_rpc_calls('bdev_lvol_set_lvs_opts')
        assert calls, "bdev_lvol_set_lvs_opts not called on secondary server"
        _, _, params = calls[0]
        assert params.get('role') == 'secondary', \
            f"set_lvs_opts must use role=secondary; got {params.get('role')!r}"

    def test_connect_hublvol_called(self):
        """Step 3: bdev_lvol_connect_hublvol must land on secondary server."""
        assert self.env['servers'][1].was_called('bdev_lvol_connect_hublvol'), \
            "bdev_lvol_connect_hublvol not received by secondary server"

    def test_spdk_sequence_attach_before_connect(self):
        """SPDK constraint: attach_controller must precede connect_hublvol."""
        attach_pos = _call_order(self.env['servers'][1], 'bdev_nvme_attach_controller')
        connect_pos = _call_order(self.env['servers'][1], 'bdev_lvol_connect_hublvol')
        assert attach_pos and connect_pos
        assert max(attach_pos) < min(connect_pos), \
            "bdev_nvme_attach_controller must come before bdev_lvol_connect_hublvol"

    def test_spdk_sequence_set_opts_before_connect(self):
        """SPDK constraint: set_lvs_opts (sets node_role) must precede connect_hublvol."""
        opts_pos = _call_order(self.env['servers'][1], 'bdev_lvol_set_lvs_opts')
        connect_pos = _call_order(self.env['servers'][1], 'bdev_lvol_connect_hublvol')
        assert opts_pos and connect_pos
        assert max(opts_pos) < min(connect_pos), \
            "bdev_lvol_set_lvs_opts must come before bdev_lvol_connect_hublvol"


# ---------------------------------------------------------------------------
# Tertiary — connect_to_hublvol (tertiary role, with failover)
# ---------------------------------------------------------------------------

class TestTertiaryConnect:
    """Tertiary connects to primary hublvol: 2 paths, both multipath, full 3-step."""

    @pytest.fixture(autouse=True)
    def _run(self, env):
        self.env = env
        n0, n1, n2 = env['nodes'][0], env['nodes'][1], env['nodes'][2]
        # Primary hublvol (n0)
        n0.hublvol = HubLVol({
            'uuid': str(_uuid_mod.uuid4()),
            'nqn': _hublvol_nqn(),
            'bdev_name': f'{_LVS}/hublvol',
            'model_number': str(_uuid_mod.uuid4()),
            'nguid': 'ab' * 16,
            'nvmf_port': 4430,
        })
        # Secondary (n1) used as failover node — tertiary connects to primary
        # but also adds n1's IP as the ANA non_optimized path
        n1.hublvol = HubLVol({
            'uuid': n0.hublvol.uuid,
            'nqn': n0.hublvol.nqn,
            'bdev_name': n0.hublvol.bdev_name,
            'model_number': n0.hublvol.model_number,
            'nguid': n0.hublvol.nguid,
            'nvmf_port': n0.hublvol.nvmf_port,
        })
        # Tertiary connects via n0 primary, with n1 as failover
        n2.connect_to_hublvol(n0, failover_node=n1, role="tertiary")

    def test_attach_controller_called(self):
        """Step 1: bdev_nvme_attach_controller must land on tertiary server."""
        assert self.env['servers'][2].was_called('bdev_nvme_attach_controller'), \
            "bdev_nvme_attach_controller not received by tertiary server"

    def test_exactly_two_paths_attached(self):
        """Tertiary must attach 2 NVMe paths: primary IP + sec_1 IP."""
        calls = _attach_calls(self.env['servers'][2], 'hublvol')
        assert len(calls) == 2, \
            f"Tertiary must attach 2 paths (primary + sec_1); got {len(calls)}"

    def test_both_paths_use_multipath_mode(self):
        """Both tertiary paths must use multipath='multipath' for ANA failover."""
        calls = _attach_calls(self.env['servers'][2], 'hublvol')
        for _, _, params in calls:
            assert params.get('multipath') == 'multipath', \
                f"Tertiary path must use multipath='multipath'; got {params.get('multipath')!r}"

    def test_paths_target_distinct_ips(self):
        """The two paths must target different IPs (primary vs sec_1)."""
        calls = _attach_calls(self.env['servers'][2], 'hublvol')
        ips = {params.get('traddr') for _, _, params in calls}
        assert len(ips) == 2, \
            f"Tertiary paths must target 2 distinct IPs; got {ips}"
        assert '10.0.0.1' in ips, "Primary IP must be one of the two paths"
        assert '10.0.0.2' in ips, "Sec_1 IP must be one of the two paths"

    def test_set_lvs_opts_role_tertiary(self):
        """Step 2: bdev_lvol_set_lvs_opts must set role=tertiary on tertiary server."""
        calls = self.env['servers'][2].get_rpc_calls('bdev_lvol_set_lvs_opts')
        assert calls, "bdev_lvol_set_lvs_opts not called on tertiary server"
        _, _, params = calls[0]
        assert params.get('role') == 'tertiary', \
            f"set_lvs_opts must use role=tertiary; got {params.get('role')!r}"

    def test_connect_hublvol_called(self):
        """Step 3: bdev_lvol_connect_hublvol must land on tertiary server."""
        assert self.env['servers'][2].was_called('bdev_lvol_connect_hublvol'), \
            "bdev_lvol_connect_hublvol not received by tertiary server"

    def test_spdk_sequence_all_attaches_before_connect(self):
        """SPDK constraint: all attach_controller calls must precede connect_hublvol."""
        attach_pos = _call_order(self.env['servers'][2], 'bdev_nvme_attach_controller')
        connect_pos = _call_order(self.env['servers'][2], 'bdev_lvol_connect_hublvol')
        assert len(attach_pos) == 2, f"Expected 2 attach calls; got {len(attach_pos)}"
        assert connect_pos
        assert max(attach_pos) < min(connect_pos), \
            "All attach_controller calls must precede connect_hublvol on tertiary"


# ---------------------------------------------------------------------------
# Full activate sequence: primary → secondary → tertiary
# ---------------------------------------------------------------------------

class TestFullActivateSequence:
    """End-to-end activate: all three nodes go through the full sequence."""

    @pytest.fixture(autouse=True)
    def _run(self, env):
        self.env = env
        n0, n1, n2 = env['nodes'][0], env['nodes'][1], env['nodes'][2]

        # Primary creates hublvol
        n0.create_hublvol(cluster_nqn=_CLUSTER_NQN)

        # Secondary creates its secondary hublvol (same NQN, non_optimized)
        n1.create_secondary_hublvol(n0, _CLUSTER_NQN)

        # Secondary connects to primary's hublvol
        n1.connect_to_hublvol(n0, failover_node=None, role="secondary")

        # Tertiary connects with both paths
        n2.connect_to_hublvol(n0, failover_node=n1, role="tertiary")

    def test_primary_and_secondary_share_nqn(self):
        """Primary and sec_1 must expose the same NQN for NVMe ANA multipath."""
        nqn = _hublvol_nqn()
        assert nqn in self.env['servers'][0].state.subsystems, \
            "NQN not found on primary server"
        assert nqn in self.env['servers'][1].state.subsystems, \
            "NQN not found on secondary server — identical NQN required for ANA multipath"

    def test_primary_ana_optimized(self):
        """Primary hublvol listener must be optimized."""
        states = _ana_states(self.env['servers'][0], _hublvol_nqn())
        assert 'optimized' in states, \
            f"Primary must expose optimized ANA; got {states}"

    def test_secondary_ana_non_optimized(self):
        """Secondary hublvol listener must be non_optimized."""
        states = _ana_states(self.env['servers'][1], _hublvol_nqn())
        assert 'non_optimized' in states, \
            f"Secondary must expose non_optimized ANA; got {states}"

    def test_secondary_connects_one_path(self):
        """Secondary must have exactly 1 path to primary hublvol."""
        paths = self.env['servers'][1].state.nvme_controller_paths.get(f'{_LVS}/hublvol', [])
        assert len(paths) == 1, \
            f"Secondary must have 1 hublvol path; got {len(paths)}"

    def test_tertiary_connects_two_paths(self):
        """Tertiary must have exactly 2 paths (primary + sec_1)."""
        paths = self.env['servers'][2].state.nvme_controller_paths.get(f'{_LVS}/hublvol', [])
        assert len(paths) == 2, \
            f"Tertiary must have 2 hublvol paths; got {len(paths)}"

    def test_tertiary_paths_multipath(self):
        """Both tertiary paths must use multipath mode."""
        paths = self.env['servers'][2].state.nvme_controller_paths.get(f'{_LVS}/hublvol', [])
        for path in paths:
            assert path.get('multipath') == 'multipath', \
                f"Tertiary path must be multipath; got {path}"

    def test_connect_hublvol_not_called_on_primary(self):
        """Primary must never receive bdev_lvol_connect_hublvol (it's not a secondary)."""
        assert not self.env['servers'][0].was_called('bdev_lvol_connect_hublvol'), \
            "Primary must never receive bdev_lvol_connect_hublvol"

    def test_secondary_does_not_get_primary_role_in_set_opts(self):
        """Secondary server must never receive role=primary in set_lvs_opts."""
        opts_calls = self.env['servers'][1].get_rpc_calls('bdev_lvol_set_lvs_opts')
        roles = [p.get('role') for _, _, p in opts_calls]
        assert 'primary' not in roles, \
            f"Secondary server must not receive role=primary; got {roles}"


# ---------------------------------------------------------------------------
# Shared fixture helper
# ---------------------------------------------------------------------------

def _primary_hublvol():
    return HubLVol({
        'uuid': str(_uuid_mod.uuid4()),
        'nqn': _hublvol_nqn(),
        'bdev_name': f'{_LVS}/hublvol',
        'model_number': str(_uuid_mod.uuid4()),
        'nguid': 'ab' * 16,
        'nvmf_port': 4430,
    })


# ---------------------------------------------------------------------------
# Hublvol connected state tracking
# ---------------------------------------------------------------------------

class TestHublvolConnectedState:
    """Verify mock server correctly tracks hublvol connection state."""

    @pytest.fixture(autouse=True)
    def _setup(self, env):
        self.env = env
        self.n0 = env['nodes'][0]
        self.n1 = env['nodes'][1]
        self.n0.hublvol = _primary_hublvol()

    def test_not_connected_before_connect_call(self):
        """hublvol_connected must be False before connect_to_hublvol is called."""
        assert not self.env['servers'][1].hublvol_connected(_LVS), \
            "hublvol must not be connected before connect_to_hublvol"

    def test_connected_after_successful_connect(self):
        """hublvol_connected must be True after a successful connect_to_hublvol."""
        self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        assert self.env['servers'][1].hublvol_connected(_LVS), \
            "hublvol must be connected after connect_to_hublvol succeeds"

    def test_not_created_before_create_call(self):
        """hublvol_created must be False before create_hublvol is called."""
        assert not self.env['servers'][0].hublvol_created(_LVS), \
            "hublvol must not be marked created before create_hublvol is called"

    def test_created_after_create_hublvol(self):
        """hublvol_created must be True after a successful create_hublvol."""
        self.n0.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        assert self.env['servers'][0].hublvol_created(_LVS), \
            "hublvol_created must reflect that bdev_lvol_create_hublvol was received"

    def test_secondary_created_after_create_secondary_hublvol(self):
        """hublvol_created on secondary server must be True after create_secondary_hublvol."""
        self.n1.create_secondary_hublvol(self.n0, _CLUSTER_NQN)
        assert self.env['servers'][1].hublvol_created(_LVS), \
            "Secondary server must record hublvol_created after create_secondary_hublvol"

    def test_state_is_isolated_per_server(self):
        """hublvol_connected on server[1] must not affect server[0] or server[2]."""
        self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        assert not self.env['servers'][0].hublvol_connected(_LVS), \
            "Primary server must not report connected after secondary connects"
        assert not self.env['servers'][2].hublvol_connected(_LVS), \
            "Tertiary server must not report connected after secondary connects"


# ---------------------------------------------------------------------------
# Error injection: create_hublvol failures
# ---------------------------------------------------------------------------

class TestHublvolCreateErrors:
    """Verify create_hublvol handles RPC failures without leaving inconsistent state."""

    @pytest.fixture(autouse=True)
    def _setup(self, env):
        self.env = env
        self.n0 = env['nodes'][0]
        self.n1 = env['nodes'][1]

    def test_create_hublvol_raises_on_bdev_create_failure(self):
        """create_hublvol must raise RPCException when bdev_lvol_create_hublvol fails."""
        from simplyblock_core.rpc_client import RPCException
        self.env['servers'][0].fail_method('bdev_lvol_create_hublvol', 'Disk full')
        with pytest.raises(RPCException):
            self.n0.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        self.env['servers'][0].clear_fail_method('bdev_lvol_create_hublvol')

    def test_create_hublvol_node_hublvol_remains_none_on_failure(self):
        """node.hublvol must remain None when bdev_lvol_create_hublvol fails."""
        from simplyblock_core.rpc_client import RPCException
        self.env['servers'][0].fail_method('bdev_lvol_create_hublvol', 'Disk full')
        try:
            self.n0.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        except RPCException:
            pass
        self.env['servers'][0].clear_fail_method('bdev_lvol_create_hublvol')
        assert self.n0.hublvol is None, \
            "node.hublvol must remain None when bdev creation fails"

    def test_create_hublvol_no_subsystem_on_failure(self):
        """No NVMe subsystem must be created when bdev_lvol_create_hublvol fails."""
        from simplyblock_core.rpc_client import RPCException
        self.env['servers'][0].fail_method('bdev_lvol_create_hublvol', 'Disk full')
        try:
            self.n0.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        except RPCException:
            pass
        self.env['servers'][0].clear_fail_method('bdev_lvol_create_hublvol')
        nqn = _hublvol_nqn()
        assert nqn not in self.env['servers'][0].state.subsystems, \
            "No NVMe subsystem must be created when bdev creation fails"

    def test_create_secondary_hublvol_returns_none_on_bdev_failure(self):
        """create_secondary_hublvol must return None when bdev creation fails."""
        self.n0.hublvol = _primary_hublvol()
        self.env['servers'][1].fail_method('bdev_lvol_create_hublvol', 'LVS not found')
        result = self.n1.create_secondary_hublvol(self.n0, _CLUSTER_NQN)
        self.env['servers'][1].clear_fail_method('bdev_lvol_create_hublvol')
        assert result is None, \
            "create_secondary_hublvol must return None when bdev creation fails"

    def test_create_secondary_hublvol_no_subsystem_on_failure(self):
        """No subsystem must be exposed when secondary bdev creation fails."""
        self.n0.hublvol = _primary_hublvol()
        self.env['servers'][1].fail_method('bdev_lvol_create_hublvol', 'LVS not found')
        self.n1.create_secondary_hublvol(self.n0, _CLUSTER_NQN)
        self.env['servers'][1].clear_fail_method('bdev_lvol_create_hublvol')
        nqn = _hublvol_nqn()
        assert nqn not in self.env['servers'][1].state.subsystems, \
            "Secondary must not expose a subsystem when bdev creation fails"

    def test_create_secondary_hublvol_not_created_in_server_state(self):
        """hublvol_created must be False on secondary server when bdev creation fails."""
        self.n0.hublvol = _primary_hublvol()
        self.env['servers'][1].fail_method('bdev_lvol_create_hublvol', 'LVS not found')
        self.n1.create_secondary_hublvol(self.n0, _CLUSTER_NQN)
        self.env['servers'][1].clear_fail_method('bdev_lvol_create_hublvol')
        assert not self.env['servers'][1].hublvol_created(_LVS), \
            "hublvol_created must be False when bdev creation fails"


# ---------------------------------------------------------------------------
# Error injection: connect_to_hublvol failures
# ---------------------------------------------------------------------------

class TestHublvolConnectErrors:
    """Verify connect_to_hublvol handles RPC failures correctly."""

    @pytest.fixture(autouse=True)
    def _setup(self, env):
        self.env = env
        self.n0 = env['nodes'][0]
        self.n1 = env['nodes'][1]
        self.n2 = env['nodes'][2]
        self.n0.hublvol = _primary_hublvol()
        # sec_1 used as failover for tertiary tests
        self.n1.hublvol = HubLVol({
            'uuid': self.n0.hublvol.uuid,
            'nqn': self.n0.hublvol.nqn,
            'bdev_name': self.n0.hublvol.bdev_name,
            'model_number': self.n0.hublvol.model_number,
            'nguid': self.n0.hublvol.nguid,
            'nvmf_port': self.n0.hublvol.nvmf_port,
        })

    def test_attach_failure_skips_connect_hublvol_and_returns_false(self):
        """When every primary-path attach_controller fails, connect_to_hublvol
        must return False and must NOT proceed to set_lvs_opts or
        bdev_lvol_connect_hublvol.

        Rationale: the remote hublvol bdev does not exist without a working
        attach, so any downstream lvs_opts / connect_hublvol call would
        reference a missing bdev. The restart flow uses the boolean return
        to decide whether to abort the primary's restart before unblocking
        the secondary port — silently calling connect_hublvol anyway would
        mask the real failure.
        """
        self.env['servers'][1].fail_method(
            'bdev_nvme_attach_controller', 'Target unreachable')
        ok = self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        self.env['servers'][1].clear_fail_method('bdev_nvme_attach_controller')

        assert ok is False, \
            "connect_to_hublvol must return False when all primary attaches fail"
        assert not self.env['servers'][1].was_called('bdev_lvol_connect_hublvol'), \
            "connect_hublvol must NOT be called when all primary attaches fail"

    def test_attach_failure_no_path_in_server_state(self):
        """When attach_controller fails, no path must be stored in nvme_controller_paths."""
        self.env['servers'][1].fail_method(
            'bdev_nvme_attach_controller', 'Target unreachable')
        self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        self.env['servers'][1].clear_fail_method('bdev_nvme_attach_controller')

        paths = self.env['servers'][1].state.nvme_controller_paths.get(f'{_LVS}/hublvol', [])
        assert len(paths) == 0, \
            f"No path must be stored when attach_controller fails; got {len(paths)}"

    def test_connect_hublvol_rpc_failure_reflected_in_state(self):
        """When bdev_lvol_connect_hublvol fails, hublvol_connected must remain False."""
        self.env['servers'][1].fail_method(
            'bdev_lvol_connect_hublvol', 'LVS busy')
        self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        self.env['servers'][1].clear_fail_method('bdev_lvol_connect_hublvol')

        assert not self.env['servers'][1].hublvol_connected(_LVS), \
            "hublvol_connected must be False when bdev_lvol_connect_hublvol RPC fails"

    def test_connect_hublvol_rpc_failure_attach_still_occurred(self):
        """Even when connect_hublvol fails, attach_controller must have been attempted."""
        self.env['servers'][1].fail_method(
            'bdev_lvol_connect_hublvol', 'LVS busy')
        self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        self.env['servers'][1].clear_fail_method('bdev_lvol_connect_hublvol')

        assert self.env['servers'][1].was_called('bdev_nvme_attach_controller'), \
            "attach_controller must be attempted even when connect_hublvol later fails"

    def test_set_lvs_opts_failure_skips_connect_hublvol_and_returns_false(self):
        """When bdev_lvol_set_lvs_opts fails, connect_to_hublvol must return
        False and must NOT proceed to bdev_lvol_connect_hublvol. The restart
        flow depends on this to abort before unblocking the secondary port.
        """
        self.env['servers'][1].fail_method(
            'bdev_lvol_set_lvs_opts', 'LVS not found')
        ok = self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        self.env['servers'][1].clear_fail_method('bdev_lvol_set_lvs_opts')

        assert ok is False, \
            "connect_to_hublvol must return False when set_lvs_opts fails"
        assert not self.env['servers'][1].was_called('bdev_lvol_connect_hublvol'), \
            "connect_hublvol must NOT be called when set_lvs_opts failed"

    def test_tertiary_one_attach_fails_still_connects(self):
        """If the failover path attach fails, connect_hublvol is still called on tertiary."""
        call_count = [0]

        def hook(method, params):
            if method == 'bdev_nvme_attach_controller':
                call_count[0] += 1
                if call_count[0] == 2:
                    # Fail the second attach (failover path)
                    raise Exception("Simulated failover path unreachable")
            return True

        self.env['servers'][2].set_rpc_hook(hook)
        self.n2.connect_to_hublvol(self.n0, failover_node=self.n1, role="tertiary")
        self.env['servers'][2].clear_rpc_hook()

        # Primary path succeeded (call 1), failover path failed (call 2)
        # The tertiary should still call connect_hublvol
        assert self.env['servers'][2].was_called('bdev_lvol_connect_hublvol'), \
            "connect_hublvol must be called on tertiary even when one attach path fails"

    def test_tertiary_one_attach_fails_only_one_path_stored(self):
        """If the failover path attach fails, only 1 path must be in nvme_controller_paths."""
        call_count = [0]

        def hook(method, params):
            if method == 'bdev_nvme_attach_controller':
                call_count[0] += 1
                if call_count[0] == 2:
                    raise Exception("Simulated failover path unreachable")
            return True

        self.env['servers'][2].set_rpc_hook(hook)
        self.n2.connect_to_hublvol(self.n0, failover_node=self.n1, role="tertiary")
        self.env['servers'][2].clear_rpc_hook()

        paths = self.env['servers'][2].state.nvme_controller_paths.get(f'{_LVS}/hublvol', [])
        assert len(paths) == 1, \
            f"Only 1 path must be stored when failover attach fails; got {len(paths)}"

    def test_all_attach_calls_fail_skips_connect_hublvol(self):
        """Even when the attach loop runs through every NIC, if none succeed
        connect_to_hublvol must return False and must NOT attempt
        bdev_lvol_connect_hublvol.
        """
        self.env['servers'][1].fail_method(
            'bdev_nvme_attach_controller', 'All paths unreachable')
        ok = self.n1.connect_to_hublvol(self.n0, failover_node=None, role="secondary")
        self.env['servers'][1].clear_fail_method('bdev_nvme_attach_controller')

        assert ok is False, \
            "connect_to_hublvol must return False when all attach calls fail"
        assert self.env['servers'][1].was_called('bdev_nvme_attach_controller'), \
            "attach_controller must have been attempted"
        assert not self.env['servers'][1].was_called('bdev_lvol_connect_hublvol'), \
            "connect_hublvol must NOT be attempted when no attach succeeded"
