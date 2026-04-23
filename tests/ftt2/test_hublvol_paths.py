# coding=utf-8
"""
test_hublvol_paths.py – Integration tests for hublvol NVMe multipath setup.

Verifies that during ACTIVATE, REACTIVATE (primary restart), SECONDARY RESTART,
and TERTIARY RESTART, all required hublvol NVMe paths are established with the
correct SPDK three-step sequence:
  1. bdev_nvme_attach_controller  – establishes NVMe controller / bdev
  2. bdev_lvol_set_lvs_opts       – sets lvs->node_role (must precede step 3)
  3. bdev_lvol_connect_hublvol    – binds lvstore to hub bdev via spdk_bdev_open_ext

Key invariants:
  - Primary exposes hublvol NQN with ANA state = optimized
  - Secondary (sec_1) exposes the IDENTICAL NQN with ANA state = non_optimized
  - Secondary connects to primary's hublvol with 1 NVMe path
  - Tertiary connects with 2 paths: primary (optimized) + sec_1 (non_optimized)
  - On secondary restart: tertiary adds sec_1's path via bdev_nvme_attach_controller
    with multipath="multipath" only — does NOT repeat bdev_lvol_connect_hublvol

Topology (from conftest round-robin):
  LVS_0: primary=n0, secondary=n1, tertiary=n2
  LVS_1: primary=n1, secondary=n2, tertiary=n3
  LVS_2: primary=n2, secondary=n3, tertiary=n0
  LVS_3: primary=n3, secondary=n0, tertiary=n1
"""

import uuid as _uuid_mod

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.storage_node import StorageNode

from tests.ftt2.conftest import (
    patch_externals,
    prepare_node_for_restart,
    create_test_lvol,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HUBLVOL_NQN_TMPL = "{cluster_nqn}:hublvol:{lvstore}"


def _hublvol_nqn(cluster_nqn, lvstore):
    return f"{cluster_nqn}:hublvol:{lvstore}"


def _setup_hublvols_in_db(env, node_indices=None):
    """Pre-populate HubLVol attribute on nodes in FDB.

    Required before any restart test so that connect_to_hublvol() can read
    primary_node.hublvol without raising ValueError.
    """
    cluster = env['cluster']
    db = env['db']
    if node_indices is None:
        node_indices = range(len(env['nodes']))
    for i in node_indices:
        node = env['nodes'][i]
        node.hublvol = HubLVol({
            'uuid': str(_uuid_mod.uuid4()),
            'nqn': _hublvol_nqn(cluster.nqn, node.lvstore),
            'bdev_name': f'{node.lvstore}/hublvol',
            'model_number': str(_uuid_mod.uuid4()),
            'nguid': 'ab' * 16,
            'nvmf_port': node.lvstore_ports[node.lvstore]['hublvol_port'],
        })
        node.write_to_db(db.kv_store)


def _run_restart(env, node_idx):
    """Run recreate_all_lvstores on the given node (with patches)."""
    node = env['nodes'][node_idx]
    db = env['db']
    patches = patch_externals()
    for p in patches:
        p.start()
    try:
        snode = db.get_storage_node_by_id(node.uuid)
        snode.status = StorageNode.STATUS_RESTARTING
        snode.write_to_db(db.kv_store)
        result = storage_node_ops.recreate_all_lvstores(snode)
        if result:
            snode = db.get_storage_node_by_id(node.uuid)
            snode.status = StorageNode.STATUS_ONLINE
            snode.write_to_db(db.kv_store)
        return result
    finally:
        for p in patches:
            p.stop()


def _get_set_opts_roles(env, server_idx):
    """Return all 'role' values seen in bdev_lvol_set_lvs_opts calls on a server."""
    calls = env['servers'][server_idx].get_rpc_calls('bdev_lvol_set_lvs_opts')
    return [p.get('role', '') for _, _, p in calls]


def _attach_calls_for_nqn(env, server_idx, nqn_fragment):
    """Return bdev_nvme_attach_controller calls whose NQN contains nqn_fragment."""
    calls = env['servers'][server_idx].get_rpc_calls('bdev_nvme_attach_controller')
    return [(ts, m, p) for ts, m, p in calls if nqn_fragment in p.get('subnqn', '')]


def _connect_hublvol_calls_for_lvs(env, server_idx, lvs_name):
    """Return bdev_lvol_connect_hublvol calls whose lvs_name param matches."""
    calls = env['servers'][server_idx].get_rpc_calls('bdev_lvol_connect_hublvol')
    return [(ts, m, p) for ts, m, p in calls if p.get('lvs_name') == lvs_name]


# ---------------------------------------------------------------------------
# ACTIVATE: direct method calls against mock RPC servers
# ---------------------------------------------------------------------------

class TestHublvolActivate:
    """
    Tests for the ACTIVATE code path:
      create_hublvol → create_secondary_hublvol → connect_to_hublvol (sec, tert)

    Uses node 0 as primary for LVS_0, node 1 as secondary, node 2 as tertiary.
    Calls the individual StorageNode methods directly so RPC calls land on mock servers.
    """

    @pytest.fixture(autouse=True)
    def _activate(self, ftt2_env):
        """Run the three-step activate sequence once, then let tests inspect state."""
        self.env = ftt2_env
        cluster = ftt2_env['cluster']
        n0, n1, n2 = ftt2_env['nodes'][0], ftt2_env['nodes'][1], ftt2_env['nodes'][2]

        # Step 1: primary creates hublvol
        n0.create_hublvol(cluster_nqn=cluster.nqn)

        # Step 2: sec_1 creates secondary hublvol (same NQN, non_optimized)
        n1.create_secondary_hublvol(n0, cluster.nqn)

        # Step 3a: secondary connects to primary's hublvol (1 path)
        n1.connect_to_hublvol(n0, failover_node=None, role="secondary")

        # Step 3b: tertiary connects with 2 paths (primary + sec_1)
        n2.connect_to_hublvol(n0, failover_node=n1, role="tertiary")

    def test_primary_hublvol_optimized_ana(self):
        """Primary's hublvol listener must use ANA state = optimized."""
        nqn = _hublvol_nqn(self.env['cluster'].nqn, 'LVS_0')
        sub = self.env['servers'][0].state.subsystems.get(nqn)
        assert sub is not None, f"Hublvol subsystem {nqn} not found on primary"
        ana_states = [la.get('ana_state') for la in sub.get('listen_addresses', [])]
        assert 'optimized' in ana_states, \
            f"Primary must expose optimized ANA state; got {ana_states}"

    def test_secondary_hublvol_non_optimized_ana(self):
        """Sec_1's hublvol listener must use ANA state = non_optimized."""
        nqn = _hublvol_nqn(self.env['cluster'].nqn, 'LVS_0')
        sub = self.env['servers'][1].state.subsystems.get(nqn)
        assert sub is not None, f"Secondary hublvol subsystem {nqn} not found on sec_1"
        ana_states = [la.get('ana_state') for la in sub.get('listen_addresses', [])]
        assert 'non_optimized' in ana_states, \
            f"Sec_1 must expose non_optimized ANA state; got {ana_states}"

    def test_primary_and_secondary_expose_identical_nqn(self):
        """Primary and sec_1 must expose the same NQN for NVMe ANA multipath."""
        nqn = _hublvol_nqn(self.env['cluster'].nqn, 'LVS_0')
        assert nqn in self.env['servers'][0].state.subsystems, \
            "NQN not found on primary"
        assert nqn in self.env['servers'][1].state.subsystems, \
            "NQN not found on sec_1 — identical NQN required for ANA multipath"

    def test_secondary_connects_one_path(self):
        """Secondary must attach exactly 1 NVMe path to primary's hublvol."""
        paths = self.env['servers'][1].state.nvme_controller_paths.get('LVS_0/hublvol', [])
        assert len(paths) == 1, \
            f"Secondary must have 1 path to primary hublvol; got {len(paths)}"

    def test_tertiary_connects_two_paths(self):
        """Tertiary must attach 2 NVMe paths: primary IP + sec_1 IP."""
        paths = self.env['servers'][2].state.nvme_controller_paths.get('LVS_0/hublvol', [])
        assert len(paths) == 2, \
            f"Tertiary must have 2 paths (primary + sec_1); got {len(paths)}"

    def test_tertiary_paths_use_multipath_mode(self):
        """Both tertiary paths must be attached with multipath='multipath'."""
        paths = self.env['servers'][2].state.nvme_controller_paths.get('LVS_0/hublvol', [])
        for path in paths:
            assert path.get('multipath') == 'multipath', \
                f"Tertiary path must use multipath mode; got {path}"

    def test_connect_hublvol_called_after_attach_on_secondary(self):
        """SPDK sequence: bdev_nvme_attach_controller must precede bdev_lvol_connect_hublvol."""
        all_calls = [m for _, m, _ in self.env['servers'][1].get_rpc_calls()]
        assert 'bdev_nvme_attach_controller' in all_calls, "attach_controller not called on secondary"
        assert 'bdev_lvol_connect_hublvol' in all_calls, "connect_hublvol not called on secondary"
        attach_idx = next(i for i, m in enumerate(all_calls) if m == 'bdev_nvme_attach_controller')
        connect_idx = next(i for i, m in enumerate(all_calls) if m == 'bdev_lvol_connect_hublvol')
        assert attach_idx < connect_idx, \
            "connect_hublvol must be called AFTER attach_controller (SPDK requires bdev to exist first)"

    def test_connect_hublvol_called_after_attach_on_tertiary(self):
        """Same SPDK sequence requirement on tertiary."""
        all_calls = [m for _, m, _ in self.env['servers'][2].get_rpc_calls()]
        assert 'bdev_nvme_attach_controller' in all_calls
        assert 'bdev_lvol_connect_hublvol' in all_calls
        last_attach_idx = max(i for i, m in enumerate(all_calls) if m == 'bdev_nvme_attach_controller')
        connect_idx = next(i for i, m in enumerate(all_calls) if m == 'bdev_lvol_connect_hublvol')
        assert last_attach_idx < connect_idx, \
            "connect_hublvol must come after all attach_controller calls on tertiary"

    def test_set_lvs_opts_role_secondary(self):
        """bdev_lvol_set_lvs_opts with role=secondary must be called on secondary node."""
        roles = _get_set_opts_roles(self.env, 1)
        assert 'secondary' in roles, \
            f"bdev_lvol_set_lvs_opts with role=secondary not called on secondary; got {roles}"

    def test_set_lvs_opts_role_tertiary(self):
        """bdev_lvol_set_lvs_opts with role=tertiary must be called on tertiary node."""
        roles = _get_set_opts_roles(self.env, 2)
        assert 'tertiary' in roles, \
            f"bdev_lvol_set_lvs_opts with role=tertiary not called on tertiary; got {roles}"

    def test_secondary_does_not_get_set_lvs_opts_primary_role(self):
        """Secondary node must not receive role=primary in set_lvs_opts."""
        roles = _get_set_opts_roles(self.env, 1)
        assert 'primary' not in roles, \
            "Secondary node must never receive role=primary in set_lvs_opts"


# ---------------------------------------------------------------------------
# REACTIVATE: primary restart
# ---------------------------------------------------------------------------

class TestHublvolPrimaryRestart:
    """
    Tests for primary node restart (reactivate).
    n0 is primary for LVS_0; after restart it must:
      - Recreate hublvol with optimized ANA state
      - Trigger sec_1 (n1) to create secondary hublvol
      - Reconnect secondary (n1) with 1 path
      - Reconnect tertiary (n2) with 2 paths
    """

    @pytest.fixture(autouse=True)
    def _restart(self, ftt2_env):
        self.env = ftt2_env
        _setup_hublvols_in_db(ftt2_env)  # all nodes need hublvol pre-set
        create_test_lvol(ftt2_env, 0, name="reactivate-vol")
        prepare_node_for_restart(ftt2_env, 0)
        self.result = _run_restart(ftt2_env, 0)

    def test_restart_succeeds(self):
        assert self.result is True, "Primary restart must succeed"

    def test_primary_recreates_hublvol(self):
        """recreate_hublvol must be called — bdev_lvol_create_hublvol or bdev_get_bdevs on primary."""
        # recreate_hublvol calls create_hublvol only if bdev doesn't exist; mock returns []
        assert self.env['servers'][0].was_called('bdev_lvol_create_hublvol'), \
            "Primary must (re)create hublvol bdev"

    def test_primary_hublvol_optimized_ana_after_restart(self):
        """Recreated hublvol must have optimized ANA state — not the default None."""
        nqn = _hublvol_nqn(self.env['cluster'].nqn, 'LVS_0')
        sub = self.env['servers'][0].state.subsystems.get(nqn)
        assert sub is not None, "Hublvol subsystem must be re-exposed on primary after restart"
        ana_states = [la.get('ana_state') for la in sub.get('listen_addresses', [])]
        assert 'optimized' in ana_states, \
            f"Reactivated primary must expose optimized ANA state; got {ana_states}"

    def test_secondary_hublvol_created_on_primary_restart(self):
        """sec_1 must create/expose its secondary hublvol when primary restarts."""
        assert self.env['servers'][1].was_called('bdev_lvol_create_hublvol'), \
            "sec_1 must call bdev_lvol_create_hublvol for secondary hublvol"

    def test_secondary_hublvol_non_optimized_after_primary_restart(self):
        """sec_1 secondary hublvol listener must be non_optimized."""
        nqn = _hublvol_nqn(self.env['cluster'].nqn, 'LVS_0')
        sub = self.env['servers'][1].state.subsystems.get(nqn)
        assert sub is not None, "Secondary hublvol subsystem not found on sec_1"
        ana_states = [la.get('ana_state') for la in sub.get('listen_addresses', [])]
        assert 'non_optimized' in ana_states, \
            f"sec_1 must expose non_optimized; got {ana_states}"

    def test_secondary_reconnects_to_primary_hublvol(self):
        """Secondary must reconnect to primary hublvol after primary restart."""
        assert self.env['servers'][1].was_called('bdev_lvol_connect_hublvol'), \
            "sec_1 must reconnect to primary hublvol after restart"

    def test_secondary_reconnects_one_path(self):
        """Secondary reconnects with exactly 1 path (no failover)."""
        nqn_fragment = 'hublvol:LVS_0'
        calls = _attach_calls_for_nqn(self.env, 1, nqn_fragment)
        assert len(calls) == 1, \
            f"Secondary must reconnect with 1 path to LVS_0 hublvol; got {len(calls)}"

    def test_tertiary_reconnects_two_paths_after_primary_restart(self):
        """Tertiary reconnects with 2 NVMe paths (primary + sec_1) after primary restart."""
        nqn_fragment = 'hublvol:LVS_0'
        calls = _attach_calls_for_nqn(self.env, 2, nqn_fragment)
        assert len(calls) == 2, \
            f"Tertiary must reconnect with 2 paths to LVS_0 hublvol; got {len(calls)}"


# ---------------------------------------------------------------------------
# SECONDARY RESTART
# ---------------------------------------------------------------------------

class TestHublvolSecondaryRestart:
    """
    Tests for secondary node restart.
    n1 is secondary for LVS_0 (primary=n0, tertiary=n2).
    After n1 restarts:
      - n1 creates its secondary hublvol (non_optimized, same NQN as primary)
      - n1 connects to n0's hublvol: full 3-step (attach → set_opts → connect_hublvol)
      - n2 (tertiary) gets bdev_nvme_attach_controller(multipath) for n1's IPs (step 10)
      - n2 must NOT receive bdev_lvol_connect_hublvol for LVS_0 (already connected)
    """

    @pytest.fixture(autouse=True)
    def _restart(self, ftt2_env):
        self.env = ftt2_env
        _setup_hublvols_in_db(ftt2_env)  # n0 must have hublvol for connect_to_hublvol
        create_test_lvol(ftt2_env, 0, name="sec-restart-vol")
        prepare_node_for_restart(ftt2_env, 1)
        self.result = _run_restart(ftt2_env, 1)

    def test_restart_succeeds(self):
        assert self.result is True, "Secondary restart must succeed"

    def test_secondary_creates_own_hublvol(self):
        """Restarting secondary must create its secondary hublvol bdev."""
        assert self.env['servers'][1].was_called('bdev_lvol_create_hublvol'), \
            "Restarting secondary must call bdev_lvol_create_hublvol for its secondary hublvol"

    def test_secondary_hublvol_exposed_non_optimized(self):
        """Secondary hublvol must be exposed with non_optimized ANA state."""
        nqn = _hublvol_nqn(self.env['cluster'].nqn, 'LVS_0')
        sub = self.env['servers'][1].state.subsystems.get(nqn)
        assert sub is not None, "Secondary hublvol subsystem not found after restart"
        ana_states = [la.get('ana_state') for la in sub.get('listen_addresses', [])]
        assert 'non_optimized' in ana_states, \
            f"Restarting secondary must expose non_optimized ANA; got {ana_states}"

    def test_secondary_full_three_step_connect_to_primary(self):
        """Restarting secondary must do all three SPDK steps to connect to primary hublvol."""
        srv1 = self.env['servers'][1]
        assert srv1.was_called('bdev_nvme_attach_controller'), \
            "Step 1 missing: bdev_nvme_attach_controller not called on secondary"
        assert srv1.was_called('bdev_lvol_connect_hublvol'), \
            "Step 3 missing: bdev_lvol_connect_hublvol not called on secondary"
        # Verify ordering: attach before connect
        all_calls = [m for _, m, _ in srv1.get_rpc_calls()]
        attach_idx = next(i for i, m in enumerate(all_calls) if m == 'bdev_nvme_attach_controller')
        connect_idx = next(i for i, m in enumerate(all_calls) if m == 'bdev_lvol_connect_hublvol')
        assert attach_idx < connect_idx, "connect_hublvol must follow attach_controller"

    def test_secondary_connects_one_path_to_primary(self):
        """Secondary must connect to primary with exactly 1 NVMe path."""
        nqn_fragment = 'hublvol:LVS_0'
        calls = _attach_calls_for_nqn(self.env, 1, nqn_fragment)
        assert len(calls) == 1, \
            f"Secondary must connect with 1 path to primary hublvol; got {len(calls)}"

    def test_tertiary_gets_secondary_multipath_path_step10(self):
        """Step 10: tertiary must receive bdev_nvme_attach_controller with multipath for secondary's IPs."""
        nqn_fragment = 'hublvol:LVS_0'
        calls = _attach_calls_for_nqn(self.env, 2, nqn_fragment)
        assert len(calls) >= 1, \
            "Tertiary must receive attach_controller for secondary hublvol path (step 10)"
        # All step-10 calls must use multipath mode
        for _, _, p in calls:
            assert p.get('multipath') == 'multipath', \
                f"Step 10 must use multipath='multipath'; got {p.get('multipath')}"

    def test_tertiary_does_not_get_connect_hublvol_for_lv0(self):
        """Step 10 adds only an NVMe path — tertiary must NOT repeat bdev_lvol_connect_hublvol for LVS_0."""
        calls = _connect_hublvol_calls_for_lvs(self.env, 2, 'LVS_0')
        assert len(calls) == 0, \
            ("Tertiary must not receive bdev_lvol_connect_hublvol for LVS_0 on secondary restart; "
             f"found {len(calls)} call(s). Adding a multipath path needs only attach_controller.")

    def test_secondary_gets_secondary_role_in_set_opts(self):
        """bdev_lvol_set_lvs_opts with role=secondary must be issued on secondary node."""
        roles = _get_set_opts_roles(self.env, 1)
        assert 'secondary' in roles, \
            f"bdev_lvol_set_lvs_opts role=secondary not found on secondary; got {roles}"


# ---------------------------------------------------------------------------
# TERTIARY RESTART
# ---------------------------------------------------------------------------

class TestHublvolTertiaryRestart:
    """
    Tests for tertiary node restart.
    n2 is tertiary for LVS_0 (primary=n0, secondary=n1).
    After n2 restarts:
      - n2 connects to n0's hublvol with 2 paths: n0 (optimized) + n1 (non_optimized)
      - n2 must NOT create a hublvol for LVS_0 (that's sec_1's job)
      - Both paths must use multipath mode
    """

    @pytest.fixture(autouse=True)
    def _restart(self, ftt2_env):
        self.env = ftt2_env
        _setup_hublvols_in_db(ftt2_env)  # n0 and n1 must have hublvol for connect_to_hublvol
        create_test_lvol(ftt2_env, 0, name="tert-restart-vol")
        prepare_node_for_restart(ftt2_env, 2)
        self.result = _run_restart(ftt2_env, 2)

    def test_restart_succeeds(self):
        assert self.result is True, "Tertiary restart must succeed"

    def test_tertiary_connects_two_paths_on_restart(self):
        """Restarting tertiary must connect to primary hublvol with 2 NVMe paths."""
        nqn_fragment = 'hublvol:LVS_0'
        calls = _attach_calls_for_nqn(self.env, 2, nqn_fragment)
        assert len(calls) == 2, \
            f"Tertiary must connect with 2 paths to LVS_0 hublvol; got {len(calls)}"

    def test_tertiary_paths_use_multipath_mode(self):
        """Both paths must use multipath='multipath' for ANA-based failover."""
        nqn_fragment = 'hublvol:LVS_0'
        calls = _attach_calls_for_nqn(self.env, 2, nqn_fragment)
        for _, _, p in calls:
            assert p.get('multipath') == 'multipath', \
                f"Tertiary hublvol path must use multipath mode; got {p}"

    def test_tertiary_gets_tertiary_role_in_set_opts(self):
        """bdev_lvol_set_lvs_opts must use role=tertiary on the tertiary node."""
        roles = _get_set_opts_roles(self.env, 2)
        assert 'tertiary' in roles, \
            f"bdev_lvol_set_lvs_opts role=tertiary not found on tertiary; got {roles}"

    def test_tertiary_full_three_step_connect(self):
        """Tertiary must do all three SPDK steps: attach → (set_opts) → connect_hublvol."""
        srv2 = self.env['servers'][2]
        assert srv2.was_called('bdev_nvme_attach_controller'), \
            "Step 1 missing: attach_controller not called on tertiary"
        assert srv2.was_called('bdev_lvol_connect_hublvol'), \
            "Step 3 missing: connect_hublvol not called on tertiary"

    def test_tertiary_does_not_create_secondary_hublvol_for_lv0(self):
        """Tertiary must NOT call bdev_lvol_create_hublvol for LVS_0 (that's sec_1's job)."""
        create_calls = self.env['servers'][2].get_rpc_calls('bdev_lvol_create_hublvol')
        lv0_creates = [p for _, _, p in create_calls if p.get('lvs_name') == 'LVS_0']
        assert len(lv0_creates) == 0, \
            "Tertiary must not create a secondary hublvol for LVS_0"
