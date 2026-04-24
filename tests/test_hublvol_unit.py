# coding=utf-8
"""
test_hublvol_unit.py – Unit tests for StorageNode hublvol methods.

Tests individual methods (create_hublvol, create_secondary_hublvol,
recreate_hublvol, connect_to_hublvol) with a mocked RPCClient.
No FDB, no HTTP server — pure unit tests.

SPDK three-step sequence for secondary/tertiary:
  1. bdev_nvme_attach_controller  – NVMe bdev must exist before step 3
  2. bdev_lvol_set_lvs_opts       – sets lvs->node_role
  3. bdev_lvol_connect_hublvol    – binds lvstore to hub bdev
"""

import unittest
import uuid
from unittest.mock import MagicMock, patch

from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CLUSTER_NQN = "nqn.2023-02.io.simplyblock:testcluster01"
_PRIMARY_PORT = 4430
_PRIMARY_LVS = "LVS_0"
_PRIMARY_IP = "10.0.0.1"
_SECONDARY_IP = "10.0.0.2"
_TERTIARY_IP = "10.0.0.3"


def _make_nic(ip: str, trtype: str = "TCP") -> IFace:
    nic = IFace()
    nic.uuid = str(uuid.uuid4())
    nic.if_name = "eth0"
    nic.ip4_address = ip
    nic.trtype = trtype
    nic.net_type = "data"
    return nic


def _make_hublvol(lvstore: str = _PRIMARY_LVS, port: int = _PRIMARY_PORT) -> HubLVol:
    return HubLVol({
        'uuid': str(uuid.uuid4()),
        'nqn': f"{_CLUSTER_NQN}:hublvol:{lvstore}",
        'bdev_name': f'{lvstore}/hublvol',
        'model_number': str(uuid.uuid4()),
        'nguid': 'ab' * 16,
        'nvmf_port': port,
    })


def _make_node(ip: str, lvstore: str, jm_vuid: int = 100, port: int = 5000) -> StorageNode:
    """Create a minimal StorageNode for unit testing (no FDB write)."""
    n = StorageNode()
    n.uuid = str(uuid.uuid4())
    n.cluster_id = "test-cluster"
    n.status = StorageNode.STATUS_ONLINE
    n.hostname = f"host-{ip}"
    n.mgmt_ip = "127.0.0.1"
    n.rpc_port = port
    n.rpc_username = "spdkuser"
    n.rpc_password = "spdkpass"
    n.active_tcp = True
    n.active_rdma = False
    n.data_nics = [_make_nic(ip)]
    n.lvstore = lvstore
    n.jm_vuid = jm_vuid
    n.lvstore_ports = {lvstore: {"lvol_subsys_port": 4420, "hublvol_port": _PRIMARY_PORT}}
    n.hublvol = None
    return n


def _mock_rpc(return_bdev_create=str(uuid.uuid4()),
              bdev_exists=False,
              subsystem_exists=False):
    """Build a MagicMock RPCClient with sensible defaults for hublvol tests."""
    rpc = MagicMock()
    rpc.bdev_lvol_create_hublvol.return_value = return_bdev_create
    rpc.get_bdevs.return_value = [{}] if bdev_exists else []
    rpc.subsystem_list.return_value = {} if subsystem_exists else None
    rpc.subsystem_create.return_value = True
    rpc.listeners_create.return_value = True
    rpc.nvmf_subsystem_add_ns.return_value = True
    rpc.bdev_nvme_attach_controller.side_effect = (
        lambda name, nqn, ip, port, trtype, multipath=None: [f"{name}n1"]
    )
    rpc.bdev_lvol_set_lvs_opts.return_value = True
    rpc.bdev_lvol_connect_hublvol.return_value = True
    return rpc


# ---------------------------------------------------------------------------
# TestCreateHublvolUnit
# ---------------------------------------------------------------------------

class TestCreateHublvolUnit(unittest.TestCase):
    """create_hublvol — primary creates its hub bdev and exposes it NVMe-oF."""

    def setUp(self):
        self.node = _make_node(_PRIMARY_IP, _PRIMARY_LVS)
        self.rpc = _mock_rpc()
        patcher = patch(
            'simplyblock_core.models.storage_node.RPCClient',
            return_value=self.rpc,
        )
        self.addCleanup(patcher.stop)
        patcher.start()
        # Suppress DB write
        self.node.write_to_db = MagicMock()

    def test_creates_bdev(self):
        """bdev_lvol_create_hublvol must be called with the node's lvstore."""
        self.node.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        self.rpc.bdev_lvol_create_hublvol.assert_called_once_with(_PRIMARY_LVS)

    def test_hublvol_nqn_uses_shared_scheme(self):
        """When cluster_nqn is given, NQN must follow the shared scheme for ANA multipath."""
        self.node.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        expected_nqn = f"{_CLUSTER_NQN}:hublvol:{_PRIMARY_LVS}"
        assert self.node.hublvol is not None
        assert self.node.hublvol.nqn == expected_nqn

    def test_expose_bdev_with_optimized_ana(self):
        """Primary hublvol listener must be created with ANA state = optimized."""
        self.node.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        listener_calls = self.rpc.listeners_create.call_args_list
        assert len(listener_calls) >= 1, "listeners_create must be called at least once"
        for c in listener_calls:
            kwargs = c.kwargs if c.kwargs else {}
            args = c.args if c.args else []
            # ana_state may be positional or keyword
            ana_state = kwargs.get('ana_state', args[4] if len(args) > 4 else None)
            assert ana_state == 'optimized', \
                f"Primary hublvol must have ana_state=optimized; got {ana_state}"

    def test_subsystem_created_for_hublvol_nqn(self):
        """subsystem_create must be called with the hublvol NQN."""
        self.node.create_hublvol(cluster_nqn=_CLUSTER_NQN)
        expected_nqn = f"{_CLUSTER_NQN}:hublvol:{_PRIMARY_LVS}"
        create_call = self.rpc.subsystem_create.call_args
        assert create_call is not None, "subsystem_create must be called"
        called_nqn = create_call.kwargs.get('nqn') or create_call.args[0]
        assert called_nqn == expected_nqn


# ---------------------------------------------------------------------------
# TestCreateSecondaryHublvolUnit
# ---------------------------------------------------------------------------

class TestCreateSecondaryHublvolUnit(unittest.TestCase):
    """create_secondary_hublvol — sec_1 exposes same NQN as primary, non_optimized."""

    def setUp(self):
        self.primary = _make_node(_PRIMARY_IP, _PRIMARY_LVS, jm_vuid=100)
        self.primary.hublvol = _make_hublvol(_PRIMARY_LVS, _PRIMARY_PORT)

        self.secondary = _make_node(_SECONDARY_IP, "LVS_1", jm_vuid=200)
        self.rpc = _mock_rpc()
        patcher = patch(
            'simplyblock_core.models.storage_node.RPCClient',
            return_value=self.rpc,
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_uses_primary_nqn(self):
        """Secondary hublvol must be exposed under the primary's shared NQN."""
        self.secondary.create_secondary_hublvol(self.primary, _CLUSTER_NQN)
        expected_nqn = self.primary.hublvol.nqn
        # subsystem_create is called with the same NQN
        create_call = self.rpc.subsystem_create.call_args
        assert create_call is not None
        called_nqn = create_call.kwargs.get('nqn') or create_call.args[0]
        assert called_nqn == expected_nqn, \
            f"Secondary must use primary NQN {expected_nqn}; got {called_nqn}"

    def test_exposes_non_optimized_ana(self):
        """Secondary hublvol listener must use ana_state = non_optimized."""
        self.secondary.create_secondary_hublvol(self.primary, _CLUSTER_NQN)
        listener_calls = self.rpc.listeners_create.call_args_list
        assert len(listener_calls) >= 1, "listeners_create must be called"
        for c in listener_calls:
            kwargs = c.kwargs if c.kwargs else {}
            args = c.args if c.args else []
            ana_state = kwargs.get('ana_state', args[4] if len(args) > 4 else None)
            assert ana_state == 'non_optimized', \
                f"Secondary hublvol must have ana_state=non_optimized; got {ana_state}"

    def test_uses_primary_hublvol_port(self):
        """Secondary's NVMe-oF listener must use the primary's hublvol port."""
        self.secondary.create_secondary_hublvol(self.primary, _CLUSTER_NQN)
        listener_calls = self.rpc.listeners_create.call_args_list
        assert len(listener_calls) >= 1
        for c in listener_calls:
            kwargs = c.kwargs if c.kwargs else {}
            args = c.args if c.args else []
            trsvcid = kwargs.get('trsvcid', args[3] if len(args) > 3 else None)
            assert trsvcid == _PRIMARY_PORT, \
                f"Secondary must use primary port {_PRIMARY_PORT}; got {trsvcid}"

    def test_creates_bdev_when_missing(self):
        """bdev_lvol_create_hublvol must be called when the bdev doesn't exist."""
        # get_bdevs returns [] → bdev absent
        self.rpc.get_bdevs.return_value = []
        self.secondary.create_secondary_hublvol(self.primary, _CLUSTER_NQN)
        self.rpc.bdev_lvol_create_hublvol.assert_called_once_with(_PRIMARY_LVS)

    def test_skips_bdev_create_when_already_exists(self):
        """bdev_lvol_create_hublvol must NOT be called when bdev already exists."""
        self.rpc.get_bdevs.return_value = [{'name': f'{_PRIMARY_LVS}/hublvol'}]
        self.secondary.create_secondary_hublvol(self.primary, _CLUSTER_NQN)
        self.rpc.bdev_lvol_create_hublvol.assert_not_called()


# ---------------------------------------------------------------------------
# TestRecreateHublvolUnit
# ---------------------------------------------------------------------------

class TestRecreateHublvolUnit(unittest.TestCase):
    """recreate_hublvol — primary re-exposes hublvol after restart."""

    def setUp(self):
        self.node = _make_node(_PRIMARY_IP, _PRIMARY_LVS)
        self.node.hublvol = _make_hublvol(_PRIMARY_LVS, _PRIMARY_PORT)
        self.rpc = _mock_rpc()
        patcher = patch(
            'simplyblock_core.models.storage_node.RPCClient',
            return_value=self.rpc,
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_expose_bdev_with_optimized_ana(self):
        """Recreated hublvol must be exposed with ana_state = optimized."""
        self.rpc.get_bdevs.return_value = [{}]  # bdev already exists
        self.node.recreate_hublvol()
        listener_calls = self.rpc.listeners_create.call_args_list
        assert len(listener_calls) >= 1, "listeners_create must be called on recreate"
        for c in listener_calls:
            kwargs = c.kwargs if c.kwargs else {}
            args = c.args if c.args else []
            ana_state = kwargs.get('ana_state', args[4] if len(args) > 4 else None)
            assert ana_state == 'optimized', \
                f"Recreated primary hublvol must have ana_state=optimized; got {ana_state}"

    def test_creates_bdev_when_missing(self):
        """If the bdev is gone, bdev_lvol_create_hublvol must be called to recreate it."""
        self.rpc.get_bdevs.return_value = []  # bdev absent after restart
        self.node.recreate_hublvol()
        self.rpc.bdev_lvol_create_hublvol.assert_called_once_with(_PRIMARY_LVS)

    def test_skips_bdev_create_when_exists(self):
        """If the bdev already exists, bdev_lvol_create_hublvol must NOT be called."""
        self.rpc.get_bdevs.return_value = [{'name': f'{_PRIMARY_LVS}/hublvol'}]
        self.node.recreate_hublvol()
        self.rpc.bdev_lvol_create_hublvol.assert_not_called()

    def test_returns_true_on_success(self):
        """recreate_hublvol must return True when it succeeds."""
        self.rpc.get_bdevs.return_value = [{}]
        result = self.node.recreate_hublvol()
        assert result is True


# ---------------------------------------------------------------------------
# TestAdoptHublvolUnit
# ---------------------------------------------------------------------------

class TestAdoptHublvolUnit(unittest.TestCase):
    """adopt_hublvol — takeover leader creates hub bdev for the taken-over LVS
    and re-exposes it under the same NQN/port/UUID as the original primary.

    The bug this pins:
      - ``create_hublvol`` on the takeover node used ``self.lvstore`` (self's
        OWN primary), not the taken-over lvstore → EEXIST against self's
        already-present primary hublvol bdev, and wrong bdev name if it had
        not been there.
      - No probe-before-create, so the second restart retry was guaranteed to
        hit EEXIST on the adopted bdev.

    adopt_hublvol must (a) target ``lvs_node.lvstore``, (b) reuse
    ``lvs_node.hublvol`` metadata so clients see the same NQN/port/UUID,
    (c) probe before creating, (d) be safely re-entrant across retries.
    """

    _TAKEOVER_LVS = "LVS_remote"

    def setUp(self):
        # Takeover node has its own primary lvstore (LVS_self) and a
        # separate adopted lvstore (LVS_remote).
        self.takeover_node = _make_node(_PRIMARY_IP, "LVS_self", jm_vuid=100)
        self.takeover_node.hublvol = _make_hublvol("LVS_self", _PRIMARY_PORT)

        # The offline peer whose LVS is being taken over.
        self.offline_peer = _make_node(_SECONDARY_IP, self._TAKEOVER_LVS, jm_vuid=200)
        self.offline_peer.hublvol = _make_hublvol(self._TAKEOVER_LVS, 4431)

        self.rpc = _mock_rpc()
        patcher = patch(
            'simplyblock_core.models.storage_node.RPCClient',
            return_value=self.rpc,
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_creates_bdev_for_taken_over_lvstore_not_self(self):
        """bdev_lvol_create_hublvol must target the peer's lvstore, not self's."""
        self.rpc.get_bdevs.return_value = []  # bdev absent
        self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        self.rpc.bdev_lvol_create_hublvol.assert_called_once_with(self._TAKEOVER_LVS)

    def test_skips_create_when_bdev_already_exists(self):
        """Idempotent: probe succeeds → no create call, no EEXIST."""
        self.rpc.get_bdevs.return_value = [
            {'name': f'{self._TAKEOVER_LVS}/hublvol'}
        ]
        self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        self.rpc.bdev_lvol_create_hublvol.assert_not_called()

    def test_exposes_with_peer_hublvol_metadata(self):
        """Subsystem must be exposed with the peer's existing UUID/port so
        that surviving clients don't see a new NQN and keep their paths."""
        self.rpc.get_bdevs.return_value = [{}]
        self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        listener_calls = self.rpc.listeners_create.call_args_list
        assert listener_calls, "listeners_create must be called"
        for c in listener_calls:
            kwargs = c.kwargs
            args = c.args
            trsvcid = kwargs.get('trsvcid', args[3] if len(args) > 3 else None)
            assert trsvcid == self.offline_peer.hublvol.nvmf_port, (
                f"Adopted hublvol must use peer port "
                f"{self.offline_peer.hublvol.nvmf_port}; got {trsvcid}"
            )
        add_ns_call = self.rpc.nvmf_subsystem_add_ns.call_args
        assert add_ns_call is not None
        called_uuid = add_ns_call.kwargs.get('uuid')
        assert called_uuid == self.offline_peer.hublvol.uuid, (
            "Adopted namespace must reuse peer hublvol UUID for client continuity"
        )

    def test_exposes_under_shared_nqn_for_lvs(self):
        """NQN must be the deterministic shared hublvol NQN for the taken-over
        lvstore — not self's primary NQN."""
        self.rpc.get_bdevs.return_value = [{}]
        self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        expected = f"{_CLUSTER_NQN}:hublvol:{self._TAKEOVER_LVS}"
        create_call = self.rpc.subsystem_create.call_args
        called_nqn = create_call.kwargs.get('nqn') or create_call.args[0]
        assert called_nqn == expected, f"NQN must be {expected}; got {called_nqn}"

    def test_exposes_with_optimized_ana(self):
        """Takeover leader is the NEW primary for the adopted LVS → ANA optimized."""
        self.rpc.get_bdevs.return_value = [{}]
        self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        listener_calls = self.rpc.listeners_create.call_args_list
        assert listener_calls
        for c in listener_calls:
            kwargs = c.kwargs
            args = c.args
            ana_state = kwargs.get('ana_state', args[4] if len(args) > 4 else None)
            assert ana_state == 'optimized', (
                f"Adopted hublvol must have ana_state=optimized; got {ana_state}"
            )

    def test_does_not_mutate_self_hublvol(self):
        """Takeover runs in addition to self's own primary — self.hublvol
        (self's own lvstore's hub) must not be overwritten by adoption."""
        self.rpc.get_bdevs.return_value = []
        original_self_hublvol = self.takeover_node.hublvol
        self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        assert self.takeover_node.hublvol is original_self_hublvol

    def test_raises_when_peer_has_no_hublvol_metadata(self):
        """If the offline peer has no hublvol metadata in FDB, adopt must
        fail loudly — silent no-op would leave IO unreachable."""
        from simplyblock_core.rpc_client import RPCException
        self.offline_peer.hublvol = None
        with self.assertRaises(RPCException):
            self.takeover_node.adopt_hublvol(self.offline_peer, _CLUSTER_NQN)
        self.rpc.bdev_lvol_create_hublvol.assert_not_called()


# ---------------------------------------------------------------------------
# TestConnectToHublvolUnit
# ---------------------------------------------------------------------------

class TestConnectToHublvolUnit(unittest.TestCase):
    """connect_to_hublvol — secondary/tertiary attach NVMe path(s) and do full SPDK sequence."""

    def setUp(self):
        self.primary = _make_node(_PRIMARY_IP, _PRIMARY_LVS, jm_vuid=100)
        self.primary.hublvol = _make_hublvol(_PRIMARY_LVS, _PRIMARY_PORT)
        self.primary.lvstore_ports = {_PRIMARY_LVS: {"lvol_subsys_port": 4420,
                                                      "hublvol_port": _PRIMARY_PORT}}

        self.secondary = _make_node(_SECONDARY_IP, "LVS_1", jm_vuid=200)

        # Separate failover node (sec_1) — tertiary sees it as the failover
        self.sec1 = _make_node(_SECONDARY_IP, "LVS_1", jm_vuid=200, port=5001)
        self.sec1.hublvol = _make_hublvol(_PRIMARY_LVS, _PRIMARY_PORT)

        self.tertiary = _make_node(_TERTIARY_IP, "LVS_2", jm_vuid=300, port=5002)

        self.rpc = _mock_rpc()
        patcher = patch(
            'simplyblock_core.models.storage_node.RPCClient',
            return_value=self.rpc,
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    # --- secondary (no failover) ---

    def test_secondary_attaches_one_path(self):
        """Secondary must attach exactly 1 NVMe path (primary IP, no failover)."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        attach_calls = self.rpc.bdev_nvme_attach_controller.call_args_list
        assert len(attach_calls) == 1, \
            f"Secondary must call attach_controller once; called {len(attach_calls)} times"

    def test_secondary_attaches_primary_ip(self):
        """Secondary's single path must target the primary node's data IP."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        attach_call = self.rpc.bdev_nvme_attach_controller.call_args
        called_ip = attach_call.args[2] if len(attach_call.args) > 2 else attach_call.kwargs.get('traddr')
        assert called_ip == _PRIMARY_IP, \
            f"Secondary must attach to primary IP {_PRIMARY_IP}; got {called_ip}"

    def test_secondary_no_multipath_mode(self):
        """Secondary (no failover, single NIC) must NOT use multipath mode."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        attach_call = self.rpc.bdev_nvme_attach_controller.call_args
        multipath = attach_call.kwargs.get('multipath')
        assert multipath != 'multipath', \
            f"Secondary with no failover must not use multipath='multipath'; got {multipath!r}"

    def test_secondary_set_lvs_opts_role(self):
        """bdev_lvol_set_lvs_opts must be called with role='secondary' on secondary node."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        set_opts_call = self.rpc.bdev_lvol_set_lvs_opts.call_args
        assert set_opts_call is not None, "bdev_lvol_set_lvs_opts must be called"
        role = set_opts_call.kwargs.get('role')
        assert role == 'secondary', \
            f"set_lvs_opts must receive role='secondary'; got {role!r}"

    def test_secondary_connect_hublvol_called(self):
        """bdev_lvol_connect_hublvol must be called on secondary after attaching."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        self.rpc.bdev_lvol_connect_hublvol.assert_called_once()

    def test_secondary_connect_hublvol_uses_correct_bdev(self):
        """bdev_lvol_connect_hublvol must reference the primary's hublvol bdev (with n1 suffix)."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        connect_call = self.rpc.bdev_lvol_connect_hublvol.call_args
        expected_remote_bdev = f"{self.primary.hublvol.bdev_name}n1"
        called_bdev = connect_call.args[1] if len(connect_call.args) > 1 else connect_call.kwargs.get('remote_bdev')
        assert called_bdev == expected_remote_bdev, \
            f"connect_hublvol must use remote_bdev={expected_remote_bdev!r}; got {called_bdev!r}"

    # --- tertiary (with failover) ---

    def test_tertiary_attaches_two_paths(self):
        """Tertiary must attach 2 NVMe paths: primary IP + sec_1 IP."""
        self.tertiary.connect_to_hublvol(self.primary, failover_node=self.sec1, role="tertiary")
        attach_calls = self.rpc.bdev_nvme_attach_controller.call_args_list
        assert len(attach_calls) == 2, \
            f"Tertiary must call attach_controller twice (primary + sec_1); got {len(attach_calls)}"

    def test_tertiary_both_paths_use_multipath_mode(self):
        """Both tertiary NVMe paths must be attached with multipath='multipath' for ANA."""
        self.tertiary.connect_to_hublvol(self.primary, failover_node=self.sec1, role="tertiary")
        for i, c in enumerate(self.rpc.bdev_nvme_attach_controller.call_args_list):
            multipath = c.kwargs.get('multipath')
            assert multipath == 'multipath', \
                f"Tertiary path {i} must use multipath='multipath'; got {multipath!r}"

    def test_tertiary_set_lvs_opts_role(self):
        """bdev_lvol_set_lvs_opts must be called with role='tertiary' on tertiary node."""
        self.tertiary.connect_to_hublvol(self.primary, failover_node=self.sec1, role="tertiary")
        set_opts_call = self.rpc.bdev_lvol_set_lvs_opts.call_args
        assert set_opts_call is not None
        role = set_opts_call.kwargs.get('role')
        assert role == 'tertiary', \
            f"set_lvs_opts must receive role='tertiary'; got {role!r}"

    def test_tertiary_connect_hublvol_called(self):
        """bdev_lvol_connect_hublvol must be called on tertiary (step 3 of SPDK sequence)."""
        self.tertiary.connect_to_hublvol(self.primary, failover_node=self.sec1, role="tertiary")
        self.rpc.bdev_lvol_connect_hublvol.assert_called_once()

    # --- SPDK sequence ordering ---

    def _call_order(self, method_name: str) -> list[int]:
        """Return 0-based positions of method_name in the overall RPC call sequence."""
        positions = []
        for i, c in enumerate(self.rpc.method_calls):
            if c[0] == method_name:
                positions.append(i)
        return positions

    def test_attach_before_connect_hublvol_on_secondary(self):
        """SPDK constraint: bdev must exist before bdev_lvol_connect_hublvol is called."""
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        attach_positions = self._call_order('bdev_nvme_attach_controller')
        connect_positions = self._call_order('bdev_lvol_connect_hublvol')
        assert attach_positions, "attach_controller not called"
        assert connect_positions, "connect_hublvol not called"
        assert max(attach_positions) < min(connect_positions), \
            ("SPDK requires bdev to exist before connect_hublvol — "
             "all attach_controller calls must precede connect_hublvol")

    def test_attach_before_connect_hublvol_on_tertiary(self):
        """Same SPDK sequence requirement on tertiary with 2 paths."""
        self.tertiary.connect_to_hublvol(self.primary, failover_node=self.sec1, role="tertiary")
        attach_positions = self._call_order('bdev_nvme_attach_controller')
        connect_positions = self._call_order('bdev_lvol_connect_hublvol')
        assert len(attach_positions) == 2, f"Expected 2 attach calls; got {len(attach_positions)}"
        assert connect_positions, "connect_hublvol not called"
        assert max(attach_positions) < min(connect_positions), \
            "All attach_controller calls must precede connect_hublvol on tertiary"

    def test_set_opts_before_connect_hublvol_on_secondary(self):
        """set_lvs_opts must run BEFORE connect_hublvol.

        The lvstore role (secondary/tertiary) must be committed before the
        hublvol connection is established — otherwise SPDK rejects the
        connect_hublvol RPC with "-22 nonsecondary node" because from its
        point of view the lvstore is still marked primary.
        """
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        set_positions = self._call_order('bdev_lvol_set_lvs_opts')
        connect_positions = self._call_order('bdev_lvol_connect_hublvol')
        assert set_positions, "bdev_lvol_set_lvs_opts not called"
        assert connect_positions, "bdev_lvol_connect_hublvol not called"
        assert max(set_positions) < min(connect_positions), \
            ("set_lvs_opts must commit the non-leader role before connect_hublvol, "
             "otherwise SPDK rejects the connect with -22 nonsecondary node")

    def test_set_opts_before_connect_hublvol_on_tertiary(self):
        """Same ordering requirement on tertiary."""
        self.tertiary.connect_to_hublvol(self.primary, failover_node=self.sec1, role="tertiary")
        set_positions = self._call_order('bdev_lvol_set_lvs_opts')
        connect_positions = self._call_order('bdev_lvol_connect_hublvol')
        assert set_positions, "bdev_lvol_set_lvs_opts not called"
        assert connect_positions, "bdev_lvol_connect_hublvol not called"
        assert max(set_positions) < min(connect_positions), \
            "set_lvs_opts must precede connect_hublvol on tertiary"

    def test_connect_hublvol_not_called_if_set_opts_fails(self):
        """If set_lvs_opts fails, connect_hublvol must not be called — the
        role must be committed before the hublvol connection is attempted.
        """
        self.rpc.bdev_lvol_set_lvs_opts.return_value = False
        result = self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        assert result is False, "connect_to_hublvol must return False on set_lvs_opts failure"
        self.rpc.bdev_lvol_connect_hublvol.assert_not_called()

    # --- error handling ---

    def test_raises_if_primary_hublvol_none(self):
        """connect_to_hublvol must raise ValueError when primary has no hublvol."""
        self.primary.hublvol = None
        with self.assertRaises(ValueError):
            self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")

    def test_skips_attach_if_bdev_already_exists(self):
        """If the remote bdev already exists, attach_controller must not be called again."""
        # Simulate bdev already attached (e.g. after a partial restart)
        self.rpc.get_bdevs.return_value = [{'name': f'{_PRIMARY_LVS}/hubvoln1'}]
        self.secondary.connect_to_hublvol(self.primary, failover_node=None, role="secondary")
        self.rpc.bdev_nvme_attach_controller.assert_not_called()


if __name__ == '__main__':
    unittest.main()
