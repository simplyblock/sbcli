# coding=utf-8
"""
test_scalability.py – scalability e2e test for volume migration.

Generates a large topology:
  - 250 volumes on a single source node
  - Each volume has 1–10 random snapshots (chain)
  - Volumes are grouped into ~50 shared subsystems (average 5 volumes per NQN)
  - Migrate a sample of volumes and verify correctness

This test exercises:
  - FDB scalability under many concurrent records
  - Subsystem sharing (multiple volumes per NQN)
  - Pre-existing snapshot detection across sibling migrations
  - Correct subsystem reuse on the target
"""

import random
import time
import pytest

from simplyblock_core.controllers import migration_controller
from simplyblock_core.models.lvol_migration import LVolMigration

from tests.migration.conftest import run_migration_task
from tests.migration.topology_loader import TestContext, load_topology

# ---------------------------------------------------------------------------
# Lazy DB
# ---------------------------------------------------------------------------

_db_instance = None


def _get_db():
    global _db_instance
    if _db_instance is None:
        from simplyblock_core.db_controller import DBController
        _db_instance = DBController()
    return _db_instance


class _LazyDb:
    def __getattr__(self, name):
        return getattr(_get_db(), name)


db = _LazyDb()


# ---------------------------------------------------------------------------
# Topology generation
# ---------------------------------------------------------------------------

NUM_VOLUMES = 250
MIN_SNAPS = 1
MAX_SNAPS = 10
AVG_VOLS_PER_SUBSYS = 5


def _generate_scalability_spec(rng: random.Random) -> dict:
    """
    Build a topology spec dict with NUM_VOLUMES volumes, random snapshot
    chains, and shared namespace groups.
    """
    # Assign volumes to namespace groups (shared subsystems)
    num_groups = max(1, NUM_VOLUMES // AVG_VOLS_PER_SUBSYS)
    groups = [f"grp{i}" for i in range(num_groups)]

    volumes = []
    snapshots = []

    for vi in range(NUM_VOLUMES):
        vol_id = f"v{vi}"
        vol_name = f"vol_{vi}"
        grp = rng.choice(groups)
        num_snaps = rng.randint(MIN_SNAPS, MAX_SNAPS)

        volumes.append({
            "id": vol_id,
            "name": vol_name,
            "size": "512M",
            "node_id": "src",
            "pool_id": "pool1",
            "status": "online",
            "namespace_group": grp,
        })

        # Build a linear snapshot chain for this volume
        prev_snap_ref = ""
        for si in range(num_snaps):
            snap_id = f"s{vi}_{si}"
            snap_name = f"snap_{vi}_{si}"
            snapshots.append({
                "id": snap_id,
                "name": snap_name,
                "lvol_id": vol_id,
                "snap_ref_id": prev_snap_ref,
                "status": "online",
            })
            prev_snap_ref = snap_id

    spec = {
        "cluster": {},
        "nodes": [
            {
                "id": "src",
                "hostname": "host-src",
                "mgmt_ip": "127.0.0.1",
                "rpc_port": 9901,
                "rpc_username": "spdkuser",
                "rpc_password": "spdkpass",
                "lvstore": "lvs_src",
                "lvol_subsys_port": 9090,
                "status": "online",
                "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}],
            },
            {
                "id": "tgt",
                "hostname": "host-tgt",
                "mgmt_ip": "127.0.0.1",
                "rpc_port": 9902,
                "rpc_username": "spdkuser",
                "rpc_password": "spdkpass",
                "lvstore": "lvs_tgt",
                "lvol_subsys_port": 9090,
                "status": "online",
                "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}],
            },
        ],
        "pools": [
            {"id": "pool1", "name": "scale-pool", "status": "active"},
        ],
        "volumes": volumes,
        "snapshots": snapshots,
    }
    return spec


# ---------------------------------------------------------------------------
# Mock server seeding (batch)
# ---------------------------------------------------------------------------

def _seed_all(mock_srv, ctx: TestContext, node_sym: str):
    """Seed all lvols and snapshots for a given node onto the mock RPC server."""
    node = ctx.node(node_sym)
    with mock_srv.state.lock:
        for lvol in ctx._lvols.values():
            if lvol.node_id == node.uuid:
                composite = f"{node.lvstore}/{lvol.lvol_bdev}"
                blobid = mock_srv.state.next_blobid()
                mock_srv.state.lvols[composite] = {
                    'name': lvol.lvol_bdev,
                    'composite': composite,
                    'uuid': lvol.lvol_uuid or lvol.uuid,
                    'blobid': blobid,
                    'size_mib': 512,
                    'migration_flag': False,
                    'driver_specific': {
                        'lvol': {
                            'blobid': blobid,
                            'lvs_name': node.lvstore,
                            'base_snapshot': None,
                            'clone': False,
                            'snapshot': False,
                            'num_allocated_clusters': 512,
                        }
                    }
                }

        for snap in ctx._snaps.values():
            if snap.lvol and snap.lvol.node_id == node.uuid:
                short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
                composite = f"{node.lvstore}/{short}"
                blobid = mock_srv.state.next_blobid()
                mock_srv.state.snapshots[composite] = {
                    'name': short,
                    'composite': composite,
                    'uuid': snap.snap_uuid or snap.uuid,
                    'blobid': blobid,
                    'size_mib': 512,
                    'driver_specific': {
                        'lvol': {
                            'blobid': blobid,
                            'lvs_name': node.lvstore,
                            'base_snapshot': None,
                            'clone': False,
                            'snapshot': True,
                            'num_allocated_clusters': 512,
                        }
                    }
                }


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestScalability:
    """
    Scalability test: 250 volumes with random snapshot chains and shared
    subsystems.  Migrate a sample and verify correctness.
    """

    @pytest.fixture()
    def scale_topology(self, ensure_cluster, mock_src_server, mock_tgt_server):
        """Load the large topology, seed mock servers, yield context."""
        mock_src_server.reset_state()
        mock_src_server.set_failure_rate(0.0)
        mock_tgt_server.reset_state()
        mock_tgt_server.set_failure_rate(0.0)

        rng = random.Random(42)  # deterministic for reproducibility
        spec = _generate_scalability_spec(rng)

        # Patch RPC ports to match session-scoped mock servers
        import os
        worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
        try:
            offset = int(worker.replace("gw", "")) * 10
        except ValueError:
            offset = 0

        for node in spec["nodes"]:
            if node["id"] == "src":
                node["rpc_port"] = 9901 + offset
            elif node["id"] == "tgt":
                node["rpc_port"] = 9902 + offset

        ctx = load_topology(spec)
        _seed_all(mock_src_server, ctx, "src")

        yield ctx, rng

        ctx.teardown()

    def test_topology_creation(self, scale_topology):
        """Verify the topology was created with expected counts."""
        ctx, rng = scale_topology

        assert len(ctx._lvols) == NUM_VOLUMES
        assert len(ctx._snaps) > NUM_VOLUMES  # at least 1 snap per vol

        # Verify namespace groups — volumes sharing the same NQN
        nqn_counts = {}
        for lvol in ctx._lvols.values():
            nqn_counts[lvol.nqn] = nqn_counts.get(lvol.nqn, 0) + 1

        # Should have ~50 groups, average 5 per group
        num_groups = len(nqn_counts)
        avg_per_group = NUM_VOLUMES / num_groups
        assert 30 <= num_groups <= 70, f"Expected ~50 namespace groups, got {num_groups}"
        assert 3 <= avg_per_group <= 8, f"Expected ~5 vols/group, got {avg_per_group:.1f}"

    def test_migrate_sample_volumes(self, scale_topology, mock_tgt_server):
        """Migrate 10 random volumes sequentially and verify each completes."""
        ctx, rng = scale_topology
        tgt = ctx.node("tgt")

        # Pick 10 random volumes to migrate
        vol_syms = [f"v{i}" for i in rng.sample(range(NUM_VOLUMES), 10)]

        for vol_sym in vol_syms:
            lvol_uuid = ctx.lvol_uuid(vol_sym)
            mig_id, err = migration_controller.start_migration(
                lvol_uuid, tgt.uuid, max_retries=30)
            assert err is None, f"start_migration({vol_sym}) failed: {err}"

            m = run_migration_task(mig_id, max_steps=3000, step_sleep=0.01)
            assert m.status == LVolMigration.STATUS_DONE, (
                f"{vol_sym}: expected DONE, got {m.status}; error={m.error_message}")

            # Verify DB updated
            lvol = db.get_lvol_by_id(lvol_uuid)
            assert lvol.node_id == tgt.uuid, f"{vol_sym}: node_id not updated"
            assert lvol.hostname == tgt.hostname, f"{vol_sym}: hostname not updated"

    def test_migrate_shared_subsystem_group(self, scale_topology, mock_tgt_server):
        """
        Find a namespace group with multiple volumes and migrate all of them.
        The target should reuse the subsystem for subsequent volumes.
        """
        ctx, rng = scale_topology
        tgt = ctx.node("tgt")

        # Find a group with at least 3 volumes
        nqn_to_vols = {}
        for sym_id, lvol_uuid in ctx._lvol_uuid.items():
            lvol = ctx._lvols[lvol_uuid]
            nqn_to_vols.setdefault(lvol.nqn, []).append(sym_id)

        # Pick the first group with >= 3 volumes
        group_vols = None
        group_nqn = None
        for nqn, vols in nqn_to_vols.items():
            if len(vols) >= 3:
                group_vols = vols[:5]  # cap at 5 for speed
                group_nqn = nqn
                break

        assert group_vols is not None, "No namespace group with >= 3 volumes found"

        for i, vol_sym in enumerate(group_vols):
            lvol_uuid = ctx.lvol_uuid(vol_sym)
            mig_id, err = migration_controller.start_migration(
                lvol_uuid, tgt.uuid, max_retries=30)
            assert err is None, f"start_migration({vol_sym}) failed: {err}"

            m = run_migration_task(mig_id, max_steps=3000, step_sleep=0.01)
            assert m.status == LVolMigration.STATUS_DONE, (
                f"{vol_sym}: expected DONE, got {m.status}; error={m.error_message}")

        # Verify all volumes share the same subsystem on the target
        sub = mock_tgt_server.state.subsystems.get(group_nqn)
        assert sub is not None, f"Shared subsystem {group_nqn!r} not on target"
        ns_count = len(sub['namespaces'])
        assert ns_count == len(group_vols), (
            f"Expected {len(group_vols)} namespaces in shared subsystem, got {ns_count}")

    def test_preexisting_detection_at_scale(self, scale_topology):
        """
        Migrate two volumes from the same snapshot chain.
        The second migration should detect pre-existing snapshots.
        """
        ctx, rng = scale_topology
        tgt = ctx.node("tgt")

        # Find a volume with multiple snapshots (>= 3)
        vol_with_many_snaps = None
        vol_snaps = {}
        for snap_sym, snap_uuid in ctx._snap_uuid.items():
            snap = ctx._snaps[snap_uuid]
            vid = snap.lvol.uuid
            vol_snaps.setdefault(vid, []).append(snap_sym)
        for vid, snap_syms in vol_snaps.items():
            if len(snap_syms) >= 3:
                vol_with_many_snaps = vid
                break

        assert vol_with_many_snaps is not None, "No volume with >= 3 snaps found"

        # Find the symbolic id for this volume
        vol_sym = None
        for sym, uuid in ctx._lvol_uuid.items():
            if uuid == vol_with_many_snaps:
                vol_sym = sym
                break

        # Migrate it
        mig_id, err = migration_controller.start_migration(
            vol_with_many_snaps, tgt.uuid, max_retries=30)
        assert err is None
        m = run_migration_task(mig_id, max_steps=3000, step_sleep=0.01)
        assert m.status == LVolMigration.STATUS_DONE

        # Now migrate a second volume that is unrelated — its snaps should NOT
        # be pre-existing
        other_vol = None
        for sym in ctx._lvol_uuid:
            if sym != vol_sym:
                other_vol = sym
                break

        mig_id2, err2 = migration_controller.start_migration(
            ctx.lvol_uuid(other_vol), tgt.uuid, max_retries=30)
        assert err2 is None
        m2 = run_migration_task(mig_id2, max_steps=3000, step_sleep=0.01)
        assert m2.status == LVolMigration.STATUS_DONE

    def test_migration_throughput(self, scale_topology):
        """
        Migrate 20 volumes sequentially and measure throughput.
        This is not a pass/fail test — it prints timing stats for CI monitoring.
        """
        ctx, rng = scale_topology
        tgt = ctx.node("tgt")

        vol_syms = [f"v{i}" for i in range(20)]
        total_snaps = 0

        t0 = time.time()
        for vol_sym in vol_syms:
            lvol_uuid = ctx.lvol_uuid(vol_sym)
            mig_id, err = migration_controller.start_migration(
                lvol_uuid, tgt.uuid, max_retries=30)
            assert err is None, f"start_migration({vol_sym}) failed: {err}"

            m = run_migration_task(mig_id, max_steps=3000, step_sleep=0.01)
            assert m.status == LVolMigration.STATUS_DONE, (
                f"{vol_sym}: expected DONE, got {m.status}; error={m.error_message}")
            total_snaps += len(m.snaps_migrated)

        elapsed = time.time() - t0
        vols_per_sec = 20 / elapsed
        snaps_per_sec = total_snaps / elapsed

        print(f"\n--- Scalability throughput ---")
        print(f"  Volumes migrated: 20")
        print(f"  Snapshots migrated: {total_snaps}")
        print(f"  Elapsed: {elapsed:.1f}s")
        print(f"  Volumes/sec: {vols_per_sec:.2f}")
        print(f"  Snapshots/sec: {snaps_per_sec:.2f}")


# ---------------------------------------------------------------------------
# Large single-volume test (1 volume, 250 snapshots)
# ---------------------------------------------------------------------------

NUM_LARGE_SNAPS = 250


def _generate_large_volume_spec() -> dict:
    """Build a topology with 1 volume and 250 linear snapshots."""
    snapshots = []
    prev_snap_ref = ""
    for si in range(NUM_LARGE_SNAPS):
        snap_id = f"s{si}"
        snapshots.append({
            "id": snap_id,
            "name": f"snap_{si}",
            "lvol_id": "bigvol",
            "snap_ref_id": prev_snap_ref,
            "status": "online",
        })
        prev_snap_ref = snap_id

    return {
        "cluster": {},
        "nodes": [
            {
                "id": "src",
                "hostname": "host-src",
                "mgmt_ip": "127.0.0.1",
                "rpc_port": 9901,
                "rpc_username": "spdkuser",
                "rpc_password": "spdkpass",
                "lvstore": "lvs_src",
                "lvol_subsys_port": 9090,
                "status": "online",
                "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}],
            },
            {
                "id": "tgt",
                "hostname": "host-tgt",
                "mgmt_ip": "127.0.0.1",
                "rpc_port": 9902,
                "rpc_username": "spdkuser",
                "rpc_password": "spdkpass",
                "lvstore": "lvs_tgt",
                "lvol_subsys_port": 9090,
                "status": "online",
                "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}],
            },
        ],
        "pools": [
            {"id": "pool1", "name": "large-pool", "status": "active"},
        ],
        "volumes": [
            {
                "id": "bigvol",
                "name": "big_volume",
                "size": "1G",
                "node_id": "src",
                "pool_id": "pool1",
                "status": "online",
            },
        ],
        "snapshots": snapshots,
    }


class TestLargeVolumeMigration:
    """
    Migrate a single volume with 250 snapshots — happy path (no RPC errors,
    no node outages, no process interruption).
    """

    @pytest.fixture()
    def large_vol_topology(self, ensure_cluster, mock_src_server, mock_tgt_server):
        """Load topology with 1 volume + 250 snapshots, seed source mock."""
        mock_src_server.reset_state()
        mock_src_server.set_failure_rate(0.0)
        mock_tgt_server.reset_state()
        mock_tgt_server.set_failure_rate(0.0)

        spec = _generate_large_volume_spec()

        import os
        worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
        try:
            offset = int(worker.replace("gw", "")) * 10
        except ValueError:
            offset = 0

        for node in spec["nodes"]:
            if node["id"] == "src":
                node["rpc_port"] = 9901 + offset
            elif node["id"] == "tgt":
                node["rpc_port"] = 9902 + offset

        ctx = load_topology(spec)
        _seed_all(mock_src_server, ctx, "src")

        yield ctx

        ctx.teardown()

    def test_large_volume_migration(self, large_vol_topology, mock_tgt_server):
        """Migrate a volume with 250 snapshots end-to-end."""
        ctx = large_vol_topology
        tgt = ctx.node("tgt")

        lvol_uuid = ctx.lvol_uuid("bigvol")
        mig_id, err = migration_controller.start_migration(
            lvol_uuid, tgt.uuid, max_retries=30)
        assert err is None, f"start_migration failed: {err}"

        t0 = time.time()
        m = run_migration_task(mig_id, max_steps=10000, step_sleep=0.01)
        elapsed = time.time() - t0

        assert m.status == LVolMigration.STATUS_DONE, (
            f"Expected DONE, got {m.status}; error={m.error_message}")

        # All 250 snapshots + intermediate snaps should have been migrated
        num_intermediate = len(m.intermediate_snaps)
        expected_total = NUM_LARGE_SNAPS + num_intermediate
        assert len(m.snaps_migrated) == expected_total, (
            f"Expected {expected_total} snaps migrated ({NUM_LARGE_SNAPS} original + "
            f"{num_intermediate} intermediate), got {len(m.snaps_migrated)}")

        # No pre-existing snapshots (fresh target)
        assert len(m.snaps_preexisting_on_target) == 0, (
            f"Expected 0 pre-existing, got {len(m.snaps_preexisting_on_target)}")

        # DB should reflect the target node
        lvol = db.get_lvol_by_id(lvol_uuid)
        assert lvol.node_id == tgt.uuid, "node_id not updated"
        assert lvol.hostname == tgt.hostname, "hostname not updated"

        # All snapshots should have updated snap_bdev prefix
        for snap_uuid in m.snaps_migrated:
            snap = db.get_snapshot_by_id(snap_uuid)
            if snap and snap.snap_bdev and '/' in snap.snap_bdev:
                assert snap.snap_bdev.startswith(tgt.lvstore + "/"), (
                    f"snap {snap_uuid} snap_bdev not updated: {snap.snap_bdev}")

        print(f"\n--- Large volume migration ---")
        print(f"  Snapshots: {NUM_LARGE_SNAPS}")
        print(f"  Snaps migrated: {len(m.snaps_migrated)}")
        print(f"  Elapsed: {elapsed:.1f}s")
        print(f"  Snaps/sec: {NUM_LARGE_SNAPS / elapsed:.2f}")
