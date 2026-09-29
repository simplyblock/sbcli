"""The snapshot chain a backup is built from, against records the product wrote.

``backup.controller._get_snapshot_chain`` decides which snapshots a backup must
cover. A single backup uploads one snapshot's blob, which holds only the
clusters written since that snapshot's own parent, so a chain that stops short
of a standalone blob restores a volume with holes in it and reports success
while doing it.

These tests drive the real ``snapshot_controller.add`` against a real
FoundationDB so the pointers the walk reads are the ones the product writes,
then compare the walk's output against the true blob ancestry.

The ancestry is known by construction rather than derived: :class:`Topology`
performs each operation and records, at the moment it performs it, what the
resulting snapshot's blob parent is. Taking a snapshot freezes the volume's
current blob as the snapshot's and gives the volume a fresh blob parented on
it; cloning parents a new volume's blob on the snapshot cloned from; inflating
folds every ancestor into the volume's live blob, so the next snapshot taken
from it stands alone while those already taken keep their parents.

Only the data plane is mocked -- ``lvol_create_snapshot`` and ``get_bdevs``,
which is the whole RPC surface of the non-HA snapshot path
(``snapshot_controller.py:751-769``). Cloning is seeded rather than driven
through ``snapshot_controller.clone``: that path pulls in subsystem allocation,
KMS rekeying and pool capacity, and its entire contribution to ancestry is the
one assignment mirrored in :meth:`Topology.clone`.
"""
import random
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode

#: Enough operations to reach clones of clones and lineages several deep,
#: without making a tier that writes every snapshot to FDB slow.
TOPOLOGY_OPERATIONS = 14
TOPOLOGY_SAMPLES = 12


@pytest.fixture(scope="session")
def ensure_db():
    from simplyblock_core.db_controller import DBController

    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    yield db


class Topology:
    """Real volumes and snapshots, with the true blob ancestry recorded.

    Every snapshot here is created by ``snapshot_controller.add``, so
    ``snap_ref_id``, ``prev_snap_uuid`` and the ``lvol_snaps`` index carry
    whatever the product decided to put there.
    """

    def __init__(self, db, cluster, pool, node):
        self._db = db
        self._cluster = cluster
        self._pool = pool
        self._node = node
        self._seq = 0

        #: snapshot uuid -> its true blob parent, or None if the blob stands alone
        self.blob_parent: dict[str, str | None] = {}
        #: volume uuid -> the blob parent its *next* snapshot will have
        self._next_parent: dict[str, str | None] = {}
        self.volumes: list[str] = []
        self.snapshots: list[str] = []

    def _name(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}{self._seq:03d}"

    def _write_volume(self, cloned_from_snap: str) -> str:
        lvol = LVol()
        lvol.uuid = str(uuid_mod.uuid4())
        lvol.lvol_name = self._name("vol")
        lvol.pool_uuid = self._pool.get_id()
        lvol.node_id = self._node.get_id()
        lvol.nodes = [self._node.get_id()]
        lvol.status = LVol.STATUS_ONLINE
        lvol.ha_type = "single"
        lvol.size = 1024 ** 3
        lvol.max_size = 0
        lvol.lvs_name = self._node.lvstore
        lvol.lvol_bdev = f"LVOL_{lvol.lvol_name}"
        lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
        lvol.base_bdev = "raid_0"
        lvol.cloned_from_snap = cloned_from_snap
        lvol.write_to_db(self._db.kv_store)

        self.volumes.append(lvol.uuid)
        return lvol.uuid

    def add_volume(self) -> str:
        """A volume that is not a clone. Its live blob stands alone."""
        lvol_id = self._write_volume("")
        self._next_parent[lvol_id] = None
        return lvol_id

    def clone(self, snapshot_id: str) -> str:
        """A volume cloned from a snapshot.

        Mirrors the one line of ``snapshot_controller.clone`` that ancestry
        depends on -- ``new_lvol.cloned_from_snap = snapshot.get_id()``
        (``lvol_controller.py:4330``, ``:5339``).
        """
        lvol_id = self._write_volume(snapshot_id)
        self._next_parent[lvol_id] = snapshot_id
        return lvol_id

    def inflate(self, lvol_id: str) -> None:
        """Fold every ancestor into the volume's live blob.

        Mirrors ``lvol_controller.py:3700``, the only state change the inflate
        path makes that ancestry can see.
        """
        lvol = self._db.get_lvol_by_id(lvol_id)
        lvol.cloned_from_snap = ""
        lvol.write_to_db(self._db.kv_store)
        self._next_parent[lvol_id] = None

    def snapshot(self, lvol_id: str) -> str:
        snapshot_id, error = snapshot_controller.add(
            lvol_id, self._name("snap"), lock=False)
        assert not error, f"snapshot_controller.add refused: {error}"

        self.blob_parent[snapshot_id] = self._next_parent[lvol_id]
        self._next_parent[lvol_id] = snapshot_id
        self.snapshots.append(snapshot_id)
        return snapshot_id

    def true_ancestry(self, snapshot_id: str) -> list[str]:
        """Every snapshot a restore of this one needs, oldest first."""
        chain = []
        current: str | None = snapshot_id
        while current is not None:
            chain.append(current)
            current = self.blob_parent[current]
        chain.reverse()
        return chain

    def chain_uuids(self, snapshot_id: str) -> list[str]:
        """What the product would back up for this snapshot, oldest first."""
        snapshot = self._db.get_snapshot_by_id(snapshot_id)
        return [snap.get_id() for snap in backup_controller._get_snapshot_chain(snapshot)]

    def taken_on_a_clone(self, snapshot_id: str) -> bool:
        """Whether this snapshot was taken on a cloned volume.

        ``snap_ref_id`` is written only then (see ``SnapShot.snap_ref_id``),
        which makes it a cheap marker for the shape whose ancestry reaches
        past its own volume.
        """
        return bool(self._db.get_snapshot_by_id(snapshot_id).snap_ref_id)

    def randomize(self, operations: int = TOPOLOGY_OPERATIONS) -> None:
        """Extend the topology by a random operation sequence.

        Draws from the ``random`` module, which ``pytest-randomly`` reseeds per
        test; see tests/AGENTS.md § Randomness and seeds for replaying a run.
        """
        if not self.volumes:
            self.add_volume()

        for _ in range(operations):
            actions = ["snapshot"] * 4 + ["add_volume"]
            if self.snapshots:
                actions += ["clone"] * 2
            clones = [v for v in self.volumes
                      if self._db.get_lvol_by_id(v).cloned_from_snap]
            if clones:
                actions.append("inflate")

            action = random.choice(actions)
            if action == "snapshot":
                self.snapshot(random.choice(self.volumes))
            elif action == "add_volume":
                self.add_volume()
            elif action == "clone":
                self.clone(random.choice(self.snapshots))
            else:
                self.inflate(random.choice(clones))


@pytest.fixture()
def topology(ensure_db):
    """A cluster, pool and node in FDB, with the data plane mocked out."""
    db = ensure_db

    cluster = Cluster()
    cluster.uuid = str(uuid_mod.uuid4())
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{cluster.uuid[:8]}"
    cluster.page_size_in_blocks = 2097152
    cluster.blk_size = 4096
    # Unlimited, so check_snapshot_capacity admits without consulting stats.
    cluster.prov_cap_crit = 0
    cluster.write_to_db(db.kv_store)

    pool = Pool()
    pool.uuid = str(uuid_mod.uuid4())
    pool.pool_name = "chain-pool"
    pool.cluster_id = cluster.uuid
    pool.status = Pool.STATUS_ACTIVE
    pool.lvol_max_size = 0
    pool.pool_max_size = 0
    pool.write_to_db(db.kv_store)

    node = StorageNode()
    node.uuid = str(uuid_mod.uuid4())
    node.cluster_id = cluster.uuid
    node.status = StorageNode.STATUS_ONLINE
    node.hostname = "chain-host"
    node.mgmt_ip = "127.0.0.1"
    node.lvstore = "LVS_CHAIN"
    node.lvstore_status = "ready"
    node.lvstore_stack = []
    node.max_lvol = 10_000
    node.write_to_db(db.kv_store)

    rpc = MagicMock()
    rpc.lvol_create_snapshot.return_value = True
    rpc.get_bdevs.side_effect = lambda *_, **__: [{
        "uuid": str(uuid_mod.uuid4()),
        "driver_specific": {"lvol": {"blobid": 1, "num_allocated_clusters": 1}},
    }]

    with patch.object(StorageNode, "rpc_client", lambda *_, **__: rpc):
        yield Topology(db, cluster, pool, node)


def reported_topology(topology):
    """The topology from the bug report: volume A with P1 then P2, clone C taken
    from P2 with S1, S2, S3 on it."""
    volume_a = topology.add_volume()
    p1 = topology.snapshot(volume_a)
    p2 = topology.snapshot(volume_a)

    clone_c = topology.clone(p2)
    s1 = topology.snapshot(clone_c)
    s2 = topology.snapshot(clone_c)
    s3 = topology.snapshot(clone_c)

    return {"P1": p1, "P2": p2, "S1": s1, "S2": s2, "S3": s3}


class TestPlainVolumes:

    def test_a_single_snapshot_is_its_own_chain(self, topology):
        volume = topology.add_volume()
        snapshot = topology.snapshot(volume)

        assert topology.chain_uuids(snapshot) == [snapshot]

    def test_successive_snapshots_chain_oldest_first(self, topology):
        volume = topology.add_volume()
        taken = [topology.snapshot(volume) for _ in range(3)]

        assert topology.chain_uuids(taken[-1]) == taken


class TestClonedVolumes:

    def test_the_reported_topology_yields_its_whole_lineage(self, topology):
        named = reported_topology(topology)
        lineage = [named["P1"], named["P2"], named["S1"], named["S2"], named["S3"]]

        assert topology.true_ancestry(named["S3"]) == lineage
        assert topology.chain_uuids(named["S3"]) == lineage

    def test_the_chain_covers_the_clones_own_earlier_snapshots(self, topology):
        named = reported_topology(topology)
        walked = topology.chain_uuids(named["S3"])

        assert named["S1"] in walked
        assert named["S2"] in walked

    def test_the_chain_covers_the_source_volumes_earlier_snapshots(self, topology):
        named = reported_topology(topology)

        assert named["P1"] in topology.chain_uuids(named["S3"])


class TestEveryChain:

    def test_no_chain_omits_an_ancestor(self, topology):
        """The property a restore depends on.

        Over-covering costs an upload. Under-covering means the restore writes
        nothing for clusters that live only in the snapshots left out, and
        reports success anyway, so this is the direction that must never fail.
        """
        for _ in range(TOPOLOGY_SAMPLES):
            topology.randomize()

        # A clone's snapshot whose lineage runs deeper than two is the shape
        # that reaches past its own volume, and so the one an ancestry walk can
        # get wrong. Asserted first, so the property below cannot pass on a
        # topology that happened not to build one.
        exposed = [snapshot_id for snapshot_id in topology.snapshots
                   if topology.taken_on_a_clone(snapshot_id)
                   and len(topology.true_ancestry(snapshot_id)) > 2]
        assert len(exposed) >= 5, (
            f"only {len(exposed)} of {len(topology.snapshots)} snapshots are a "
            "clone's with a lineage deeper than two; the topology generator is "
            "no longer exercising the case this asserts")

        missing = {}
        for snapshot_id in topology.snapshots:
            absent = set(topology.true_ancestry(snapshot_id)) - set(
                topology.chain_uuids(snapshot_id))
            if absent:
                missing[snapshot_id] = sorted(absent)

        # Only the first few are shown: on a failing walk this is dozens of
        # entries, and the dict of every one of them buries the count that
        # says how bad it is.
        sample = "; ".join(
            f"{snapshot_id} is missing {len(absent)} of "
            f"{len(topology.true_ancestry(snapshot_id))}"
            for snapshot_id, absent in list(missing.items())[:3])
        assert not missing, (
            f"{len(missing)} of {len(topology.snapshots)} snapshots have a "
            f"chain that omits an ancestor: {sample}")

    def test_every_chain_ends_at_the_snapshot_asked_for(self, topology):
        topology.randomize()

        for snapshot_id in topology.snapshots:
            assert topology.chain_uuids(snapshot_id)[-1] == snapshot_id
