"""The backup chain: what it can be built from, and what it refuses.

Pure logic -- `chain.py` reads no database, so the populations here are plain
lists. The entry points that use it (create, restore, import) are covered
against real FoundationDB in `tests/integration/test_backup_validation.py`.

The theme throughout: coherence is enforced by *construction*, not by a check a
caller has to remember. Every "is refused" test below asserts that building the
chain fails, not that some later validation call does.
"""
import zlib
from uuid import UUID

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers.backup.chain import BackupChain, location_of
from simplyblock_core.controllers.backup.manifest import (
    BackupManifest, DataPlane, FDBKeyDescriptor, Source, Volume)
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupLocation


def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


LOCATION = BackupLocation(bucket_name="backups", region="eu-central-1")
ELSEWHERE = BackupLocation(bucket_name="other-bucket", region="eu-central-1")


def _record(name, prev=None, s3_id=1, location=LOCATION, encrypted=False,
            status=Backup.STATUS_COMPLETED) -> Backup:
    backup = Backup()
    backup.uuid = str(_id(name))
    backup.s3_id = s3_id
    backup.prev_backup_id = str(_id(prev)) if prev is not None else ""
    backup.location = location.model_dump(exclude_none=True)
    backup.encrypted = encrypted
    backup.status = status
    return backup


def _manifest(name, prev=None, s3_id=1, with_compression=False,
              encrypted=False) -> BackupManifest:
    return BackupManifest(
        backup_id=_id(name),
        s3_id=s3_id,
        created_at=100,
        completed_at=200,
        size=4096,
        prev_backup_id=_id(prev) if prev is not None else None,
        encryption=FDBKeyDescriptor(dek_path="keys/x") if encrypted else None,
        source=Source(cluster_id=_id("cluster"), node_id=_id("node")),
        volume=Volume(
            lvol_id=_id("volume"), lvol_name="vol",
            snapshot_id=_id("snapshot"), snapshot_name="snap", size=4096),
        dataplane=DataPlane(with_compression=with_compression),
    )


def _line_of_records(length, **overrides):
    return [
        _record(f"b-{i}", prev=f"b-{i - 1}" if i else None, s3_id=i + 1, **overrides)
        for i in range(length)
    ]


def _line_of_manifests(length, **overrides):
    return [
        _manifest(f"b-{i}", prev=f"b-{i - 1}" if i else None, s3_id=i + 1, **overrides)
        for i in range(length)
    ]


class TestWalkingRecords:

    def test_walks_to_the_root_oldest_first(self):
        line = _line_of_records(3)
        chain = BackupChain.of_backups(_id("b-2"), line)
        assert [b.uuid for b in chain.records()] == [b.uuid for b in line]

    def test_a_full_backup_is_its_own_chain(self):
        line = _line_of_records(3)
        chain = BackupChain.of_backups(_id("b-0"), line)
        assert chain.records() == [line[0]]

    def test_order_of_the_population_does_not_matter(self):
        line = _line_of_records(3)
        chain = BackupChain.of_backups(_id("b-2"), list(reversed(line)))
        assert [b.uuid for b in chain.records()] == [b.uuid for b in line]

    def test_ignores_backups_outside_the_chain(self):
        line = _line_of_records(3)
        chain = BackupChain.of_backups(_id("b-2"), line + [_record("unrelated", s3_id=9)])
        assert len(chain.links) == 3

    def test_a_missing_ancestor_is_reported_not_truncated(self):
        """Truncating would restore a volume with holes in it."""
        line = _line_of_records(3)
        with pytest.raises(PreconditionError, match="not a known backup"):
            BackupChain.of_backups(_id("b-2"), line[1:])

    def test_a_cycle_is_reported_rather_than_looping(self):
        """The record walk had no cycle detection at all and would spin forever."""
        a = _record("b-a", prev="b-b")
        b = _record("b-b", prev="b-a")
        with pytest.raises(PreconditionError, match="cyclic"):
            BackupChain.of_backups(_id("b-a"), [a, b])

    def test_an_unknown_head_is_refused(self):
        with pytest.raises(PreconditionError, match="not a known backup"):
            BackupChain.of_backups(_id("b-9"), _line_of_records(2))

    def test_the_head_supplies_the_chain_s_settings(self):
        chain = BackupChain.of_backups(
            _id("b-1"), _line_of_records(2, location=ELSEWHERE, encrypted=True))
        assert chain.location == ELSEWHERE
        assert chain.encrypted


class TestWalkingManifests:

    def test_walks_to_the_root_oldest_first(self):
        line = _line_of_manifests(3)
        chain = BackupChain.of_manifests(line[-1], line, LOCATION)
        assert [m.backup_id for m in chain.links] == [m.backup_id for m in line]

    def test_a_missing_ancestor_is_reported_not_truncated(self):
        line = _line_of_manifests(3)
        with pytest.raises(PreconditionError, match=str(_id("b-0"))):
            BackupChain.of_manifests(line[-1], line[1:], LOCATION)

    def test_a_cycle_is_reported_rather_than_looping(self):
        a = _manifest("b-a", prev="b-b")
        b = _manifest("b-b", prev="b-a")
        with pytest.raises(PreconditionError, match="cyclic"):
            BackupChain.of_manifests(a, [a, b], LOCATION)

    def test_the_bucket_comes_from_the_reader(self):
        """A manifest describes its objects, not how to reach them -- which is
        what lets a replicated bucket be read as itself."""
        chain = BackupChain.of_manifests(
            _line_of_manifests(1)[0], _line_of_manifests(1), ELSEWHERE)
        assert chain.location.bucket_name == "other-bucket"

    def test_records_are_refused_from_a_manifest_chain(self):
        line = _line_of_manifests(2)
        chain = BackupChain.of_manifests(line[-1], line, LOCATION)
        with pytest.raises(TypeError, match="manifests"):
            chain.records()


class TestCoherence:
    """A chain that could not be restored as a unit cannot be built at all."""

    def test_a_chain_spanning_buckets_is_refused(self):
        line = _line_of_records(2)
        line[0].location = ELSEWHERE.model_dump(exclude_none=True)
        with pytest.raises(PreconditionError, match="other-bucket"):
            BackupChain.of_backups(_id("b-1"), line)

    def test_a_chain_mixing_encryption_is_refused(self):
        line = _line_of_records(2)
        line[0].encrypted = True
        with pytest.raises(PreconditionError, match="encrypted"):
            BackupChain.of_backups(_id("b-1"), line)

    def test_a_chain_mixing_encodings_is_refused(self):
        """`with_compression` is part of the location: read under the wrong
        answer the bodies are garbage rather than an error."""
        line = _line_of_manifests(2)
        line[0] = _manifest("b-0", s3_id=1, with_compression=True)
        with pytest.raises(PreconditionError):
            BackupChain.of_manifests(line[-1], line, LOCATION)

    def test_an_empty_chain_is_coherent(self):
        assert BackupChain.assemble(LOCATION, False, []).links == ()

    def test_assemble_holds_links_to_the_declared_settings(self):
        """The creation path declares what it INTENDS to write, so a cluster
        repointed mid-chain is caught rather than inherited."""
        with pytest.raises(PreconditionError, match="other-bucket"):
            BackupChain.assemble(LOCATION, False, _line_of_records(2, location=ELSEWHERE))

    def test_assemble_catches_a_volume_that_gained_encryption(self):
        with pytest.raises(PreconditionError, match="encrypted"):
            BackupChain.assemble(LOCATION, True, _line_of_records(2))


class TestImporting:
    """A chain being imported may reach back out of the batch into stored records."""

    def test_a_chain_satisfied_within_the_batch(self):
        line = _line_of_manifests(3)
        pending = {m.backup_id: m for m in line}
        chain = BackupChain.importing(line[-1], pending, [], LOCATION)
        assert len(chain.links) == 3

    def test_a_chain_reaching_into_stored_records(self):
        stored = _line_of_records(2)
        head = _manifest("b-2", prev="b-1", s3_id=3)
        chain = BackupChain.importing(head, {head.backup_id: head}, stored, LOCATION)
        assert len(chain.links) == 3

    def test_an_ancestor_in_neither_is_refused(self):
        head = _manifest("b-2", prev="b-1", s3_id=3)
        with pytest.raises(PreconditionError, match=str(_id("b-1"))):
            BackupChain.importing(head, {head.backup_id: head}, [], LOCATION)

    def test_a_chain_reaching_into_another_bucket_is_refused(self):
        stored = _line_of_records(2, location=ELSEWHERE)
        head = _manifest("b-2", prev="b-1", s3_id=3)
        with pytest.raises(PreconditionError, match="other-bucket"):
            BackupChain.importing(head, {head.backup_id: head}, stored, LOCATION)

    def test_a_chain_mixing_encryption_across_the_boundary_is_refused(self):
        """This is the rule import used to lack: it checked buckets and length
        but not encryption, so such a batch imported cleanly and failed at
        restore -- during the recovery it was meant to serve."""
        stored = _line_of_records(2, encrypted=True)
        head = _manifest("b-2", prev="b-1", s3_id=3, encrypted=False)
        with pytest.raises(PreconditionError, match="encrypted"):
            BackupChain.importing(head, {head.backup_id: head}, stored, LOCATION)


class TestRequireRestorable:

    def _chain(self, length=2, **overrides):
        line = _line_of_records(length, **overrides)
        return BackupChain.of_backups(_id(f"b-{length - 1}"), line)

    def test_a_chain_at_the_limit_is_accepted(self):
        self._chain(constants.BACKUP_MAX_CHAIN_LENGTH).require_restorable()

    def test_a_chain_past_the_limit_is_refused(self):
        with pytest.raises(PreconditionError, match="at most"):
            self._chain(constants.BACKUP_MAX_CHAIN_LENGTH + 1).require_restorable()

    def test_the_eventual_length_is_what_counts_at_creation(self):
        """At creation the ancestors are snapshots with no backup yet, so the
        chain is shorter than what it is about to become."""
        chain = BackupChain.assemble(LOCATION, False, [])
        chain.require_restorable(length=constants.BACKUP_MAX_CHAIN_LENGTH)
        with pytest.raises(PreconditionError, match="at most"):
            chain.require_restorable(length=constants.BACKUP_MAX_CHAIN_LENGTH + 1)

    def test_a_tiering_layout_bucket_holds_no_backups(self):
        """snapshot_backups=False keys objects {tiering_id}/{lpgi}, which the
        restore path can never address."""
        tiering = BackupLocation(bucket_name="tiering", snapshot_backups=False)
        with pytest.raises(PreconditionError, match="snapshot_backups"):
            BackupChain.assemble(tiering, False, []).require_restorable()

    def test_an_unfinished_link_is_refused_only_when_asked(self):
        line = _line_of_records(2)
        line[0].status = Backup.STATUS_IN_PROGRESS
        chain = BackupChain.of_backups(_id("b-1"), line)

        chain.require_restorable()
        with pytest.raises(PreconditionError, match="Incomplete"):
            chain.require_restorable(completed=True)

    def test_the_wording_names_what_was_refused(self):
        with pytest.raises(PreconditionError, match="This snapshot chain"):
            self._chain(constants.BACKUP_MAX_CHAIN_LENGTH + 1).require_restorable(
                what="This snapshot chain")


class TestS3Ids:

    def test_newest_first(self):
        """The data plane claims each cluster for the first id that offers it,
        so the newest backup's data has to win."""
        chain = BackupChain.of_backups(_id("b-2"), _line_of_records(3))
        assert chain.s3_ids_newest_first() == [3, 2, 1]


class TestLocationOf:

    def test_the_encoding_is_the_manifest_s_and_the_bucket_the_reader_s(self):
        location = location_of(_manifest("b-0", with_compression=True), ELSEWHERE)
        assert location.bucket_name == "other-bucket"
        assert location.with_compression

    def test_credentials_cannot_leak_into_a_record(self):
        """A BackupConfig is a BackupLocation, so it can be passed as one."""
        from simplyblock_core.models.backup_config import BackupConfig

        config = BackupConfig.model_validate({
            "bucket_name": "backups",
            "credentials": {"access_key_id": "AKIA", "secret_access_key": "s3cret"},
        })
        assert not hasattr(location_of(_manifest("b-0"), config), "credentials")
