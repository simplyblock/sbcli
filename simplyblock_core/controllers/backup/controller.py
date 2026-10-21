"""Creating, restoring, importing, exporting and discovering backups."""
import logging
import time
from collections.abc import Iterable
from uuid import UUID, uuid4

from pydantic import ValidationError

from simplyblock_core.controllers import backup_events, tasks_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.chain import BackupChain, location_of
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.kms import (
    KMSException,
    backup_dek_path,
    backup_kek_name,
    create_kms_connection,
    lvol_dek_path,
    pool_kek_name,
)
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import (
    BackupConfig,
    BackupLocation,
    S3Credentials,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

logger = logging.getLogger()

db_controller = DBController()


def _generate_backup_id():
    return str(uuid4())


async def watch_backups(cluster_id):
    """Stream backup changes for one cluster (same scope as the list endpoint)."""
    async for batch in db_controller.watch(
            Backup, scope=(cluster_id,),
            ancestors=[(Cluster, (), cluster_id)]):
        yield batch


def get_latest_backup_for_lvol(lvol_id):
    """Get the most recent non-failed backup for a given lvol.

    Includes pending/in-progress backups so that chain links are set
    even when multiple backups are created in quick succession before
    the earlier ones complete.
    """
    backups = db_controller.get_backups_by_lvol_id(lvol_id)
    valid = [b for b in backups if b.status in (
        Backup.STATUS_COMPLETED, Backup.STATUS_IN_PROGRESS, Backup.STATUS_PENDING)]
    if not valid:
        return None
    valid.sort(key=lambda b: b.created_at, reverse=True)
    return valid[0]


def _existing_chain_backups(snap_chain) -> list:
    """The backups that already exist for a snapshot chain, oldest first."""
    existing = []
    for snap in snap_chain:
        for backup in db_controller.get_backups_by_snapshot_id(snap.get_id()):
            if backup.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS,
                                 Backup.STATUS_COMPLETED):
                existing.append(backup)
    return existing


def build_manifest(backup: Backup) -> backup_manifest.BackupManifest:
    """Assemble the self-describing record for a completed backup.

    Everything a restore needs is collected here, from the backup itself and
    from the volume/pool/cluster it came from, so that after this point no part
    of the restore path has to consult the originating cluster.

    This function exists because `Backup` and `BackupManifest` describe the same
    thing in two shapes. Most of that overlap is not justified, and this is the
    seam where it shows:

    * Justified: `status` and `error_message` are on the record and not in the
      manifest, because they are mutable control-plane state with no meaning in a
      bucket. `schema_version` and `dataplane` are in the manifest and not on the
      record, because they are claims about the byte format, which the cluster
      that wrote them does not need told back to it.
    * Not justified: `pool_uuid` on the record against `volume.pool_name` in the
      manifest -- the same fact, keyed differently, so neither can be derived from
      the other. `encrypted` beside `encryption` on the record, where the manifest
      keeps one optional document, so the record can still say a thing the
      manifest cannot express (see below). And the volume's settings, which the
      manifest records and the record does not, so an imported backup knows less
      about its volume than the manifest it was imported from did.
    * Actively wrong: `dataplane.cluster_size` and `source` are recomputed here
      from the *current* cluster, because the record does not keep what an import
      read. Re-exporting an imported backup therefore restamps both with the
      importing cluster's identity and page size, silently, though they describe
      objects a different cluster wrote. Nothing reads either yet, so nothing is
      broken today -- and a bucket's own manifests, which is what a recovery
      reads, are written once by the cluster that made the backup and are correct.

    The fix for all three is the same and is not attempted here: make the
    manifest the canonical document, store it on the record, and reduce `Backup`
    to control-plane state plus the fields FoundationDB is queried by. That is
    cheaper than it looks -- every backup query in `db_controller` already filters
    in Python over a full scan, so nesting costs nothing there -- but it touches
    `BackupDTO`, the `backup list` table and existing records, so it wants its own
    change.

    Raises:
        ValueError: The backup has no recorded location, is encrypted without
            recording where its key is, or carries an id that is not a UUID --
            none of the three is describable. The record types its ids as plain
            strings and so can hold an empty or malformed one; the manifest
            cannot, and this is the seam where that is discovered, while the
            backup is being written rather than during a recovery.
    """
    # Only for the encoding the manifest records. Where the bucket is does not
    # travel with the manifest -- but a record that cannot say how its objects
    # are encoded cannot be described either, so this still has to resolve.
    location = backup.get_location()

    # A record that predates self-describing backups carries `encrypted` without
    # a descriptor. No manifest says that: an absent descriptor means plaintext,
    # so writing one here would advertise ciphertext as readable and a restore
    # from it would silently produce a plaintext volume over the ciphertext.
    if backup.encrypted and not backup.encryption:
        raise ValueError(
            f"Backup {backup.uuid} is encrypted but records nothing about its "
            "key; it predates self-describing backups and cannot be described "
            "by a manifest")

    # The volume's own settings, where it still exists. Absent together once it
    # is gone, which is a different answer from 0 -- for a QoS cap that means
    # unlimited.
    try:
        lvol = db_controller.get_lvol_by_id(backup.lvol_id)
    except KeyError:
        logger.warning("Volume %s is gone; manifest for backup %s records only "
                       "the shape carried on the backup itself",
                       backup.lvol_id, backup.uuid)
        settings = {}
    else:
        settings = {
            "pool_name": lvol.pool_name,
            "ha_type": lvol.ha_type or None,
            "fabric": lvol.fabric or None,
            "lvol_priority_class": lvol.lvol_priority_class,
            "max_size": lvol.max_size,
            "rw_ios_per_sec": lvol.rw_ios_per_sec,
            "rw_mbytes_per_sec": lvol.rw_mbytes_per_sec,
            "r_mbytes_per_sec": lvol.r_mbytes_per_sec,
            "w_mbytes_per_sec": lvol.w_mbytes_per_sec,
        }

    # Validated in one go rather than copied onto: model_copy does not validate,
    # and every value above comes off an untyped record -- an ha_type the model
    # does not recognise has to be caught here, while the backup is still being
    # written, not by whoever reads the manifest during a recovery.
    volume = backup_manifest.ManifestVolume.model_validate({
        "lvol_id": backup.lvol_id,
        "lvol_name": backup.lvol_name,
        "snapshot_id": backup.snapshot_id,
        "snapshot_name": backup.snapshot_name,
        "size": backup.size,
        "max_namespace_per_subsys": backup.max_namespace_per_subsys or None,
        **settings,
    })

    cluster_name = None
    cluster_size = None
    try:
        cluster = db_controller.get_cluster_by_id(backup.cluster_id)
    except KeyError:
        logger.warning("Cluster %s is gone; manifest for backup %s records no "
                       "object size", backup.cluster_id, backup.uuid)
    else:
        cluster_name = cluster.cluster_name
        # Same sizing as create_lvstore, for the same reason as the stat
        # collector: the manifest records object sizes derived from it, and a
        # cluster that records no data-chunk count falls back to one page
        # rather than describing its objects as zero-sized.
        cluster_size = cluster.page_size_in_blocks * max(cluster.distr_ndcs, 1)

    return backup_manifest.BackupManifest(
        backup_id=UUID(backup.uuid),
        s3_id=backup.s3_id,
        created_at=backup.created_at,
        completed_at=backup.completed_at,
        size=backup.size,
        prev_backup_id=UUID(backup.prev_backup_id) if backup.prev_backup_id else None,
        encryption=(backup_manifest.parse_key_descriptor(backup.encryption)
                    if backup.encryption else None),
        source=backup_manifest.ManifestSource(
            cluster_id=UUID(backup.cluster_id),
            cluster_name=cluster_name,
            node_id=UUID(backup.node_id),
        ),
        volume=volume,
        dataplane=backup_manifest.ManifestDataPlane(
            cluster_size=cluster_size,
            with_compression=location.with_compression,
        ),
    )


def _credentials_for_foreign_location(backup: Backup, location: BackupLocation,
                                      own: BackupConfig) -> S3Credentials | None:
    """The cluster's own credentials, where they can speak for another location.

    Credentials authenticate against an S3 service, not against one bucket: keys
    that open the cluster's own bucket open every bucket the same account can
    reach at the same endpoint. That is what an ordinary cross-cluster restore
    looks like -- two clusters backing up to their own buckets of one store -- so
    the configuration the cluster already has is the answer, and demanding the
    same keys again on the command line would be asking for what is already
    known.

    Returns ``None`` where the cluster names no keys, which is a configuration
    and not a gap: the nodes' instance role may well cover the other bucket, and
    nothing here can tell whether it does.

    Raises:
        PreconditionError: The backup lives at a different endpoint, where the
            cluster's keys authenticate nothing, and none were supplied for it.
            Refused here because the alternative is a failure deep in the data
            plane with nothing to point at the cause.
    """
    if own.endpoint_url != location.endpoint_url:
        if own.credentials is not None:
            raise PreconditionError(
                f"Backup {backup.uuid} lives in bucket {location.bucket_name} at "
                f"{location.endpoint_url or 'AWS S3'}, which this cluster's own "
                f"credentials do not authenticate against. Supply credentials for "
                f"that bucket, or configure the nodes with an instance role that "
                "can read it.")
        return None

    if own.credentials is not None:
        logger.info("Reaching bucket %s for backup %s with this cluster's own "
                    "credentials; they authenticate against the same endpoint (%s)",
                    location.bucket_name, backup.uuid,
                    location.endpoint_url or "AWS S3")

    return own.credentials


def foreign_bucket_config(backup: Backup, cluster,
                          credentials: S3Credentials | None) -> BackupConfig | None:
    """How to reach this backup's bucket, when it is not the cluster's own.

    Returns ``None`` when the node's existing backup device already points at
    the right bucket -- there is no foreign bucket, so there is nothing to
    describe. Otherwise returns the configuration for a device that reads the
    backup's own recorded location, which is what makes a restore from another
    cluster's bucket possible at all.

    Explicit credentials win; absent them the cluster's own are inherited where
    they can apply, per :func:`_credentials_for_foreign_location`.

    Decides only. The device itself is created by the task runner, which is the
    component that knows which node the volume landed on and the only one that
    can put the device back after a node restart mid-restore.

    Raises:
        PreconditionError: The bucket is foreign and unreachable -- it sits at
            another endpoint than the cluster's own credentials authenticate
            against, and none were supplied for it.
    """
    location = backup.get_location()

    try:
        own = cluster.get_backup_config()
    except ValueError:
        # No usable configuration of its own, so every bucket is foreign to it.
        own = None

    if own is not None and own.location() == location and credentials is None:
        return None

    if credentials is None and own is not None:
        credentials = _credentials_for_foreign_location(backup, location, own)

    return BackupConfig.model_validate({
        **location.model_dump(exclude_none=True),
        **({"credentials": credentials.model_dump()} if credentials is not None else {}),
    })


def _resolve_crypto_key(backup: Backup, cluster):
    """Recover the key needed to read an encrypted backup.

    An encrypted backup is ciphertext whose key lives in a KMS, and the backup
    records which one. Restoring it therefore needs that KMS reachable -- the
    assumption being that a KMS is recoverable independently of any one cluster.
    Nothing about the key travels with the backup.

    Returns None for an unencrypted backup.

    Raises:
        PreconditionError: The backup records nothing about its key, or too
            little of it to reach the KMS, so no amount of reachable
            infrastructure can decrypt it.
        RuntimeError: The recorded KMS could not be reached. Raised before the
            volume is created, so a restore that cannot decrypt fails without
            leaving a half-built volume behind -- and, more importantly, without
            silently producing a plaintext volume over ciphertext.
    """
    if not backup.encrypted:
        return None

    if not backup.encryption:
        raise PreconditionError(
            f"Backup {backup.uuid} is encrypted but records nothing about its "
            "key; it predates self-describing backups and cannot be restored")

    try:
        descriptor = backup_manifest.parse_key_descriptor(backup.encryption)
    except ValidationError as e:
        raise PreconditionError(
            f"Backup {backup.uuid} is encrypted but records nothing about its "
            f"key that a KMS can be reached with: {e}") from e

    try:
        with create_kms_connection(cluster) as kms:
            return descriptor.read_keys(kms)
    except KMSException as e:
        raise RuntimeError(
            f"Cannot reach the key for backup {backup.uuid} at "
            f"{descriptor.dek_path} using {descriptor.type}, which has to be "
            f"reachable to restore it: {e}") from e


def _config_for(backup: Backup) -> BackupConfig:
    """Credentials for a backup's own bucket.

    The location comes from the backup; only the credentials come from the
    cluster, and only because a manifest must never carry them.

    Raises:
        PreconditionError: The cluster's configured bucket is not the one this
            backup lives in, so its credentials cannot be assumed to reach it.
            A precondition rather than a failure: it means the cluster's backup
            configuration was repointed while this backup was in flight, which is
            visible through GET /clusters/{id}/backup-config.
    """
    config = db_controller.get_cluster_by_id(backup.cluster_id).get_backup_config()
    location = backup.get_location()

    if config.location() != location:
        raise PreconditionError(
            f"Backup {backup.uuid} lives in bucket {location.bucket_name}, but "
            f"cluster {backup.cluster_id} is configured for "
            f"{config.bucket_name}; supply credentials for the backup's bucket")

    return config


def write_manifest(backup: Backup) -> None:
    """Publish a backup's manifest.

    Raises:
        ManifestError: the manifest could not be stored.
        PreconditionError: the backup's bucket is not the cluster's own.
    """
    backup_manifest.write(_config_for(backup), build_manifest(backup))


def delete_manifest(backup: Backup) -> None:
    backup_manifest.delete(_config_for(backup), UUID(backup.uuid))


def _blob_parent_id(snapshot: SnapShot) -> str | None:
    """The snapshot this one's blob is a delta against, or ``None`` at a root.

    See ``SnapShot.prev_snap_uuid`` and ``SnapShot.snap_ref_id`` for which
    pointer carries blob ancestry and which only looks like it does.
    """
    if snapshot.prev_snap_uuid:
        return snapshot.prev_snap_uuid

    if snapshot.lvol and snapshot.lvol.cloned_from_snap:
        return snapshot.lvol.cloned_from_snap

    return None


def _get_snapshot_chain(snapshot: SnapShot) -> list[SnapShot]:
    """Every snapshot a restore of this one needs, oldest first.

    One backup uploads one snapshot's blob, which holds only the clusters
    written since that snapshot's own parent, so the chain has to reach back
    to a snapshot whose blob stands alone.

    Raises:
        PreconditionError: an ancestor cannot be reached, so no restorable
            chain exists. Reported rather than truncated: a short chain
            restores a volume with holes in it and reports success.
    """
    chain = [snapshot]
    seen = {snapshot.get_id()}

    while (parent_id := _blob_parent_id(chain[-1])) is not None:
        if parent_id in seen:
            raise PreconditionError(
                f"Snapshot {snapshot.get_id()} has a cyclic ancestry at {parent_id}")

        try:
            parent = db_controller.get_snapshot_by_id(parent_id)
        except KeyError as e:
            raise PreconditionError(
                f"Snapshot {chain[-1].get_id()} is a delta against {parent_id}, "
                "which no longer exists, so its ancestry cannot be backed up as "
                "a restorable chain.") from e

        seen.add(parent_id)
        chain.append(parent)

    chain.reverse()
    return chain


def _snapshot_has_backup(snapshot_id):
    """Check if a snapshot already has a non-failed backup."""
    backups = db_controller.get_backups_by_snapshot_id(snapshot_id)
    return any(b.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS,
                            Backup.STATUS_COMPLETED, Backup.STATUS_MERGED) for b in backups)


def _build_key_descriptor(cluster, backup: Backup) -> backup_manifest.KeyDescriptor:
    """Describe where an encrypted backup's key lives.

    The dependency on a KMS is not removed -- it is written down. Nothing before
    this recorded it at all, so an encrypted backup's key was reachable only by
    someone who already knew which cluster had made it and how that cluster was
    configured.
    """
    dek_path = backup_dek_path(cluster.get_id(), backup.uuid)
    vault = cluster.hashicorp_vault_settings

    if vault is None:
        return backup_manifest.FDBKeyDescriptor(dek_path=dek_path)

    return backup_manifest.HCPKeyDescriptor(
        dek_path=dek_path,
        kek_name=backup_kek_name(backup.uuid),
        vault_base_url=vault.base_url or None,
        transit_mount=vault.transit_mount,
        kv_mount=vault.kv_mount,
    )


def create_single_backup(snapshot, lvol, node_id, cluster_id, prev_backup, location: BackupLocation):
    """Create a single backup record and task for one snapshot.

    Args:
        location: where this backup's objects will be written. Passed in rather
            than read from the cluster here so that every backup in one chain is
            guaranteed to share it -- the caller resolves it once and validates
            the chain against it.

    Returns the created Backup object.
    """
    backup_id = _generate_backup_id()

    backup = Backup()
    backup.uuid = backup_id
    backup.s3_id = db_controller.next_s3_id()
    backup.cluster_id = cluster_id
    backup.location = location.model_dump(mode="json")
    backup.lvol_id = lvol.get_id()
    backup.lvol_name = lvol.lvol_name
    backup.snapshot_id = snapshot.get_id()
    backup.snapshot_name = snapshot.snap_name
    backup.node_id = node_id
    backup.pool_uuid = lvol.pool_uuid
    backup.prev_backup_id = prev_backup.uuid if prev_backup else ""
    backup.size = snapshot.size
    backup.created_at = int(time.time())
    backup.status = Backup.STATUS_PENDING
    backup.encrypted = bool(lvol.crypto_bdev)
    # The subsystem sharing the restore reapplies (Backup.max_namespace_per_subsys).
    backup.max_namespace_per_subsys = int(getattr(lvol, "max_namespace_per_subsys", 0) or 1)

    if backup.encrypted:
        cluster = db_controller.get_cluster_by_id(cluster_id)
        with create_kms_connection(cluster) as kms:
            kms.create_key_encryption_key(backup_kek_name(backup.uuid))
            kms.rekey_data_encryption_keys(
                lvol_dek_path(cluster_id, lvol.get_id()),
                pool_kek_name(lvol.pool_uuid),
                backup_dek_path(cluster_id, backup.uuid),
                backup_kek_name(backup.uuid),
            )
        backup.encryption = _build_key_descriptor(cluster, backup).model_dump(mode="json")

    backup.write_to_db()

    backup_events.backup_created(cluster_id, node_id, backup)
    tasks_controller.add_backup_task(backup)

    return backup


def _require_restorable_chain(location: BackupLocation, encrypted: bool,
                              backed_snapshots: list[SnapShot], length: int,
                              what: str) -> None:
    """Refuse a chain of ``length`` links, continuing whatever backups already
    exist for ``backed_snapshots``, that would not be restorable.

    One rule, so a refusal before a snapshot is taken
    (`chain_would_be_restorable`) and a refusal while actually writing one
    (`ensure_snapshot_chain_backed_up`) can never disagree about the same
    volume.

    Raises:
        PreconditionError: see `BackupChain.require_restorable`.
    """
    BackupChain.assemble(
        location, encrypted, _existing_chain_backups(backed_snapshots),
    ).require_restorable(length=length, what=what)


def chain_would_be_restorable(lvol, location: BackupLocation, what: str) -> None:
    """Refuse if the snapshot a scheduled backup is about to take could never
    be folded into a restorable chain.

    Checked against the volume's real snapshot ancestry and the backups that
    already exist for it, one link short of what `ensure_snapshot_chain_backed_up`
    will see once that snapshot exists -- so a schedule refuses before taking
    it and leaving an orphan `auto_*` snapshot behind, for the same reason
    `ensure_snapshot_chain_backed_up` would refuse afterwards.

    Raises:
        PreconditionError: the eventual chain would be longer than the data
            plane accepts, or the volume's existing chain lives in a
            different bucket or encoding than `location` declares.
    """
    latest_snapshot = db_controller.get_lvol_latest_snapshot(lvol.get_id())
    existing_chain = _get_snapshot_chain(latest_snapshot) if latest_snapshot is not None else []
    _require_restorable_chain(
        location, bool(lvol.crypto_bdev), existing_chain, len(existing_chain) + 1, what)


def ensure_snapshot_chain_backed_up(snapshot, lvol, node_id, cluster_id,
                                    location: BackupLocation) -> str:
    """Make every ancestor of ``snapshot`` backed up, and return the id of the
    backup covering ``snapshot`` itself.

    A single snapshot's backup is only a delta: the data plane uploads just
    the clusters written since the snapshot's own parent
    (``_get_snapshot_chain``'s docstring), and is never told anything about
    `Backup.prev_backup_id` -- `bdev_lvol_s3_backup` takes only a snapshot
    name. So the one thing that is allowed to decide a new backup's
    predecessor is the snapshot's own parent, walked here, never "whatever
    `Backup` record happens to be newest for this lvol": the moment an
    ancestor's backup fails, those two stop agreeing, and a backup computed
    the second way silently records itself as a full backup while the data
    plane only ever sent its own delta. A restore of it then pulls only its
    own objects and produces a volume missing everything the failed ancestor
    covered -- see `backup-retry-empty-issue.md`. This is the one place that
    walk happens, so every entry point -- the CLI, the API, or a schedule --
    goes through it rather than re-deriving the same answer.

    This also serializes concurrent callers that would otherwise race to
    link the same gap in the chain (`acquire_backup_chain_locks`).

    Args:
        location: where this chain's objects will be written. Resolved by the
            caller so every backup in it is guaranteed to share it.

    Raises:
        PreconditionError: the resulting chain would not be restorable (wrong
            bucket, wrong encryption, too long), or another backup of this
            same snapshot chain is already being prepared, or `snapshot`
            already has a valid backup (nothing to do).
    """
    # Everything that could make this backup unrestorable is checked here,
    # before the chain lock is taken, before any KMS key is created and before
    # any task is enqueued. A backup either is restorable or was never created.
    snap_chain = _get_snapshot_chain(snapshot)
    _require_restorable_chain(
        location, bool(lvol.crypto_bdev), snap_chain,
        # Every snapshot in the chain gets a backup below, including the ones
        # that have none yet, so the eventual length is the chain's.
        length=len(snap_chain),
        what="This snapshot chain")

    chain_snapshot_ids = [snap.get_id() for snap in snap_chain]
    acquired, existing_lock = db_controller.acquire_backup_chain_locks(
        chain_snapshot_ids, snapshot.get_id(), lvol.get_id())
    if not acquired:
        lock_snapshot = getattr(existing_lock, "requested_snapshot_id", "") or getattr(existing_lock, "snapshot_id", "")
        raise PreconditionError(
            "A backup request is already preparing this snapshot chain"
            + (f" (requested snapshot {lock_snapshot})" if lock_snapshot else ""))

    prev_backup = get_latest_backup_for_lvol(lvol.get_id())
    final_backup_id = None
    try:
        # Walk the snapshot chain and back up all unbacked ancestors first
        for snap in snap_chain:
            if _snapshot_has_backup(snap.get_id()):
                # Already backed up — update prev_backup pointer for chain linking
                backups = db_controller.get_backups_by_snapshot_id(snap.get_id())
                existing = next(
                    (b for b in backups if b.status in (
                        Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS,
                        Backup.STATUS_COMPLETED)),
                    None)
                if existing:
                    prev_backup = existing
                continue

            backup = create_single_backup(snap, lvol, node_id, cluster_id, prev_backup, location)
            time.sleep(1)
            prev_backup = backup
            if snap.get_id() == snapshot.get_id():
                final_backup_id = backup.uuid
    finally:
        db_controller.release_backup_chain_locks(chain_snapshot_ids)

    if not final_backup_id:
        # The target snapshot was already backed up
        raise PreconditionError(f"Snapshot {snapshot.get_id()} already has a backup")

    return final_backup_id


#: Prefix on the name of the snapshot `backup_lvol` creates for itself.
#: Fixed, and never influenced by a caller: a later cleanup pass that wants
#: to find and prune a direct backup's own snapshot only gets to rely on a
#: name shaped like this if nothing but `backup_lvol` ever produces one.
#: Leading underscore so it cannot collide with a name a user supplies -- see
#: `is_reserved_snapshot_name`, which every entry point taking a
#: user-supplied snapshot name checks to enforce that.
DIRECT_BACKUP_SNAPSHOT_PREFIX = "_backup_"

#: Prefix on the name of the snapshot the scheduler's auto-backups create for
#: themselves (`backup.policy._auto_backup_lvol`). Reserved for the same
#: reason as `DIRECT_BACKUP_SNAPSHOT_PREFIX`.
SCHEDULED_BACKUP_SNAPSHOT_PREFIX = "auto_"

#: Every prefix a user-supplied snapshot name must not start with.
RESERVED_SNAPSHOT_NAME_PREFIXES = (
    DIRECT_BACKUP_SNAPSHOT_PREFIX,
    SCHEDULED_BACKUP_SNAPSHOT_PREFIX,
)

#: Shared wording for every entry point's rejection of a reserved name, so
#: the message is one string to update rather than one per entry point.
RESERVED_SNAPSHOT_NAME_ERROR = (
    f"Snapshot name must not start with any of {RESERVED_SNAPSHOT_NAME_PREFIXES} "
    "(reserved for backups created by the system)"
)


def is_reserved_snapshot_name(name: str) -> bool:
    """Whether ``name`` collides with a prefix reserved for the backup
    subsystem's own snapshots.

    A predicate, not a check: an entry point that takes a snapshot name from
    a caller (CLI, v1, v2) decides what to do with a collision itself -- a
    printed error, an HTTP response, a pydantic validator -- rather than this
    function raising the same way for all of them. Never consulted inside
    `snapshot_controller.add` itself, since the subsystem's own internal
    callers (`backup_lvol`, `backup.policy._auto_backup_lvol`) go through
    that same function to create exactly these reserved names.
    """
    return name.startswith(RESERVED_SNAPSHOT_NAME_PREFIXES)


def _create_snapshot_and_back_up(lvol, snap_name: str) -> str:
    """Create ``snap_name`` on ``lvol``, then back up its whole unbacked ancestry.

    The shared core of `backup_lvol` (a direct user-requested backup) and
    `backup.policy._auto_backup_lvol` (the scheduler's auto-backups): both
    want "create a snapshot of this volume, then back it up" and nothing
    more. The cluster is always the volume's own node's cluster -- derived
    here, never taken from a caller -- so nothing can record this backup
    under a cluster other than the one the snapshot actually lives in.
    Refuses up front, via `chain_would_be_restorable`, if the chain the new
    snapshot would join could never be restorable -- before taking it, so a
    refusal never leaves an orphaned snapshot behind. The snapshot it creates
    is then handed to `ensure_snapshot_chain_backed_up`, the function that
    backs up an existing snapshot, rather than re-deriving that walk here: a
    caller that already has a snapshot calls `backup_snapshot` directly and
    this function is never on its path.

    Takes the snapshot's name rather than generating one, because the two
    callers use different, non-overridable naming schemes to stay tellable
    apart afterwards (`auto_*` vs `DIRECT_BACKUP_SNAPSHOT_PREFIX`); neither
    exposes this parameter to anything outside this package.

    Returns the id of the backup covering the new snapshot.

    Raises:
        PreconditionError: the volume's node or cluster cannot be found or
            used, or the chain would not be restorable.
        RuntimeError: the snapshot itself could not be created -- a
            data-plane failure, not a precondition, since every precondition
            on taking it was already checked above.
    """
    from simplyblock_core.controllers import snapshot_controller

    try:
        node_id = lvol.node_id
        snode = db_controller.get_storage_node_by_id(node_id)
        if snode.status != StorageNode.STATUS_ONLINE:
            raise PreconditionError(f"Node {node_id} is not online (status: {snode.status})")

        cluster_id = snode.cluster_id

        location = db_controller.get_cluster_by_id(cluster_id).get_backup_config().location()
        chain_would_be_restorable(
            lvol, location, what=f"The backup chain of volume {lvol.lvol_name}")
    except (KeyError, ValueError) as e:
        raise PreconditionError(str(e)) from e

    snap_id, error = snapshot_controller.add(lvol.get_id(), snap_name)
    if error:
        raise RuntimeError(error)

    try:
        snapshot = db_controller.get_snapshot_by_id(snap_id)
    except KeyError as e:
        raise RuntimeError(f"Snapshot {snap_id} not found right after creation: {e}") from e

    return ensure_snapshot_chain_backed_up(snapshot, lvol, node_id, cluster_id, location)


def backup_lvol(lvol_id) -> str:
    """Create a backup directly from a volume, without an existing snapshot.

    The snapshot taken to do this is named here, under
    `DIRECT_BACKUP_SNAPSHOT_PREFIX`, and a caller has no way to influence that
    name -- unlike a snapshot a user takes directly, this one exists purely
    to serve this backup, and a future cleanup pass needs to be able to find
    and prune it by that marker alone. The cluster it backs up into is
    likewise never a caller's choice: it is always the volume's own node's
    cluster (see `_create_snapshot_and_back_up`).

    Returns the id of the backup covering the new snapshot.

    Raises:
        PreconditionError: the volume, its node or its cluster cannot be
            found or used, or the chain would not be restorable.
        RuntimeError: the snapshot itself could not be created.
    """
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        raise PreconditionError(str(e)) from e

    snap_name = f"{DIRECT_BACKUP_SNAPSHOT_PREFIX}{lvol.lvol_name}_{int(time.time())}"
    return _create_snapshot_and_back_up(lvol, snap_name)


def backup_snapshot(snapshot_id, cluster_id=None):
    """Create a backup from an existing snapshot.

    Walks the snapshot chain to ensure all ancestor snapshots are also
    backed up, since a single snapshot backup is only a delta and cannot
    be restored without its ancestors. See `ensure_snapshot_chain_backed_up`
    for why that walk has to happen from the snapshot's own ancestry rather
    than from `Backup` records.

    Returns (backup_id, error_message) where backup_id is the ID of the
    backup for the requested snapshot.
    """
    try:
        snapshot = db_controller.get_snapshot_by_id(snapshot_id)
    except KeyError as e:
        return None, str(e)

    lvol = snapshot.lvol
    node_id = lvol.node_id
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError as e:
        return None, str(e)

    if snode.status != StorageNode.STATUS_ONLINE:
        return None, f"Node {node_id} is not online (status: {snode.status})"

    if not cluster_id:
        cluster_id = snode.cluster_id

    try:
        location = db_controller.get_cluster_by_id(cluster_id).get_backup_config().location()
        backup_id = ensure_snapshot_chain_backed_up(
            snapshot, lvol, node_id, cluster_id, location)
    except (KeyError, ValueError, PreconditionError) as e:
        return None, str(e)

    return backup_id, None


def restore_backup(backup_id: str, lvol_name: str, pool_id_or_name: str,
                   target_node_id: str | None = None,
                   s3_credentials: S3Credentials | None = None,
                   consistency_group: str | None = None):
    """Restore a backup chain into a new fully-accessible lvol.

    Creates the volume (with subsystem, listeners, namespace) via
    lvol_controller.add_lvol_ha, then schedules an async task to
    fill in the data from S3.  The volume is in STATUS_RESTORING
    until the data transfer completes.

    The volume is created with the subsystem sharing its source had
    (``Backup.max_namespace_per_subsys``): a member of a shared subsystem comes
    back namespaced under the same cap, a volume that owned its subsystem comes
    back owning one. A record from before the field was kept takes
    add_lvol_ha's default.

    Args:
        consistency_group: Name of the consistency group the restored volume
            joins AT CREATION, which is what lets a namespaced member take the
            group's own subsystem (cg_colocation) rather than any subsystem of
            the pool with a free slot. A group restore passes the group it is
            forming; a single-volume restore leaves it unset.
        s3_credentials: Credentials for the backup's bucket, when that is not
            the cluster's own. Omit to use the nodes' instance role.
        target_node_id: Optional node to restore onto. If not provided, a node
            of the target cluster is auto-selected. Any node in the cluster
            can restore any backup because S3 keys are node-agnostic
            ({s3_id}/{mid_flag}/{extent}) and all nodes share the same
            S3 bucket and credentials.

    Returns the uuid of the created volume.
    """
    from simplyblock_core.controllers import lvol_controller
    from simplyblock_core.models.lvol_model import LVol

    try:
        backup = db_controller.get_backup_by_id(backup_id)
        pool = db_controller.get_pool_by_id_or_name(pool_id_or_name)
        cluster = db_controller.get_cluster_by_id(pool.cluster_id)
        target_node = db_controller.get_storage_node_by_id(target_node_id) if target_node_id is not None else None
        chain = BackupChain.of_backups(UUID(backup_id), db_controller.get_backups())
    except (KeyError, ValueError) as e:
        raise PreconditionError(str(e)) from e

    # The chain has to be restorable as a unit, finished, and short enough for
    # the data plane. Checked before the volume is created so a doomed restore
    # leaves nothing behind.
    chain.require_restorable(completed=True,
                             what=f"The chain ending at backup {backup_id}")

    size = backup.size
    if size <= 0:
        raise PreconditionError("Backup has no size information")

    if target_node is not None:
        if target_node.cluster_id != cluster.uuid:
            raise PreconditionError(
                f"Target node {target_node_id} belongs to cluster "
                f"{target_node.cluster_id[:8]}, not {cluster.uuid[:8]}")

        if target_node.status != StorageNode.STATUS_ONLINE:
            raise PreconditionError(f"Target node {target_node_id} is not online "
                                    f"(status: {target_node.status})")

        if not target_node.lvstore:
            raise PreconditionError(
                f"Target node {target_node_id} has no lvstore (S3 bdev requires lvstore)")

    crypto_key = _resolve_crypto_key(backup, cluster)
    # Resolved before the volume exists, so an unreachable bucket is refused
    # here rather than after a volume has been created for a restore that
    # cannot run.
    s3_config = foreign_bucket_config(backup, cluster, s3_credentials)

    # 0 is a record that predates the field: leave both unset, and add_lvol_ha
    # gives the volume a subsystem of its own as it always did.
    ns_cap = backup.max_namespace_per_subsys or 0
    lvol_id, error = lvol_controller.add_lvol_ha(
        name=lvol_name,
        size=size,
        pool_id_or_name=pool_id_or_name,
        use_crypto=backup.encrypted,
        max_size=0,
        max_rw_iops=0,
        max_rw_mbytes=0,
        max_r_mbytes=0,
        max_w_mbytes=0,
        host_id_or_name=target_node_id,
        ha_type="default",
        crypto_key=crypto_key,
        use_comp=False,
        distr_vuid=0,
        lvol_priority_class=0,
        fabric="tcp",
        namespaced=ns_cap > 1,
        max_namespace_per_subsys=ns_cap if ns_cap > 0 else None,
        consistency_group=consistency_group,
    )
    if error or not lvol_id:
        raise RuntimeError(f"Failed to create restore volume: {error}")

    # Mark volume as restoring
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        raise RuntimeError(f"Volume created but not found in DB: {lvol_id}") from e

    lvol.status = LVol.STATUS_RESTORING
    lvol.write_to_db()

    # The bdev name the data plane expects (e.g. LVS_7744/LVOL_12345)
    bdev_name = f"{lvol.lvs_name}/{lvol.lvol_bdev}"

    if not tasks_controller.add_backup_restore_task(
            pool.cluster_id, lvol.node_id, backup_id, bdev_name,
            chain.s3_ids_newest_first(), lvol_id=lvol_id,
            s3_config=s3_config.model_dump(exclude_none=True) if s3_config is not None else None):
        raise RuntimeError("Failed to create restore task")

    return lvol_id


def _cleanup_backup_kms_keys(backups):
    encrypted = [b for b in backups if b.encrypted]
    if not encrypted:
        return
    try:
        cluster = db_controller.get_cluster_by_id(encrypted[0].cluster_id)
        with create_kms_connection(cluster) as kms:
            for b in encrypted:
                try:
                    kms.delete_data_encryption_keys(backup_dek_path(b.cluster_id, b.uuid))
                    kms.delete_key_encryption_key(backup_kek_name(b.uuid))
                except KMSException:
                    logger.exception(f"Failed to delete keys for backup {b.uuid}")
    except (KMSException, KeyError):
        logger.exception("Failed to clean up backup KMS keys")


def delete_backups(lvol_id):
    """Delete all backups for a given lvol.

    Removes the database records, not the objects: bdev_lvol_s3_delete does not
    exist on the data plane, so the S3 data outlives this call. The manifests
    are deliberately left in place too -- they are the only thing that can still
    identify those objects, and deleting them would turn a reclaimable orphan
    set into anonymous bucket weight. `backup discover` therefore keeps showing
    them, which is the honest answer about what the bucket contains.

    Returns (success, error_message).
    """
    backups = db_controller.get_backups_by_lvol_id(lvol_id)
    if not backups:
        return False, f"No backups found for lvol {lvol_id}"

    _cleanup_backup_kms_keys(backups)

    # Find node to run delete RPC on
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if not completed:
        # Just remove from DB
        for b in backups:
            b.remove(db_controller.kv_store)
        return True, None

    node_id = completed[0].node_id
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        # Node gone, just clean up DB
        for b in backups:
            b.remove(db_controller.kv_store)
        return True, None

    # Call S3 delete RPC (dummy for now)
    if snode.status == StorageNode.STATUS_ONLINE:
        rpc_client = snode.rpc_client()
        s3_ids = [b.s3_id for b in completed]
        try:
            rpc_client.bdev_lvol_s3_delete(s3_ids)
        except Exception as e:
            logger.error(f"Error deleting S3 backups: {e}")

    cluster_id = completed[0].cluster_id
    for b in backups:
        backup_events.backup_deleted(cluster_id, node_id, b)
        b.remove(db_controller.kv_store)

    return True, None


def delete_backup(backup_id):
    """Delete ONE backup: its record, its KMS keys and, best effort, its S3
    objects -- the single-record form of :func:`delete_backups`, for a backup
    that nothing chains against any more (a pruned group generation's leaf
    copy, design-consistency-group-backup.md §9).

    Raises:
        KeyError: no backup has this id.
        PreconditionError: another backup names this one as its predecessor,
            so deleting it would make every descendant unrestorable -- merge it
            into its successor instead (``trigger_merge``), after which it is
            MERGED, not deleted, and its objects are the survivor's; or it is a
            member of a group backup that still exists, which deleting it would
            tear.
    """
    backup = db_controller.get_backup_by_id(backup_id)
    dependents = [b.uuid for b in db_controller.get_backups_by_lvol_id(backup.lvol_id)
                  if b.prev_backup_id == backup.uuid]
    if dependents:
        raise PreconditionError(
            f"backup {backup.uuid} is the predecessor of "
            f"{', '.join(d[:8] for d in dependents)}; merge it into its "
            "successor instead of deleting it")
    if backup.group_backup_id:
        try:
            db_controller.get_group_backup_by_id(backup.group_backup_id)
        except KeyError:
            pass        # its group backup is gone: an orphaned member copy
        else:
            raise PreconditionError(
                f"backup {backup.uuid} is a member of group backup "
                f"{backup.group_backup_id}; delete the group backup instead")

    _cleanup_backup_kms_keys([backup])
    if backup.status == Backup.STATUS_COMPLETED:
        try:
            snode = db_controller.get_storage_node_by_id(backup.node_id)
        except KeyError:
            snode = None
        if snode is not None and snode.status == StorageNode.STATUS_ONLINE:
            try:
                snode.rpc_client().bdev_lvol_s3_delete([backup.s3_id])
            except RPCException as e:
                # Best effort, as delete_backups: the data plane does not
                # implement the S3 delete yet, and the record goes regardless.
                logger.error(f"Error deleting S3 objects of backup {backup.uuid}: {e}")
    backup_events.backup_deleted(backup.cluster_id, backup.node_id, backup)
    backup.remove(db_controller.kv_store)


def list_backups(cluster_id=None, show_all=False):
    """List all backups, optionally filtered by cluster.
    
    Merged backups are hidden unless `show_all` is set -- a merge folds a
    backup's data into its successor, so the record stays for chain history
    but is no longer something an operator restores from.
    """
    backups = db_controller.get_backups(cluster_id)
    if not show_all:
        backups = [b for b in backups if b.status != Backup.STATUS_MERGED]
    backups = sorted(backups, key=lambda b: (b.created_at, b.uuid), reverse=True)
    data = []
    for b in backups:
        entry = {
            "ID": b.uuid,
            "S3 ID": b.s3_id,
            "LVol": b.lvol_name,
            "Snapshot": b.snapshot_name,
            "Node": b.node_id[:8] if b.node_id else "",
            "Status": b.status,
            "Prev": b.prev_backup_id[:8] if b.prev_backup_id else "-",
            "Created": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(b.created_at)) if b.created_at else "",
        }
        data.append(entry)
    return data


def export_backups(cluster_id=None, lvol_name=None,
                   backup_id=None) -> backup_manifest.BackupExport:
    """Export completed backups, for import into another cluster.

    The manifests are the same shape that lives in the bucket, so a
    hand-carried file and a bucket read describe a backup identically.
    Previously this produced a third, narrower format of its own -- which is how
    it came to omit `encrypted`.

    Args:
        backup_id: Export the chain ending at this backup, and nothing else --
            which is the unit an import wants, and the unit a restore needs. This
            used to be answered with "every backup of the same volume", which is
            both wider (a volume can hold several chains) and narrower (a chain
            can outlive the volume's name).
        lvol_name: Export every completed backup of a volume, chains and all.

    Returns the manifests grouped by the location they live in, which the
    records carry and the manifests deliberately do not. A cluster can hold
    backups in more than one bucket -- its own, plus any it has imported -- so
    the broader selections above can span several, while a chain never does.

    Raises:
        PreconditionError: `backup_id` names a backup whose chain is broken or
            not there.
        ValueError: One of the selected backups records no usable location, and
            so cannot be described.
    """
    if backup_id is not None:
        try:
            chain = BackupChain.of_backups(
                UUID(str(backup_id)), db_controller.get_backups(cluster_id))
        except ValueError as e:
            raise PreconditionError(str(e)) from e
        return _group_by_location(chain.records())

    backups = db_controller.get_backups(cluster_id)
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if lvol_name:
        completed = [b for b in completed if b.lvol_name == lvol_name]

    return _group_by_location(completed)


def _group_by_location(backups: Iterable[Backup]) -> backup_manifest.BackupExport:
    """Collect records into one group per location they live in.

    Keyed by the serialized location rather than the model so that two records
    naming the same bucket in the same way land together whatever order their
    fields were written in.
    """
    groups: dict[str, tuple[BackupLocation, list[backup_manifest.BackupManifest]]] = {}
    for backup in backups:
        location = backup.get_location()
        key = location.model_dump_json()
        groups.setdefault(key, (location, []))[1].append(build_manifest(backup))

    return backup_manifest.BackupExport(groups=[
        backup_manifest.LocatedManifests(location=location, manifests=manifests)
        for location, manifests in (
            groups[key] for key in sorted(groups))
    ])


def discover_backups(config: BackupConfig) -> list[backup_manifest.BackupManifest]:
    """Every backup in a bucket, read from its manifests alone.

    The disaster-recovery entry point: given a bucket and credentials for it,
    this answers "what is in here" with no reference to any cluster, live or
    dead.

    Raises:
        ManifestError: The bucket could not be listed, or one of its manifests
            could not be parsed.
    """
    return backup_manifest.list_all(config)


def import_backups(export: backup_manifest.BackupExport, cluster_id=None) -> int:
    """Register backups described by manifests into this cluster's database.

    The backups keep their original ids -- both their uuid and their s3_id,
    which names their objects in the bucket and therefore cannot be reassigned.

    Args:
        export: validated manifests grouped by the location they live in, from
            `discover_backups`, `export_backups`, or a file parsed into one.
            Taking the models rather than dicts means "is this a manifest at all"
            is answered by whoever read the bytes -- the API by its request
            body's type, the CLI when it parses the file -- and reported where
            the input came from.

            Grouped because a manifest does not say where its objects are, and
            one import can carry backups from several buckets: a cluster that has
            imported from elsewhere holds records for more than one, and
            exporting it emits all of them. Applying a single location to the
            whole batch would leave every backup outside that bucket pointing at
            the wrong one, restorable-looking and unrestorable.
        cluster_id: Target cluster to import into, so the backups are visible in
            its namespace.

    Raises:
        PreconditionError: One of the backup IDs is already known -- backup
            lookups are not scoped by cluster, so a UUID reused across clusters
            would make either record unaddressable. Everything is checked before
            the first record is written, so a bad batch imports nothing rather
            than half of itself. That holds across the whole document, not one
            group at a time: a failure in the last group leaves the first
            unimported.
        ValueError: The same backup is listed twice, in one group or across two.
    """
    pending: dict[UUID, backup_manifest.BackupManifest] = {}
    located: dict[UUID, BackupLocation] = {}
    for group in export.groups:
        for manifest in group.manifests:
            backup_id = manifest.backup_id

            if backup_id in pending:
                raise ValueError(f"Backup {backup_id} is listed more than once")

            try:
                existing = db_controller.get_backup_by_id(str(backup_id))
            except KeyError:
                pending[backup_id] = manifest
                located[backup_id] = group.location
            else:
                raise PreconditionError(f"Backup {backup_id} already exists in cluster {existing.cluster_id}")

    # Each manifest's own chain has to be restorable once landed. An import that
    # lands a backup whose ancestors are missing produces a record that looks
    # restorable in `backup list` and fails only when someone tries it --
    # typically during the recovery it was meant to serve. A chain may reach back
    # out of the batch into records already stored, so both are given to it.
    stored = db_controller.get_backups()
    for manifest in pending.values():
        BackupChain.importing(
            manifest, pending, stored,
            located[manifest.backup_id]).require_restorable(
                what=f"The chain of backup {manifest.backup_id}")

    # Back to strings on the way into the record: `Backup` is a hand-rolled
    # model whose fields are plain `str`, and `db_controller` looks its ids up
    # by `==` against them.
    for backup_id, manifest in pending.items():
        backup = Backup()
        backup.uuid = str(backup_id)
        backup.s3_id = manifest.s3_id
        backup.cluster_id = cluster_id or str(manifest.source.cluster_id)
        backup.lvol_id = str(manifest.volume.lvol_id)
        backup.lvol_name = manifest.volume.lvol_name
        backup.snapshot_id = str(manifest.volume.snapshot_id)
        backup.snapshot_name = manifest.volume.snapshot_name
        backup.node_id = str(manifest.source.node_id)
        backup.prev_backup_id = str(manifest.prev_backup_id) if manifest.prev_backup_id else ""
        backup.size = manifest.size
        backup.max_namespace_per_subsys = manifest.volume.max_namespace_per_subsys or 0
        backup.created_at = manifest.created_at
        backup.completed_at = manifest.completed_at
        backup.status = Backup.STATUS_COMPLETED
        backup.location = location_of(manifest, located[backup_id]).model_dump(mode="json")
        # Import used to drop this, so an imported encrypted backup restored as
        # use_crypto=False -- a plaintext volume over ciphertext, silently.
        backup.encrypted = manifest.encryption is not None
        backup.encryption = (
            manifest.encryption.model_dump(mode="json")
            if manifest.encryption is not None else {})
        backup.write_to_db()

    return len(pending)


def import_from_bucket(config: BackupConfig, cluster_id=None) -> int:
    """Import every backup found in a bucket.

    Raises:
        ManifestError: the bucket could not be read.
        PreconditionError: the manifests it holds cannot be imported as a batch.
    """
    return import_backups(
        backup_manifest.BackupExport(groups=[
            backup_manifest.LocatedManifests(
                location=config.location(),
                manifests=discover_backups(config)),
        ]),
        cluster_id=cluster_id)


def trigger_merge(keep_backup, old_backup):
    """Merge ``old_backup`` into ``keep_backup``, its successor in one volume's
    chain: mark the old one merging and queue the merge task.

    Both must be completed; otherwise nothing happens and a later pass retries.
    Used by the single-volume retention (policy) and by the group backup's
    retirement of a pruned generation's members (group_backup).
    """
    if old_backup.status != Backup.STATUS_COMPLETED:
        return
    if keep_backup.status != Backup.STATUS_COMPLETED:
        return

    old_backup.status = Backup.STATUS_MERGING
    old_backup.write_to_db()

    tasks_controller.add_backup_merge_task(
        keep_backup.cluster_id,
        keep_backup.node_id,
        keep_backup.uuid,
        old_backup.uuid)
