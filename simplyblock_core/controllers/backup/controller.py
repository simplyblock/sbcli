# coding=utf-8
"""Creating, restoring, importing, exporting and discovering backups."""
import logging
import time
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import UUID, uuid4

from pydantic import ValidationError

from simplyblock_core import constants
from simplyblock_core.controllers import backup_events, tasks_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.validation import (
    chain_fits, require_restorable)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import (
    BackupConfig, BackupLocation, S3Credentials)
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.kms import (
    KMSException, backup_dek_path, backup_kek_name, create_kms_connection,
    lvol_dek_path, pool_kek_name,
)
from simplyblock_core.exceptions import PreconditionError

logger = logging.getLogger()

db_controller = DBController()


def _generate_backup_id():
    return str(uuid4())


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
    # and every value above comes off an untyped record -- an ha_type or an
    # allowed-host entry the model does not recognise has to be caught here,
    # while the backup is still being written, not by whoever reads the manifest
    # during a recovery.
    volume = backup_manifest.Volume.model_validate({
        "lvol_id": backup.lvol_id,
        "lvol_name": backup.lvol_name,
        "snapshot_id": backup.snapshot_id,
        "snapshot_name": backup.snapshot_name,
        "size": backup.size,
        "allowed_hosts": backup.allowed_hosts or [],
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
        cluster_size = cluster.page_size_in_blocks * constants.LVOL_CLUSTER_RATIO

    return backup_manifest.BackupManifest(
        backup_id=UUID(backup.uuid),
        s3_id=backup.s3_id,
        created_at=backup.created_at,
        completed_at=backup.completed_at,
        size=backup.size,
        prev_backup_id=UUID(backup.prev_backup_id) if backup.prev_backup_id else None,
        encryption=(backup_manifest.parse_key_descriptor(backup.encryption)
                    if backup.encryption else None),
        source=backup_manifest.Source(
            cluster_id=UUID(backup.cluster_id),
            cluster_name=cluster_name,
            node_id=UUID(backup.node_id),
        ),
        volume=volume,
        dataplane=backup_manifest.DataPlane(
            cluster_size=cluster_size,
            with_compression=location.with_compression,
        ),
    )


def _credentials_for_foreign_location(backup: Backup, location: BackupLocation,
                                      own: BackupConfig) -> Optional[S3Credentials]:
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
                          credentials: Optional[S3Credentials]) -> Optional[BackupConfig]:
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


def _get_snapshot_chain(snapshot):
    """Build the snapshot chain ending at this snapshot, oldest first.

    For cloned volumes, walks snap_ref_id upward.  For regular volumes
    (no snap_ref_id), collects all snapshots of the same lvol that were
    created at or before this snapshot, ordered by created_at.
    """
    if snapshot.snap_ref_id:
        # Clone-based chain: walk snap_ref_id
        chain = [snapshot]
        current = snapshot
        while current.snap_ref_id:
            try:
                parent = db_controller.get_snapshot_by_id(current.snap_ref_id)
                chain.append(parent)
                current = parent
            except KeyError:
                break
        chain.reverse()  # oldest first
        return chain

    # Regular volume: all snapshots of the same lvol up to this one
    lvol_id = snapshot.lvol.get_id() if snapshot.lvol else None
    if not lvol_id:
        return [snapshot]

    all_snaps = db_controller.get_snapshots_by_lvol_id(lvol_id)
    # Filter to snapshots created at or before this one, sort oldest first
    chain = [s for s in all_snaps if s.created_at <= snapshot.created_at]
    chain.sort(key=lambda s: s.created_at)
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
    # NQNs only. The volume's entries also carry that host's DHCHAP keys and
    # PSK; copying them here would duplicate live authentication material into a
    # second record, and from there into every manifest, for no reader -- restore
    # uses the NQNs and mints fresh keys from the target pool.
    backup.allowed_hosts = [{"nqn": host["nqn"]} for host in (lvol.allowed_hosts or [])]
    backup.created_at = int(time.time())
    backup.status = Backup.STATUS_PENDING
    backup.encrypted = bool(lvol.crypto_bdev)

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


def backup_snapshot(snapshot_id, cluster_id=None):
    """Create a backup from an existing snapshot.

    Walks the snapshot chain to ensure all ancestor snapshots are also
    backed up, since a single snapshot backup is only a delta and cannot
    be restored without its ancestors.

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

    snap_chain = _get_snapshot_chain(snapshot)

    # Everything that could make this backup unrestorable is checked here,
    # before the chain lock is taken, before any KMS key is created and before
    # any task is enqueued. A backup either is restorable or was never created.
    try:
        location = db_controller.get_cluster_by_id(cluster_id).get_backup_config().location()
        require_restorable(
            location,
            backups=_existing_chain_backups(snap_chain),
            # Every snapshot in the chain gets a backup below, including the ones
            # that have none yet, so the eventual length is the chain's.
            chain_length=len(snap_chain),
            encrypted=bool(lvol.crypto_bdev),
            what="This snapshot chain")
    except (KeyError, ValueError, PreconditionError) as e:
        return None, str(e)

    chain_snapshot_ids = [snap.get_id() for snap in snap_chain]
    acquired, existing_lock = db_controller.acquire_backup_chain_locks(
        chain_snapshot_ids, snapshot_id, lvol.get_id())
    if not acquired:
        lock_snapshot = getattr(existing_lock, "requested_snapshot_id", "") or getattr(existing_lock, "snapshot_id", "")
        return None, (
            "A backup request is already preparing this snapshot chain"
            + (f" (requested snapshot {lock_snapshot})" if lock_snapshot else "")
        )

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
            if snap.get_id() == snapshot_id:
                final_backup_id = backup.uuid
    finally:
        db_controller.release_backup_chain_locks(chain_snapshot_ids)

    if not final_backup_id:
        # The target snapshot was already backed up
        return None, f"Snapshot {snapshot_id} already has a backup"

    return final_backup_id, None


def restore_backup(backup_id: str, lvol_name: str, pool_id_or_name: str,
                   target_node_id: Optional[str] = None,
                   s3_credentials: Optional[S3Credentials] = None):
    """Restore a backup chain into a new fully-accessible lvol.

    Creates the volume (with subsystem, listeners, namespace) via
    lvol_controller.add_lvol_ha, then schedules an async task to
    fill in the data from S3.  The volume is in STATUS_RESTORING
    until the data transfer completes.

    Args:
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
        chain = db_controller.get_backup_chain(backup_id)
        if (incomplete := [
            backup for backup in chain
            if backup.status != Backup.STATUS_COMPLETED
        ]):
            raise PreconditionError("Incomplete backups in chain: " + ", ".join(backup.uuid for backup in incomplete))
    except KeyError as e:
        raise PreconditionError(str(e)) from e

    # The chain has to be restorable as a unit, and short enough for the data
    # plane. Checked before the volume is created so a doomed restore leaves
    # nothing behind.
    require_restorable(backup.get_location(), backups=chain,
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

    logger.info(f"Backup allowed hosts: {backup.allowed_hosts}")
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
        allowed_hosts=[h["nqn"] if isinstance(h, dict) else h
                       for h in (backup.allowed_hosts or [])] or None,
        fabric="tcp",
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

    # Data plane processes s3_ids in array order: the first entry's clusters
    # take priority (skip-if-populated).  Newest-first means the latest
    # incremental data wins, with older backups filling any remaining gaps.
    if not tasks_controller.add_backup_restore_task(
            pool.cluster_id, lvol.node_id, backup_id, bdev_name,
            [b.s3_id for b in reversed(chain)], lvol_id=lvol_id,
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


def list_backups(cluster_id=None):
    """List all backups, optionally filtered by cluster."""
    backups = db_controller.get_backups(cluster_id)
    backups = sorted(backups, key=lambda b: (b.created_at, b.uuid), reverse=True)
    data = []
    for b in backups:
        logger.debug(b)
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


def export_backups(cluster_id=None, lvol_name=None) -> List[backup_manifest.BackupManifest]:
    """Export completed backups as manifests, for import into another cluster.

    Emits the same shape that lives in the bucket, so a hand-carried file and a
    bucket read are interchangeable. Previously this produced a third, narrower
    format of its own -- which is how it came to omit `encrypted`.

    Returns the manifests themselves; whoever is writing them out decides how
    they are rendered.
    """
    backups = db_controller.get_backups(cluster_id)
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if lvol_name:
        completed = [b for b in completed if b.lvol_name == lvol_name]

    return [build_manifest(b) for b in completed]


def discover_backups(config: BackupConfig) -> List[backup_manifest.BackupManifest]:
    """Every backup in a bucket, read from its manifests alone.

    The disaster-recovery entry point: given a bucket and credentials for it,
    this answers "what is in here" with no reference to any cluster, live or
    dead.

    Raises:
        ManifestError: The bucket could not be listed, or one of its manifests
            could not be parsed.
    """
    return backup_manifest.list_all(config)


def _require_importable(pending: Dict[UUID, backup_manifest.BackupManifest],
                        location: BackupLocation) -> None:
    """Refuse a batch of manifests that would not form something restorable.

    An import that lands a backup whose ancestors are missing produces a record
    that looks restorable in ``backup list`` and fails only when someone tries it
    -- typically during the recovery it was meant to serve. Each manifest names
    its predecessor, so this is answerable up front by walking the batch.

    Separate from ``require_restorable`` because the rule is about the batch, not
    about a chain: whether every ancestor is either in this import or already in
    the database is a question only the importer can ask. The rules that ARE about
    the chain are deferred to it.

    Args:
        location: Where this batch is being imported from, which is where every
            backup in it will be recorded as living. Only ancestors already in
            the database can contradict it -- the batch itself comes from one
            bucket by construction.

    Raises:
        PreconditionError: A chain is incomplete, cyclic, too long for the data
            plane, or spans buckets.
    """
    for backup_id, manifest in pending.items():
        # A chain reaching back before this batch spans both shapes of the same
        # thing -- manifests being imported and Backup records already stored,
        # which name their id and their location differently. Reduced to the two
        # facts the rules below need, the two become comparable.
        chain: List[Tuple[UUID, BackupLocation]] = [
            (backup_id, _location_of(manifest, location))]
        seen = {backup_id}
        current = manifest

        while (previous := current.prev_backup_id) is not None:
            if previous in seen:
                raise PreconditionError(
                    f"Backup {backup_id} has a cyclic chain at {previous}")
            seen.add(previous)

            if previous in pending:
                current = pending[previous]
                chain.append((previous, _location_of(current, location)))
                continue

            if not _backup_exists(previous):
                raise PreconditionError(
                    f"Backup {backup_id} is a delta against {previous}, which is "
                    "neither in this import nor already known. A backup cannot "
                    "be restored without its chain.")

            # Already imported, and checked then. Its own ancestry still counts
            # towards the length the data plane has to accept.
            chain.extend((UUID(stored.uuid), stored.get_location())
                         for stored in db_controller.get_backup_chain(str(previous)))
            break

        # Locations are values, so coherence over the chain is a plain
        # comparison; require_restorable wants Backup records, and the manifests
        # in this batch are not in the database yet.
        own = _location_of(manifest, location)
        divergent = next(
            (other for other, other_location in chain if other_location != own),
            None)
        if divergent is not None:
            raise PreconditionError(
                f"Backup {backup_id} shares a chain with {divergent}, "
                "which is in a different bucket or encoding")

        if not chain_fits(len(chain)):
            raise PreconditionError(
                f"The chain of backup {backup_id} is {len(chain)} backups long; "
                f"the data plane accepts at most "
                f"{constants.BACKUP_MAX_CHAIN_LENGTH}.")


def _location_of(manifest: backup_manifest.BackupManifest,
                 location: BackupLocation) -> BackupLocation:
    """Where an imported backup lives: the bucket it was found in, its own encoding.

    A manifest describes its objects but not how to reach them, so the bucket,
    region and endpoint come from whoever read it -- which is what makes a
    replicated bucket importable at all, rather than importable and then
    unrestorable because every record points back at the original.

    Only the encoding is the manifest's to state, and only ``with_compression``
    is still variable; the key layout it also records has one value that holds
    backups, which ``location_holds_backups`` already requires of the bucket.

    Narrowed rather than copied, so a ``BackupConfig`` passed in as the location
    it also is cannot carry its credentials into a stored record.
    """
    return BackupLocation.model_validate({
        **location.model_dump(include=set(BackupLocation.model_fields)),
        "with_compression": manifest.dataplane.with_compression,
    })


def _backup_exists(backup_id: UUID) -> bool:
    try:
        db_controller.get_backup_by_id(str(backup_id))
    except KeyError:
        return False
    return True


def import_backups(manifests: Iterable[backup_manifest.BackupManifest],
                   location: BackupLocation, cluster_id=None) -> int:
    """Register backups described by manifests into this cluster's database.

    The backups keep their original ids -- both their uuid and their s3_id,
    which names their objects in the bucket and therefore cannot be reassigned.

    Args:
        manifests: validated manifests, from `discover_backups`,
            `export_backups`, or a file parsed into them. Taking the models
            rather than dicts means "is this a manifest at all" is answered by
            whoever read the bytes -- the API by its request body's type, the CLI
            when it parses the file -- and reported where the input came from.
        location: The bucket these manifests describe backups in. Required
            because a manifest does not say: it is read out of a bucket the
            reader already named, and an export file is just those manifests in
            another envelope, so the bucket has to be named again there. That is
            what lets a replicated bucket be imported as itself rather than as
            the original it was copied from.
        cluster_id: Target cluster to import into, so the backups are visible in
            its namespace.

    Raises:
        PreconditionError: One of the backup IDs is already known -- backup
            lookups are not scoped by cluster, so a UUID reused across clusters
            would make either record unaddressable. Everything is checked before
            the first record is written, so a bad batch imports nothing rather
            than half of itself.
        ValueError: The same backup is listed twice.
    """
    pending: Dict[UUID, backup_manifest.BackupManifest] = {}
    for manifest in manifests:
        backup_id = manifest.backup_id

        if backup_id in pending:
            raise ValueError(f"Backup {backup_id} is listed more than once")

        try:
            existing = db_controller.get_backup_by_id(str(backup_id))
        except KeyError:
            pending[backup_id] = manifest
        else:
            raise PreconditionError(f"Backup {backup_id} already exists in cluster {existing.cluster_id}")

    _require_importable(pending, location)

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
        backup.allowed_hosts = [{"nqn": nqn} for nqn in manifest.volume.allowed_hosts]
        backup.created_at = manifest.created_at
        backup.completed_at = manifest.completed_at
        backup.status = Backup.STATUS_COMPLETED
        backup.location = _location_of(manifest, location).model_dump(mode="json")
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
        discover_backups(config), config.location(), cluster_id=cluster_id)
