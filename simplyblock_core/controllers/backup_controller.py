# coding=utf-8
import logging
import re
import time
import uuid
from typing import Optional

from botocore.exceptions import BotoCoreError, ClientError
from pydantic import SecretStr

from simplyblock_core import backup_key_wrapping, backup_manifest, constants
from simplyblock_core.controllers import backup_events, tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup, BackupPolicy, BackupPolicyAttachment
from simplyblock_core.models.backup_config import BackupConfig, BackupLocation
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.kms import (
    KMSException, backup_dek_path, backup_kek_name, create_kms_connection,
    lvol_dek_path, pool_kek_name,
)
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.rpc_client import RPCException

logger = logging.getLogger()

db_controller = DBController()


def _generate_backup_id():
    return str(uuid.uuid4())


def _parse_age_string(age_str):
    """Parse age strings like '2d', '12h', '1w', '30m' into seconds."""
    match = re.match(r'^(\d+)([mhdw])$', age_str.strip())
    if not match:
        raise ValueError(f"Invalid age format: {age_str}. Use <number><m|h|d|w> e.g. 2d, 12h, 1w")
    value = int(match.group(1))
    unit = match.group(2)
    multipliers = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800}
    return value * multipliers[unit]


def _parse_schedule(schedule_str):
    """Parse schedule string like '15m,4 60m,11 24h,7' into list of (interval_seconds, keep_count) tuples.
    Returns sorted list by interval ascending. Raises ValueError on invalid input."""
    if not schedule_str or not schedule_str.strip():
        return []
    tiers = []
    for part in schedule_str.strip().split():
        parts = part.split(',')
        if len(parts) != 2:
            raise ValueError(f"Invalid schedule tier: {part}. Expected format: <interval>,<count> e.g. 15m,4")
        interval_seconds = _parse_age_string(parts[0])
        try:
            keep_count = int(parts[1])
        except ValueError:
            raise ValueError(f"Invalid keep count in tier: {part}. Must be an integer.")
        if keep_count < 1:
            raise ValueError(f"Keep count must be >= 1 in tier: {part}")
        tiers.append((interval_seconds, keep_count))
    tiers.sort(key=lambda t: t[0])
    # Validate intervals are strictly increasing
    for i in range(1, len(tiers)):
        if tiers[i][0] <= tiers[i - 1][0]:
            raise ValueError("Schedule tier intervals must be strictly increasing")
    return tiers


def _get_latest_backup_for_lvol(lvol_id):
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


def _compute_s3_cpu_masks(node):
    """Compute CPU masks for the S3 bdev.
    Returns (bdb_lcpu_mask, s3_lcpu_mask):
        bdb_lcpu_mask: app_thread core (SPDK lightweight thread, low overhead)
        s3_lcpu_mask: all system vCPUs (no pinning — let Linux scheduler handle
                      the AWS SDK thread pool; the data plane default would
                      wrongly pin onto SPDK reactor cores)
    """
    # SPDK thread for the bdev poller — reuse the app thread core
    bdb_lcpu_mask = 0
    if node.app_thread_mask:
        bdb_lcpu_mask = int(node.app_thread_mask, 16)

    # AWS SDK thread pool — set all system vCPU bits so threads are unconstrained
    s3_lcpu_mask = (1 << node.cpu) - 1 if node.cpu > 0 else 0

    return bdb_lcpu_mask, s3_lcpu_mask


def _validate_chain_length(length: int, what: str) -> None:
    """Refuse a chain the data plane cannot accept.

    Raises:
        PreconditionError: The chain is longer than the data plane's fixed
            arrays, where it would smash the storage node's stack rather than
            return an error.
    """
    if length > constants.BACKUP_MAX_CHAIN_LENGTH:
        raise PreconditionError(
            f"{what} is {length} backups long; the data plane accepts at most "
            f"{constants.BACKUP_MAX_CHAIN_LENGTH}. Merge older backups to "
            "shorten the chain, or start a new chain with a full backup.")


def _validate_backup_target(config: BackupConfig) -> None:
    """Check that the configured location can hold backups at all.

    Raises:
        PreconditionError: The location selects the secondary-tiering object
            layout, whose keys are ``{tiering_id}/{lpgi}``. Backups written
            there would be unreadable by the restore path, which addresses
            ``{s3_id}/{mid}/{extent}``.
    """
    if not config.snapshot_backups:
        raise PreconditionError(
            f"Bucket {config.bucket_name} is configured with snapshot_backups "
            "disabled, which selects the secondary-tiering object layout. "
            "Backups cannot be written there.")


def _validate_key_wrapping(config: BackupConfig, encrypted: bool) -> None:
    """Check the configured key-wrapping secret can actually wrap a key.

    A cluster that asked for key wrapping must not silently get backups without
    it -- that is the difference between a recoverable backup and one that dies
    with its cluster. Checked here so an unusable secret fails the request
    rather than the background task, after the snapshot already exists.

    Raises:
        PreconditionError: The secret is configured but unusable.
    """
    if not encrypted or config.key_wrapping_secret is None:
        return

    try:
        backup_key_wrapping.wrap(("probe", "probe"), config.key_wrapping_secret)
    except backup_key_wrapping.KeyWrappingError as e:
        raise PreconditionError(
            f"Cluster is configured to wrap backup keys, but the configured "
            f"secret cannot be used: {e}") from e


def _existing_chain_backups(snap_chain) -> list:
    """The backups that already exist for a snapshot chain, oldest first."""
    existing = []
    for snap in snap_chain:
        for backup in db_controller.get_backups_by_snapshot_id(snap.get_id()):
            if backup.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS,
                                 Backup.STATUS_COMPLETED):
                existing.append(backup)
    return existing


def _validate_chain_is_coherent(backups, location: BackupLocation,
                                will_be_encrypted: Optional[bool] = None) -> None:
    """Check that a set of backups can actually be restored together.

    A restore reads clusters from every backup in the chain in one operation,
    against one bucket, decrypting all of it with one key. So the chain has to
    agree on where it lives, how it is encoded, and whether it is encrypted --
    nothing anywhere in the stack could express a chain split across two buckets
    or half encrypted.

    Checked at creation, at import and at restore, because each is a point where
    a chain could otherwise become incoherent without anyone noticing. At
    creation the new backup does not exist yet, hence ``will_be_encrypted``.

    Raises:
        PreconditionError: The backups disagree.
    """
    for backup in backups:
        if backup.get_location() != location:
            raise PreconditionError(
                f"Backup {backup.uuid} lives in bucket "
                f"{backup.get_location().bucket_name}, but the rest of its "
                f"chain is in {location.bucket_name}. A chain cannot span "
                "buckets or encodings; start a new chain with a full backup.")

    encrypted = {b.encrypted for b in backups}
    if will_be_encrypted is not None:
        encrypted.add(will_be_encrypted)

    if len(encrypted) > 1:
        raise PreconditionError(
            "A chain cannot mix encrypted and unencrypted backups: "
            + ", ".join(f"{b.uuid}={'encrypted' if b.encrypted else 'plain'}"
                        for b in backups)
            + (f", new backup={'encrypted' if will_be_encrypted else 'plain'}"
               if will_be_encrypted is not None else ""))


def build_manifest(backup: Backup) -> backup_manifest.BackupManifest:
    """Assemble the self-describing record for a completed backup.

    Everything a restore needs is collected here, from the backup itself and
    from the volume/pool/cluster it came from, so that after this point no part
    of the restore path has to consult the originating cluster.
    """
    chain = db_controller.get_backup_chain(backup.uuid)

    volume = backup_manifest.Volume(
        lvol_id=backup.lvol_id,
        lvol_name=backup.lvol_name,
        snapshot_id=backup.snapshot_id,
        snapshot_name=backup.snapshot_name,
        size=backup.size,
        allowed_hosts=backup.allowed_hosts or [],
    )

    # The volume's own shape, where it still exists. Restore currently hardcodes
    # these; recording them is what will let it stop.
    try:
        lvol = db_controller.get_lvol_by_id(backup.lvol_id)
    except KeyError:
        logger.warning("Volume %s is gone; manifest for backup %s records only "
                       "the shape carried on the backup itself",
                       backup.lvol_id, backup.uuid)
    else:
        volume.pool_name = lvol.pool_name
        volume.ha_type = lvol.ha_type or "default"
        volume.fabric = lvol.fabric or "tcp"
        volume.lvol_priority_class = lvol.lvol_priority_class
        volume.max_size = lvol.max_size
        volume.rw_ios_per_sec = lvol.rw_ios_per_sec
        volume.rw_mbytes_per_sec = lvol.rw_mbytes_per_sec
        volume.r_mbytes_per_sec = lvol.r_mbytes_per_sec
        volume.w_mbytes_per_sec = lvol.w_mbytes_per_sec

    source = backup_manifest.Source(
        cluster_id=backup.source_cluster_id or backup.cluster_id,
        node_id=backup.node_id,
    )
    dataplane = backup_manifest.DataPlane()
    try:
        cluster = db_controller.get_cluster_by_id(backup.cluster_id)
    except KeyError:
        pass
    else:
        source.cluster_name = cluster.cluster_name
        dataplane.cluster_size = cluster.page_size_in_blocks * constants.LVOL_CLUSTER_RATIO

    return backup_manifest.BackupManifest(
        backup_id=backup.uuid,
        s3_id=backup.s3_id,
        created_at=backup.created_at,
        completed_at=backup.completed_at,
        size=backup.size,
        prev_backup_id=backup.prev_backup_id,
        # backup.encrypted is authoritative -- overlaying it here means the two
        # cannot disagree in a manifest, whatever is stored in the dict.
        encryption=backup_manifest.Encryption.model_validate(
            {**(backup.encryption or {}), "encrypted": backup.encrypted}),
        location=backup.get_location(),
        chain=[backup_manifest.ChainEntry(backup_id=b.uuid, s3_id=b.s3_id) for b in chain],
        source=source,
        volume=volume,
        dataplane=dataplane,
    )


def _resolve_crypto_key(backup: Backup, cluster, key_wrapping_passphrase: Optional[SecretStr]):
    """Recover the key needed to read an encrypted backup.

    Two routes, in order of independence from the cluster that made the backup:

    1. Key wrapping -- the key wrapped into the backup's own metadata. Needs only the
       operator's passphrase, so it works when the originating cluster is gone.
    2. The KMS named in the backup's descriptor. Needs that KMS to still be
       reachable, which after a disaster it may not be.

    Returns None for an unencrypted backup.

    Raises:
        PreconditionError: The key cannot be obtained. Raised before the volume
            is created, so a restore that cannot decrypt fails without leaving a
            half-built volume behind -- and, more importantly, without silently
            producing a plaintext volume over ciphertext.
    """
    if not backup.encrypted:
        return None

    encryption = backup_manifest.Encryption.model_validate(
        {**(backup.encryption or {}), "encrypted": backup.encrypted})

    if encryption.wrapped_key is not None:
        if key_wrapping_passphrase is None:
            raise PreconditionError(
                f"Backup {backup.uuid} is encrypted and its key is wrapped; "
                "supply the wrapped_key passphrase to restore it")
        try:
            return backup_key_wrapping.unwrap(encryption.wrapped_key, key_wrapping_passphrase)
        except backup_key_wrapping.KeyWrappingError as e:
            raise PreconditionError(f"Cannot open wrapped key: {e}") from e

    descriptor = encryption.descriptor
    if descriptor is None:
        raise PreconditionError(
            f"Backup {backup.uuid} is encrypted but records nothing about its "
            "key; it predates self-describing backups and cannot be restored")

    if not backup.dr_capable:
        logger.info(
            "Backup %s has no wrapped key; resolving it via %s as recorded in "
            "its descriptor", backup.uuid, descriptor.kms or "its KMS")

    try:
        with create_kms_connection(cluster) as kms:
            return kms.get_data_encryption_keys(descriptor.dek_path, descriptor.kek_name)
    except KMSException as e:
        raise PreconditionError(
            f"Cannot reach the key for backup {backup.uuid} at "
            f"{descriptor.dek_path}. It was written by cluster "
            f"{backup.source_cluster_id or backup.cluster_id} using "
            f"{descriptor.kms or 'an unrecorded KMS'}, and no key was wrapped "
            f"with the backup: {e}") from e


def _config_for(backup: Backup) -> BackupConfig:
    """Credentials for a backup's own bucket.

    The location comes from the backup; only the credentials come from the
    cluster, and only because a manifest must never carry them.

    Raises:
        PreconditionError: The cluster's configured bucket is not the one this
            backup lives in, so its credentials cannot be assumed to reach it.
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
    backup_manifest.delete(_config_for(backup), backup.uuid)


def _s3_bucket_exists(config: BackupConfig, bucket_name) -> bool:
    try:
        backup_manifest.s3_client(config).head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            return False
        raise


def _ensure_s3_bucket(config: BackupConfig, bucket_name):
    try:
        s3_client = backup_manifest.s3_client(config)
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"S3 bucket already exists: {bucket_name}")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"S3 bucket created: {bucket_name}")
            else:
                raise
    except BotoCoreError as e:
        raise RuntimeError(f"Error ensuring S3 bucket {bucket_name} exists") from e


def create_s3_bdev(node, config: BackupConfig) -> None:
    """Create the S3 bdev and attach it to a node's lvstore.
    Called during cluster activate / node restart.
    Args:
        node: StorageNode with lvstore set
        config: the cluster's validated backup configuration
    """
    if not node.lvstore:
        raise PreconditionError("Node does not have an lvstore")

    rpc_client = node.rpc_client()
    s3_bdev_name = f"s3_{node.lvstore}"

    bdb_lcpu_mask, s3_lcpu_mask = _compute_s3_cpu_masks(node)

    # NO bdev_lvol_create_poller_group here: the lvstore-create poller group
    # is created exactly ONCE per SPDK process lifetime — right after
    # framework init in the add-node / restart-node flows, on the JC
    # singleton's thread/core. This function used to re-call it with
    # app_thread_mask (fix f0fed785, which predates the bring-up call from
    # #938): a second creation with a different mask that either failed
    # noisily on every activate or put the pollers on the wrong core.

    # The data plane still takes the pre-BackupConfig parameter shape; phase 2
    # replaces it. Two lossy mappings live here until then:
    #  * `local_testing` is not a mode, it is the only condition under which the
    #    data plane honours an endpoint override at all (bdev_s3_impl.hpp
    #    init_client), so it tracks "an endpoint was configured".
    #  * region, verify_tls and use_path_style have nowhere to go -- the data
    #    plane hardcodes us-east-1 and path-style under local_testing, and
    #    resolves the region from the environment otherwise.
    try:
        rpc_client.bdev_s3_create(
            name=s3_bdev_name,
            secondary_target=config.secondary_target.wire_value,
            with_compression=config.with_compression,
            snapshot_backups=config.snapshot_backups,
            local_testing=config.endpoint is not None,
            local_endpoint=config.endpoint_url or "",
            access_key_id=config.credentials.access_key_id if config.credentials else "",
            secret_access_key=config.credentials.secret_access_key if config.credentials else "",
            bdb_lcpu_mask=bdb_lcpu_mask,
            s3_lcpu_mask=s3_lcpu_mask,
            s3_thread_pool_size=config.s3_thread_pool_size or 0,
        )

        _ensure_s3_bucket(config, config.bucket_name)

        rpc_client.bdev_s3_add_bucket_name(s3_bdev_name, config.bucket_name, allow_existing=True)
        logger.info(f"S3 bdev bucket set: {config.bucket_name} on {s3_bdev_name}")

        rpc_client.bdev_lvol_s3_bdev(node.lvstore, s3_bdev_name)
        logger.info(f"S3 bdev created and attached: {s3_bdev_name} on node {node.get_id()}")
    except (RPCException, RuntimeError) as e:
        raise RuntimeError(f"Error S3 bdev on node {node.get_id()}") from e


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


def _build_encryption(cluster, backup: Backup, kms) -> backup_manifest.Encryption:
    """Describe a backup's key, and wrapped_key it when the cluster is configured to.

    The descriptor alone makes the dependency on the originating KMS explicit
    but does not remove it. Key wrapping removes it, at the cost of putting the key --
    wrapped under a passphrase held only by the operator -- next to the
    ciphertext. That trade is the cluster's to make, hence opt-in.

    Args:
        kms: an open connection, reused so this does not re-authenticate.

    Raises:
        KMSException: the keys could not be read back for wrapped_key. Raised rather
            than degraded to a backup without a wrapped key, because a cluster that asked
            for wrapped_key must not silently get a backup without it.
        KeyWrappingError: the keys could not be wrapped.
    """
    descriptor = backup_manifest.KeyDescriptor(
        dek_path=backup_dek_path(cluster.get_id(), backup.uuid),
        kek_name=backup_kek_name(backup.uuid),
    )
    if cluster.hashicorp_vault_settings is not None:
        vault = cluster.hashicorp_vault_settings
        descriptor.kms = "hashicorp_vault"
        descriptor.vault_base_url = vault.base_url
        descriptor.transit_mount = vault.transit_mount
        descriptor.kv_mount = vault.kv_mount
    else:
        descriptor.kms = "local"

    encryption = backup_manifest.Encryption(encrypted=True, descriptor=descriptor)

    config = cluster.get_backup_config()
    if config.key_wrapping_secret is not None:
        keys = kms.get_data_encryption_keys(descriptor.dek_path, descriptor.kek_name)
        encryption.wrapped_key = backup_key_wrapping.wrap(keys, config.key_wrapping_secret)

    return encryption


def _create_single_backup(snapshot, lvol, node_id, cluster_id, prev_backup, location: BackupLocation):
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
    backup.source_cluster_id = cluster_id  # provenance only
    backup.location = location.model_dump(mode="json")
    backup.lvol_id = lvol.get_id()
    backup.lvol_name = lvol.lvol_name
    backup.snapshot_id = snapshot.get_id()
    backup.snapshot_name = snapshot.snap_name
    backup.node_id = node_id
    backup.pool_uuid = lvol.pool_uuid
    backup.prev_backup_id = prev_backup.uuid if prev_backup else ""
    backup.size = snapshot.size
    backup.allowed_hosts = lvol.allowed_hosts
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
            backup.encryption = _build_encryption(cluster, backup, kms).model_dump(mode="json")

        if not backup.dr_capable:
            logger.warning(
                "Backup %s of encrypted volume %s has no wrapped key: restoring "
                "it will require reaching %s. Configure key_wrapping_secret on cluster "
                "%s to make it recoverable on its own.",
                backup.uuid, lvol.get_id(),
                backup.encryption.get("descriptor", {}).get("kms", "its KMS"),
                cluster_id)

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

    # Block new backups when S3 source is switched to an external cluster
    if not is_local_backup_source(snode.cluster_id):
        return None, ("Cannot create backups while backup source is "
                      "switched to an external cluster. Switch back "
                      "to local first.")

    if snode.status != StorageNode.STATUS_ONLINE:
        return None, f"Node {node_id} is not online (status: {snode.status})"

    if not cluster_id:
        cluster_id = snode.cluster_id

    snap_chain = _get_snapshot_chain(snapshot)

    # Everything that could make this backup unrestorable is checked here,
    # before the chain lock is taken, before any KMS key is created and before
    # any task is enqueued. A backup either is restorable or was never created.
    try:
        config = db_controller.get_cluster_by_id(cluster_id).get_backup_config()
        location = config.location()
        _validate_backup_target(config)
        _validate_chain_length(len(snap_chain), "This snapshot chain")
        _validate_chain_is_coherent(
            _existing_chain_backups(snap_chain), location,
            will_be_encrypted=bool(lvol.crypto_bdev))
        _validate_key_wrapping(config, encrypted=bool(lvol.crypto_bdev))
    except (KeyError, PreconditionError) as e:
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

    prev_backup = _get_latest_backup_for_lvol(lvol.get_id())
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

            backup = _create_single_backup(snap, lvol, node_id, cluster_id, prev_backup, location)
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
                   key_wrapping_passphrase: Optional[SecretStr] = None):
    """Restore a backup chain into a new fully-accessible lvol.

    Creates the volume (with subsystem, listeners, namespace) via
    lvol_controller.add_lvol_ha, then schedules an async task to
    fill in the data from S3.  The volume is in STATUS_RESTORING
    until the data transfer completes.

    Args:
        key_wrapping_passphrase: Required when the backup's key is wrapped. Never
            persisted -- it is used to open the key and discarded.
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

    # Verify the backup's source matches the active S3 source.
    # If the backup came from an external cluster, the S3 bdev must be
    # switched to that cluster's bucket before restoring.
    backup_src = backup.source_cluster_id or backup.cluster_id
    active_src = cluster.backup_source or cluster.uuid
    if backup_src != active_src:
        raise PreconditionError(
            f"Backup source is {backup_src[:8]} but active S3 source "
            f"is {active_src[:8]}. Use 'sbctl backup source-switch "
            f"{backup_src}' first.")

    # The chain has to be restorable as a unit, and short enough for the data
    # plane. Checked before the volume is created so a doomed restore leaves
    # nothing behind.
    _validate_chain_length(len(chain), f"The chain ending at backup {backup_id}")
    _validate_chain_is_coherent(chain, backup.get_location())

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

    crypto_key = _resolve_crypto_key(backup, cluster, key_wrapping_passphrase)

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
            [b.s3_id for b in reversed(chain)], lvol_id=lvol_id):
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
        source = b.source_cluster_id or b.cluster_id
        is_external = source != b.cluster_id
        entry = {
            "ID": b.uuid,
            "S3 ID": b.s3_id,
            "LVol": b.lvol_name,
            "Snapshot": b.snapshot_name,
            "Node": b.node_id[:8] if b.node_id else "",
            "Status": b.status,
            "Prev": b.prev_backup_id[:8] if b.prev_backup_id else "-",
            "Created": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(b.created_at)) if b.created_at else "",
            "Source": source[:8] if is_external else "local",
            # Visible here rather than discovered during a recovery.
            "DR": "yes" if b.dr_capable else "needs source KMS",
        }
        data.append(entry)
    return data


def export_backups(cluster_id=None, lvol_name=None):
    """Export completed backups as manifests, for import into another cluster.

    Emits the same shape that lives in the bucket, so a hand-carried file and a
    bucket read are interchangeable. Previously this produced a third, narrower
    format of its own -- which is how it came to omit `encrypted`.
    """
    backups = db_controller.get_backups(cluster_id)
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if lvol_name:
        completed = [b for b in completed if b.lvol_name == lvol_name]

    return [build_manifest(b).model_dump(mode="json") for b in completed]


def discover_backups(config: BackupConfig) -> list:
    """Every backup in a bucket, read from its manifests alone.

    The disaster-recovery entry point: given a bucket and credentials for it,
    this answers "what is in here" with no reference to any cluster, live or
    dead.

    Raises:
        ManifestError: The bucket could not be listed, or one of its manifests
            could not be parsed.
    """
    return [m.model_dump(mode="json") for m in backup_manifest.list_all(config)]


def _validate_importable(pending: dict) -> None:
    """Check a batch of manifests forms something restorable before writing any.

    An import that lands a backup whose ancestors are missing produces a record
    that looks restorable in ``backup list`` and fails only when someone tries
    it -- typically during the recovery it was meant to serve. The chain is
    recorded in each manifest precisely so this is answerable up front.

    Raises:
        PreconditionError: A chain is incomplete, too long for the data plane,
            or spans buckets.
    """
    known = set(pending)
    for backup_id, manifest in pending.items():
        _validate_chain_length(len(manifest.chain), f"The chain of backup {backup_id}")

        missing = [
            entry.backup_id for entry in manifest.chain
            if entry.backup_id not in known
            and not _backup_exists(entry.backup_id)
        ]
        if missing:
            raise PreconditionError(
                f"Backup {backup_id} depends on backups that are neither in "
                f"this import nor already known: {', '.join(missing)}. A "
                "backup is a delta and cannot be restored without its chain.")

        divergent = [
            entry.backup_id for entry in manifest.chain
            if entry.backup_id in pending
            and pending[entry.backup_id].location != manifest.location
        ]
        if divergent:
            raise PreconditionError(
                f"Backup {backup_id} shares a chain with backups in a "
                f"different bucket or encoding: {', '.join(divergent)}")


def _backup_exists(backup_id: str) -> bool:
    try:
        db_controller.get_backup_by_id(backup_id)
    except KeyError:
        return False
    return True


def import_backups(manifests, cluster_id=None):
    """Register backups described by manifests into this cluster's database.

    The backups keep their original ids -- both their uuid and their s3_id,
    which names their objects in the bucket and therefore cannot be reassigned.

    Args:
        manifests: manifest dicts, from `discover_backups`, `export_backups`,
            or a file produced by either.
        cluster_id: Target cluster to import into, so the backups are visible in
            its namespace.

    Raises:
        PreconditionError: A manifest is malformed, or one of the backup IDs is
            already known -- backup lookups are not scoped by cluster, so a UUID
            reused across clusters would make either record unaddressable.
            Everything is checked before the first record is written, so a bad
            batch imports nothing rather than half of itself.
    """
    pending = {}
    for data in manifests:
        backup_id = data.get("backup_id")
        if not backup_id:
            continue

        if backup_id in pending:
            raise PreconditionError(f"Backup {backup_id} is listed more than once")

        try:
            manifest = backup_manifest.BackupManifest.model_validate(data)
        except ValueError as e:
            raise PreconditionError(f"Backup {backup_id} has a malformed manifest: {e}") from e

        try:
            existing = db_controller.get_backup_by_id(backup_id)
        except KeyError:
            pending[backup_id] = manifest
        else:
            raise PreconditionError(f"Backup {backup_id} already exists in cluster {existing.cluster_id}")

    _validate_importable(pending)

    for backup_id, manifest in pending.items():
        backup = Backup()
        backup.uuid = backup_id
        backup.s3_id = manifest.s3_id
        backup.cluster_id = cluster_id or manifest.source.cluster_id
        backup.source_cluster_id = manifest.source.cluster_id
        backup.lvol_id = manifest.volume.lvol_id
        backup.lvol_name = manifest.volume.lvol_name
        backup.snapshot_id = manifest.volume.snapshot_id
        backup.snapshot_name = manifest.volume.snapshot_name
        backup.node_id = manifest.source.node_id
        backup.prev_backup_id = manifest.prev_backup_id
        backup.size = manifest.size
        backup.allowed_hosts = manifest.volume.allowed_hosts
        backup.created_at = manifest.created_at
        backup.completed_at = manifest.completed_at
        backup.status = Backup.STATUS_COMPLETED
        backup.location = manifest.location.model_dump(mode="json")
        # Import used to drop this, so an imported encrypted backup restored as
        # use_crypto=False -- a plaintext volume over ciphertext, silently.
        backup.encrypted = manifest.encryption.encrypted
        backup.encryption = manifest.encryption.model_dump(mode="json")
        backup.write_to_db()

    return len(pending)


def import_from_bucket(config: BackupConfig, cluster_id=None) -> int:
    """Import every backup found in a bucket.

    Raises:
        ManifestError: the bucket could not be read.
        PreconditionError: the manifests it holds cannot be imported as a batch.
    """
    return import_backups(discover_backups(config), cluster_id=cluster_id)


def get_backup_sources(cluster_id):
    """List all distinct backup sources (local + imported clusters).

    Returns a list of dicts with source_cluster_id, count, and whether
    it is the currently active source.
    """
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        return []

    backups = db_controller.get_backups(cluster_id)
    sources = {}
    for b in backups:
        src = b.source_cluster_id or cluster_id
        if src not in sources:
            sources[src] = {"source_cluster_id": src, "count": 0, "is_local": src == cluster_id}
        sources[src]["count"] += 1

    active_source = cluster.backup_source or cluster_id
    result = []
    for src_id, info in sources.items():
        info["active"] = (src_id == active_source)
        result.append(info)

    # Always include local even if no backups
    if cluster_id not in sources:
        result.append({
            "source_cluster_id": cluster_id,
            "count": 0,
            "is_local": True,
            "active": active_source == cluster_id,
        })

    return result


def switch_backup_source(cluster_id, source_cluster_id) -> None:
    """Switch the active backup source for all nodes in the cluster.

    Reconfigures the S3 bdev on every node to read from the bucket
    belonging to source_cluster_id.  While switched to an external
    source, new backups cannot be created.

    Args:
        cluster_id: The local cluster ID.
        source_cluster_id: The cluster ID whose S3 bucket to activate.
            Use the local cluster_id (or "local") to switch back.

    Returns (success, error_message).
    """
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError as e:
        raise PreconditionError("Precondition not met") from e

    if source_cluster_id == "local":
        source_cluster_id = cluster_id

    # Determine the bucket name for the source cluster
    config = cluster.get_backup_config()
    if source_cluster_id == cluster_id:
        bucket_name = config.bucket_name
    else:
        bucket_name = f"simplyblock-backup-{source_cluster_id}"

    # Verify the bucket exists
    try:
        if not _s3_bucket_exists(config, bucket_name):
            raise PreconditionError(f"S3 bucket {bucket_name} does not exist")
    except BotoCoreError as e:
        raise RuntimeError(f"S3 bucket {bucket_name} not accessible: {e}")

    # Reconfigure S3 bdev bucket on all online nodes
    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    for node in nodes:
        if node.status != StorageNode.STATUS_ONLINE or not node.lvstore:
            continue

        rpc_client = node.rpc_client()
        s3_bdev_name = f"s3_{node.lvstore}"
        rpc_client.bdev_s3_add_bucket_name(s3_bdev_name, bucket_name, allow_existing=True)
        logger.info(f"Switched S3 bucket to {bucket_name} on node {node.get_id()}")

    # Persist the active source in the cluster record. Atomic: the long
    # per-node RPC loop above means a concurrent cluster.status change could be
    # clobbered by a full write here (lost-update class — incident 2026-06-18).
    db_controller.atomic_update(
        db_controller.get_cluster_by_id(cluster_id),
        lambda c, v=source_cluster_id: setattr(c, "backup_source", v))


def is_local_backup_source(cluster_id):
    """Check if the cluster is currently using its own local backup source."""
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        return True
    return not cluster.backup_source or cluster.backup_source == cluster_id


# ---- Backup Policy Management ----

def add_policy(cluster_id, name, max_versions=0, max_age="", schedule=""):
    """Create a new backup policy.
    Returns (policy_id, error_message)."""
    max_age_seconds = 0
    if max_age:
        try:
            max_age_seconds = _parse_age_string(max_age)
        except ValueError as e:
            return None, str(e)

    if schedule:
        try:
            _parse_schedule(schedule)
        except ValueError as e:
            return None, str(e)

    if max_versions <= 0 and max_age_seconds <= 0 and not schedule:
        return None, "At least one of --versions, --age, or --schedule must be specified"

    # Check name uniqueness
    for p in db_controller.get_backup_policies(cluster_id):
        if p.policy_name == name:
            return None, f"Policy name already exists: {name}"

    policy = BackupPolicy()
    policy.uuid = str(uuid.uuid4())
    policy.cluster_id = cluster_id
    policy.policy_name = name
    policy.max_versions = max_versions
    policy.max_age_seconds = max_age_seconds
    policy.max_age_display = max_age
    policy.backup_schedule = schedule
    policy.status = BackupPolicy.STATUS_ACTIVE
    policy.write_to_db()

    return policy.uuid, None


def remove_policy(policy_id):
    """Remove a backup policy and all its attachments.
    Returns (success, error_message)."""
    try:
        policy = db_controller.get_backup_policy_by_id(policy_id)
    except KeyError as e:
        return False, str(e)

    # Remove attachments
    for att in db_controller.get_backup_policy_attachments(policy.cluster_id):
        if att.policy_id == policy_id:
            att.remove(db_controller.kv_store)

    policy.remove(db_controller.kv_store)
    return True, None


def attach_policy(policy_id, target_type, target_id):
    """Attach a backup policy to a pool or lvol.
    Returns (attachment_id, error_message)."""
    try:
        policy = db_controller.get_backup_policy_by_id(policy_id)
    except KeyError as e:
        return None, str(e)

    if target_type not in ("pool", "lvol"):
        return None, f"Invalid target_type: {target_type}. Use 'pool' or 'lvol'"

    # Validate target exists
    try:
        if target_type == "pool":
            db_controller.get_pool_by_id(target_id)
        else:
            db_controller.get_lvol_by_id(target_id)
    except KeyError as e:
        return None, str(e)

    # Check if already attached
    for att in db_controller.get_backup_policy_attachments(policy.cluster_id):
        if att.policy_id == policy_id and att.target_type == target_type and att.target_id == target_id:
            return att.uuid, None  # already attached

    att = BackupPolicyAttachment()
    att.uuid = str(uuid.uuid4())
    att.cluster_id = policy.cluster_id
    att.policy_id = policy_id
    att.target_type = target_type
    att.target_id = target_id
    att.write_to_db()

    return att.uuid, None


def detach_policy(policy_id, target_type, target_id):
    """Detach a backup policy from a pool or lvol.
    Returns (success, error_message)."""
    try:
        policy = db_controller.get_backup_policy_by_id(policy_id)
    except KeyError as e:
        return False, str(e)

    for att in db_controller.get_backup_policy_attachments(policy.cluster_id):
        if att.policy_id == policy_id and att.target_type == target_type and att.target_id == target_id:
            att.remove(db_controller.kv_store)
            return True, None

    return False, "Attachment not found"


def list_policies(cluster_id=None):
    """List all backup policies."""
    policies = db_controller.get_backup_policies(cluster_id)
    data = []
    for p in policies:
        data.append({
            "ID": p.uuid,
            "Name": p.policy_name,
            "Versions": p.max_versions if p.max_versions > 0 else "-",
            "Max Age": p.max_age_display if p.max_age_display else "-",
            "Schedule": p.backup_schedule if p.backup_schedule else "-",
            "Status": p.status,
        })
    return data


def evaluate_policy(lvol):
    """Evaluate backup policy for an lvol and trigger merges if needed.
    Called by the backup merge service."""
    policy = db_controller.get_policy_for_lvol(lvol)
    if not policy:
        return

    backups = db_controller.get_backups_by_lvol_id(lvol.get_id())
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if len(completed) < 2:
        return

    completed.sort(key=lambda b: b.created_at)
    now = int(time.time())

    versions_exceeded = policy.max_versions > 0 and len(completed) > policy.max_versions
    age_exceeded = False
    if policy.max_age_seconds > 0 and completed:
        oldest_age = now - completed[0].created_at
        age_exceeded = oldest_age > policy.max_age_seconds

    # Either condition triggers a merge
    if versions_exceeded or age_exceeded:
        oldest = completed[0]
        second = completed[1]
        _trigger_merge(second, oldest)


def evaluate_schedule(lvol):
    """Evaluate the backup schedule for an lvol and trigger auto-backups + tiered merges.
    Called by the backup merge service."""
    policy = db_controller.get_policy_for_lvol(lvol)
    if not policy or not policy.backup_schedule:
        return

    try:
        tiers = _parse_schedule(policy.backup_schedule)
    except ValueError:
        return

    if not tiers:
        return

    now = int(time.time())

    # Check if we need to create a new auto-backup based on the smallest tier interval
    smallest_interval = tiers[0][0]
    backups = db_controller.get_backups_by_lvol_id(lvol.get_id())
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    pending_or_running = [b for b in backups if b.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS)]

    # Don't create a new backup if one is already in progress
    if not pending_or_running:
        needs_backup = True
        if completed:
            completed.sort(key=lambda b: b.created_at, reverse=True)
            latest = completed[0]
            elapsed = now - latest.created_at
            if elapsed < smallest_interval:
                needs_backup = False

        if needs_backup:
            _auto_backup_lvol(lvol)
            return  # Skip merge evaluation this cycle — let the backup complete first

    # Tiered merge: enforce keep_count per tier.
    # Each tier covers an age range.  Backups age from tier 0 (newest)
    # into higher tiers.  When a tier exceeds its keep_count, the oldest
    # backup in that tier is merged into its successor.
    # All tiers are evaluated each cycle so limits are maintained in parallel.
    if len(completed) < 2:
        return

    completed.sort(key=lambda b: b.created_at)

    # Don't merge while another merge is already in progress
    merging = [b for b in backups if b.status == Backup.STATUS_MERGING]
    if merging:
        return

    for tier_idx, (interval, keep_count) in enumerate(tiers):
        # Age boundaries for this tier
        if tier_idx == 0:
            lower_age = 0
        else:
            lower_age = tiers[tier_idx - 1][0]

        if tier_idx + 1 < len(tiers):
            upper_age = tiers[tier_idx + 1][0]
        else:
            upper_age = float('inf')

        tier_backups = [b for b in completed
                        if lower_age <= (now - b.created_at) < upper_age]

        if len(tier_backups) > keep_count:
            tier_backups.sort(key=lambda b: b.created_at)
            oldest = tier_backups[0]
            second = tier_backups[1]
            _trigger_merge(second, oldest)
            return  # One merge per cycle to avoid conflicts


def _auto_backup_lvol(lvol):
    """Create an automatic snapshot + backup for scheduled backups.

    Unlike manual backup_snapshot() which walks the full ancestor chain,
    auto-backups create a single snapshot and a single backup for it.
    The prev_backup_id is set to the latest existing backup so the
    incremental chain is maintained without re-backing all ancestors.
    """
    from simplyblock_core.controllers import snapshot_controller

    # Resolve everything the backup needs BEFORE taking the snapshot. This used
    # to create the snapshot first and discover afterwards that the node or
    # cluster was unusable, leaving an orphaned auto_* snapshot behind on every
    # scheduler tick.
    node_id = lvol.node_id
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
        cluster_id = snode.cluster_id
        location = db_controller.get_cluster_by_id(cluster_id).get_backup_config().location()
    except (KeyError, PreconditionError) as e:
        logger.warning(f"Auto-backup skipped for lvol {lvol.get_id()}: {e}")
        return

    snap_name = f"auto_{lvol.lvol_name}_{int(time.time())}"
    snap_id, error = snapshot_controller.add(lvol.get_id(), snap_name)
    if error:
        logger.warning(f"Auto-backup snapshot failed for lvol {lvol.get_id()}: {error}")
        return

    try:
        snapshot = db_controller.get_snapshot_by_id(snap_id)
    except KeyError:
        logger.warning(f"Auto-backup: snapshot {snap_id} not found after creation")
        return

    prev_backup = _get_latest_backup_for_lvol(lvol.get_id())
    _create_single_backup(snapshot, lvol, node_id, cluster_id, prev_backup, location)


def _trigger_merge(keep_backup, old_backup):
    """Trigger a merge of old_backup into keep_backup."""
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
