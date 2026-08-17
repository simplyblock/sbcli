# coding=utf-8
import logging
import re
import time
import uuid
from typing import Iterable, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from simplyblock_core import backup_manifest, constants
from simplyblock_core.controllers import backup_events, tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup, BackupPolicy, BackupPolicyAttachment
from simplyblock_core.models.backup_config import (
    BackupConfig, BackupLocation, S3Credentials)
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


def _compute_s3_cpu_masks(node: StorageNode):
    """CPU masks for the S3 bdev, or None where the node does not say.

    Returns (bdb_lcpu_mask, s3_lcpu_mask):
        bdb_lcpu_mask: app_thread core (SPDK lightweight thread, low overhead)
        s3_lcpu_mask: all system vCPUs (no pinning — let Linux scheduler handle
                      the AWS SDK thread pool; the data plane default would
                      wrongly pin onto SPDK reactor cores)

    None rather than 0 for "the node does not tell us": a zero mask selects no
    CPUs at all, and the data plane reads it as "unset" anyway, so returning it
    would be a sentinel dressed as a value.
    """
    # SPDK thread for the bdev poller — reuse the app thread core
    bdb_lcpu_mask = int(node.app_thread_mask, 16) if node.app_thread_mask else None

    # AWS SDK thread pool — set all system vCPU bits so threads are unconstrained
    s3_lcpu_mask = (1 << node.cpu) - 1 if node.cpu > 0 else None

    return bdb_lcpu_mask, s3_lcpu_mask


# --- Restorability rules ---------------------------------------------------
#
# Predicates, so the same rule can answer a question ("can this bucket hold
# backups?") as well as block an operation. `require_restorable` is the one place
# that turns a false answer into a refusal, so the wording an operator sees is
# written once rather than at each of the entry points that enforce the rules.


def chain_fits(length: int) -> bool:
    """Whether a chain this long is accepted.

    The bound is the control plane's own; see BACKUP_MAX_CHAIN_LENGTH for what it
    is a bound on. The data plane's own limit is higher and refuses rather than
    overruns, so this is where a too-long chain is reported usefully.
    """
    return length <= constants.BACKUP_MAX_CHAIN_LENGTH


def location_holds_backups(location: BackupLocation) -> bool:
    """Whether backups written to this location could be read back.

    ``snapshot_backups=False`` selects the secondary-tiering object layout, whose
    keys are ``{tiering_id}/{lpgi}``. The restore path addresses
    ``{s3_id}/{mid}/{extent}``, so it can never find them.
    """
    return location.snapshot_backups


def chain_is_coherent(backups, location: BackupLocation,
                      encrypted: Optional[bool] = None) -> bool:
    """Whether these backups can be restored together.

    A restore reads clusters from every backup in the chain in one operation,
    against one bucket, decrypting all of it with one key. So the chain has to
    agree on where it lives, how it is encoded, and whether it is encrypted --
    nothing anywhere in the stack could express a chain split across two buckets
    or half encrypted.

    ``encrypted`` folds in a backup that does not exist yet, which is the case at
    creation time.
    """
    if any(backup.get_location() != location for backup in backups):
        return False

    variants = {backup.encrypted for backup in backups}
    if encrypted is not None:
        variants.add(encrypted)
    return len(variants) <= 1


def _describe_incoherence(backups, location: BackupLocation,
                          encrypted: Optional[bool]) -> str:
    for backup in backups:
        if backup.get_location() != location:
            return (
                f"backup {backup.uuid} lives in bucket "
                f"{backup.get_location().bucket_name}, but the rest of its chain "
                f"is in {location.bucket_name}. A chain cannot span buckets or "
                "encodings; start a new chain with a full backup")

    return (
        "a chain cannot mix encrypted and unencrypted backups: "
        + ", ".join(f"{b.uuid}={'encrypted' if b.encrypted else 'plain'}"
                    for b in backups)
        + (f", new backup={'encrypted' if encrypted else 'plain'}"
           if encrypted is not None else ""))


def require_restorable(location: BackupLocation, backups=(),
                       chain_length: Optional[int] = None,
                       encrypted: Optional[bool] = None,
                       what: str = "This chain") -> None:
    """Refuse a chain that could not be restored, naming the rule it breaks.

    Applied at creation, at import and at restore, because each is a point where
    a chain could otherwise become unrestorable without anyone noticing -- and
    each used to find out from whatever failed first, usually the data plane
    mid-operation.

    Args:
        chain_length: The eventual length, where it differs from ``len(backups)``
            -- at creation the ancestors are snapshots that have no backup yet.
        encrypted: Whether the backup about to be created will be encrypted.

    Raises:
        PreconditionError: One of the rules above does not hold.
    """
    if not location_holds_backups(location):
        raise PreconditionError(
            f"Bucket {location.bucket_name} is configured with snapshot_backups "
            "disabled, which selects the secondary-tiering object layout. "
            "Backups cannot be written there.")

    length = len(backups) if chain_length is None else chain_length
    if not chain_fits(length):
        raise PreconditionError(
            f"{what} is {length} backups long; the data plane accepts at most "
            f"{constants.BACKUP_MAX_CHAIN_LENGTH}. Merge older backups to "
            "shorten the chain, or start a new chain with a full backup.")

    if not chain_is_coherent(backups, location, encrypted):
        raise PreconditionError(
            f"{what} cannot be restored as a unit: "
            + _describe_incoherence(backups, location, encrypted))


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
      the other. `encrypted` living both as its own field and inside `encryption`,
      which is why this function has to overlay one onto the other so a manifest
      cannot contradict itself. And the volume's settings, which the manifest
      records and the record does not, so an imported backup knows less about its
      volume than the manifest it was imported from did.
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
    """
    volume = backup_manifest.Volume(
        lvol_id=backup.lvol_id,
        lvol_name=backup.lvol_name,
        snapshot_id=backup.snapshot_id,
        snapshot_name=backup.snapshot_name,
        size=backup.size,
        allowed_hosts=backup.allowed_hosts or [],
    )

    # The volume's own settings, where it still exists. Absent together once it
    # is gone, which is a different answer from 0 -- for a QoS cap that means
    # unlimited.
    try:
        lvol = db_controller.get_lvol_by_id(backup.lvol_id)
    except KeyError:
        logger.warning("Volume %s is gone; manifest for backup %s records only "
                       "the shape carried on the backup itself",
                       backup.lvol_id, backup.uuid)
    else:
        volume = volume.model_copy(update={
            "pool_name": lvol.pool_name,
            "ha_type": lvol.ha_type or "default",
            "fabric": lvol.fabric or "tcp",
            "lvol_priority_class": lvol.lvol_priority_class,
            "max_size": lvol.max_size,
            "rw_ios_per_sec": lvol.rw_ios_per_sec,
            "rw_mbytes_per_sec": lvol.rw_mbytes_per_sec,
            "r_mbytes_per_sec": lvol.r_mbytes_per_sec,
            "w_mbytes_per_sec": lvol.w_mbytes_per_sec,
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
        backup_id=backup.uuid,
        s3_id=backup.s3_id,
        created_at=backup.created_at,
        completed_at=backup.completed_at,
        size=backup.size,
        prev_backup_id=backup.prev_backup_id or None,
        # backup.encrypted is authoritative -- overlaying it here means the two
        # cannot disagree in a manifest, whatever is stored in the dict.
        encryption=backup_manifest.Encryption.model_validate(
            {**(backup.encryption or {}), "encrypted": backup.encrypted}),
        location=backup.get_location(),
        source=backup_manifest.Source(
            cluster_id=backup.cluster_id,
            cluster_name=cluster_name,
            node_id=backup.node_id,
        ),
        volume=volume,
        dataplane=backup_manifest.DataPlane(cluster_size=cluster_size),
    )


def primary_s3_bdev_name(node: StorageNode) -> str:
    """The S3 device holding the cluster's own backup bucket."""
    return f"s3_{node.lvstore}"


def create_restore_s3_bdev(node: StorageNode, config: BackupConfig, name: str) -> None:
    """Attach a second S3 device to a node, for a bucket that is not its own.

    A restore from a foreign bucket needs different credentials, a different
    endpoint and a different region than the node's own backup device carries.
    Since a device holds exactly one bucket, the way to read another one is to
    create another device -- which the lvstore supports, its transfer devices
    being a list.

    The caller owns the result and must delete it when the restore ends.
    """
    rpc_client = node.rpc_client()
    bdb_lcpu_mask, s3_lcpu_mask = _compute_s3_cpu_masks(node)

    try:
        rpc_client.bdev_s3_create(
            name=name,
            bucket_name=config.bucket_name,
            secondary_target=config.secondary_target,
            with_compression=config.with_compression,
            snapshot_backups=config.snapshot_backups,
            endpoint=config.endpoint_url,
            region=config.region,
            verify_tls=config.verify_tls,
            use_path_style=config.use_path_style,
            access_key_id=config.credentials.access_key_id if config.credentials else None,
            secret_access_key=config.credentials.secret_access_key if config.credentials else None,
            bdb_lcpu_mask=bdb_lcpu_mask,
            s3_lcpu_mask=s3_lcpu_mask,
            s3_thread_pool_size=config.s3_thread_pool_size,
        )
        rpc_client.bdev_lvol_s3_bdev(node.lvstore, name)
    except RPCException as e:
        raise RuntimeError(
            f"Failed to attach S3 device {name} for bucket {config.bucket_name} "
            f"on node {node.get_id()}") from e

    logger.info("Attached restore S3 device %s for bucket %s on node %s",
                name, config.bucket_name, node.get_id())


def delete_restore_s3_bdev(node: StorageNode, name: str) -> None:
    """Detach a device created by :func:`create_restore_s3_bdev`.

    Best-effort by design: this runs on the restore's terminal paths, and a
    failure to clean up must not turn a completed restore into a failed one. It
    is logged rather than raised, because the consequence is a leaked device --
    which does block the lvstore from being destroyed, so it is worth noticing.
    """
    try:
        node.rpc_client().bdev_s3_delete(name)
    except Exception as e:
        # Deliberately broad and deliberately not re-raised: this runs on a
        # restore's terminal paths, where the alternative to a leaked device is
        # reporting a completed restore as failed.
        logger.warning("Could not delete restore S3 device %s on node %s: %s",
                       name, node.get_id(), e)
    else:
        logger.info("Deleted restore S3 device %s on node %s", name, node.get_id())


def foreign_bucket_config(backup: Backup, cluster,
                          credentials: Optional[S3Credentials]) -> Optional[BackupConfig]:
    """How to reach this backup's bucket, when it is not the cluster's own.

    Returns ``None`` when the node's existing backup device already points at
    the right bucket -- there is no foreign bucket, so there is nothing to
    describe. Otherwise returns the configuration for a device that reads the
    backup's own recorded location, which is what makes a restore from another
    cluster's bucket possible at all.

    Decides only. The device itself is created by the task runner, which is the
    component that knows which node the volume landed on and the only one that
    can put the device back after a node restart mid-restore.

    Raises:
        PreconditionError: The bucket is foreign and unreachable -- no
            credentials were supplied for it, while the cluster uses static
            credentials that say nothing about it.
    """
    location = backup.get_location()

    try:
        own = cluster.get_backup_config()
    except ValueError:
        # No usable configuration of its own, so every bucket is foreign to it.
        own = None

    if own is not None and own.location() == location and credentials is None:
        return None

    config = BackupConfig.model_validate({
        **location.model_dump(exclude_none=True),
        **({"credentials": credentials.model_dump()} if credentials is not None else {}),
    })

    if config.credentials is None and own is not None and own.credentials is not None:
        # Falling back to the cluster's own static credentials would fail deep
        # in the data plane with nothing to point at the cause.
        raise PreconditionError(
            f"Backup {backup.uuid} lives in bucket {location.bucket_name}, which "
            f"is not this cluster's own. Supply credentials for that bucket, or "
            "configure the nodes with an instance role that can read it.")

    return config


def restore_s3_bdev_name(backup_id: str) -> str:
    """Name of the device created to read a foreign bucket for one restore.

    Derived from the backup id so a retry re-derives the same name rather than
    leaking a device per attempt.
    """
    return f"s3_restore_{backup_id[:8]}"


def _resolve_crypto_key(backup: Backup, cluster):
    """Recover the key needed to read an encrypted backup.

    An encrypted backup is ciphertext whose key lives in a KMS, and the backup
    records which one. Restoring it therefore needs that KMS reachable -- the
    assumption being that a KMS is recoverable independently of any one cluster.
    Nothing about the key travels with the backup.

    Returns None for an unencrypted backup.

    Raises:
        PreconditionError: The backup records nothing about its key, so no amount
            of reachable infrastructure can decrypt it.
        RuntimeError: The recorded KMS could not be reached. Raised before the
            volume is created, so a restore that cannot decrypt fails without
            leaving a half-built volume behind -- and, more importantly, without
            silently producing a plaintext volume over ciphertext.
    """
    if not backup.encrypted:
        return None

    encryption = backup_manifest.Encryption.model_validate(
        {**(backup.encryption or {}), "encrypted": backup.encrypted})

    descriptor = encryption.descriptor
    if descriptor is None:
        raise PreconditionError(
            f"Backup {backup.uuid} is encrypted but records nothing about its "
            "key; it predates self-describing backups and cannot be restored")

    try:
        with create_kms_connection(cluster) as kms:
            return kms.get_data_encryption_keys(descriptor.dek_path, descriptor.kek_name)
    except KMSException as e:
        raise RuntimeError(
            f"Cannot reach the key for backup {backup.uuid} at "
            f"{descriptor.dek_path} using {descriptor.kms}, which has to be "
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
    backup_manifest.delete(_config_for(backup), backup.uuid)


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


def create_s3_bdev(node: StorageNode, config: BackupConfig) -> None:
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

    try:
        _ensure_s3_bucket(config, config.bucket_name)

        rpc_client.bdev_s3_create(
            name=s3_bdev_name,
            bucket_name=config.bucket_name,
            secondary_target=config.secondary_target,
            with_compression=config.with_compression,
            snapshot_backups=config.snapshot_backups,
            endpoint=config.endpoint_url,
            region=config.region,
            verify_tls=config.verify_tls,
            use_path_style=config.use_path_style,
            access_key_id=config.credentials.access_key_id if config.credentials else None,
            secret_access_key=config.credentials.secret_access_key if config.credentials else None,
            bdb_lcpu_mask=bdb_lcpu_mask,
            s3_lcpu_mask=s3_lcpu_mask,
            s3_thread_pool_size=config.s3_thread_pool_size,
        )

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


def _build_encryption(cluster, backup: Backup) -> backup_manifest.Encryption:
    """Describe where an encrypted backup's key lives.

    The dependency on a KMS is not removed -- it is written down. Nothing before
    this recorded it at all, so an encrypted backup's key was reachable only by
    someone who already knew which cluster had made it and how that cluster was
    configured.
    """
    descriptor = backup_manifest.KeyDescriptor(
        kms="local",
        dek_path=backup_dek_path(cluster.get_id(), backup.uuid),
        kek_name=backup_kek_name(backup.uuid),
    )
    if cluster.hashicorp_vault_settings is not None:
        vault = cluster.hashicorp_vault_settings
        descriptor = descriptor.model_copy(update={
            "kms": "hashicorp_vault",
            "vault_base_url": vault.base_url,
            "transit_mount": vault.transit_mount,
            "kv_mount": vault.kv_mount,
        })

    return backup_manifest.Encryption(encrypted=True, descriptor=descriptor)


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
        backup.encryption = _build_encryption(cluster, backup).model_dump(mode="json")

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


def _require_importable(pending: dict) -> None:
    """Refuse a batch of manifests that would not form something restorable.

    An import that lands a backup whose ancestors are missing produces a record
    that looks restorable in ``backup list`` and fails only when someone tries it
    -- typically during the recovery it was meant to serve. Each manifest names
    its predecessor, so this is answerable up front by walking the batch.

    Separate from ``require_restorable`` because the rule is about the batch, not
    about a chain: whether every ancestor is either in this import or already in
    the database is a question only the importer can ask. The rules that ARE about
    the chain are deferred to it.

    Raises:
        PreconditionError: A chain is incomplete, cyclic, too long for the data
            plane, or spans buckets.
    """
    for backup_id, manifest in pending.items():
        chain, seen = [manifest], {backup_id}

        while (previous := chain[-1].prev_backup_id) is not None:
            if previous in seen:
                raise PreconditionError(
                    f"Backup {backup_id} has a cyclic chain at {previous}")
            seen.add(previous)

            if previous in pending:
                chain.append(pending[previous])
                continue

            if not _backup_exists(previous):
                raise PreconditionError(
                    f"Backup {backup_id} is a delta against {previous}, which is "
                    "neither in this import nor already known. A backup cannot "
                    "be restored without its chain.")

            # Already imported, and checked then. Its own ancestry still counts
            # towards the length the data plane has to accept.
            chain.extend(db_controller.get_backup_chain(previous))
            break

        # Manifests carry their location as a value, so coherence over the batch
        # is a plain comparison; require_restorable wants Backup records, and
        # these are not in the database yet.
        divergent = next(
            (m for m in chain if m.location != manifest.location), None)
        if divergent is not None:
            raise PreconditionError(
                f"Backup {backup_id} shares a chain with {divergent.backup_id}, "
                "which is in a different bucket or encoding")

        if not chain_fits(len(chain)):
            raise PreconditionError(
                f"The chain of backup {backup_id} is {len(chain)} backups long; "
                f"the data plane accepts at most "
                f"{constants.BACKUP_MAX_CHAIN_LENGTH}.")


def _backup_exists(backup_id: str) -> bool:
    try:
        db_controller.get_backup_by_id(backup_id)
    except KeyError:
        return False
    return True


def import_backups(manifests: Iterable[backup_manifest.BackupManifest],
                   cluster_id=None) -> int:
    """Register backups described by manifests into this cluster's database.

    The backups keep their original ids -- both their uuid and their s3_id,
    which names their objects in the bucket and therefore cannot be reassigned.

    Args:
        manifests: validated manifests, from `discover_backups`,
            `export_backups`, or a file parsed into them. Taking the models
            rather than dicts means "is this a manifest at all" is answered by
            whoever read the bytes -- the API by its request body's type, the CLI
            when it parses the file -- and reported where the input came from.
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
    pending: dict = {}
    for manifest in manifests:
        backup_id = manifest.backup_id

        if backup_id in pending:
            raise ValueError(f"Backup {backup_id} is listed more than once")

        try:
            existing = db_controller.get_backup_by_id(backup_id)
        except KeyError:
            pending[backup_id] = manifest
        else:
            raise PreconditionError(f"Backup {backup_id} already exists in cluster {existing.cluster_id}")

    _require_importable(pending)

    for backup_id, manifest in pending.items():
        backup = Backup()
        backup.uuid = backup_id
        backup.s3_id = manifest.s3_id
        backup.cluster_id = cluster_id or manifest.source.cluster_id
        backup.lvol_id = manifest.volume.lvol_id
        backup.lvol_name = manifest.volume.lvol_name
        backup.snapshot_id = manifest.volume.snapshot_id
        backup.snapshot_name = manifest.volume.snapshot_name
        backup.node_id = manifest.source.node_id
        backup.prev_backup_id = manifest.prev_backup_id or ""
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
    except (KeyError, ValueError) as e:
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
