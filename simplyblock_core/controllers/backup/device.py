# coding=utf-8
"""The S3 devices a node reads and writes backups through.

A device holds exactly one bucket with one set of credentials, so a node runs one
for the cluster's own backup bucket and, during a restore from somebody else's
bucket, a second one attached for the duration. Naming them is part of this
module's job: a restore device's name derives from the backup id, so a retry
re-derives it instead of leaking a device per attempt.
"""
import logging

from botocore.exceptions import BotoCoreError, ClientError

from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

logger = logging.getLogger()


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


def primary_s3_bdev_name(node: StorageNode) -> str:
    """The S3 device holding the cluster's own backup bucket."""
    return f"s3_{node.lvstore}"


def restore_s3_bdev_name(backup_id: str) -> str:
    """Name of the device created to read a foreign bucket for one restore.

    Derived from the backup id so a retry re-derives the same name rather than
    leaking a device per attempt.
    """
    return f"s3_restore_{backup_id[:8]}"


def create_restore_s3_bdev(node: StorageNode, config: BackupConfig, name: str) -> None:
    """Attach a second S3 device to a node, for a bucket that is not its own.

    A restore from a foreign bucket needs different credentials, a different
    endpoint and a different region than the node's own backup device carries.
    Since a device holds exactly one bucket, the way to read another one is to
    create another device -- which the lvstore supports, its transfer devices
    being a list.

    Idempotent, because a restore re-enters it on every attempt: a device left
    over from an earlier attempt is the device this one wants, its name deriving
    from the backup and so naming the same bucket. Adopting it is not just an
    optimisation -- the data plane refuses a duplicate name, which would strand
    every retry that happens without a node restart in between. The lvstore
    attach still runs, since an earlier attempt may have failed between the two
    calls; the data plane hands back the transfer device it already has.

    The caller owns the result and must delete it when the restore ends.
    """
    rpc_client = node.rpc_client()
    bdb_lcpu_mask, s3_lcpu_mask = _compute_s3_cpu_masks(node)

    try:
        if rpc_client.get_bdevs(name):
            logger.info("Reusing restore S3 device %s on node %s", name, node.get_id())
        else:
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
