# coding=utf-8
"""When a backup chain can be restored, and when it cannot.

Predicates rather than assertions, so a rule can answer a question as well as
block an operation -- ``location_holds_backups`` is a thing a caller may want to
know without being refused. :func:`require_restorable` is the one place that turns
a false answer into a refusal, so the wording an operator sees is written once
rather than at each entry point that enforces the same rules.
"""
from typing import Optional

from simplyblock_core import constants
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup_config import BackupLocation


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
