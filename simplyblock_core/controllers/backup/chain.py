# coding=utf-8
"""The restorable unit: a backup and every backup it is a delta against.

A single backup is a delta. What can actually be restored is the line of them
reaching back to a full backup, and that -- not its newest link -- is what a
user means by "a backup". :class:`BackupChain` is that unit.

Where it lives and whether it is encrypted are properties of the chain rather
than of each link, because a restore reads every link through one S3 device and
decrypts all of it with one key: nothing anywhere in the stack could express a
chain split across two buckets or half encrypted. Keeping those two on the chain
is what makes an incoherent one unrepresentable instead of merely refusable --
:meth:`BackupChain.assemble` is the only constructor, and it refuses. A
``BackupChain`` in hand is one that agrees with itself, so the rules left on
:meth:`BackupChain.require_restorable` are only the ones construction could not
settle.

Chains are derived, never stored. See ``manifest.BackupManifest.prev_backup_id``
for why: a stored chain would have to be rewritten in every descendant's manifest
each time a merge folded a backup away.

A chain is walked over ``Backup`` records, over ``BackupManifest`` documents, or
over both at once when an import reaches back into what is already stored -- the
two describe the same thing in two shapes. Neither is asked to implement an
interface for it, and neither is copied into one: the four facts the rules need
are read off a link where they are needed, by the ``_``-prefixed accessors
below, which are also where a manifest's id (a ``UUID``) and a record's (a
``str``) are reconciled.

Nothing here reads the database. Callers supply the population to walk over.
"""
from dataclasses import dataclass
from typing import (
    Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TypeVar, Union)
from uuid import UUID

from simplyblock_core import constants
from simplyblock_core.controllers.backup.manifest import BackupManifest
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupLocation


#: What a chain can be made of. ``Backup`` records and ``BackupManifest``
#: documents describe the same backup in two shapes; see the note on
#: ``controller.build_manifest`` for where they differ.
Link = Union[Backup, BackupManifest]

#: Preserved through a walk, so a population known to hold only records yields
#: only records -- which is what lets ``of_backups`` read its head's location and
#: encryption off the head itself.
L = TypeVar("L", bound=Link)


def location_of(manifest: BackupManifest, location: BackupLocation) -> BackupLocation:
    """Where a backup described by a manifest lives: the bucket it was found in,
    its own encoding.

    A manifest describes its objects but not how to reach them, so the bucket,
    region and endpoint come from whoever read it -- which is what makes a
    replicated bucket importable at all, rather than importable and then
    unrestorable because every record points back at the original.

    Only the encoding is the manifest's to state, and only ``with_compression``
    is still variable; the key layout it also records has one value that holds
    backups, which :meth:`BackupChain.require_restorable` already requires of
    the bucket.

    Narrowed rather than copied, so a ``BackupConfig`` passed in as the location
    it also is cannot carry its credentials into a stored record.
    """
    return BackupLocation.model_validate({
        **location.model_dump(include=set(BackupLocation.model_fields)),
        "with_compression": manifest.dataplane.with_compression,
    })


def _id_of(link: Link) -> UUID:
    """A link's own backup id, whichever shape the link comes in."""
    return UUID(link.uuid) if isinstance(link, Backup) else link.backup_id


def _prev_id_of(link: Link) -> Optional[UUID]:
    """The backup a link is a delta against, or ``None`` at the root of a chain."""
    if isinstance(link, Backup):
        return UUID(link.prev_backup_id) if link.prev_backup_id else None

    return link.prev_backup_id


def _location_of(link: Link, location: BackupLocation) -> BackupLocation:
    """Where a link's objects live.

    A record carries its own and ignores ``location``; a manifest states only its
    encoding and is read against the bucket it was found in.
    """
    return link.get_location() if isinstance(link, Backup) else location_of(link, location)


def _encrypted(link: Link) -> bool:
    return link.encrypted if isinstance(link, Backup) else link.encryption is not None


def _index(links: Iterable[L]) -> Dict[UUID, L]:
    return {_id_of(link): link for link in links}


def _walk(head_id: UUID, links: Mapping[UUID, L], *,
          absence: str = "not among the backups given") -> List[L]:
    """Follow ``prev_backup_id`` from a head back to a root, oldest first.

    Args:
        absence: How to describe a predecessor that could not be found, as a
            clause completing "which is ...". Where a caller looked is the one
            part of this refusal the walk cannot know, and it is the part an
            operator needs: an import has two places it could have come from.

    Raises:
        PreconditionError: A link names a predecessor that is not in the
            population, so the chain cannot be completed from it, or one that is
            already in the chain. Reported rather than truncated: a short chain
            restores a volume with holes in it.
    """
    if head_id not in links:
        raise PreconditionError(f"Backup {head_id} is {absence}")

    chain = [links[head_id]]
    seen = {head_id}

    while (previous := _prev_id_of(chain[-1])) is not None:
        if previous in seen:
            raise PreconditionError(
                f"Backup {head_id} has a cyclic chain at {previous}")
        seen.add(previous)

        if previous not in links:
            raise PreconditionError(
                f"Backup {_id_of(chain[-1])} is a delta against {previous}, "
                f"which is {absence}. A backup cannot be restored without its "
                "chain.")

        chain.append(links[previous])

    chain.reverse()
    return chain


def _describe_incoherence(links: Sequence[Link], location: BackupLocation,
                          encrypted: bool) -> str:
    for link in links:
        if (elsewhere := _location_of(link, location)) != location:
            return (
                f"backup {_id_of(link)} lives in bucket "
                f"{elsewhere.bucket_name}, but the rest of its chain "
                f"is in {location.bucket_name}. A chain cannot span buckets or "
                "encodings; start a new chain with a full backup")

    return (
        "a chain cannot mix encrypted and unencrypted backups: "
        + ", ".join(f"{_id_of(link)}={'encrypted' if _encrypted(link) else 'plain'}"
                    for link in links)
        + f", chain={'encrypted' if encrypted else 'plain'}")


@dataclass(frozen=True)
class BackupChain:
    """A backup and every backup it is a delta against, oldest first.

    See the module docstring for why ``location`` and ``encrypted`` are held here
    rather than read off each link, and why that makes this the only place
    coherence has to be checked.
    """

    location: BackupLocation
    encrypted: bool

    #: Oldest first, ending at the head -- the reverse of the order the data
    #: plane's ``s3_ids`` argument wants; see :meth:`s3_ids_newest_first`.
    links: Tuple[Link, ...]

    @classmethod
    def assemble(cls, location: BackupLocation, encrypted: bool,
                 links: Iterable[Link]) -> "BackupChain":
        """The only place a chain is constructed, and so the only place coherence
        is enforced.

        Takes the order as given rather than walking ``prev_backup_id``, for the
        caller that already knows it. At creation that caller is the one holding
        the snapshot chain, and the pair it declares is the *intended* one rather
        than one read off an existing link -- which is what catches a cluster
        repointed mid-chain, or a volume that has gained encryption since its
        last backup, instead of inheriting the old answer.

        Raises:
            PreconditionError: A link lives elsewhere, is encoded differently, or
                disagrees about whether it is encrypted.
        """
        links = tuple(links)

        if any(_location_of(link, location) != location for link in links) or any(
                _encrypted(link) != encrypted for link in links):
            raise PreconditionError(
                "This chain cannot be restored as a unit: "
                + _describe_incoherence(links, location, encrypted))

        return cls(location=location, encrypted=encrypted, links=links)

    @classmethod
    def of_backups(cls, head_id: UUID, backups: Iterable[Backup]) -> "BackupChain":
        """The chain ending at a stored backup.

        Its location and encryption are the head's, since a stored record carries
        both; the rest of the chain is held to them.
        """
        links = _walk(head_id, _index(backups), absence="not a known backup")
        head = links[-1]
        return cls.assemble(head.get_location(), head.encrypted, links)

    @classmethod
    def of_manifests(cls, head: BackupManifest, manifests: Iterable[BackupManifest],
                     location: BackupLocation) -> "BackupChain":
        """The chain ending at a manifest read from a bucket.

        ``location`` is supplied because a manifest describes its objects but not
        how to reach them -- which is what makes a replicated bucket readable as
        itself rather than as the original it was copied from.
        """
        links = _walk(head.backup_id, _index(manifests))
        return cls.assemble(location_of(head, location),
                            head.encryption is not None, links)

    @classmethod
    def importing(cls, head: BackupManifest, pending: Mapping[UUID, BackupManifest],
                  stored: Iterable[Backup], location: BackupLocation) -> "BackupChain":
        """The chain of a manifest being imported.

        A chain reaching back before this batch spans both shapes of the same
        thing -- manifests being imported and records already stored. Indexing
        them into one population is all it takes for the walk to cross that
        boundary, and for the rules to apply across it.
        """
        population: Dict[UUID, Link] = {**_index(pending.values()), **_index(stored)}
        links = _walk(head.backup_id, population,
                      absence="neither in this import nor already known")
        return cls.assemble(location_of(head, location),
                            head.encryption is not None, links)

    def require_restorable(self, *, length: Optional[int] = None,
                           completed: bool = False,
                           what: str = "This chain") -> None:
        """Refuse a chain that could not be restored, naming the rule it breaks.

        Only the rules construction could not settle: whether the location holds
        backups at all, whether the chain is short enough for the data plane,
        and -- for a restore -- whether every link has actually finished.
        Coherence is absent because a chain that exists is already coherent.

        Applied at creation, at import and at restore, because each is a point
        where a chain could otherwise become unrestorable without anyone
        noticing, and each used to find out from whatever failed first, usually
        the data plane mid-operation.

        Args:
            length: The eventual length, where the chain is still being built and
                so is longer than the links it has: at creation the ancestors are
                snapshots that have no backup yet.
            completed: Also require every link to be a finished backup. Only
                records can fail this -- a manifest in a bucket describes a backup
                that is finished by definition.

        Raises:
            PreconditionError: One of the rules above does not hold.
        """
        # snapshot_backups=False selects the secondary-tiering object layout,
        # whose keys are {tiering_id}/{lpgi}; the restore path addresses
        # {s3_id}/{mid}/{extent} and so could never find them.
        if not self.location.snapshot_backups:
            raise PreconditionError(
                f"Bucket {self.location.bucket_name} is configured with snapshot_backups "
                "disabled, which selects the secondary-tiering object layout. "
                "Backups cannot be written there.")

        # Refused here rather than by the data plane, which would only find out
        # mid-transfer, with objects already written.
        length = len(self.links) if length is None else length
        if length < 1:
            raise PreconditionError(f"{what} holds no backups")
        if length > constants.BACKUP_MAX_CHAIN_LENGTH:
            raise PreconditionError(
                f"{what} is {length} backups long; the data plane accepts at most "
                f"{constants.BACKUP_MAX_CHAIN_LENGTH}. Merge older backups to "
                "shorten the chain, or start a new chain with a full backup.")

        if completed and (incomplete := [
            link for link in self.links
            if isinstance(link, Backup) and link.status != Backup.STATUS_COMPLETED
        ]):
            raise PreconditionError(
                "Incomplete backups in chain: "
                + ", ".join(link.uuid for link in incomplete))

    def records(self) -> List[Backup]:
        """The chain's links as stored records, oldest first.

        A chain walked over manifests describes backups this cluster may know
        nothing about, so anything wanting a record -- its status, its node, the
        volume it belongs to -- has to say so. Checked rather than assumed, since
        which shape a chain holds is decided by which constructor built it.

        Raises:
            TypeError: This chain was built from manifests. A caller asking for
                records from one is asking the bucket for something only the
                database has.
        """
        if not all(isinstance(link, Backup) for link in self.links):
            raise TypeError(
                "This chain was built from manifests, so it holds no stored records")

        return [link for link in self.links if isinstance(link, Backup)]

    def s3_ids_newest_first(self) -> List[int]:
        """The chain as the data plane's ``bdev_lvol_s3_recovery`` wants it.

        Newest first: it claims each cluster for the first id that offers it
        (``prepare_s3_clusters`` is first-writer-wins), so the latest incremental
        data has to win and older backups fill whatever gaps remain.
        """
        return [link.s3_id for link in reversed(self.links)]
