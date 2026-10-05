"""Stored form of one leased, named mutex.

A single record type serves every lock domain, in a keyspace of its own:

    lock/<name>  ->  [10-byte versionstamp][{"name": ..., "owner": ..., ...}]

``<name>`` is namespaced as ``"<domain>/<resource-id>"``, so a new use case adds
a namespace rather than a model. That makes ``lock/`` a fifth top-level prefix
beside ``object/``, ``index/``, ``index_meta/`` and ``watch_index/``; nothing in
the tree walks the keyspace by model, so it costs no adaptation.

Stdlib-only leaf module — importable from ``models/`` without a cycle and
without ``fdb``, exactly like :mod:`simplyblock_core.models.indices` and
:mod:`simplyblock_core.models.watches`. The ``fdb`` calls that write these bytes
live in :mod:`simplyblock_core.models.lock.store`.
"""

import json
import struct
from dataclasses import asdict, dataclass

KEY_PREFIX = 'lock/'

#: Bytes of versionstamp FoundationDB writes into the head of the value at
#: commit: eight of commit version, big-endian, plus two of intra-batch order.
FENCE_LEN = 10
EMPTY_FENCE = b'\x00' * FENCE_LEN


def key(name: str) -> bytes:
    """The FoundationDB key holding ``name``'s lease.

    The name is the whole tail, so it needs none of the escaping
    :func:`~simplyblock_core.models.indices.escape` performs on a middle
    segment, and the ``/`` a namespaced name already contains makes a domain a
    prefix range: ``lock/cluster_add/`` enumerates every node-add lock. Point
    reads stay exact, so ``"a/1"`` and ``"a/10"`` cannot collide.
    """
    return (KEY_PREFIX + name).encode()


@dataclass
class DbLockRecord:
    """One leased, named mutex, shared by every lock domain.

    A record is free once ``expires_at`` has passed: the holder is presumed
    dead, since a live one pushes that deadline forward on every heartbeat. The
    deadline is written by the holder rather than derived by the reader, so two
    control-plane versions with different lease widths — a rolling upgrade —
    each judge a record by the width its own holder promised.

    ``expires_at``, ``heartbeat_at`` and ``acquired_at`` are wall clock
    (:func:`time.time`) because they are compared across hosts — a holder's own
    self-expiry deadline is monotonic and lives on the hold, not here.
    ``heartbeat_at`` is not what expiry is judged against; it stays because it
    is what an operator reads to see when a holder last checked in, and because
    a value ahead of the reader's clock is the one available evidence that the
    two hosts disagree about the time.

    The fencing token is not a field. It is the 10-byte commit versionstamp
    FoundationDB writes into the head of the stored value, so it is minted by
    the database rather than derived from the record, and survives the key being
    deleted and recreated. A counter read from the record and incremented could
    not: ``release()`` deletes the record, so the next acquire would restart
    from nothing and two unrelated grants of one name would present the same
    number.

    Deliberately not a ``BaseModel``. That base would contribute a key format
    this record does not want, nine unused inherited fields, and a ``name``
    attribute that is key material — ``get_db_id()`` spells the key as
    ``object/<name>/<get_id()>`` — which this record's own ``name`` would
    shadow.
    """

    name: str
    owner: str = ''
    acquired_at: float = 0.0
    heartbeat_at: float = 0.0
    expires_at: float = 0.0

    def to_stamped_value(self) -> bytes:
        """Value for ``tr.set_versionstamped_value``: placeholder, body, offset.

        FoundationDB strips the trailing little-endian offset and overwrites the
        ten bytes at that position with this transaction's versionstamp. The
        offset is mandatory from API version 520 on; the cluster runs 730.
        """
        return EMPTY_FENCE + json.dumps(asdict(self)).encode() + struct.pack('<I', 0)

    def to_value(self, fence: bytes) -> bytes:
        """Value for a plain set that must preserve an already-minted token.

        The refresh path: a heartbeat rewrites the body and leaves the head
        alone, so only a genuine change of hands moves the fence. Re-stamping
        here would move the token on every heartbeat and fence the live holder
        out of its own critical section.
        """
        return fence + json.dumps(asdict(self)).encode()

    @classmethod
    def from_value(cls, raw: bytes) -> tuple['DbLockRecord', bytes]:
        """Split the stored value into the record and its fencing token.

        Reads field by field, so a record written by another version of the
        control plane still loads: ``BaseModel.from_dict`` ignores unknown keys
        and defaults missing ones, where ``cls(**data)`` would raise on both.
        That tolerance is the one property of the base worth carrying over.
        """
        fence, body = bytes(raw[:FENCE_LEN]), bytes(raw[FENCE_LEN:])
        data = json.loads(body)
        return cls(
            name=data['name'],
            owner=data.get('owner', ''),
            acquired_at=float(data.get('acquired_at', 0.0)),
            heartbeat_at=float(data.get('heartbeat_at', 0.0)),
            expires_at=float(data.get('expires_at', 0.0)),
        ), fence
