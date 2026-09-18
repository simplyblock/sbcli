"""Declared secondary indices for the FoundationDB-backed models.

Entities live under a flat key ``object/<ClassName>/<id>``, so the only access
path :meth:`BaseModel.read_from_db` can offer is a prefix scan. Every lookup
that is not "by full primary key" therefore used to be a full-table scan plus
an in-memory filter. An :class:`Index` declares, on the model itself, a value
the write path derives from each record and stores under a second key:

    index/<ClassName>/<index-name>/<value...>/<entity-id>   -> b''
    index/<ClassName>/<index-name>/<value...>               -> entity id  (Unique)
    index_meta/<ClassName>/<index-name>                     -> state record

A non-unique entry stores nothing. The entity's ``get_id()`` — the suffix under
``object/<Class>/`` — is already the tail of the entry's own key, and a second
copy of it in the value could only ever disagree with the first.
:meth:`Index.entry_id` reads it back by skipping the index's :attr:`~Index.arity`
value segments, which is unambiguous because a value segment is escaped
(:func:`escape`) while the id is appended raw: the id is the only part of a key
that may contain ``/``. That is what keeps the scheme working for the classes
whose ``get_id()`` embeds a parent (``JobSchedule``, ``Backup``,
``ReplicationPolicy``, ...), where indexing the bare uuid is precisely what
turns a lookup into a point read and the id it yields is ``<cluster>/<date>/<uuid>``.

The ``Unique`` variant omits the trailing entity id, so the key *is* the
constraint: two entities carrying the same value collide on one key and the
write transaction detects it. Its value is not a duplicate of anything — it is
the only record of which entity holds the key.

Stdlib-only leaf module — importable from ``models/`` without a cycle, exactly
like :mod:`simplyblock_core.models.watches`. In particular it does not import ``fdb``:
the binding injects its API at ``fdb.api_version()`` time and is stubbed out in
the unit tier, and models must stay importable without it.
"""

from collections.abc import Callable, Iterable, Sequence
from typing import Any, cast

INDEX_PREFIX = 'index/'
INDEX_META_PREFIX = 'index_meta/'

#: Writes maintain the index, reads do NOT trust it: it may still be
#: incomplete. The state every index starts in.
STATE_BUILDING = 'building'
#: The backfill completed; reads use the index.
STATE_READY = 'ready'
#: Writes skip the index and reads fall back to a scan. The kill switch.
STATE_DISABLED = 'disabled'

STATES = (STATE_BUILDING, STATE_READY, STATE_DISABLED)

#: Width of the zero-padded decimal encoding integers get in an ``ordered``
#: index. 20 digits hold any unsigned 64-bit value; it is the width the
#: hand-rolled ``lvol_snaps/`` keys already used.
ORDERED_INT_WIDTH = 20


class UniqueIndexViolation(Exception):
    """A write would give two live entities the same value of a unique index.

    Never an expected outcome: every user-facing uniqueness rule is enforced by
    a pre-check that returns a clean error long before the write. Reaching this
    means the pre-check did not run or the data is already inconsistent, so it
    propagates as an invariant breach rather than being mapped to a 4xx.
    """

    def __init__(self, model_name: str, index_name: str, values: Sequence[Any],
                 holder: str, candidate: str) -> None:
        super().__init__(
            f'{model_name}.{index_name}={tuple(values)!r} is already held by '
            f'{holder!r}, cannot assign it to {candidate!r}')
        self.model_name = model_name
        self.index_name = index_name
        self.values = tuple(values)
        self.holder = holder
        self.candidate = candidate


def escape(segment: str) -> str:
    """Percent-escape the two characters that are structural in an index key.

    ``%`` first, so the escapes introduced for ``/`` are never re-escaped;
    :func:`unescape` undoes them in the mirrored order.
    """
    return segment.replace('%', '%25').replace('/', '%2F')


def unescape(segment: str) -> str:
    return segment.replace('%2F', '/').replace('%25', '%')


def encode_value(value: Any, *, ordered: bool = False) -> str:
    """One index-key segment for one extracted value.

    Percent-escaping is not byte-order preserving, so an ``ordered`` index
    cannot use it: its values are restricted to non-negative integers (encoded
    zero-padded to a fixed width, which *is* lexicographic) and to strings that
    contain neither ``/`` nor ``%`` and therefore need no escaping at all.
    """
    if not ordered:
        return escape('' if value is None else str(value))

    if isinstance(value, bool):
        raise ValueError('an ordered index cannot encode a bool')
    if isinstance(value, int):
        if value < 0:
            raise ValueError(
                f'an ordered index cannot encode the negative value {value}: '
                'zero-padding only preserves order for non-negative integers')
        return f'{value:0{ORDERED_INT_WIDTH}d}'
    text = '' if value is None else str(value)
    if '/' in text or '%' in text:
        raise ValueError(
            f'an ordered index cannot encode {text!r}: escaping "/" or "%" '
            'would break the lexicographic ordering the index exists for')
    return text


def _is_blank(value: Any) -> bool:
    return value is None or value == ''


class Index:
    """A non-unique secondary index on a model class.

    ``extract`` says how to derive the indexed values from a record and comes
    in three forms:

    * a field name — the common case, and the default when only a name is
      given, so ``Index('pool_uuid')`` indexes ``obj.pool_uuid``;
    * a tuple of field names, for a composite index. Passing the tuple as the
      *name* derives the index name from it, so
      ``Unique(('pool_uuid', 'lvol_name'))`` is the whole declaration;
    * a callable returning an iterable of value tuples — the form that lets a
      single record contribute many entries (one ``StorageNode`` record holds
      every one of its devices) or index through a nested model. It declares
      ``arity``, the width of every tuple it returns, which the field forms
      derive from the fields themselves.

    A tuple with a blank (``None`` or ``''``) component is dropped rather than
    indexed: a blank is never a meaningful thing to look up, and indexing it
    would pile every unset record onto one key.

    ``ordered=True`` promises the index's values are range-queryable, which
    constrains their encoding — see :func:`encode_value`.
    """

    unique = False

    def __init__(self, name, extract=None, *, ordered: bool = False,
                 arity: int | None = None) -> None:
        if isinstance(name, tuple):
            if extract is None:
                extract = name
            name = '+'.join(name)
        if not isinstance(name, str) or not name:
            raise ValueError('an index needs a non-empty name')
        if '/' in name:
            raise ValueError(f'index name {name!r} must not contain "/"')

        if extract is None:
            extract = name

        self.name: str = name
        self.ordered: bool = ordered
        self.fields: tuple[str, ...] | None
        self._callable: Callable[[Any], Iterable[Sequence[Any]]] | None

        if isinstance(extract, str):
            self.fields, self._callable = (extract,), None
        elif isinstance(extract, tuple):
            if not extract or not all(isinstance(f, str) and f for f in extract):
                raise ValueError(f'index {name!r}: extract tuple must name fields')
            self.fields, self._callable = extract, None
        elif callable(extract):
            self.fields = None
            self._callable = cast(Callable[[Any], Iterable[Sequence[Any]]], extract)
        else:
            raise TypeError(
                f'index {name!r}: extract must be a field name, a tuple of '
                f'field names or a callable, not {type(extract).__name__}')

        if self.fields is not None:
            if arity is not None:
                raise ValueError(
                    f'index {name!r}: arity is derived from the fields it names')
            self._arity = len(self.fields)
        elif arity is None or arity < 1:
            raise ValueError(
                f'index {name!r}: a callable extractor must declare the arity of '
                'the tuples it returns — reading an entry back means skipping '
                'exactly that many value segments to reach the entity id')
        else:
            self._arity = arity

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.name!r})'

    @property
    def arity(self) -> int:
        """Number of value segments a key carries."""
        return self._arity

    def tuples(self, obj) -> list[tuple]:
        """Every value tuple ``obj`` contributes to this index."""
        if obj is None:
            return []
        if self._callable is not None:
            raw: Iterable[Sequence[Any]] = self._callable(obj)
        else:
            assert self.fields is not None
            raw = [tuple(getattr(obj, field) for field in self.fields)]
        return [
            tuple(values) for values in raw
            if not any(_is_blank(value) for value in values)
        ]

    def value_path(self, values: Sequence[Any]) -> str:
        """The ``<value...>`` part of a key, escaped and joined."""
        return '/'.join(encode_value(value, ordered=self.ordered) for value in values)

    def _reject_blank(self, values: Sequence[Any]) -> None:
        """No index key ever carries a blank segment.

        :meth:`tuples` drops a blank tuple rather than indexing it, so a blank
        names no stored key. Encoding one anyway would build a key the write
        path never writes — which reads back as "nothing matches" through the
        index and, because a blank value path is also the empty prefix, as
        "everything matches" through the scan.
        """
        if any(_is_blank(value) for value in values):
            raise ValueError(
                f'index {self.name!r}: blank component in {tuple(values)!r} — '
                'a blank is never indexed, so no entry can carry it')

    def _full_path(self, values: Sequence[Any]) -> str:
        """:meth:`value_path` of a COMPLETE value tuple.

        An entry whose key carries the wrong number of value segments cannot be
        read back — :meth:`entry_id` would return a value where the id belongs —
        so a short or long tuple is refused where the key is built rather than
        stored and found later.
        """
        if len(values) != self.arity:
            raise ValueError(
                f'index {self.name!r} carries {self.arity} value(s) per entry, '
                f'got {len(values)}: {tuple(values)!r}')
        self._reject_blank(values)
        return self.value_path(values)

    def _query_path(self, values: Sequence[Any]) -> str | None:
        """The value path a lookup for ``values`` selects on, or ``None`` for all.

        The single derivation behind both read paths — :meth:`prefix` for the
        index and :meth:`match_paths` for the scan — so neither can grow a
        predicate the other does not have.

        A tuple wider than :attr:`arity` is refused rather than encoded: an id
        is appended to an entry key raw, so an over-long prefix can land inside
        the id of a class whose ``get_id()`` embeds separators and select
        entries the scan would never return.
        """
        if len(values) > self.arity:
            raise ValueError(
                f'index {self.name!r} carries {self.arity} value(s) per entry, '
                f'cannot look up {len(values)}: {tuple(values)!r}')
        self._reject_blank(values)
        return self.value_path(values) if values else None

    def base(self, model_cls) -> str:
        return f'{INDEX_PREFIX}{model_cls.__name__}/{self.name}/'

    def key(self, model_cls, values: Sequence[Any], entity_id: str) -> bytes:
        return (self.base(model_cls) + self._full_path(values) + '/' + str(entity_id)).encode()

    def entry_value(self, entity_id: str) -> bytes:
        """What the write path stores under :meth:`key`.

        Nothing: the id is the key's own tail. See the module docstring.
        """
        return b''

    def entry_id(self, model_cls, key: bytes, value: bytes) -> str:
        """The entity a stored entry names.

        Read off the key by skipping its :attr:`arity` value segments. Those are
        escaped and the id is not, so what remains is the id whole — including
        the embedded separators of a composite ``get_id()``.
        """
        base = self.base(model_cls)
        text = key.decode()
        if not text.startswith(base):
            raise ValueError(
                f'{text!r} is not an entry of {model_cls.__name__}.{self.name}')
        parts = text[len(base):].split('/', self.arity)
        if len(parts) <= self.arity:
            raise ValueError(f'{text!r} carries no entity id')
        return parts[-1]

    def keys(self, model_cls, obj) -> set[bytes]:
        """Every index key ``obj`` owns. Empty for ``obj is None``."""
        if obj is None:
            return set()
        entity_id = obj.get_id()
        return {self.key(model_cls, values, entity_id) for values in self.tuples(obj)}

    def prefix(self, model_cls, values: Sequence[Any]) -> bytes:
        """Range-read prefix for a lookup by (a prefix of) the index values.

        The trailing ``/`` is what keeps the read exact: without it a lookup
        for ``abc`` would also return the entries of ``abcd``.
        """
        path = self._query_path(values)
        base = self.base(model_cls)
        return (base if path is None else base + path + '/').encode()

    def match_paths(self, obj, values: Sequence[Any]) -> list[str]:
        """The value paths of ``obj`` that a lookup for ``values`` selects.

        This is what lets the scan fallback answer *exactly* what the index
        would, ordering included — the predicate is derived from the same
        declaration rather than restated at the call site.
        """
        wanted = self._query_path(values)
        out = []
        for candidate in self.tuples(obj):
            path = self.value_path(candidate)
            if wanted is None or path == wanted or path.startswith(wanted + '/'):
                out.append(path)
        return out


class Unique(Index):
    """An index the write path also enforces as a constraint.

    The key omits the entity id, so two entities carrying the same values
    collide on a single key and the write transaction refuses the second with
    :class:`UniqueIndexViolation`.
    """

    unique = True

    def key(self, model_cls, values: Sequence[Any], entity_id: str) -> bytes:
        return (self.base(model_cls) + self._full_path(values)).encode()

    def entry_value(self, entity_id: str) -> bytes:
        """The holder's id — the only place a unique entry records it."""
        return str(entity_id).encode()

    def entry_id(self, model_cls, key: bytes, value: bytes) -> str:
        return value.decode()

    def point_key(self, model_cls, values: Sequence[Any]) -> bytes:
        """The single key holding ``values``. Only defined at full arity."""
        return self.key(model_cls, values, '')


def indexes_of(model_cls) -> tuple[Index, ...]:
    return tuple(getattr(model_cls, '_INDEXES', ()) or ())


def get_index(model_cls, name) -> Index:
    """Resolve an index by name, or by identity when one is passed straight in."""
    if isinstance(name, Index):
        return name
    for index in indexes_of(model_cls):
        if index.name == name:
            return index
    raise KeyError(f'{model_cls.__name__} declares no index {name!r}')


def index_meta_key(model_cls, index_name) -> bytes:
    """State-record key for one index.

    ``index_meta/`` is disjoint from ``index/`` (neither is a prefix of the
    other) and from the ``object/`` entity scans.
    """
    name = index_name.name if isinstance(index_name, Index) else index_name
    return f'{INDEX_META_PREFIX}{model_cls.__name__}/{name}'.encode()
