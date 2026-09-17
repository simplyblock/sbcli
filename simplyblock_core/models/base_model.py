import json
from collections import ChainMap
from collections.abc import Callable, Mapping
from inspect import get_annotations, ismethod, isfunction
from types import UnionType
from typing import ClassVar, TypeVar, Union, cast, get_args, get_origin

from pydantic import SecretBytes, SecretStr

from simplyblock_core import indices, watches


_T = TypeVar('_T')

#: `typing.Union[X, Y]` and `X | Y` are distinct objects before 3.14, where
#: `types.UnionType` became `typing.Union`.
_UNION_ORIGINS = (Union, UnionType)


class _DefaultFactory:
    """Marker standing in for a field default that must be built per instance."""

    __slots__ = ('factory',)

    def __init__(self, factory: Callable[[], object]) -> None:
        self.factory = factory


def default_factory(factory: Callable[[], _T]) -> _T:
    """Declare a per-instance default for a mutable model field.

    Model defaults are plain class attributes, so a literal
    ``nodes: List[str] = []`` stores ONE list on the class. Every instance
    that receives no value for the field then aliases — and mutates — that
    single object, so one ``lvol.nodes.append(...)`` is visible on every other
    lvol and on every instance created later in the process.

    This mirrors ``dataclasses.field(default_factory=...)``: the class holds
    only a marker, and :meth:`BaseModel.get_attrs_map` calls the factory once
    per instance. The declared return type is the factory's result so static
    type checkers still see the field's real type.
    """
    return cast(_T, _DefaultFactory(factory))


def _is_fdb_store(kv_store) -> bool:
    """True when ``kv_store`` is a live FoundationDB handle.

    Index maintenance has to READ the record it is replacing, to move the
    entries that record owns; unlike a plain single-key write it cannot run
    against a store that only records what it is told. So a caller that hands a
    model a mock store — several controller tests patch ``DBController`` inside
    the module under test — gets the plain write, exactly as before.

    The names are looked up at call time: the ``fdb`` binding injects its API at
    ``fdb.api_version()`` time, and the unit tier's stub defines neither, which
    is the same "no indices here" answer.
    """
    import fdb
    handles = tuple(
        handle for handle
        in (getattr(fdb, 'Database', None), getattr(fdb, 'Transaction', None))
        if isinstance(handle, type)
    )
    return bool(handles) and isinstance(kv_store, handles)


def _detached(value: _T) -> _T:
    """A copy of ``value`` that shares no mutable structure with it.

    ``from_dict`` is handed a payload it does not own — an FDB record, a
    request body, another model's ``to_dict()`` — and several of its branches
    store the payload's own list or dict on the model. Source and model then
    alias each other, so an ``append`` on either side is visible on the other
    and on every further model built from the same payload.

    ``BaseModel`` children are already built fresh by ``from_dict`` and are
    handed back as they are.
    """
    if isinstance(value, dict):
        return cast(_T, {k: _detached(v) for k, v in value.items()})
    if isinstance(value, list):
        return cast(_T, [_detached(item) for item in value])
    if isinstance(value, set):
        return cast(_T, set(value))
    if isinstance(value, bytearray):
        return cast(_T, bytearray(value))
    return value


class BaseModel:

    _STATUS_CODE_MAP: ClassVar[dict] = {}

    # When True, write_to_db()/remove() atomically maintain the watch_index/
    # version index (rollup + per-entity version keyed by watch_scope()) in the
    # same FDB transaction so watchers (SSE API) wake up. Plain class attribute,
    # not an annotation: must stay out of get_attrs_map()/to_dict().
    _WATCHED = False

    # Declared secondary indices (see simplyblock_core/indices.py).
    # write_to_db()/remove()/DBController.atomic_update() maintain every entry
    # in the SAME FDB transaction as the entity mutation, so an index can never
    # be left describing a record that was never written. Plain class
    # attribute like _WATCHED, not an annotation, so it stays out of
    # get_attrs_map()/to_dict() and is never serialized.
    _INDEXES: ClassVar[tuple] = ()

    id: str = ""
    uuid: str = ""
    name: str = ""
    status: str = ""
    deleted: bool = False
    updated_at: str = ""
    create_dt: str= ""
    remove_dt: str= ""
    object_type: str= "object"


    def __init__(self, data=None):
        self.name = self.__class__.__name__
        self.from_dict(data)

    @classmethod
    def all_annotations(cls) -> Mapping[str, type]:
        """Returns a dictionary-like ChainMap that includes annotations for all
           attributes defined in cls or inherited from superclasses."""
        return ChainMap(*(
            get_annotations(c)
            for c
            in cls.__mro__
        ))

    def get_id(self):
        return self.uuid

    @classmethod
    def _annotated_attrs(cls):
        """Per-class cache of ``[(attr, type)]`` for all public annotated data
        attributes.

        The annotation walk (``all_annotations`` -> ``inspect.get_annotations``
        over the full MRO) and the method/underscore filter are class-level
        constants, yet they used to be re-derived on EVERY object
        construction, ``to_dict`` and ``keys()`` call — measured at 6.8 ms of
        GIL-held CPU per fat StorageNode (97 nested device models) and
        ~216 ms per ``get_storage_nodes_by_cluster_id`` (32 nodes). Across
        ~30 control-plane threads this reflection convoy inflated every RPC
        round-trip (6 ms at the proxy -> 135 ms CP-observed, n=3837) and
        every FDB transaction (0.8-1.4 s inside restart port-block windows),
        pushing client-port blocks past the 6 s nvmf ack-timeout reject
        (2026-07-21 FD-reboot: 7 volumes EIO'd).

        Only the reflection is cached. Defaults still come from
        ``getattr(self, attr)`` at call time in :meth:`get_attrs_map`, so
        ``from_dict`` on a populated instance keeps its merge semantics and
        no new sharing of mutable defaults is introduced. The filter checks
        both ``ismethod`` and ``isfunction``: on an instance a class function
        appears as a bound method, but on the class (where we now evaluate)
        it is a plain function.

        ``ClassVar`` attributes are class constants shared on purpose (a
        status-code map), not per-instance data: they are excluded so they are
        neither serialized into the FDB record nor overwritten by
        ``from_dict``.
        """
        cached = cls.__dict__.get('_annotated_attrs_cache')
        if cached is None:
            cached = [
                (s, t) for s, t in cls.all_annotations().items()
                if not s.startswith("_")
                and get_origin(t) is not ClassVar
                and not ismethod(getattr(cls, s, None))
                and not isfunction(getattr(cls, s, None))
            ]
            cls._annotated_attrs_cache = cached  # type: ignore[attr-defined]
        return cached

    def get_attrs_map(self):
        attrs = {}
        for s, t in self.__class__._annotated_attrs():
            default = getattr(self, s)
            if isinstance(default, _DefaultFactory):
                default = default.factory()
            attrs[s] = {"type": t, "default": default}
        return attrs

    def get_db_id(self, use_this_id=None):
        if use_this_id:
            return "%s/%s/%s" % (self.object_type, self.name, use_this_id)
        else:
            return "%s/%s/%s" % (self.object_type, self.name, self.get_id())

    def from_dict(self, data):
        for attr, value_dict in self.get_attrs_map().items():
            value = value_dict['default']
            if data is not None and attr in data:
                dtype = value_dict['type']
                # `get_origin` is the only spelling-independent way to ask what
                # a generic annotation is: a PEP 604 union (`X | None`) carries
                # no `__origin__` before 3.14, and a bare `list` carries none on
                # any version.
                generic_origin = get_origin(dtype)
                origin = generic_origin or dtype
                args = get_args(dtype)
                value = data[attr]
                if dtype in [int, float, str, bool]:
                    try:
                        value = dtype(value)
                    except Exception:
                        if type(value) is list and dtype is int:
                            value = len(value)

                elif dtype is SecretStr:
                    value = value if isinstance(value, SecretStr) else SecretStr(value or "")
                elif dtype is SecretBytes:
                    if isinstance(value, SecretBytes):
                        pass
                    elif isinstance(value, (bytes, bytearray)):
                        value = SecretBytes(bytes(value))
                    else:
                        value = SecretBytes((value or "").encode())

                elif origin is list:
                    if args and hasattr(args[0], "from_dict"):
                        value = [args[0]().from_dict(item) for item in data[attr]]
                    else:
                        value = data[attr]
                elif origin is Mapping:
                    if args and hasattr(args[1], "from_dict"):
                        value = {item: args[1]().from_dict(data[attr][item]) for item in data[attr]}
                    else:
                        value = dtype(data[attr])
                elif origin in _UNION_ORIGINS:
                    if data[attr] is None:
                        value = None
                    else:
                        inner_types = [t for t in args if t is not type(None)]
                        inner = inner_types[0] if inner_types else None
                        if inner is not None and hasattr(inner, "from_dict"):
                            value = inner().from_dict(data[attr])
                        elif inner is SecretStr:
                            value = data[attr] if isinstance(data[attr], SecretStr) else SecretStr(data[attr] or "")
                        elif inner is SecretBytes:
                            raw = data[attr]
                            if isinstance(raw, SecretBytes):
                                value = raw
                            elif isinstance(raw, (bytes, bytearray)):
                                value = SecretBytes(bytes(raw))
                            else:
                                value = SecretBytes((raw or "").encode())
                        elif inner is not None:
                            value = inner(data[attr])
                elif generic_origin is not None:
                    # A parameterized generic with no rule of its own
                    # (`dict[str, str]`, `tuple[int, ...]`): the payload is
                    # already the right shape, and the alias is not always
                    # callable — `typing.Dict[str, str]()` raises.
                    value = data[attr]
                else:
                    value = dtype(data[attr])

                value = _detached(value)
            setattr(self, attr, value)
        self.id = self.uuid
        return self

    def to_dict(self, unwrap_secrets: bool = False):
        """Serialize to a plain dict.

        With ``unwrap_secrets=False`` (default), ``SecretStr``/``SecretBytes``
        instances stay wrapped — safe for logging, ``repr``, and ``pprint``. With
        ``unwrap_secrets=True``, wrappers are replaced by their plaintext value,
        producing a JSON-serializable structure for FoundationDB persistence.

        ``unwrap_secrets`` propagates into nested ``BaseModel`` children so a
        single ``write_to_db`` call unwraps end-to-end.
        """
        def _maybe_to_dict(value):
            if isinstance(value, BaseModel):
                return value.to_dict(unwrap_secrets=unwrap_secrets)
            if isinstance(value, (SecretStr, SecretBytes)):
                return value.get_secret_value() if unwrap_secrets else value
            if isinstance(value, dict):
                return {k: _maybe_to_dict(v) for k, v in value.items()}
            if hasattr(value, "to_dict"):
                return value.to_dict()
            return value

        result: dict = {}
        for attr in self.get_attrs_map():
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = [_maybe_to_dict(x) for x in value]
            elif isinstance(value, BaseModel):
                result[attr] = value.to_dict(unwrap_secrets=unwrap_secrets)
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = {k: _maybe_to_dict(v) for k, v in value.items()}
            elif isinstance(value, (SecretStr, SecretBytes)):
                result[attr] = value.get_secret_value() if unwrap_secrets else value
            else:
                result[attr] = value

        return result

    def get_clean_dict(self, unwrap_secrets: bool = False):
        data = self.to_dict(unwrap_secrets=unwrap_secrets)
        for key in ['name', 'object_type']:
            del data[key]
        data['status_code'] = self.get_status_code()
        return data

    def to_str(self):
        return str(self.to_dict())

    # Per-chunk row count for one range-read transaction. An unbounded
    # get_range_startswith over a large prefix (e.g. the job-task table during
    # a mass test) is a single FDB transaction: once it exceeds the 5s
    # transaction limit it fails with 1031 and the binding's on_error retry
    # restarts the SAME full scan — it never completes, and on 2026-07-16 it
    # killed TasksNodeAddRunner at cluster start.
    #
    # This is a CHUNK size, not a result cap: read_from_db() below continues
    # key-range pagination until the prefix is exhausted and always returns
    # the complete result set. Every key that exists for the whole duration
    # of the scan is returned exactly once. What the chunking does trade away
    # (when kv_store is a Database, so each chunk is its own transaction) is
    # single-snapshot isolation: a row created/deleted WHILE the scan runs
    # may or may not be included — the same guarantee class as any paginated
    # enumeration. Callers whose invariants depend on concurrent mutations
    # (e.g. free-slot accounting over all lvols) must enforce them at claim
    # time (atomic claim / re-validation), not at scan time — a single-txn
    # snapshot is equally stale by the time it is acted upon. When kv_store
    # is a Transaction the loop runs inside that one transaction and keeps
    # snapshot semantics (and its 5s budget) unchanged.
    _READ_CHUNK_SIZE = 2000

    @classmethod
    def keyspace_prefix(cls) -> bytes:
        """``<object_type>/<ClassName>/`` — the key range one class occupies.

        NOT ``get_db_id()`` of a fresh instance: a class whose ``get_id()``
        composes a parent id renders an empty record as ``object/Class//``,
        which matches nothing. The equivalent spelling at existing call sites
        is ``read_from_db(id=" ")``.
        """
        prototype = cls()
        return f'{prototype.object_type}/{prototype.name}/'.encode()

    @staticmethod
    def _next_prefix(prefix: bytes) -> bytes:
        """Smallest key strictly greater than every key starting with
        ``prefix`` (equivalent of fdb's ``strinc``, implemented locally: the
        fdb binding injects its API at ``fdb.api_version()`` time, so mypy
        cannot see ``fdb.KeySelector``/``fdb.impl``, and ``fdb.impl`` is
        private anyway)."""
        stripped = prefix.rstrip(b'\xff')
        if not stripped:
            raise ValueError('prefix consists solely of 0xff bytes')
        return stripped[:-1] + bytes([stripped[-1] + 1])

    def read_from_db(self, kv_store, id="", limit=0, reverse=False):
        if not kv_store:
            from simplyblock_core.db_controller import DBController
            kv_store = DBController().kv_store
        try:
            objects = []
            prefix = self.get_db_id(id).strip().encode('utf-8')

            if (limit and limit <= self._READ_CHUNK_SIZE) or not hasattr(kv_store, 'get_range'):
                # Single scan: either the read is bounded and small (one
                # transaction is fine), or the store does not support raw
                # key-range reads — the unit-tier fdb stub and the fake
                # stores in tests implement only get_range_startswith.
                for k, v in kv_store.get_range_startswith(prefix, limit=limit, reverse=reverse):
                    objects.append(self.__class__().from_dict(json.loads(v)))
                return objects

            # Chunked pagination over [prefix, next_prefix(prefix)) with plain
            # byte keys (begin inclusive, end exclusive — the binding turns
            # them into KeySelectors). Continuation: forward moves begin to
            # the successor of the last key seen (key + b'\x00' is the
            # smallest key strictly greater than key); reverse moves the
            # exclusive end down onto the last (smallest) key seen.
            begin = prefix
            end = self._next_prefix(prefix)
            while True:
                n = self._READ_CHUNK_SIZE
                if limit:
                    n = min(n, limit - len(objects))
                    if n <= 0:
                        break
                kvs = list(kv_store.get_range(begin, end, limit=n, reverse=reverse))
                for kv in kvs:
                    objects.append(self.__class__().from_dict(json.loads(kv.value)))
                if len(kvs) < n:
                    break
                if reverse:
                    end = bytes(kvs[-1].key)
                else:
                    begin = bytes(kvs[-1].key) + b'\x00'
            return objects
        except Exception as e:
            from simplyblock_core import utils
            logger = utils.get_logger(__name__)
            logger.exception('Error reading from FDB')
            raise e

    def get_last(self, kv_store):
        id = self.get_db_id(" ")
        objects = self.read_from_db(kv_store, id=id, limit=1, reverse=True)
        if objects:
            return objects[0]
        return None

    def watch_scope(self):
        """Parent-id path locating this entity in the watch index.

        Watched subclasses override to return their ancestor ids (e.g.
        ``(self.pool_uuid,)``); the default empty tuple places the entity
        directly under its class (a root, e.g. Cluster).
        """
        return ()

    @classmethod
    def active_indexes(cls, kv_store):
        """The indices this process maintains on a write to ``kv_store``.

        Resolving an index's state is a DB read (``index_meta/<Class>/<name>``,
        TTL-cached per class), so a class with no declarations never pays for
        one, and neither does a write to a store that cannot maintain an index
        at all (see :func:`_is_fdb_store`).
        """
        declared = indices.indexes_of(cls)
        if not declared or not _is_fdb_store(kv_store):
            return ()
        from simplyblock_core.db_controller import DBController
        db = DBController()
        return tuple(
            index for index in declared
            if db.index_state(cls, index) != indices.STATE_DISABLED
        )

    @staticmethod
    def _read_record(tr, key, model_cls):
        """The record currently stored at ``key``, or ``None``."""
        raw = tr.get(key).wait()
        if raw is None or not raw.present():
            return None
        return model_cls().from_dict(json.loads(bytes(raw)))

    @staticmethod
    def index_keys(model_cls, index_list, obj) -> dict:
        """``{index name: keys}`` for one record — the "before" of a write diff."""
        return {index.name: index.keys(model_cls, obj) for index in index_list}

    @staticmethod
    def _apply_index_diff(tr, model_cls, index_list, old_keys, obj):
        """Move every index entry named by ``old_keys`` onto ``obj``.

        ``old_keys`` comes from the record read inside this same transaction (or
        from the object before the caller mutated it), so the diff is computed
        against what is actually stored rather than against whatever the caller
        last saw.
        """
        entity_id = str(obj.get_id())
        encoded_id = entity_id.encode()
        for index in index_list:
            old = old_keys.get(index.name, set())
            new = index.keys(model_cls, obj)
            if index.unique:
                for values in index.tuples(obj):
                    key = index.key(model_cls, values, entity_id)
                    if key in old:
                        continue
                    held = tr.get(key).wait()
                    if held is not None and held.present():
                        holder = bytes(held).decode()
                        if holder != entity_id:
                            raise indices.UniqueIndexViolation(
                                model_cls.__name__, index.name, values,
                                holder, entity_id)
            for key in old - new:
                tr.clear(key)
            for key in new:
                tr[key] = encoded_id

    @staticmethod
    def _write_tx(tr, key, value, model_cls, obj, index_list, rollup_key, version_key):
        if index_list:
            BaseModel._apply_index_diff(
                tr, model_cls, index_list,
                BaseModel.index_keys(
                    model_cls, index_list,
                    BaseModel._read_record(tr, key, model_cls)),
                obj)
        tr.set(key, value)
        if rollup_key is not None:
            tr.add(rollup_key, watches.ONE_LE64)
            tr.add(version_key, watches.ONE_LE64)

    @staticmethod
    def _remove_tx(tr, key, model_cls, obj, index_list, rollup_key, version_key):
        if index_list:
            # Clear the keys of the record that is actually stored: the caller's
            # copy may be stale, and for a unique index a stale value could name
            # a key another entity has since taken over.
            stored = BaseModel._read_record(tr, key, model_cls) or obj
            for index in index_list:
                for index_key in index.keys(model_cls, stored):
                    tr.clear(index_key)
        tr.clear(key)
        if rollup_key is not None:
            tr.add(rollup_key, watches.ONE_LE64)
            tr.clear(version_key)

    def write_to_db(self, kv_store=None):
        if not kv_store:
            from simplyblock_core.db_controller import DBController
            kv_store = DBController().kv_store
        try:
            if self.name == "StorageNode":
                # Tripwire (2026-07-21 d3fc2c16 incident): a full-object write
                # of a STALE StorageNode copy silently resurrected
                # status=in_restart within 2.5s of the restart's committed
                # ONLINE flip — no event, no log — and the runner's
                # _reset_if_transient then killed SPDK on a healthy node.
                # Every full node write now names its caller so the next
                # occurrence identifies the writer instantly. Full-object
                # node writes are rare (hot paths use atomic_update); prefer
                # atomic_update for ANY new node-record mutation.
                import os.path
                import traceback
                from simplyblock_core import utils
                frames = [
                    f"{os.path.basename(fs.filename)}:{fs.lineno}:{fs.name}"
                    for fs in traceback.extract_stack(limit=6)[:-1]
                ]
                utils.get_logger(__name__).info(
                    "[NODE-WRITE] full-object write of %s status=%s by %s",
                    self.get_id(), getattr(self, "status", "?"),
                    " <- ".join(reversed(frames)))
            key = self.get_db_id().encode()
            value = json.dumps(self.to_dict(unwrap_secrets=True)).encode()
            index_list = self.active_indexes(kv_store)
            if self._WATCHED or index_list:
                import fdb
                fdb.transactional(BaseModel._write_tx)(
                    kv_store, key, value, type(self), self, index_list,
                    *self._watch_keys())
            else:
                kv_store.set(key, value)
            return True
        except indices.UniqueIndexViolation:
            # An invariant breach, not a write failure: the pre-check that
            # produces the clean "name already exists" error either did not run
            # or the data is already inconsistent. It propagates (to a 500 and
            # the cluster's error log) rather than being swallowed like a
            # transport error — and rather than taking the process down.
            from simplyblock_core import utils
            utils.get_logger(__name__).exception(
                "Unique index violation writing %s", self.get_db_id())
            raise
        except Exception:
            from simplyblock_core import utils
            utils.get_logger(__name__).exception("Error writing to FDB")
            exit(1)

    def _watch_keys(self):
        """``(rollup_key, version_key)`` for a watched class, else ``(None, None)``."""
        if not self._WATCHED:
            return (None, None)
        scope = self.watch_scope()
        return (
            watches.watch_index_rollup_key(type(self), scope),
            watches.watch_index_version_key(type(self), scope, self.get_id()),
        )

    def remove(self, kv_store):
        key = self.get_db_id().encode()
        index_list = self.active_indexes(kv_store)
        if not (self._WATCHED or index_list):
            return kv_store.clear(key)
        import fdb
        return fdb.transactional(BaseModel._remove_tx)(
            kv_store, key, type(self), self, index_list, *self._watch_keys())

    def keys(self):
        return self.get_attrs_map().keys()

    def get_status_code(self):
        if self.status in self._STATUS_CODE_MAP:
            return self._STATUS_CODE_MAP[self.status]
        else:
            return -1

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def __ne__(self, other):
        return not self == other

    def __getitem__(self, item):
        if isinstance(item, str) and item in self.get_attrs_map().keys():
            return getattr(self, item)
        return False


class BaseNodeObject(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_SUSPENDED = 'suspended'
    STATUS_IN_SHUTDOWN = 'in_shutdown'
    STATUS_REMOVED = 'removed'
    STATUS_RESTARTING = 'in_restart'

    STATUS_IN_CREATION = 'in_creation'
    STATUS_UNREACHABLE = 'unreachable'
    STATUS_SCHEDULABLE = 'schedulable'
    STATUS_DOWN = 'down'
    STATUS_IN_REMOVAL = 'in_removal'
    STATUS_PENDING_REMOVAL = 'pending_removal'

    _STATUS_CODE_MAP: ClassVar[dict] = {
        STATUS_ONLINE: 0,
        STATUS_OFFLINE: 1,
        STATUS_SUSPENDED: 2,
        STATUS_REMOVED: 3,
        STATUS_IN_CREATION: 10,
        STATUS_IN_SHUTDOWN: 11,
        STATUS_RESTARTING: 12,
        STATUS_UNREACHABLE: 20,
        STATUS_SCHEDULABLE: 30,
        STATUS_DOWN: 40,
        STATUS_IN_REMOVAL: 41,
        STATUS_PENDING_REMOVAL: 42,
    }
