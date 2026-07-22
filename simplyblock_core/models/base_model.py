# coding=utf-8
import pprint

import json
from inspect import ismethod, isfunction
import sys
from typing import Mapping, Type, Union
from collections import ChainMap

from pydantic import SecretBytes, SecretStr


class BaseModel(object):

    _STATUS_CODE_MAP: dict = {}

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
    def all_annotations(cls) -> Mapping[str, Type]:
        """Returns a dictionary-like ChainMap that includes annotations for all
           attributes defined in cls or inherited from superclasses."""
        if sys.version_info >= (3, 10):
            from inspect import get_annotations
            return ChainMap(*(
                get_annotations(c)
                for c
                in cls.__mro__
            ))
        else:
            return ChainMap(*(
                c.__annotations__
                for c
                in cls.__mro__
                if '__annotations__' in c.__dict__
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
        """
        cached = cls.__dict__.get('_annotated_attrs_cache')
        if cached is None:
            cached = [
                (s, t) for s, t in cls.all_annotations().items()
                if not s.startswith("_")
                and not ismethod(getattr(cls, s, None))
                and not isfunction(getattr(cls, s, None))
            ]
            cls._annotated_attrs_cache = cached  # type: ignore[attr-defined]
        return cached

    def get_attrs_map(self):
        return {
            s: {"type": t, "default": getattr(self, s)}
            for s, t in self.__class__._annotated_attrs()
        }

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

                elif hasattr(dtype, '__origin__'):
                    if dtype.__origin__ is list:
                        if hasattr(dtype, "__args__") and hasattr(dtype.__args__[0], "from_dict"):
                            value = [dtype.__args__[0]().from_dict(item) for item in data[attr]]
                        else:
                            value = data[attr]
                    elif dtype.__origin__ == Mapping:
                        if hasattr(dtype, "__args__") and hasattr(dtype.__args__[1], "from_dict"):
                            value = {item: dtype.__args__[1]().from_dict(data[attr][item]) for item in data[attr]}
                        else:
                            value = value_dict['type'](data[attr])
                    elif dtype.__origin__ is Union:
                        if data[attr] is None:
                            value = None
                        else:
                            inner_types = [t for t in dtype.__args__ if t is not type(None)]
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
                else:
                    value = value_dict['type'](data[attr])
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
        return pprint.pformat(self.to_dict())

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

    def write_to_db(self, kv_store=None):
        if not kv_store:
            from simplyblock_core.db_controller import DBController
            kv_store = DBController().kv_store
        try:
            prefix = self.get_db_id()
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
            st = json.dumps(self.to_dict(unwrap_secrets=True))
            kv_store.set(prefix.encode(), st.encode())
            return True
        except Exception:
            from simplyblock_core import utils
            utils.get_logger(__name__).exception("Error writing to FDB")
            exit(1)

    def remove(self, kv_store):
        prefix = self.get_db_id()
        return kv_store.clear(prefix.encode())

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

    _STATUS_CODE_MAP = {
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
    }
