# coding=utf-8
import pprint

import json
from inspect import ismethod
import sys
from typing import Mapping, Type, Union
from collections import ChainMap

from pydantic import SecretBytes, SecretStr

from simplyblock_core import watches


class BaseModel(object):

    _STATUS_CODE_MAP: dict = {}

    # When True, write_to_db()/remove() atomically bump watch_seq/<ClassName>
    # in the same FDB transaction so watchers (SSE API) wake up. Plain class
    # attribute, not an annotation: must stay out of get_attrs_map()/to_dict().
    _WATCHED = False

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

    def get_attrs_map(self):
        _attribute_map = {}
        for s , t in self.all_annotations().items():
            if not s.startswith("_") and not ismethod(getattr(self, s)):
                _attribute_map[s]= {"type": t, "default": getattr(self, s)}
        return _attribute_map

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

    # Upper bound for one range-read transaction. An unbounded
    # get_range_startswith over a large prefix (e.g. the job-task table during
    # a mass test) is a single FDB transaction: once it exceeds the 5s
    # transaction limit it fails with 1031 and the binding's on_error retry
    # restarts the SAME full scan — it never completes, and on 2026-07-16 it
    # killed TasksNodeAddRunner at cluster start. Chunks of this size each run
    # in their own (auto-retried) transaction and always finish.
    _READ_CHUNK_SIZE = 2000

    def read_from_db(self, kv_store, id="", limit=0, reverse=False):
        if not kv_store:
            from simplyblock_core.db_controller import DBController
            kv_store = DBController().kv_store
        try:
            objects = []
            prefix = self.get_db_id(id).strip().encode('utf-8')

            if limit and limit <= self._READ_CHUNK_SIZE:
                # Bounded small read — a single transaction is fine.
                for k, v in kv_store.get_range_startswith(prefix, limit=limit, reverse=reverse):
                    objects.append(self.__class__().from_dict(json.loads(v)))
                return objects

            try:
                import fdb
                first_ge = fdb.KeySelector.first_greater_or_equal
                first_gt = fdb.KeySelector.first_greater_than
                strinc = fdb.impl.strinc
            except (ImportError, AttributeError):
                # fdb API unavailable (stubbed store) — legacy single scan.
                for k, v in kv_store.get_range_startswith(prefix, limit=limit, reverse=reverse):
                    objects.append(self.__class__().from_dict(json.loads(v)))
                return objects

            begin = first_ge(prefix)
            end = first_ge(strinc(prefix))
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
                    end = first_ge(kvs[-1].key)
                else:
                    begin = first_gt(kvs[-1].key)
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

    @staticmethod
    def _write_tx(tr, key, value, counter_key):
        tr.set(key, value)
        tr.add(counter_key, watches.ONE_LE64)

    @staticmethod
    def _remove_tx(tr, key, counter_key):
        tr.clear(key)
        tr.add(counter_key, watches.ONE_LE64)

    def write_to_db(self, kv_store=None):
        if not kv_store:
            from simplyblock_core.db_controller import DBController
            kv_store = DBController().kv_store
        try:
            key = self.get_db_id().encode()
            value = json.dumps(self.to_dict(unwrap_secrets=True)).encode()
            if self._WATCHED:
                import fdb
                fdb.transactional(BaseModel._write_tx)(
                    kv_store, key, value, watches.watch_counter_key(type(self)))
            else:
                kv_store.set(key, value)
            return True
        except Exception:
            from simplyblock_core import utils
            utils.get_logger(__name__).exception("Error writing to FDB")
            exit(1)

    def remove(self, kv_store):
        key = self.get_db_id().encode()
        if not self._WATCHED:
            return kv_store.clear(key)
        import fdb
        return fdb.transactional(BaseModel._remove_tx)(
            kv_store, key, watches.watch_counter_key(type(self)))

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
