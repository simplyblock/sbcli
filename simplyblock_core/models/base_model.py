# coding=utf-8
import pprint
import logging

import json
from inspect import ismethod
import sys
from typing import Mapping, Type
from collections import ChainMap


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
        for attr , value_dict in self.get_attrs_map().items():
            value = value_dict['default']
            if data is not None and attr in data:
                dtype = value_dict['type']
                value = data[attr]
                if dtype in [int, float, str, bool]:
                    try:
                        value = dtype(value)
                    except:
                        if type(value) == list and dtype == int:
                            value = len(value)

                elif hasattr(dtype, '__origin__'):
                    if dtype.__origin__ == list:
                        if hasattr(dtype, "__args__") and hasattr(dtype.__args__[0], "from_dict"):
                            value = [dtype.__args__[0]().from_dict(item) for item in data[attr]]
                        else:
                            value = data[attr]
                    elif dtype.__origin__ == Mapping:
                        if hasattr(dtype, "__args__") and hasattr(dtype.__args__[1], "from_dict"):
                            value = {item: dtype.__args__[1]().from_dict(data[attr][item]) for item in data[attr]}
                        else:
                            value = value_dict['type'](data[attr])
                else:
                    value = value_dict['type'](data[attr])
            setattr(self, attr, value)
        self.id = self.uuid
        return self

    def to_dict(self):
        result = {}
        for attr in self.get_attrs_map():
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(lambda x: x.to_dict() if hasattr(x, "to_dict") else x, value))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def get_clean_dict(self):
        data = self.to_dict()
        for key in ['name', 'object_type']:
            del data[key]
        data['status_code'] = self.get_status_code()
        return data

    def to_str(self):
        return pprint.pformat(self.to_dict())

    def read_from_db(self, kv_store, id="", limit=0, reverse=False):
        if not kv_store:
            return []
        try:
            objects = []
            prefix = self.get_db_id(id)
            for k, v in kv_store.get_range_startswith(prefix.strip().encode('utf-8'),  limit=limit, reverse=reverse):
                objects.append(self.__class__().from_dict(json.loads(v)))
            return objects
        except Exception:
            from simplyblock_core import utils
            logger = utils.get_logger(__name__)
            logger.exception('Error reading from FDB')
            return []

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
            st = json.dumps(self.to_dict())
            kv_store.set(prefix.encode(), st.encode())
            return True
        except Exception as e:
            print(f"Error Writing to FDB! {e}")
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
