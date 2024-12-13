# coding=utf-8
import pprint

import json
from inspect import ismethod
from typing import Mapping


class BaseModel(object):

    uuid: str = ""
    name: str = ""
    status: str = ""
    deleted: bool = False
    updated_at: str = ""
    create_dt: str= ""
    remove_dt: str= ""
    object_type: str= "object"

    _attribute_map = {}

    def __init__(self, data=None):
        self.name = self.__class__.__name__
        self.set_attrs(data)

    def get_id(self):
        return self.uuid

    def set_attrs(self, data):
        self._attribute_map = {}
        for s in self.__dir__():
            if not s.startswith("_") and not ismethod(getattr(self, s)):
                self._attribute_map[s]= {"type": getattr(self, s).__class__, "default": getattr(self, s)}
        self.from_dict(data)

    def get_db_id(self, use_this_id=None):
        if use_this_id:
            return "%s/%s/%s" % (self.object_type, self.name, use_this_id)
        else:
            return "%s/%s/%s" % (self.object_type, self.name, self.get_id())


    def from_dict(self, data):
        for attr in self._attribute_map:
            if data is not None and attr in data:
                dtype = self._attribute_map[attr]['type']
                value = data[attr]
                if dtype in [int, float, str, bool]:
                    value = self._attribute_map[attr]['type'](data[attr])
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
                            value = self._attribute_map[attr]['type'](data[attr])
                elif hasattr(dtype, '__origin__') and hasattr(dtype, "__args__"):
                    if hasattr(dtype.__args__[0], "from_dict"):
                        value = dtype.__args__[0]().from_dict(data[attr])
                else:
                    value = self._attribute_map[attr]['type'](data[attr])
                setattr(self, attr, value)
            else:
                setattr(self, attr, self._attribute_map[attr]['default'])
        return self

    def to_dict(self):
        result = {}
        for attr in self._attribute_map:
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
        return data

    def to_str(self):
        """Returns the string representation of the model

        :rtype: str
        """
        return pprint.pformat(self.to_dict())

    def read_from_db(self, kv_store, id="", limit=0, reverse=False):
        try:
            objects = []
            prefix = self.get_db_id(id)
            for k, v in kv_store.get_range_startswith(prefix.encode('utf-8'),  limit=limit, reverse=reverse):
                objects.append(self.__class__().from_dict(json.loads(v)))
            return objects
        except Exception as e:
            print(f"Error reading from FDB: {e}")
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
            print("Error Writing to FDB!")
            print(e)
            exit(1)

    def remove(self, kv_store):
        prefix = self.get_db_id()
        return kv_store.clear(prefix.encode())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def __ne__(self, other):
        return not self == other

    def __getitem__(self, item):
        if isinstance(item, str) and item in self._attribute_map:
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

    STATUS_CODE_MAP = {
        STATUS_ONLINE: 0,
        STATUS_OFFLINE: 1,
        STATUS_SUSPENDED: 2,
        STATUS_REMOVED: 3,

        STATUS_IN_CREATION: 10,
        STATUS_IN_SHUTDOWN: 11,
        STATUS_RESTARTING: 12,

        STATUS_UNREACHABLE: 20,

        STATUS_SCHEDULABLE: 30,
    }

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1

    def get_clean_dict(self):
        data = super(BaseNodeObject, self).get_clean_dict()
        data['status_code'] = self.get_status_code()
        return data
