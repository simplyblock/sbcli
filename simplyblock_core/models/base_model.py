# coding=utf-8
import pprint

import json
from typing import Mapping


class BaseModel(object):
    def __init__(self):
        self._attribute_map = {
            "id": {"type": str, "default": ""},
            "name": {"type": str, "default": self.__class__.__name__},
            "object_type": {"type": str, "default": ""},
            "deleted": {"type": bool, 'default': False}
        }
        self.set_attrs({}, {})

    def get_id(self):
        return self.id

    def set_attrs(self, attributes, data):
        self._attribute_map.update(attributes)
        self.from_dict(data)

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
        self.id = self.get_id()
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
            prefix = "%s/%s/%s" % (self.object_type, self.name, id)
            for k, v in kv_store.db.get_range_startswith(prefix.encode('utf-8'),  limit=limit, reverse=reverse):
                objects.append(self.__class__().from_dict(json.loads(v)))
            return objects
        except Exception as e:
            print(f"Error reading from FDB: {e}")
            return []

    def get_last(self, kv_store):
        id = "/".join(self.get_id().split("/")[:2])
        objects = self.read_from_db(kv_store, id=id, limit=1, reverse=True)
        if objects:
            return objects[0]
        return None

    def write_to_db(self, kv_store=None):
        if not kv_store:
            from simplyblock_core.kv_store import KVStore
            kv_store = KVStore()
        try:
            prefix = "%s/%s/%s" % (self.object_type, self.name, self.get_id())
            st = json.dumps(self.to_dict())
            kv_store.db.set(prefix.encode(), st.encode())
            return True
        except Exception as e:
            print("Error Writing to FDB!")
            print(e)
            exit(1)

    def remove(self, kv_store):
        prefix = "%s/%s/%s" % (self.object_type, self.name, self.get_id())
        return kv_store.db.clear(prefix.encode())

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
