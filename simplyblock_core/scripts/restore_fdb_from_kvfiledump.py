# Reads key: val: from file and writes to db
# This script expects an active FDB
# To generate the key: val: file from sqlite db:
# FDB_DUMP_DEBUG=true fdbserver --role kvfiledump --kvfile /var_host/fdb1/storage-1782ab78fa62934783e11ee890600d23.sqlite 2> /etc_host/foundationdb/kvfiledump_1782ab78fa62934783e11ee890600d23> /dev/null
# Then run this script:
# python restore_fdb_from_kvfiledump.py kvfiledump_1782ab78fa62934783e11ee890600d23

import json
import sys

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.pool import Pool

from simplyblock_core.models.storage_node import StorageNode

file = sys.argv[1]

with open(file, encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()
    print(f"processing {len(lines)} lines")
    db_objects = []
    classes = set()
    unknown_classes = set()
    lines.pop()
    line = lines.pop(0)

    try:
        while True:
            key = None
            value = None
            line = lines.pop(0)
            if not line.startswith("key"):
                continue
            key = line[5:].strip()
            value = lines.pop(0)
            value = value[4:].strip()
            if not key.startswith("object"):
                continue
            if key.startswith("object"):
                cls = key.split("/")[1]
                obj = None
                if cls == "Cluster":
                    obj = Cluster().from_dict(json.loads(value))
                elif cls == "StorageNode":
                    obj = StorageNode().from_dict(json.loads(value))
                elif cls == "LVol":
                    obj = LVol().from_dict(json.loads(value))
                elif cls == "MgmtNode":
                    obj = MgmtNode().from_dict(json.loads(value))
                elif cls == "Pool":
                    obj = Pool().from_dict(json.loads(value))
                else:
                    unknown_classes.add(cls)
                    continue
                classes.add(cls)
                db_objects.append(obj)
    except Exception as e:
        print(e)
    print("Classes: ", classes)
    print("Unknown classes: ", unknown_classes)
    print(f"Total objects: {len(db_objects)}")
    objects_count = {}
    for ob in db_objects:
        ob.write_to_db()
        if ob.name not in objects_count:
            objects_count[ob.name] = 1
        else:
            objects_count[ob.name] += 1

    for k, v in objects_count.items():
        print(f"{k}: {v}")




