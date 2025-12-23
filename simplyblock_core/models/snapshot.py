# coding=utf-8

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.lvol_model import LVol
from typing import List
import copy

class SnapshotRef():

    TYPE_LVOL = "lvol"
    TYPE_CLONE = "clone"
    TYPE_SNAP = "snap"
    type: str
    next: str


class SnapShot(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'

    base_bdev: str = ""
    blobid: int = 0
    cluster_id: str = ""
    created_at: int = 0
    health_check: bool = True
    lvol: LVol = None # type: ignore[assignment]
    mem_diff: dict = {}
    pool_uuid: str = ""
    ref_count: int = 0
    size: int = 0
    used_size: int = 0
    snap_bdev: str = "" #snapshot relative name (part without lvstore)
    snap_name: str = "" #snapshot full name
    snap_ref_id: str = ""
    snap_uuid: str = ""
    vuid: int = 0
    deletion_status: str = ""
    status: str = ""
    fabric: str = "tcp"
    frozen: bool = False
    node_id : str = ""
    successor : List[SnapshotRef] = []
    predecessor: str = ""

    def __copy__(self):
        # Shallow copy
        # 1. Copy base class attributes
        base_copy = super().__copy__() if hasattr(super(), '__copy__') else type(self)()

        # 2. Copy derived attributes
        new = type(self)()
        for attr in self.get_attrs_map():
            value = getattr(self, attr)
            # Shallow copy for mutable types
            if isinstance(value, (dict, list, set)):
                value = value.copy()
            setattr(new, attr, value)
        return new

    def __deepcopy__(self, memo):
        # 1. Deep copy base attributes
        base_copy = super().__deepcopy__(memo) if hasattr(super(), '__deepcopy__') else type(self)()

        # 2. Deep copy derived attributes
        new = type(self)()
        memo[id(self)] = new
        for attr in self.get_attrs_map():
            value = getattr(self, attr)
            setattr(new, attr, copy.deepcopy(value, memo))
        return new
