# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class QOSClass(BaseModel):

    uuid: str = ""
    cluster_id: str = ""
    class_id: int = 0
    name: str = ""
    weight: int = 0
