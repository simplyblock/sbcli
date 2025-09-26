# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class QOSClass(BaseModel):

    uuid: str = ""
    cluster_id: str = ""
    index_id: int = 0 #  class index in the qos_weights alcemls param
    name: str = ""
    weight: int = 0

    def get_next_index_id(self, cluster_id):
        from simplyblock_core.db_controller import DBController
        db = DBController()
        ids = [0,1]
        for qos in db.get_qos(cluster_id):
            ids.append(qos.index_id)

        for i in range(2,8):
            if i not in ids:
                return i

        raise ValueError("Index out of range")
