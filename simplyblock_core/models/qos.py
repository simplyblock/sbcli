from typing import ClassVar

from simplyblock_core.indices import Index
from simplyblock_core.models.base_model import BaseModel


class QOSClass(BaseModel):

    _INDEXES: ClassVar[tuple] = (
        Index('cluster_id'),
    )

    cluster_id: str = ""
    class_id: int = 0
    class_name: str = ""
    weight: int = 0
