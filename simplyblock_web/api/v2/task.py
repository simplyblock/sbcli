from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, StringConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.controllers import tasks_controller
from simplyblock_core import utils as core_utils

from .cluster import Cluster
from . import util

api = APIRouter(prefix='/tasks')
db = DBController()


class TaskDTO(BaseModel):
    id: UUID
    status: str
    canceled: bool
    function_name: str
    function_params: dict
    function_result: str
    retry: util.Unsigned
    max_retry: int

    @staticmethod
    def from_model(model: JobSchedule):
        return TaskDTO(
            id=UUID(model.get_id()),
            status=model.status,
            canceled=model.canceled,
            function_name=model.function_name,
            function_params=model.function_params,
            function_result=model.function_result,
            retry=model.retry,
            max_retry=model.max_retry,
        )


@api.get('/')
def list(cluster: Cluster) -> List[TaskDTO]:
    return [
        TaskDTO.from_model(task)
        for task
        in tasks_controller.list_tasks(cluster.get_id())
        if task.cluster_id == cluster.get_id()
    ]


instance_api = APIRouter(prefix='/<task_id>')


def _lookup_task(task_id: Annotated[str, StringConstraints(pattern=core_utils.UUID_PATTERN)]) -> JobSchedule:
    task = db.get_task_by_id(task_id)
    if task is None:
        raise HTTPException(404, 'Task does not exist')
    return task


Task = Annotated[JobSchedule, Depends(_lookup_task)]


@instance_api.get('/')
def get(cluster: Cluster, task: Task) -> TaskDTO:
    return TaskDTO.from_model(task)


api.include_router(instance_api)
