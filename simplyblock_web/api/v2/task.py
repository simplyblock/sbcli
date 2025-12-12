from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.controllers import tasks_controller

from .cluster import Cluster
from .dtos import TaskDTO

api = APIRouter(prefix='/tasks')
db = DBController()


@api.get('/', name='clusters:tasks:list')
def list(cluster: Cluster) -> List[TaskDTO]:
    return [
        TaskDTO.from_model(task)
        for task
        in tasks_controller.list_tasks(cluster.get_id())
        if task.cluster_id == cluster.get_id()
    ]


instance_api = APIRouter(prefix='/{task_id}')


def _lookup_task(task_id: UUID) -> JobSchedule:
    task = db.get_task_by_id(str(task_id))
    if task is None:
        raise HTTPException(404, 'Task does not exist')
    return task


Task = Annotated[JobSchedule, Depends(_lookup_task)]


@instance_api.get('/', name='clusters:tasks:detail')
def get(cluster: Cluster, task: Task) -> TaskDTO:
    return TaskDTO.from_model(task)
