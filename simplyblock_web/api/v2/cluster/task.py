from typing import List

from fastapi import APIRouter

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule

from .._dependencies import Cluster, Task
from .._dtos import TaskDTO


api = APIRouter()
db = DBController()


@api.get('/', name='clusters:tasks:list')
def list(cluster: Cluster) -> List[TaskDTO]:
    cluster_tasks = db.get_job_tasks(cluster.get_id(), limit=0)
    data = []
    for t in cluster_tasks:
        if t.function_name == JobSchedule.FN_DEV_MIG:
            continue
        data.append(t)
    return [TaskDTO.from_model(task) for task in data]


instance_api = APIRouter(prefix='/{task_id}')


@instance_api.get('/', name='clusters:tasks:detail')
def get(cluster: Cluster, task: Task) -> TaskDTO:
    return TaskDTO.from_model(task)


api.include_router(instance_api)
