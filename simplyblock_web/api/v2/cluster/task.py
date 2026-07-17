from typing import List, Union

from fastapi import APIRouter
from sse_starlette import EventSourceResponse

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule

from .._dependencies import Cluster, Task
from .._dtos import TaskDTO
from .._sse import WATCH_RESPONSES, WatchParam, sse_response


api = APIRouter()
db = DBController()


def _task_dto(task: JobSchedule) -> TaskDTO:
    return TaskDTO.from_model(task)


@api.get('/', name='clusters:tasks:list', response_model=List[TaskDTO], responses=WATCH_RESPONSES)
def list(cluster: Cluster, watch: WatchParam = False) -> Union[List[TaskDTO], EventSourceResponse]:
    if watch:
        return sse_response(
            tasks_controller.watch_tasks(cluster.get_id()),
            _task_dto,
        )
    cluster_tasks = db.get_job_tasks(cluster.get_id(), limit=0)
    data = []
    for t in cluster_tasks:
        if t.function_name == JobSchedule.FN_DEV_MIG:
            continue
        data.append(t)
    return [TaskDTO.from_model(task) for task in data]


instance_api = APIRouter(prefix='/{task_id}')


@instance_api.get('/', name='clusters:tasks:detail', response_model=TaskDTO, responses=WATCH_RESPONSES)
def get(cluster: Cluster, task: Task, watch: WatchParam = False) -> Union[TaskDTO, EventSourceResponse]:
    if watch:
        return sse_response(
            tasks_controller.watch_task(cluster.get_id(), task.uuid),
            _task_dto,
            single=True,
        )
    return TaskDTO.from_model(task)


api.include_router(instance_api)
