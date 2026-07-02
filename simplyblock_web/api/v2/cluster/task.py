from typing import List, Union

from fastapi import APIRouter
from sse_starlette import EventSourceResponse

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.job_schedule import JobSchedule

from .._dependencies import Cluster, Task
from .._dtos import TaskDTO
from .._watch import WATCH_RESPONSES, WatchParam, watch_response


api = APIRouter()
db = DBController()


def _task_dto(data: dict) -> TaskDTO:
    return TaskDTO.from_model(JobSchedule(data))


@api.get('/', name='clusters:tasks:list', response_model=List[TaskDTO], responses=WATCH_RESPONSES)
def list(cluster: Cluster, watch: WatchParam = False) -> Union[List[TaskDTO], EventSourceResponse]:
    if watch:
        cluster_id = cluster.get_id()
        return watch_response(
            JobSchedule,
            lambda state: {
                k: d for k, d in state.items()
                if d.get('cluster_id') == cluster_id and d.get('function_name') != JobSchedule.FN_DEV_MIG
            },
            _task_dto,
            ancestors=[(ClusterModel, cluster_id)],
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
        task_uuid = task.uuid
        return watch_response(
            JobSchedule,
            lambda state: {k: d for k, d in state.items() if d.get('uuid') == task_uuid},
            _task_dto,
            single_id=task_uuid,
            ancestors=[(ClusterModel, cluster.get_id())],
        )
    return TaskDTO.from_model(task)


api.include_router(instance_api)
