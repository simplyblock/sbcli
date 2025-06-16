
from flask import abort
from flask_openapi3 import APIBlueprint
from pydantic import Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.controllers import tasks_controller
from simplyblock_core import utils as core_utils

from .cluster import ClusterPath

api = APIBlueprint('task', __name__, url_prefix='/tasks')
db = DBController()


@api.get('/')
def list(path: ClusterPath):
    return [
        task.get_clean_dict()
        for task
        in tasks_controller.list_tasks(path.cluster_id)
        if task.cluster_id == path.cluster().get_id()
    ]


instance_api = APIBlueprint('instance', __name__, url_prefix='/<task_id>')


class TaskPath(ClusterPath):
    task_id: str = Field(pattern=core_utils.UUID_PATTERN)

    def task(self) -> JobSchedule:
        task = db.get_task_by_id(self.task_id)
        if task is None:
            abort(404, 'Task does not exist')
        return task


@instance_api.get('/')
def get(path: TaskPath):
    return path.task().get_clean_dict()
