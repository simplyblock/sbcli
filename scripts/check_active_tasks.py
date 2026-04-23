import json

from simplyblock_core import db_controller
from simplyblock_core.models.job_schedule import JobSchedule

db = db_controller.DBController()
tasks = db.get_job_tasks("7155bd9c-3bb9-48ce-b210-c027b0ce9c9d", reverse=False)
active = [
    task.get_clean_dict()
    for task in tasks
    if task.status != JobSchedule.STATUS_DONE and not getattr(task, "canceled", False)
]
print(json.dumps(active))
