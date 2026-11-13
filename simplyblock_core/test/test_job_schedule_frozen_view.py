"""A task handler's read-only view must still serialize.

On the bf12 bed (2026-11-04) tasks-runner-sync-lvol-del crash-looped: after a
task's fourth failure it logged LVOL_TASK_FAILING_REPEATEDLY with the handler's
``frozen_view()`` as ``db_object``. The event stores the task's dict, the view's
``function_params`` is a ``MappingProxyType``, ``json.dumps`` rejected it and
``write_to_db`` turned the error into ``exit(1)`` -- which the caller's
``except Exception`` cannot catch.
"""
import json
from types import MappingProxyType

import pytest

from simplyblock_core.models.base_model import BaseModel, default_factory
from simplyblock_core.models.job_schedule import JobSchedule


def _task():
    task = JobSchedule()
    task.uuid = "19b23e36-4eba-45c2-bc91-1b90ec8999cf"
    task.cluster_id = "02d97e89-aa33-4355-9175-4b23a3c04ce4"
    task.function_name = "lvol_sync_del"
    task.function_params = {"lvol_bdev_name": "LVS_22/LVOL_1744", "nested": {"a": 1}}
    return task


def test_frozen_view_params_stay_read_only():
    view = _task().frozen_view()
    assert isinstance(view.function_params, MappingProxyType)
    with pytest.raises(TypeError):
        view.function_params["x"] = 1  # type: ignore[index]


def test_frozen_view_serializes_like_the_task():
    task = _task()
    view = task.frozen_view()
    as_dict = view.to_dict(unwrap_secrets=True)
    assert type(as_dict["function_params"]) is dict
    assert json.loads(json.dumps(as_dict))["function_params"] == task.function_params


def test_frozen_view_as_event_object_dict_serializes():
    # log_event_cluster stores db_object.get_clean_dict() as the event's object_dict
    clean = _task().frozen_view().get_clean_dict(unwrap_secrets=True)
    assert json.loads(json.dumps(clean))["function_params"]["lvol_bdev_name"] == "LVS_22/LVOL_1744"


def test_nested_mapping_proxy_serializes():
    class _Holder(BaseModel):
        payload: dict = default_factory(dict)

    holder = _Holder()
    holder.payload = {"inner": MappingProxyType({"k": "v"})}
    assert json.dumps(holder.to_dict()) == json.dumps({**holder.to_dict(), "payload": {"inner": {"k": "v"}}})
