"""No model may redefine the BaseModel fields that are FDB key material.

``BaseModel.__init__`` sets ``self.name`` to the CLASS name, and
``get_db_id`` builds every record's key as ``{object_type}/{self.name}/{id}``.
A subclass that re-annotates ``name`` (or ``object_type``) as a data field
silently relocates its own keyspace: records are written under whatever the
field holds and become invisible to every prefix-scanning reader. That is not
hypothetical — ConsistencyGroup once declared ``name`` for the group's label
name, so standalone groups were written under ``object/<group name>/...``,
never found again, and the Kubernetes VolumeGroupSnapshot path 404'd on
groups it had just created (see tests/unit/test_consistency_group_store.py).

Domain names belong in a prefixed field (``pool_name``, ``lvol_name``,
``snap_name``, ``group_name``) — this test turns that convention into a gate.
"""
import importlib
import pkgutil
from inspect import get_annotations

from simplyblock_core import models as models_pkg
from simplyblock_core.models.base_model import BaseModel

#: Fields get_db_id derives key prefixes from. `uuid` is included because
#: get_id() (the key's last segment) returns it for most models; a subclass
#: may override get_id(), but must not re-declare the field itself.
RESERVED_KEY_FIELDS = ("name", "object_type", "uuid")


def _model_classes():
    """Every BaseModel subclass defined inside simplyblock_core.models."""
    for info in pkgutil.iter_modules(models_pkg.__path__):
        importlib.import_module(f"{models_pkg.__name__}.{info.name}")

    seen = set()
    stack = list(BaseModel.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
        if cls.__module__.startswith(models_pkg.__name__):
            yield cls


def test_no_model_shadows_reserved_key_fields():
    offenders = [
        f"{cls.__module__}.{cls.__name__} re-declares {field!r}"
        for cls in _model_classes()
        for field in RESERVED_KEY_FIELDS
        if field in get_annotations(cls)
    ]
    assert not offenders, (
        "reserved BaseModel key fields shadowed (records would be written "
        "outside their class keyspace): " + "; ".join(sorted(offenders)))
