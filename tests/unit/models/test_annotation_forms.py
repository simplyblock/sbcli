"""``from_dict`` must dispatch on what an annotation *means*, not how it is spelled.

The field types are read back with :func:`typing.get_origin`, so ``Optional[X]``
and ``X | None`` — and ``List`` and ``list`` — have to land in the same branch.
They did not: the dispatch used ``hasattr(dtype, '__origin__')``, which a PEP 604
union does not have before 3.14, so every ``X | None`` field fell through to the
generic ``dtype(value)`` call and raised ``TypeError: 'types.UnionType' object is
not callable`` on the 3.11 floor while staying green on the image's 3.14.
"""
import importlib
import inspect
import pkgutil
from typing import Optional

import pytest

import simplyblock_core.models
from simplyblock_core.models.base_model import BaseModel, default_factory


class Child(BaseModel):
    value: str = ""


class Spellings(BaseModel):
    optional_scalar: Optional[bool] = True  # noqa: UP045 — the point is that both spellings work
    union_scalar: bool | None = True
    optional_child: Optional[Child] = None  # noqa: UP045
    union_child: Child | None = None
    bare_list: list = default_factory(list)
    typed_list: list[str] = default_factory(list)
    child_list: list[Child] = default_factory(list)


@pytest.mark.parametrize('value', [True, False, None])
@pytest.mark.parametrize('attr', ['optional_scalar', 'union_scalar'])
def test_optional_scalar_spellings_agree(attr, value):
    assert getattr(Spellings({attr: value}), attr) is value


@pytest.mark.parametrize('attr', ['optional_child', 'union_child'])
def test_optional_model_spellings_agree(attr):
    built = getattr(Spellings({attr: {'value': 'x'}}), attr)

    assert isinstance(built, Child)
    assert built.value == 'x'
    assert getattr(Spellings({attr: None}), attr) is None


def test_list_spellings_agree():
    obj = Spellings({
        'bare_list': [{'value': 'a'}],
        'typed_list': ['b'],
        'child_list': [{'value': 'c'}],
    })

    assert obj.bare_list == [{'value': 'a'}]  # unparameterized: no element type to build
    assert obj.typed_list == ['b']
    assert [child.value for child in obj.child_list] == ['c']


def _model_classes():
    for module_info in pkgutil.iter_modules(simplyblock_core.models.__path__):
        module = importlib.import_module(f'simplyblock_core.models.{module_info.name}')
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if issubclass(cls, BaseModel) and cls.__module__ == module.__name__:
                yield cls


def test_every_model_survives_a_round_trip_through_its_own_record():
    """The defect itself, asserted on the shipped models.

    ``to_dict()`` writes a key for every field, so reading it back exercises
    ``from_dict``'s dispatch for each declared annotation — which is what an FDB
    read does on every model on every request. A field whose type spelling the
    dispatch cannot handle raises here regardless of which spelling it is.
    """
    offenders = []
    for cls in _model_classes():
        record = cls().to_dict()
        try:
            cls(record)
        except Exception as e:
            offenders.append(f'{cls.__module__}.{cls.__name__}: {e!r}')

    assert offenders == [], f'models that cannot read back their own record: {offenders}'
