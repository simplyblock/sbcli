"""Model field defaults must not be shared between instances.

``BaseModel`` fields are plain class attributes, so a literal
``nodes: List[str] = []`` stores ONE list on the class: every instance that
received no value for the field aliases it, and a single ``append`` is visible
on every other instance and on every instance created later in the process.
Mutable defaults are declared with :func:`default_factory` instead.
"""
import importlib
import inspect
import pkgutil
import typing
from typing import ClassVar

import pytest

import simplyblock_core.models
from simplyblock_core.models.base_model import BaseModel, default_factory
from simplyblock_core.models.cluster import Cluster


class Sample(BaseModel):
    plain: str = "x"
    items: list[str] = default_factory(list)
    mapping: dict[str, str] = default_factory(dict)
    seeded: list[dict] = default_factory(lambda: [{'a': 1}])
    shared: ClassVar[list[str]] = ['const']


def test_defaults_are_not_shared_between_instances():
    a, b = Sample(), Sample()

    assert a.items is not b.items
    assert a.mapping is not b.mapping

    a.items.append('leak')
    a.mapping['leak'] = 'yes'

    assert b.items == []
    assert b.mapping == {}


def test_class_holds_no_mutable_default():
    Sample().items.append('leak')
    Sample().mapping['leak'] = 'yes'
    Sample().seeded[0]['a'] = 99

    assert Sample().items == []
    assert Sample().mapping == {}
    assert Sample().seeded == [{'a': 1}]


def test_factory_result_is_the_declared_default():
    assert Sample().seeded == [{'a': 1}]
    assert Sample().get_attrs_map()['items']['default'] == []


def test_supplied_values_still_win():
    obj = Sample({'items': ['given'], 'mapping': {'k': 'v'}, 'plain': 'y'})

    assert obj.items == ['given']
    assert obj.mapping == {'k': 'v'}
    assert obj.plain == 'y'
    assert Sample().items == []


def test_serialization_round_trip():
    obj = Sample({'items': ['a']})

    assert obj.to_dict()['items'] == ['a']
    assert Sample(obj.to_dict()).items == ['a']
    assert Sample().to_dict()['items'] == []


@pytest.mark.parametrize('data', [{}, {'plain': 'y'}])
def test_from_dict_merges_into_existing_instance_state(data):
    obj = Sample({'items': ['kept']})
    items = obj.items

    obj.from_dict(data)

    assert obj.items is items
    assert obj.items == ['kept']


def test_class_var_constants_are_not_model_fields():
    """``ClassVar`` says "shared on purpose", not "per-instance field".

    The distinction matters beyond the sharing itself: an annotated public
    attribute is picked up by ``_annotated_attrs``, so without this exclusion a
    constant such as ``Cluster.STATUS_CODE_MAP`` would be serialized into every
    FDB record and overwritten by ``from_dict``.
    """
    assert 'shared' not in Sample().keys()
    assert 'shared' not in Sample().to_dict()
    assert Sample({'shared': ['injected']}).shared == ['const']
    assert Sample.shared is Sample().shared

    assert 'STATUS_CODE_MAP' not in Cluster().to_dict()
    assert '_STATUS_CODE_MAP' not in Cluster().to_dict()


def test_model_does_not_alias_the_payload_it_was_built_from():
    payload = {'items': ['a'], 'mapping': {'k': 'v'}, 'seeded': [{'a': 1}]}

    obj = Sample(payload)
    obj.items.append('b')
    obj.mapping['k'] = 'mutated'
    obj.seeded[0]['a'] = 99

    assert payload == {'items': ['a'], 'mapping': {'k': 'v'}, 'seeded': [{'a': 1}]}


def test_payload_mutated_after_construction_does_not_reach_the_model():
    payload = {'items': ['a'], 'mapping': {'k': 'v'}}

    obj = Sample(payload)
    payload['items'].append('b')
    payload['mapping']['k'] = 'mutated'

    assert obj.items == ['a']
    assert obj.mapping == {'k': 'v'}


def test_two_models_built_from_one_payload_are_independent():
    payload = {'items': ['a']}

    first, second = Sample(payload), Sample(payload)
    first.items.append('b')

    assert second.items == ['a']


def _model_classes():
    for module_info in pkgutil.iter_modules(simplyblock_core.models.__path__):
        module = importlib.import_module(f'simplyblock_core.models.{module_info.name}')
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if issubclass(cls, BaseModel) and cls.__module__ == module.__name__:
                yield cls


def test_no_model_shares_a_default_container_between_instances():
    """The defect itself, asserted on the shipped models.

    Independent of how the defaults are declared: two fresh instances of every
    model in the package must not hand out the same container. This is red
    against the pre-fix declarations (56 fields across 12 modules) and stays
    red for any future field that reintroduces the sharing, whatever form the
    declaration takes.
    """
    offenders = []
    for cls in _model_classes():
        first, second = cls(), cls()
        for name in first.keys():
            value = getattr(first, name)
            if isinstance(value, (list, dict, set, bytearray)) and value is getattr(second, name):
                offenders.append(f'{cls.__module__}.{cls.__name__}.{name}')

    assert offenders == [], (
        f'default containers shared by every instance of the model: {offenders}'
    )


def test_no_model_declares_a_mutable_class_attribute():
    """Guards the whole package, not just the fields that leaked once.

    ``ruff``'s RUF012 enforces the same rule, but only for code that is
    linted; this keeps a new ``foo: List[str] = []`` from reaching FDB records
    even when it arrives by another route.
    """
    offenders = [
        f'{cls.__module__}.{cls.__name__}.{name}'
        for cls in _model_classes()
        for name, value in list(vars(cls).items())
        if not name.startswith('_')  # a field is public; the rest is class machinery
        and isinstance(value, (list, dict, set, bytearray))
        and typing.get_origin(cls.__annotations__.get(name)) is not ClassVar
    ]

    assert offenders == [], (
        'shared mutable class attributes — declare the field as '
        'default_factory(list) / default_factory(dict), or annotate a genuine '
        f'class constant as ClassVar: {offenders}'
    )
