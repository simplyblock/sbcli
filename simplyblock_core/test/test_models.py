from typing import Optional

from simplyblock_core.models.base_model import BaseModel


class Model(BaseModel):
    x: int = 0
    y: Optional[int] = None


def test():
    assert Model({}).x == 0
    assert Model({'x': 1}).x == 1
    assert Model().y is None
    assert Model({'y': 1}).y == 1


def test_all_annotations():
    assert Model().all_annotations().get('x') is int
    assert Model().all_annotations().get('y') is Optional[int]


def test_get_attrs_map():
    print(Model().get_attrs_map())
    assert Model().get_attrs_map().get('x') == {
        'type': int,
        'default': 0,
    }
    assert Model().get_attrs_map().get('y') == {
        'type': Optional[int],
        'default': None,
    }


def test_to_dict():
    d = Model({'x': 1, 'y': 1}).to_dict()
    assert d.get('x') == 1
    assert d.get('y') == 1
    assert 'uuid' in d
    assert 'name' in d
    assert 'object_type' in d


def test_get_clean_dict():
    d = Model({'x': 1, 'y': 1}).get_clean_dict()
    assert d.get('x') == 1
    assert d.get('y') == 1
    assert 'status_code' in d
    assert 'uuid' in d
    assert 'name' not in d
    assert 'object_type' not in d


def test_to_str():
    assert "'x': 0" in Model().to_str()


def test_keys():
    assert 'x' in Model().keys()
