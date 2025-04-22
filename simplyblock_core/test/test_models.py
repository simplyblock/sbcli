from simplyblock_core.models.base_model import BaseModel


class Model(BaseModel):
    x: int = 0


def test():
    assert Model({}).x == 0
    assert Model({'x': 1}).x == 1


def test_all_annotations():
    assert Model().all_annotations().get('x') is int


def test_get_attrs_map():
    print(Model().get_attrs_map())
    assert Model().get_attrs_map().get('x') == {
        'type': int,
        'default': 0,
    }


def test_to_dict():
    d = Model({'x': 1}).to_dict()
    assert d.get('x') == 1
    assert 'uuid' in d
    assert 'name' in d
    assert 'object_type' in d


def test_get_clean_dict():
    d = Model({'x': 1}).get_clean_dict()
    assert d.get('x') == 1
    assert 'status_code' in d
    assert 'uuid' in d
    assert 'name' not in d
    assert 'object_type' not in d


def test_to_str():
    assert "'x': 0" in Model().to_str()


def test_keys():
    assert 'x' in Model().keys()
