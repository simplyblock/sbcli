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


# --- Legacy container-shaped records for scalar fields -----------------------
#
# Regression for the R26.2-PRE -> R26.3 upgrade failure: bc7c6e9eb changed
# StorageNode.lvstore_stack_secondary / _tertiary from `List[dict]` to `str`
# without a data migration. Records written by R26.2-PRE for a node that was
# never anyone's LVS peer still hold a literal `[]`, and `str([])` is the
# TRUTHY string "[]" -- so every reader treated it as a real node UUID and
# `get_storage_node_by_id("[]")` raised `KeyError: StorageNode [] not found`,
# aborting `sn restart` in _connect_to_remote_jm_devs.


class ShapeChanged(BaseModel):
    """A field that used to be declared as a container, now a scalar."""

    node_ref: str = ""
    flag: bool = False
    count: int = 0
    ratio: float = 0.0

    _LEGACY_STRINGIFIED_CONTAINER_FIELDS = frozenset({'node_ref'})


def test_container_value_for_str_field_yields_default():
    """`str([])` must not become the truthy string "[]"."""
    assert ShapeChanged({'node_ref': []}).node_ref == ""
    assert ShapeChanged({'node_ref': [{'a': 1}]}).node_ref == ""
    assert ShapeChanged({'node_ref': {}}).node_ref == ""
    assert not ShapeChanged({'node_ref': []}).node_ref


def test_container_value_for_bool_field_yields_default():
    """`bool([...])` silently flips on any non-empty container."""
    assert ShapeChanged({'flag': [{'a': 1}]}).flag is False


def test_container_value_for_float_field_yields_default():
    assert ShapeChanged({'ratio': []}).ratio == 0.0


def test_list_to_int_count_migration_is_preserved():
    """The one deliberate container->scalar coercion still applies."""
    assert ShapeChanged({'count': [1, 2, 3]}).count == 3
    assert ShapeChanged({'count': []}).count == 0


def test_stringified_container_is_healed_on_read():
    """A record already re-persisted as "[]" by an unfixed build."""
    assert ShapeChanged({'node_ref': '[]'}).node_ref == ""
    assert ShapeChanged({'node_ref': "[{'a': 1}]"}).node_ref == ""
    assert ShapeChanged({'node_ref': '{}'}).node_ref == ""


def test_healed_value_is_not_re_persisted():
    assert ShapeChanged({'node_ref': []}).to_dict()['node_ref'] == ""
    assert ShapeChanged({'node_ref': '[]'}).to_dict()['node_ref'] == ""


def test_real_values_survive_untouched():
    uuid = 'cc5e4306-7d94-4a1c-a5a7-487120a653ff'
    assert ShapeChanged({'node_ref': uuid}).node_ref == uuid
    assert ShapeChanged({'flag': True}).flag is True
    assert ShapeChanged({'count': 7}).count == 7
    assert ShapeChanged({'ratio': 1.5}).ratio == 1.5
    # Coercion from a compatible scalar still happens.
    assert ShapeChanged({'count': '7'}).count == 7
    assert ShapeChanged({'node_ref': 42}).node_ref == '42'


def test_bracketed_string_survives_on_unlisted_field():
    """The repr heuristic only applies to fields that opted in."""
    class Other(BaseModel):
        label: str = ""

    assert Other({'label': '[]'}).label == '[]'


def test_storage_node_lvstore_stack_peer_refs():
    from simplyblock_core.models.storage_node import StorageNode

    legacy = StorageNode({
        'uuid': 'u1',
        'lvstore_stack_secondary': [],
        'lvstore_stack_tertiary': [],
    })
    assert legacy.lvstore_stack_secondary == ""
    assert legacy.lvstore_stack_tertiary == ""

    repersisted = StorageNode({
        'uuid': 'u1',
        'lvstore_stack_secondary': '[]',
        'lvstore_stack_tertiary': '[]',
    })
    assert repersisted.lvstore_stack_secondary == ""
    assert repersisted.lvstore_stack_tertiary == ""

    real = StorageNode({'uuid': 'u1', 'lvstore_stack_secondary': 'primary-1'})
    assert real.lvstore_stack_secondary == 'primary-1'

    # lvstore_stack itself is still a genuine List[dict] and must be unaffected.
    stack = StorageNode({'uuid': 'u1', 'lvstore_stack': [{'type': 'bdev_lvstore'}]})
    assert stack.lvstore_stack == [{'type': 'bdev_lvstore'}]
