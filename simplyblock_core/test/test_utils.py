from typing import ContextManager

import pytest
from unittest.mock import patch

from simplyblock_core import utils
from simplyblock_core.utils import helpers, parse_thread_siblings_list
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode


@pytest.mark.parametrize('args,expected', [
    (('0',), 0),
    (('1000',), 1000),
    (('1 kB',), 1e3),
    (('1M',), 1e6),
    (('1g',), 1e9),
    (('1MB',), 1e6),
    (('1GB',), 1e9),
    (('1TB',), 1e12),
    (('1PB',), 1e15),
    (('1KiB',), 2 ** 10),
    (('1MiB',), 2 ** 20),
    (('1GiB',), 2 ** 30),
    (('1TiB',), 2 ** 40),
    (('1PiB',), 2 ** 50),
    (('1kib',), 2 ** 10),
    (('1mi',), 2 ** 20),
    (('1Gi',), 2 ** 30),
    (('1K', 'jedec'), 2 ** 10),
    (('1M', 'jedec'), 2 ** 20),
    (('1G', 'jedec'), 2 ** 30),
    (('1T', 'jedec'), 2 ** 40),
    (('1P', 'jedec'), 2 ** 50),
    (('foo',), -1),
    (('1byte',), -1),
    (('1', 'jedec', 'G',), 2 ** 30),
    (('1M', 'jedec', 'G',), 2 ** 20),
    ((1,), 1),
    ((1, 'jedec', 'G'), 2 ** 30),
])
def test_parse_size(args, expected):
    assert utils.parse_size(*args) == expected


@pytest.mark.parametrize('args,expected', [
    ((0, 'si'), '0 B'),
    ((1, 'si'), '1.0 B'),
    ((2, 'si'), '2.0 B'),
    ((1e3, 'si'), '1.0 kB'),
    ((1e6, 'si'), '1.0 MB'),
    ((1e9, 'si'), '1.0 GB'),
    ((1e12, 'si'), '1.0 TB'),
    ((1e15, 'si'), '1.0 PB'),
    ((2 ** 10, 'si'), '1.0 kB'),
    ((2 ** 20, 'si'), '1.0 MB'),
    ((2 ** 30, 'si'), '1.1 GB'),
    ((2 ** 40, 'si'), '1.1 TB'),
    ((2 ** 50, 'si'), '1.1 PB'),
    ((0, 'iec'), '0 B'),
    ((1, 'iec'), '1.0 B'),
    ((2, 'iec'), '2.0 B'),
    ((1e3, 'iec'), '1000.0 B'),
    ((1e6, 'iec'), '976.6 KiB'),
    ((1e9, 'iec'), '953.7 MiB'),
    ((1e12, 'iec'), '931.3 GiB'),
    ((1e15, 'iec'), '909.5 TiB'),
    ((2 ** 10, 'iec'), '1.0 KiB'),
    ((2 ** 20, 'iec'), '1.0 MiB'),
    ((2 ** 30, 'iec'), '1.0 GiB'),
    ((2 ** 40, 'iec'), '1.0 TiB'),
    ((2 ** 50, 'iec'), '1.0 PiB'),
    ((0, 'jedec'), '0 B'),
    ((1, 'jedec'), '1.0 B'),
    ((2, 'jedec'), '2.0 B'),
    ((1e3, 'jedec'), '1000.0 B'),
    ((1e6, 'jedec'), '976.6 KB'),
    ((1e9, 'jedec'), '953.7 MB'),
    ((1e12, 'jedec'), '931.3 GB'),
    ((1e15, 'jedec'), '909.5 TB'),
    ((2 ** 10, 'jedec'), '1.0 KB'),
    ((2 ** 20, 'jedec'), '1.0 MB'),
    ((2 ** 30, 'jedec'), '1.0 GB'),
    ((2 ** 40, 'jedec'), '1.0 TB'),
    ((2 ** 50, 'jedec'), '1.0 PB'),
])
def test_humanbytes(args, expected):
    assert utils.humanbytes(*args) == expected


@pytest.mark.parametrize('size,unit,expected', [
    (0, 'B', 0.),
    (0, 'b', pytest.raises(ValueError)),
    (0, 'foo', pytest.raises(ValueError)),
    (1, 'B', 1.),
    (10 ** 3, 'kB', 1.),
    (10 ** 6, 'MB', 1.),
    (10 ** 9, 'GB', 1.),
    (10 ** 12, 'TB', 1.),
    (10 ** 15, 'PB', 1.),
    (10 ** 18, 'EB', 1.),
    (10 ** 21, 'ZB', 1.),
    (2 ** 10, 'KiB', 1.),
    (2 ** 20, 'MiB', 1.),
    (2 ** 30, 'GiB', 1.),
    (2 ** 40, 'TiB', 1.),
    (2 ** 50, 'PiB', 1.),
    (2 ** 60, 'EiB', 1.),
    (2 ** 70, 'ZiB', 1.),
])
def test_convert_size(size, unit, expected):
    if isinstance(expected, ContextManager):
        with expected:
            utils.convert_size(size, unit)
    else:
        assert utils.convert_size(size, unit) == expected


def test_singleton():
    with pytest.raises(ValueError):
        helpers.single([])

    assert helpers.single([1]) == 1

    with pytest.raises(ValueError):
        helpers.single([1, 2])


@pytest.mark.parametrize('input,expected', [
    ("9", [9]),
    ("9,25", [9, 25]),
    ("4-7", [4, 5, 6, 7]),
    ("9,25,41,57", [9, 25, 41, 57]),
    ("25-33:4", [25, 29, 33]),
    ("0-6/3", [0, 3, 6]),
    ("2-3,10-11", [2, 3, 10, 11]),
    ("2-8,10-16", [2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16]),
    ("1,2,4-10,12-20:4", [1, 2, 4, 5, 6, 7, 8, 9, 10, 12, 16, 20]),
    ("9,  25  ,41", [9, 25, 41]),
    ("", []),
    ("a-b", pytest.raises(ValueError)),
    ("5-2", pytest.raises(ValueError)),
    ("0-5:0", pytest.raises(ValueError)),
    ("0-5:-1", pytest.raises(ValueError)),
])
def test_parse_thread_siblings_list(input, expected):
    if isinstance(expected, ContextManager):
        with expected:
            parse_thread_siblings_list(input)
    else:
        assert parse_thread_siblings_list(input) == expected


@patch.object(DBController, 'get_storage_nodes_by_cluster_id')
@patch.object(DBController, 'get_storage_node_by_id')
def run_lvol_scheduler_test(testing_nodes, db_controller_get_storage_node_by_id, db_controller_get_storage_nodes_by_cluster_id):
    RUN_PER_TEST = 10000
    print("-" * 100)
    for testing_map in testing_nodes:
        nodes = {n['uuid']: n for n in testing_map}
        def get_node_by_id(node_id):
            for node in testing_map:
                if node['uuid'] == node_id:
                    return StorageNode({"status": StorageNode.STATUS_ONLINE, **node})
        db_controller_get_storage_node_by_id.side_effect = get_node_by_id
        db_controller_get_storage_nodes_by_cluster_id.return_value = [
            StorageNode({"status": StorageNode.STATUS_ONLINE, **node_params}) for node_params in testing_map]
        cluster_id = "cluster_id"
        out = {}
        total = RUN_PER_TEST
        for i in range(total):
            selected_nodes = lvol_controller._get_next_3_nodes(cluster_id)
            for index, node in enumerate(selected_nodes):
                if node.get_id() not in out:
                    out[node.get_id()] = {f"{index}": 1}
                else:
                    out[node.get_id()][f"{index}"] = out[node.get_id()].get(f"{index}", 0) + 1
        # assert len(nodes) == 3

        for k, v in out.items():
            print(f"node {k}: size_util={nodes[k]['node_size_util']} lvols_util={nodes[k]['lvol_count_util']} stats: {[f'{sk}: {int(v[sk]/total*100)}%' for sk in sorted(v.keys())]}")
        for v in testing_map:
            if v['uuid'] not in out:
                print(f"node {v['uuid']}: size_util={v['node_size_util']} lvols_util={v['lvol_count_util']} stats: excluded")
        print("-"*100)
