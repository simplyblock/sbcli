import uuid
from typing import ContextManager
from unittest.mock import patch

import pytest

from simplyblock_core import utils, storage_node_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import JMDevice, RemoteJMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import helpers, parse_thread_siblings_list


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



@patch.object(DBController, 'get_jm_device_by_id')
def test_get_node_jm_names(db_controller_get_jm_device_by_id):

    node_1_jm = JMDevice()
    node_1_jm.uuid = "node_1_jm_id"
    node_1_jm.jm_bdev = "node_1_jm"

    node_2_jm = JMDevice()
    node_2_jm.uuid = "node_2_jm_id"
    node_2_jm.jm_bdev = "node_2_jm"

    node_3_jm = JMDevice()
    node_3_jm.uuid = "node_3_jm_id"
    node_3_jm.jm_bdev = "node_3_jm"

    node_4_jm = JMDevice()
    node_4_jm.uuid = "node_4_jm_id"
    node_4_jm.jm_bdev = "node_4_jm"

    def get_jm_device_by_id(jm_id):
        for jm in [node_1_jm, node_2_jm, node_3_jm, node_4_jm]:
            if jm.uuid == jm_id:
                return jm

    db_controller_get_jm_device_by_id.side_effect = get_jm_device_by_id

    node_1 = StorageNode()
    node_1.uuid = str(uuid.uuid4())
    node_1.enable_ha_jm = True
    node_1.ha_jm_count = 4
    node_1.jm_device = node_1_jm
    node_1.jm_ids = ["node_2_jm_id", "node_3_jm_id", "node_4_jm_id"]

    remote_node = StorageNode()
    remote_node.uuid = str(uuid.uuid4())
    remote_node.enable_ha_jm = True
    remote_node.jm_ids = []
    remote_node.jm_device = node_2_jm
    remote_node.remote_jm_devices = [
        RemoteJMDevice({"uuid": node_1_jm.uuid, "remote_bdev": f"rem_{node_1_jm.jm_bdev}"}),
        RemoteJMDevice({"uuid": node_3_jm.uuid, "remote_bdev": f"rem_{node_3_jm.jm_bdev}"}),
        RemoteJMDevice({"uuid": node_4_jm.uuid, "remote_bdev": f"rem_{node_4_jm.jm_bdev}"})]

    jm_names = storage_node_ops.get_node_jm_names(node_1, remote_node=remote_node)
    print(f"jm_names: {len(jm_names)}", jm_names)

