from typing import ContextManager

import pytest

from simplyblock_core import utils

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
    (1e3, 'kB', 1.),
    (1e6, 'MB', 1.),
    (1e9, 'GB', 1.),
    (1e12, 'TB', 1.),
    (1e15, 'PB', 1.),
    (1e18, 'EB', 1.),
    (1e21, 'ZB', 1.),
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
