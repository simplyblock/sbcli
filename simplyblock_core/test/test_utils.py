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


@pytest.mark.parametrize('size,expected', [
    (0, '0'),
    (1, '1.0 Byte'),
    (2, '2.0 Bytes'),
    (1e3, '1.0 KB'),
    (1e6, '1.0 MB'),
    (1e9, '1.0 GB'),
    (1e12, '1.0 TB'),
    (1e15, '1000.0 TB'),
    (2 ** 10, '1.0 KB'),
    (2 ** 20, '1.0 MB'),
    (2 ** 30, '1.1 GB'),
    (2 ** 40, '1.1 TB'),
    (999e3, '999.0 KB'),
])
def test_humanbytes(size, expected):
    if isinstance(expected, ContextManager):
        with expected:
            utils.humanbytes(size)
    else:
        assert utils.humanbytes(size) == expected


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
