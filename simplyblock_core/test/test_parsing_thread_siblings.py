import unittest

import pytest

from simplyblock_core.utils import parse_thread_siblings_list

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
    ("a-b", ValueError),
    ("5-2", ValueError),
    ("0-5:0", ValueError),
    ("0-5:-1", ValueError),
])
def test_parse_thread_siblings_list(input, expected):
    if isinstance(expected, ValueError):
        with expected:
            parse_thread_siblings_list(input)
    else:
        assert parse_thread_siblings_list(input) == expected
