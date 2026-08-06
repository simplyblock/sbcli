# coding=utf-8
"""Data-size parsing (SI / IEC / JEDEC units)."""

import re
from typing import Union


def _parse_unit(unit: str, mode: str = 'si/iec', strict: bool = True) -> tuple[int, int]:
    """Parse the given unit, returning the associated base and exponent

    Mode can be either 'si/iec' to parse decimal (SI) and binary (IEC) units, or
    'jedec' for binary only units. If `strict`, parsing will be case-sensitive and
    expect the 'B' suffix.
    """
    regexes = {
        'si/iec': r'^((?P<prefix>[kKMGTPEZ])(?P<binary>i)?)?' + ('B$' if strict else 'B?$'),
        'jedec': r'^(?P<prefix>[KMGTPEZ])?' + ('B$' if strict else 'B?$'),
    }

    m = re.match(regexes[mode], unit, flags=re.IGNORECASE if not strict else 0)
    if m is None:
        raise ValueError("Invalid unit")

    binary = (mode == 'jedec') or (m.group('binary') is not None)
    prefix = m.group('prefix') or ''

    if strict and (binary and (prefix == 'k')) or ((not binary) and (prefix == 'K')):
        raise ValueError("Invalid unit")

    exponent_multipliers = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']
    return (
        2 if binary else 10,
        (10 if binary else 3) * exponent_multipliers.index(prefix.upper())
    )


def parse_size(size: Union[str, int], mode: str = 'si/iec', assume_unit: str = '', strict: bool = False) -> int:
    """Parse the given data size

    If passed and not explicitly given, 'assume_unit' will be assumed.
    Mode can be either 'si/iec' to parse decimal (SI) and binary (IEC) units, or
    'jedec' for binary only units. If `strict`, parsing will be case-sensitive and
    expect the 'B' suffix.
    """
    try:
        if isinstance(size, int):
            size_in_unit = size
            unit = assume_unit
        else:
            m = re.match(r'^(?P<size_in_unit>\d+) ?(?P<unit>\w+)?$', size.strip())
            if m is None:
                raise ValueError(f"Invalid size: {size}")

            size_in_unit = int(m.group('size_in_unit'))
            unit = m.group('unit') if m.group('unit') else assume_unit

        base, exponent = _parse_unit(unit, mode, strict=strict)
        return size_in_unit * (base ** exponent)
    except ValueError:
        return -1
