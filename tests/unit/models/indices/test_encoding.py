"""Escaping and ordered encoding: what a value becomes inside a key."""
import pytest

from simplyblock_core.models import indices


@pytest.mark.parametrize('value', [
    'plain', 'with/slash', 'with%percent', '%2F', '%25', 'a/b%c/d', '', '//', '%%',
])
def test_escaping_round_trips(value):
    assert indices.unescape(indices.escape(value)) == value


@pytest.mark.parametrize('value', ['with/slash', 'a/b%c/d', '%2F'])
def test_escaped_segment_carries_no_separator(value):
    """A value containing the separator must not be able to forge one."""
    assert '/' not in indices.escape(value)


def test_distinct_values_never_collide_after_escaping():
    """Escaping is injective, which is what keeps two values off one key."""
    values = ['a/b', 'a%2Fb', 'a%b', 'a%25b', 'a', 'b']
    assert len({indices.escape(v) for v in values}) == len(values)


def test_ordered_encoding_is_lexicographic_for_integers():
    encoded = [indices.encode_value(n, ordered=True) for n in (0, 1, 9, 10, 1000, 2**63 - 1)]
    assert encoded == sorted(encoded)
    assert len({len(e) for e in encoded}) == 1


@pytest.mark.parametrize('value', ['has/slash', 'has%percent'])
def test_ordered_encoding_refuses_values_it_cannot_order(value):
    with pytest.raises(ValueError, match='ordered index'):
        indices.encode_value(value, ordered=True)


def test_ordered_encoding_refuses_negative_integers():
    with pytest.raises(ValueError, match='negative'):
        indices.encode_value(-1, ordered=True)
