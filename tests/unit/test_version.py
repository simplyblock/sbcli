"""`_version.py` must stay import-safe with only the stdlib (it's imported by
`[tool.setuptools.dynamic]` at build time), so its PEP 440 normalization is a
hand-rolled regex rather than a call into `packaging`. These pin it against
`packaging.version.Version` directly -- the thing setuptools actually
validates against -- so a future edit can't drift into a shape that parses
under the hand-rolled regex but not under PEP 440 itself.
"""
import pytest
from packaging.version import InvalidVersion, Version

from simplyblock_core._version import _to_pep440


# Every SIMPLY_BLOCK_VERSION shape this repo's git tags have actually used,
# plus the new hotfix-on-prerelease shape this fix adds support for.
@pytest.mark.parametrize(
    "raw",
    [
        "19.2.35",
        "26.3.3.1",
        "26.3.3.2",
        "26.3.3.3",
        "26.4.0-beta1",
        "26.4.0-beta6",
    ],
)
def test_already_valid_shapes_pass_through_unchanged(raw: str) -> None:
    assert _to_pep440(raw) == raw
    Version(_to_pep440(raw))  # must already be PEP 440-valid


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("26.4.0-beta6.1", "26.4.0-beta6.post1"),
        ("26.4.0-beta6.2", "26.4.0-beta6.post2"),
        ("26.4.0-beta1.10", "26.4.0-beta1.post10"),
    ],
)
def test_hotfix_on_prerelease_becomes_a_post_release(raw: str, expected: str) -> None:
    assert _to_pep440(raw) == expected
    Version(expected)  # must be PEP 440-valid


def test_raw_hotfix_on_prerelease_is_not_pep440_valid() -> None:
    # Pins down *why* the fix is needed: without it, setuptools' build-time
    # validation (packaging.version.Version) rejects this shape outright.
    with pytest.raises(InvalidVersion):
        Version("26.4.0-beta6.1")


def test_hotfix_sorts_after_its_prerelease_and_before_the_final_release() -> None:
    assert Version(_to_pep440("26.4.0-beta6")) < Version(_to_pep440("26.4.0-beta6.1"))
    assert Version(_to_pep440("26.4.0-beta6.1")) < Version("26.4.0")
