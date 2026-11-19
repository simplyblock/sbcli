"""Single source of the distribution version.

Kept separate from ``constants`` and deliberately free of third-party imports:
``[tool.setuptools.dynamic]`` in ``pyproject.toml`` resolves the version by
importing this module at build time, where only the standard library is
available.
"""
import os
import re

# A hotfix number appended after a prerelease segment, e.g. ``26.4.0-beta6.1``
# (hotfix 1 on beta6). ``base`` is everything through the prerelease word and
# its own number; ``hotfix`` is the trailing number PEP 440 doesn't have a
# slot for in that position.
_HOTFIX_ON_PRERELEASE = re.compile(
    r"^(?P<base>\d+(?:\.\d+)*-[A-Za-z]+\d+)\.(?P<hotfix>\d+)$"
)


def _read_version() -> str:
    env_var = os.path.join(os.path.dirname(os.path.realpath(__file__)), "env_var")
    with open(env_var, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("SIMPLY_BLOCK_VERSION"):
                return line.split("=", 1)[1].strip()
    return "1"


def _to_pep440(version: str) -> str:
    """Translate this repo's human version strings into a PEP 440-valid one.

    Every shape already in use (``19.2.35``, ``26.3.3.1``, ``26.4.0-beta6``)
    is already PEP 440-valid and passes through unchanged. A hotfix on a
    prerelease (``26.4.0-beta6.1``) is not -- PEP 440 has no numbered segment
    after a prerelease word except an explicit post-release -- so it becomes
    ``26.4.0-beta6.post1``, which ``packaging.version.Version`` (used by
    setuptools during ``uv sync``/``uv build``) accepts and sorts after
    ``26.4.0-beta6`` and before the final ``26.4.0``, matching intent.
    """
    match = _HOTFIX_ON_PRERELEASE.match(version)
    if match is None:
        return version
    return f"{match['base']}.post{match['hotfix']}"


# The raw, human-authored string -- used for runtime display (`sbctl
# --version`, via constants.SIMPLY_BLOCK_VERSION) and for the Docker image
# tags in release.yml. Neither needs PEP 440 compliance, so this is never
# normalized.
__version__ = os.getenv("SIMPLY_BLOCK_VERSION") or _read_version()

# The value `[tool.setuptools.dynamic]` resolves `version` to. setuptools
# validates this one via `packaging.version.Version`, so it must always be
# PEP 440-valid even when `__version__` (above) is not.
__dist_version__ = _to_pep440(__version__)
