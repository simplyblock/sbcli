"""Single source of the distribution version.

Kept separate from ``constants`` and deliberately free of third-party imports:
``[tool.setuptools.dynamic]`` in ``pyproject.toml`` resolves the version by
importing this module at build time, where only the standard library is
available.
"""
import os


def _read_version() -> str:
    env_var = os.path.join(os.path.dirname(os.path.realpath(__file__)), "env_var")
    with open(env_var, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("SIMPLY_BLOCK_VERSION"):
                return line.split("=", 1)[1].strip()
    return "1"


__version__ = os.getenv("SIMPLY_BLOCK_VERSION") or _read_version()
