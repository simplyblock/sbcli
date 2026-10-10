"""Make the product packages importable from the e2e suite.

A handful of tests read values straight out of the product rather than
restating them -- `test_object_limits.py` asserts against
`simplyblock_core.constants.MAX_LVOL_SIZE` instead of a hardcoded number, which
is the right call: a copied constant silently stops matching the day the
product changes it.

But e2e runs with `e2e/` as the working directory, so the repo root is not on
`sys.path` and `simplyblock_core` is not importable. That went unnoticed for a
long time because the runners had an sbcli pip-installed globally at some
point, which satisfied the import by accident. The moment e2e moved into a
clean venv (scripts/ensure_python.sh) the accident stopped happening and
`__init__.py` failed at import, taking every test with it.

`simplyblock_core.constants` pulls in nothing but stdlib and `_version`, so
putting the repo root on the path costs nothing and is far cheaper than
installing the product and its whole dependency tree into the e2e venv.

Appended, never inserted, for two reasons:

  - the repo root has a `scripts/` directory and so does e2e; on Python 3 any
    directory is importable as a namespace package, so prepending would let the
    repo root shadow e2e's own modules.
  - an sbcli that really is installed still wins, because site-packages comes
    first. This changes nothing for anyone who has it installed properly and
    only fills in the gap when nobody does.
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def ensure_product_importable() -> bool:
    """Append the repo root to sys.path. True if the product is now reachable.

    Guarded on `simplyblock_core` actually being there so that an e2e tree
    vendored somewhere else does not append an unrelated directory.
    """
    if not (_REPO_ROOT / "simplyblock_core").is_dir():
        return False

    root = str(_REPO_ROOT)
    if root not in sys.path:
        sys.path.append(root)
    return True


ensure_product_importable()
