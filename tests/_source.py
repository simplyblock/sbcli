"""Source-inspection helpers for tests that assert what a function's code does
or does not mention."""

import ast
import inspect
import textwrap


def code_of(fn) -> str:
    """``fn``'s code without its docstring or comments.

    A docstring commonly names exactly what the function must not do ("never
    calls create_migration"), so a check against the raw source trips on the
    explanation. Stripping it with ``getsource(fn).replace(fn.__doc__, "")``
    only works where ``__doc__`` matches the source text: Python 3.13 removes
    a docstring's indentation from ``__doc__``, the replace then matches
    nothing, and the check fails on 3.13+ while passing on 3.11. Dropping the
    docstring from the parsed function does not depend on that.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    node = tree.body[0]
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) \
            and ast.get_docstring(node, clean=False) is not None:
        node.body = node.body[1:] or [ast.Pass()]
    return ast.unparse(node)
