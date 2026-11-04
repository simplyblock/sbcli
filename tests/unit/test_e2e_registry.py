"""Every test a group lists must also be in ``ALL_TESTS``.

``e2e.py`` resolves a group keyword (``--testname pnfs``) from the group
function, but an explicitly named class (``--testname PnfsCloseToOpenK8s``)
is looked up in ``ALL_TESTS`` and nowhere else. A class registered in only
one of the two is therefore runnable one way and reports

    Test 'PnfsCloseToOpenK8s' not found.

the other way, which is how run 41604777129 died before a single case
started: twelve new Track B classes went into ``get_pnfs_tests()`` and not
into ``ALL_TESTS``.

This reads ``e2e/__init__.py`` as text rather than importing it. The e2e
tree is PEP 723 scripts with no shared virtualenv and importing it pulls in
SSH and Supabase configuration that the unit tier does not have, so a
static read is the only version of this check that can run here at all.
"""
import ast
import pathlib
import re

import pytest

E2E_INIT = (pathlib.Path(__file__).resolve().parents[2]
            / "e2e" / "__init__.py")


def _all_tests_names(source):
    """The names listed in the module-level ``ALL_TESTS``."""
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if "ALL_TESTS" not in targets:
            continue
        if not isinstance(node.value, (ast.List, ast.Tuple)):
            pytest.fail("ALL_TESTS is no longer a literal list; this guard "
                        "reads it statically and needs updating")
        return {e.id for e in node.value.elts if isinstance(e, ast.Name)}
    pytest.fail(f"no module-level ALL_TESTS assignment in {E2E_INIT}")
    return set()


def _group_functions(source):
    """``{function name: [class names it returns]}`` for each get_*_tests."""
    tree = ast.parse(source)
    groups = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if not (node.name.startswith("get_") and node.name.endswith("_tests")):
            continue
        names = []
        for sub in ast.walk(node):
            if isinstance(sub, ast.Return) and isinstance(
                    sub.value, (ast.List, ast.Tuple)):
                names += [e.id for e in sub.value.elts
                          if isinstance(e, ast.Name)]
        if names:
            groups[node.name] = names
    return groups


@pytest.fixture(scope="module")
def source():
    if not E2E_INIT.is_file():
        pytest.skip(f"{E2E_INIT} not present")
    return E2E_INIT.read_text(encoding="utf-8")


def test_every_grouped_class_is_in_all_tests(source):
    known = _all_tests_names(source)
    groups = _group_functions(source)
    assert groups, "found no get_*_tests group functions to check"

    # A group may legitimately be built by calling other groups rather than
    # by listing classes; those return no Name elements and drop out above.
    missing = {
        fn: sorted(n for n in names if n not in known)
        for fn, names in groups.items()
        if any(n not in known for n in names)
    }
    assert not missing, (
        "these classes are returned by a group but are absent from "
        "ALL_TESTS, so `--testname <ClassName>` cannot find them even "
        f"though `--testname <group>` runs them: {missing}")


def test_all_tests_has_no_duplicates(source):
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "ALL_TESTS"
                for t in node.targets):
            names = [e.id for e in node.value.elts
                     if isinstance(e, ast.Name)]
            dupes = sorted({n for n in names if names.count(n) > 1})
            assert not dupes, f"ALL_TESTS lists these twice: {dupes}"
            return


def test_imported_group_classes_resolve(source):
    """Every class a group returns is imported into the module.

    A name that is listed but never imported raises NameError at import
    time for the whole suite, not just for that group.
    """
    imported = set(re.findall(r"^\s+(\w+),\s*$", source, re.MULTILINE))
    imported |= {m.group(1) for m in
                 re.finditer(r"^from [\w.]+ import (\w+)$", source, re.MULTILINE)}
    unknown = sorted({
        n for names in _group_functions(source).values() for n in names
        if n not in imported
    })
    assert not unknown, (
        f"a group returns these but nothing imports them: {unknown}")


def _names_in_function(source, func_name):
    """Every Name in every list/tuple literal inside *func_name*."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            out = set()
            for sub in ast.walk(node):
                if isinstance(sub, (ast.List, ast.Tuple)):
                    out |= {e.id for e in sub.elts if isinstance(e, ast.Name)}
            return out
    return set()


def _e2e_py_group_functions():
    """Group functions e2e.py dispatches, read from its keyword map.

    A group reachable this way does not need to be in get_stress_tests():
    e2e.py resolves it from its own map and the class from ALL_TESTS.
    migration-stress and replication-stress are exactly that case, so a
    guard that ignores this flags two groups that are correctly registered.
    """
    e2e_main = E2E_INIT.parent / "e2e.py"
    if not e2e_main.is_file():
        pytest.skip(f"{e2e_main} not present")
    tree = ast.parse(e2e_main.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for v in node.values:
                if isinstance(v, ast.Name) and v.id.startswith("get_"):
                    names.add(v.id)
    return names


def test_stress_classes_are_reachable_from_stress_py(source):
    """A stress.py-dispatched class must be in the registry stress.py reads.

    stress.py resolves --testname against

        get_stress_tests() + get_backup_stress_tests()

    and nothing else -- not ALL_TESTS, not the class's own group. So a soak
    registered in ALL_TESTS and in its own group is still "not found" when
    the stress workflow dispatches it.

    Not hypothetical: run 41919726297 spent 75 minutes building a cluster
    and then died on

        Test 'PnfsRwxRapidNoGapK8s' not found.

    because the three Track A classes were in ALL_TESTS and in
    get_pnfs_stress_tests() but not in get_stress_tests().

    Groups e2e.py dispatches from its own keyword map are exempt, because
    that runner resolves the class from ALL_TESTS instead.
    """
    groups = _group_functions(source)
    # Not _group_functions(): it only reads a literal `return [...]`, and
    # get_stress_tests() assigns `tests = [...]` and returns the name. Read
    # every list literal in the function body instead, or the reachable set
    # comes back empty and the guard flags everything.
    reachable = _names_in_function(source, "get_stress_tests")
    reachable |= _names_in_function(source, "get_backup_stress_tests")
    by_e2e_py = _e2e_py_group_functions()

    missing = []
    for name, classes in groups.items():
        if not name.endswith("_stress_tests"):
            continue
        if name in ("get_stress_tests", "get_backup_stress_tests"):
            continue
        if name in by_e2e_py:
            continue
        for cls in classes:
            if cls not in reachable:
                missing.append(f"{cls} (listed only in {name}())")

    assert not missing, (
        "these stress classes cannot be dispatched by stress.py, which "
        "reads get_stress_tests() + get_backup_stress_tests() and nothing "
        "else: " + ", ".join(sorted(missing)))
