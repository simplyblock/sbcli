"""Defined vs imported vs registered, for every pNFS case class.

A class that is defined but never registered is dead code that reads as
coverage; one that is registered but not defined is an ImportError the
whole suite dies on. Both have happened, so this is checked rather than
eyeballed.
"""
import ast
import pathlib
import re

ROOT = pathlib.Path("e2e_tests/pnfs")

defined = {}
for f in sorted(ROOT.glob("test_pnfs_*.py")):
    tree = ast.parse(f.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            defined[node.name] = f.name

init = pathlib.Path("__init__.py").read_text(encoding="utf-8")
imported = set()
for m in re.finditer(
        r"from e2e_tests\.pnfs\.\w+ import \(([^)]*)\)", init):
    imported |= {n.strip().rstrip(",") for n in m.group(1).split("\n")
                 if n.strip().rstrip(",")}

groups = {}
for m in re.finditer(
        r"def (get_pnfs\w*_tests)\(\):(.*?)(?=\ndef |\Z)", init, re.S):
    body = m.group(2)
    ret = re.search(r"return \[(.*?)\n    \]", body, re.S)
    if not ret:
        continue
    names = re.findall(r"^\s*(Pnfs\w+),", ret.group(1), re.M)
    groups[m.group(1)] = names

registered = {n for names in groups.values() for n in names}

print(f"defined   {len(defined)}")
print(f"imported  {len(imported & set(defined))}")
print(f"registered {len(registered)}")
for g, names in sorted(groups.items()):
    print(f"  {g:34s} {len(names):3d}")

#: --testname <ClassName> resolves against ALL_TESTS only (e2e.py:338).
#: A class in a group but not in ALL_TESTS runs as part of its group and
#: answers "not found" on its own -- which is how 29 of these shipped.
m = re.search(r"\nALL_TESTS = \[(.*?)\n\]", init, re.S)
in_all = set(re.findall(r"^\s*(\w+),", m.group(1), re.M)) if m else set()
not_addressable = sorted(set(defined) - in_all)

ghosts = sorted(registered - set(defined))
unreg = sorted(set(defined) - registered)
unimp = sorted(set(defined) - imported)
dupes = sorted({n for n in registered
                if sum(names.count(n) for names in groups.values()) > 1})

print()
print(f"registered here but defined elsewhere:   {ghosts or 'none'}"
      "  (expected: the Track A classes live in stress_test/)")
print(f"defined but NOT registered (dead):        {unreg or 'none'}")
print(f"defined but NOT imported:                 {unimp or 'none'}")
print(f"registered in more than one group:        {dupes or 'none'}")
print(f"defined but NOT in ALL_TESTS:           {not_addressable or 'none'}")
assert not unreg and not unimp and not not_addressable, "cross-check failed"
print("\nOK")
