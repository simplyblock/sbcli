"""Walk the k8s path of the outage matrix and report anything that no-ops.

Looks for the failure mode that has bitten repeatedly here: a step that
iterates an empty collection, or checks a value that is always truthy, and
therefore reports success without testing anything.
"""
import inspect
import sys

sys.path.insert(0, ".")
from stress_test.lblk_outage_matrix import _LblkOutageMatrix as C  # noqa: E402

findings = []


def src(m):
    """Source of a method with docstrings stripped.

    Matching raw source is not good enough: the comments that explain each of
    these fixes name the very symbols being searched for, so every check
    reported a false finding against code that was already correct.
    """
    import ast
    t = ast.parse(inspect.getsource(getattr(C, m)).lstrip())
    for n in ast.walk(t):
        if isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.Module)):
            b = n.body
            if (b and isinstance(b[0], ast.Expr)
                    and isinstance(b[0].value, ast.Constant)
                    and isinstance(b[0].value.value, str)):
                n.body = b[1:] or [ast.Pass()]
    return ast.unparse(t)


# 1. Anything that could silently iterate nothing must be guarded.
for meth, coll in [("_stamp_all", "_lblk_devices"),
                   ("_verify_raw", "_lblk_devices"),
                   ("_assert_static_unchanged", "_static")]:
    body = src(meth) if hasattr(C, meth) else ""
    if not body:
        findings.append(f"{meth}: not found")
        continue
    if coll in body and "raise" not in body and "if not" not in body:
        findings.append(f"{meth} iterates {coll} with no empty-guard")

# 2. Truthiness checks on handles that are strings on k8s.
alive = src("_assert_fio_alive")
if "bool(handle)" in alive:
    findings.append("_assert_fio_alive still uses bool(handle) "
                    "(always true for a k8s job name)")

# 3. Final validation must raise on k8s, not warn.
fin = src("_finish_live_fio")
if "_validate_fio_dual" in fin:
    findings.append("_finish_live_fio uses _validate_fio_dual, which only "
                    "warns on k8s")
if "validate_fio_job" not in fin:
    findings.append("_finish_live_fio does not call the raising validator")

# 4. The raw lane must go through _provision_raw on k8s.
build = src("_build_static_set").replace('"', "'")
if "_create_and_connect" in build:
    findings.append("_build_static_set uses _create_and_connect "
                    "(no Block PVC, no verifier on k8s)")
if "_provision_raw" not in build:
    findings.append("_build_static_set never calls _provision_raw")

# 5. Mount points handed to the md5 reader must be absolute.
md5 = src("_static_md5").replace('"', "'")
if "startswith('/')" not in md5:
    findings.append("_static_md5 accepts a non-absolute mount")

# 6. Every flavour must appear in BOTH lanes.
live = src("_start_live_fio").replace('"', "'")
for fl in ("plain", "crypto", "dhchap", "nsvol"):
    if f"'{fl}'" not in build:
        findings.append(f"static lane missing flavour {fl}")
    if f"'{fl}'" not in live:
        findings.append(f"live lane missing flavour {fl}")

print(f"audited {len(inspect.getsource(C).splitlines())} lines of the matrix")
for f in findings:
    print("  FINDING " + f)
print("\nno silent no-ops found" if not findings else f"\n{len(findings)} finding(s)")
sys.exit(1 if findings else 0)
