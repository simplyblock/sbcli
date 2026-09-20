"""Check every attribute the outage matrix reaches for actually exists.

Catches the class of bug that has now bitten twice: a method that exists in a
module but on a different class than the object being called.
"""
import ast
import io
import sys

sys.path.insert(0, ".")

from utils.k8s_utils import K8sUtils, K8sSbcliUtils      # noqa: E402
from utils.sbcli_utils import SbcliUtils                 # noqa: E402
from utils.ssh_utils import SshUtils                     # noqa: E402
from stress_test.lblk_outage_matrix import _LblkOutageMatrix  # noqa: E402

SRC = "stress_test/lblk_outage_matrix.py"
tree = ast.parse(io.open(SRC, encoding="utf-8").read())

# receiver expression -> the type(s) it can be at runtime
TARGETS = {
    "k8s": [K8sUtils],
    "self.sbcli_utils": [K8sSbcliUtils, SbcliUtils],
    "self.ssh_obj": [SshUtils],
}


def recv(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return f"{node.value.id}.{node.attr}"
    return None


problems, checked = [], 0
for n in ast.walk(tree):
    if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)):
        continue
    r = recv(n.func.value)
    meth = n.func.attr
    if r in TARGETS:
        checked += 1
        classes = TARGETS[r]
        # sbcli_utils may be either implementation; accept if ANY provides it,
        # but report when only one does, since that is a platform-specific call.
        have = [c.__name__ for c in classes if hasattr(c, meth)]
        if not have:
            problems.append(f"{r}.{meth}() exists on NONE of "
                            f"{[c.__name__ for c in classes]}  (line {n.lineno})")
        elif len(have) < len(classes):
            missing = [c.__name__ for c in classes if not hasattr(c, meth)]
            problems.append(f"{r}.{meth}() missing on {missing} "
                            f"(ok on {have})  (line {n.lineno})")
    elif r == "self":
        checked += 1
        if not hasattr(_LblkOutageMatrix, meth):
            problems.append(f"self.{meth}() not on _LblkOutageMatrix or its "
                            f"bases  (line {n.lineno})")

print(f"checked {checked} attribute calls")
for p in problems:
    print("  PROBLEM " + p)
print("\nclean" if not problems else f"\n{len(problems)} problem(s)")
sys.exit(1 if problems else 0)
