#!/usr/bin/env python3
"""Everything that can be checked without a cluster. Run before every push.

Written because three consecutive runs of a brand-new lane failed on things
no cluster was needed to find, and each one cost hours of lab time plus the
teardown after every failing case:

  1. `volume add ... --pool testpool`   -- there is no --pool flag; the pool
     is the third positional. 19 classes, each dead in its first second.
  2. `--testname MigrationSmoke`        -- the class was in its group function
     but not in ALL_TESTS, which is what a NAMED class resolves against.
  3. `Pool not found: testpool`         -- the lane never created a pool, and
     the error string contains no "error" substring so the check passed it
     through and the test died three frames later in seed().

All three are static. All three are found below in about a second.

    python e2e/scripts/preflight.py

The shape of the mistake is always the same: a new lane does not follow a
contract the suite never states out loud. So each check here exists to state
one of those contracts and fail loudly when it is broken.
"""
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
E2E = os.path.dirname(HERE)
REPO = os.path.dirname(E2E)
sys.path.insert(0, E2E)
os.environ.setdefault("KEY_NAME", "preflight")
os.environ.setdefault("KEY_PATH", os.devnull)


def run(name, argv):
    print("=" * 68)
    print(name)
    print("=" * 68)
    r = subprocess.run([sys.executable] + argv, cwd=REPO)
    print("")
    return r.returncode == 0


def check_lane_creates_a_pool():
    """Every lane base must create a pool before any case runs.

    On docker the base setup DELETES every pool first, so nothing is
    inherited from a previous run. A lane that assumes self.pool_name already
    exists gets `Pool not found: testpool` on its first volume -- and because
    that string contains no "error", a naive output check lets it through and
    the failure surfaces later and elsewhere.

    The contract: a lane base either calls ensure_pool() in setup(), or every
    one of its run() methods creates the pool itself.
    """
    print("=" * 68)
    print("lane bases create a pool")
    print("=" * 68)
    bad = []
    bases = []
    for root, _dirs, files in os.walk(os.path.join(E2E, "e2e_tests")):
        for f in files:
            if f.endswith("_base.py") and f != "cluster_test_base.py":
                bases.append(os.path.join(root, f))
    for path in sorted(bases):
        src = open(path, encoding="utf-8").read()
        rel = os.path.relpath(path, REPO)
        if "ensure_pool(" in src or "_add_pool_dual(" in src or "_make_pool(" in src:
            print(f"  ok    {rel}")
        else:
            print(f"  MISS  {rel}")
            bad.append(rel)
    print("")
    if bad:
        print("  These lane bases never create a pool. On docker the base setup")
        print("  deletes every pool first, so the first `volume add` will fail")
        print("  with 'Pool not found' -- which contains no 'error' substring,")
        print("  so a naive check will pass it through and the test will die")
        print("  somewhere else entirely.")
        print("")
        print("  Fix: call self.ensure_pool() in the base's setup().")
        print("")
        return False
    return True


def check_no_naive_error_checks():
    """`if "error" in output` is not a failure check.

    The CLI reports failure in many shapes that do not contain the word:
    'Pool not found', 'unrecognized arguments', 'usage:', 'no such', or an
    empty response from a -d command. utils.common_utils.cli_failed knows
    them; a bare substring test does not.
    """
    print("=" * 68)
    print("no naive 'error' substring checks")
    print("=" * 68)
    # Only flag the pattern that decides SUCCESS -- `if "error" in out:` as a
    # gate. `assert err or "error" in out` is the opposite intent (asserting a
    # failure DID happen) and is correct, so matching it would make this check
    # permanently red on code that is fine.
    pat = re.compile(r'^\s*(if|elif)\s.*"error"\s+in\s')
    # Scoped to the lanes this guard was written for. Older suites use the
    # bare form widely; churning them is a separate job, and a guard nobody
    # can get to green is a guard nobody runs.
    SCOPE = (os.path.join("e2e_tests", "migration"),
             os.path.join("e2e_tests", "replication"),
             "load_tests")
    hits = []
    for root, dirs, files in os.walk(E2E):
        dirs[:] = [d for d in dirs
                   if d not in ("__pycache__", "logs", "scripts", ".git")]
        rel_root = os.path.relpath(root, E2E)
        if not any(rel_root.startswith(sc) for sc in SCOPE):
            continue
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            for i, line in enumerate(
                    open(path, encoding="utf-8", errors="replace"), 1):
                if pat.search(line) and "cli_failed" not in line:
                    hits.append((os.path.relpath(path, REPO), i, line.strip()))
    if hits:
        print(f"  {len(hits)} site(s) test for failure with a bare 'error':")
        for rel, i, line in hits[:20]:
            print(f"    {rel}:{i}")
            print(f"        {line[:96]}")
        if len(hits) > 20:
            print(f"    ... and {len(hits) - 20} more")
        print("")
        print("  Use utils.common_utils.cli_failed(out, err) instead. 'Pool not")
        print("  found: testpool' contains no 'error' and cost a nine-hour run.")
        print("")
        return False
    print("  none")
    print("")
    return True


def main():
    ok = True
    ok &= run("CLI invocations match simplyblock_cli/cli.py",
              [os.path.join("e2e", "scripts", "check_cli_usage.py")])
    ok &= run("every group class is nameable individually",
              [os.path.join("e2e", "scripts", "check_test_registry.py")])
    ok &= check_lane_creates_a_pool()
    ok &= check_no_naive_error_checks()

    print("=" * 68)
    if ok:
        print("PREFLIGHT PASSED -- nothing a cluster was needed to find")
        print("")
        print("Still unproven by this: anything that needs a real cluster.")
        print("Run the smallest case next, not the whole lane.")
        return 0
    print("PREFLIGHT FAILED -- fix the above before spending lab time")
    return 1


if __name__ == "__main__":
    sys.exit(main())
