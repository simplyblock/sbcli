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
    print()
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
    print()
    if bad:
        print("  These lane bases never create a pool. On docker the base setup")
        print("  deletes every pool first, so the first `volume add` will fail")
        print("  with 'Pool not found' -- which contains no 'error' substring,")
        print("  so a naive check will pass it through and the test will die")
        print("  somewhere else entirely.")
        print()
        print("  Fix: call self.ensure_pool() in the base's setup().")
        print()
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
        print()
        print("  Use utils.common_utils.cli_failed(out, err) instead. 'Pool not")
        print("  found: testpool' contains no 'error' and cost a nine-hour run.")
        print()
        return False
    print("  none")
    print()
    return True


def check_seed_writes_to_a_real_mount():
    """A lane must not write to self.mount_path while holding many volumes.

    self.mount_path is one suite-wide constant, "/mnt/test_location". A lane
    that creates several volumes and writes all of them there has them
    overwrite each other, and the last one standing verifies clean. Per-volume
    mounts come from _connect_and_mount_dual, which records the mount in the
    registry; _seed_volume_dual drives the whole sequence correctly.

    This is check 4 of the week. The failure it encodes: seed() called
    _connect_and_mount_dual WITHOUT mount_path -- the mount is conditional on
    that argument -- then wrote to self.mount_path, which did not exist. dd
    said "No such file or directory", create_random_files logged "Aborting."
    and returned normally, and the run reached the checksum guard with an
    empty volume.
    """
    print("=" * 68)
    print("lanes seed through the registry, not self.mount_path")
    print("=" * 68)
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
        for f in sorted(files):
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            for i, line in enumerate(
                    open(path, encoding="utf-8", errors="replace"), 1):
                if "mount_path=self.mount_path" in line.replace(" ", ""):
                    hits.append((os.path.relpath(path, REPO), i, line.strip()))
    if hits:
        print(f"  {len(hits)} site(s) write to the shared mount point:")
        for rel, i, line in hits:
            print(f"    {rel}:{i}")
            print(f"        {line[:96]}")
        print()
        print("  self.mount_path is ONE directory for the whole suite. Use")
        print("  self._seed_volume_dual(name), or take the mount point that")
        print("  _connect_and_mount_dual returns and write to that.")
        print()
        return False
    print("  none")
    print()
    return True


def check_helper_kwargs():
    """Calls into the suite's helper objects must use parameter names they have.

    `check_cli_usage.py` catches invented CLI flags. This catches the same
    mistake one layer up -- invented PYTHON keyword arguments -- which cost a
    whole case:

        self.ssh_obj.disconnect_all_active_interfaces(
            node=ip, interfaces=None, duration=90)

        TypeError: got an unexpected keyword argument 'node'

    The real signature is (node_ip, interfaces, duration_secs=300). Three
    wrong names in one call, and an identical call sat in the replication
    lane waiting to do the same on its first outage run. Nothing executes
    until the test is mid-flight, so this is cheap here and expensive later.

    Matched on the RECEIVER, not the method name: `x.connect(...)` is usually
    paramiko's connect, not SshUtils.connect, and judging every call by name
    alone reports those as errors.
    """
    import ast
    import inspect

    print("=" * 68)
    print("helper calls use parameter names the helper actually has")
    print("=" * 68)

    #: attribute name a helper is reached through -> the class behind it
    # One receiver can be several classes: self.sbcli_utils is SbcliUtils on
    # docker and K8sSbcliUtils on k8s, and the k8s one takes parameters the
    # docker one does not (allowed_nodes, storage_class_parameters). So a
    # keyword is only wrong when NO candidate class accepts it.
    RECEIVERS = {
        "ssh_obj": [("utils.ssh_utils", "SshUtils")],
        "sbcli_utils": [("utils.sbcli_utils", "SbcliUtils"),
                        ("utils.k8s_utils", "K8sSbcliUtils")],
        "common_utils": [("utils.common_utils", "CommonUtils")],
    }

    sigs, wildcard = {}, set()
    for recv, candidates in RECEIVERS.items():
        for modname, attr in candidates:
            try:
                cls = getattr(__import__(modname, fromlist=[attr]), attr)
            except Exception as exc:                  # noqa: BLE001
                print(f"  (skipped {modname}.{attr}: {str(exc)[:70]})")
                continue
            for name, fn in vars(cls).items():
                if name.startswith("__") or not callable(fn):
                    continue
                try:
                    params = inspect.signature(fn).parameters
                except (TypeError, ValueError):
                    continue
                if any(p.kind is inspect.Parameter.VAR_KEYWORD
                       for p in params.values()):
                    wildcard.add((recv, name))   # **kwargs takes anything
                    continue
                sigs.setdefault((recv, name), set()).update(
                    set(params) - {"self"})
    for key in wildcard:
        sigs.pop(key, None)

    def receiver_of(node):
        """'self.ssh_obj' / 'ssh_obj' -> 'ssh_obj'; anything else -> None."""
        v = node.func.value
        if isinstance(v, ast.Attribute):
            return v.attr
        if isinstance(v, ast.Name):
            return v.id
        return None

    bad = []
    for root, dirs, files in os.walk(E2E):
        dirs[:] = [d for d in dirs
                   if d not in ("__pycache__", "logs", ".git", "scripts")]
        for f in sorted(files):
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            try:
                tree = ast.parse(open(path, encoding="utf-8").read())
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not node.keywords:
                    continue
                if not isinstance(node.func, ast.Attribute):
                    continue
                allowed = sigs.get((receiver_of(node), node.func.attr))
                if allowed is None:
                    continue
                for kw in node.keywords:
                    if kw.arg and kw.arg not in allowed:
                        bad.append((os.path.relpath(path, REPO), node.lineno,
                                    node.func.attr, kw.arg, sorted(allowed)))
    if bad:
        print(f"  {len(bad)} call(s) pass a keyword the helper does not take:")
        for rel, line, meth, kw, allowed in bad[:20]:
            print(f"    {rel}:{line}")
            print(f"        {meth}(... {kw}=...) -- it takes: "
                  f"{', '.join(allowed)}")
        if len(bad) > 20:
            print(f"    ... and {len(bad) - 20} more")
        print()
        print("  Each raises TypeError the moment the line runs, which on a")
        print("  fault-injection path is several minutes into a case.")
        print()
        return False
    print(f"  none ({len(sigs)} helper signature(s) checked)")
    print()
    return True


def check_bringup_document_against_crd():
    """Every field the k8s bring-up emits must exist in the operator's CRD.

    Two runs died on this and neither reached a test. The apiserver decodes
    these documents STRICTLY, so one misplaced key rejects the whole thing
    in a tenth of a second, three attempts in a row:

        strict decoding error: unknown field
        "spec.discover.deviceFilter.enablePartitionedDevices"

    It is a one-level-too-deep mistake and it has now happened twice in the
    same function, to two different fields -- enableLogicalBlockDevices
    first, then enablePartitionedDevices. Both are "statements about the
    run" that sit on spec.discover, next to a deviceFilter whose members
    all narrow what is reported rather than widening it, which is exactly
    the kind of distinction a reader gets wrong.

    Strict decoding is doing us a favour: a tolerant apiserver would have
    dropped the field and handed us a cluster that quietly ignored it. But
    it only tells us at bring-up, after a lab has been wiped and an hour
    spent. This asks the question offline.

    Skips when the operator checkout is not beside us, because a developer
    without it should not be blocked -- CI has it (the workflows check out
    simplyblock-operator), which is where it matters.
    """
    import glob as _glob
    import importlib.util as _ilu

    print("=" * 68)
    print("the k8s bring-up document validates against the operator CRD")
    print("=" * 68)

    crds = []
    for root in (os.path.join("..", "..", "simplyblock-operator"),
                 os.path.join("..", "simplyblock-operator"),
                 os.environ.get("OPERATOR_REPO", "")):
        if not root:
            continue
        crds += _glob.glob(os.path.join(
            root, "operator", "config", "crd", "bases", "*operatorops*.yaml"))
    if not crds:
        print("  SKIPPED: no simplyblock-operator checkout found next to "
              "this one, so there is no CRD to validate against. Set "
              "OPERATOR_REPO to its path to enable this check.")
        print()
        return True

    try:
        import yaml as _yaml
    except ImportError:
        print("  SKIPPED: PyYAML is not available here")
        print()
        return True

    with open(crds[0], encoding="utf-8") as fh:
        crd = _yaml.safe_load(fh.read())
    try:
        schema = (crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
                  ["properties"]["spec"])
    except (KeyError, IndexError, TypeError):
        print(f"  SKIPPED: {crds[0]} is not shaped like a CRD")
        print()
        return True

    path = os.path.join("scripts", "k8s_cluster_bringup.py")
    if not os.path.exists(path):
        path = os.path.join("e2e", "scripts", "k8s_cluster_bringup.py")
    spec = _ilu.spec_from_file_location("_bringup", path)
    bringup = _ilu.module_from_spec(spec)
    spec.loader.exec_module(bringup)
    # Worker resolution is the only part that needs a live cluster.
    bringup.resolve_workers = lambda names: list(names or ["worker-0"])

    def unknown(node, sch, where="spec"):
        bad, props = [], (sch or {}).get("properties") or {}
        for key, val in (node or {}).items():
            if key not in props:
                bad.append(f"{where}.{key}")
                continue
            if isinstance(val, dict):
                bad += unknown(val, props[key], f"{where}.{key}")
        return bad

    # The combinations that differ in which fields are emitted at all.
    cases = (
        ({"ENABLE_PARTITIONED": "true",
          "DRIVE_SIZE_RANGE": "1500G-2000G"}, "nvme, partitioned waived"),
        ({"ENABLE_PARTITIONED": "false"}, "nvme, partitioned refused"),
        ({"DEVICE_MODE": "lblk",
          "ENABLE_PARTITIONED": "true"}, "lblk, partitioned waived"),
        ({"PCIE_MODEL": "SAMSUNG"}, "nvme, pcie model"),
        ({}, "defaults"),
    )
    touched = ("ENABLE_PARTITIONED", "DRIVE_SIZE_RANGE", "DEVICE_MODE",
               "PCIE_MODEL", "BLOCK_DEVICES", "WORKER_NODES")
    saved = {k: os.environ.get(k) for k in touched}
    bad_any = []
    try:
        for env, label in cases:
            for key in touched:
                os.environ.pop(key, None)
            os.environ.update(env)
            os.environ["WORKER_NODES"] = "worker-0,worker-1"
            try:
                body = bringup.build_discovery("preflight-check")
            except Exception as exc:                  # noqa: BLE001
                print(f"  {label}: could not build a document: "
                      f"{str(exc)[:160]}")
                bad_any.append((label, ["<build failed>"]))
                continue
            doc = body.get("spec", body)
            bad = unknown(doc, schema)
            print(f"  {label:26} {'OK' if not bad else 'UNKNOWN FIELDS'}")
            if bad:
                bad_any.append((label, bad))
    finally:
        for key, val in saved.items():
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val

    if bad_any:
        print()
        for label, bad in bad_any:
            for field in bad:
                print(f"  {label}: {field} is not in the CRD at that level")
        print()
        print("  The apiserver decodes these strictly, so this rejects the")
        print("  whole document and every discovery attempt fails in a")
        print("  tenth of a second. Check whether the field belongs one")
        print("  level up -- spec.discover rather than its deviceFilter.")
        print()
        return False
    print()
    return True


def check_no_attribute_shadows_a_method():
    """No ``self.x = ...`` where ``x`` is also a method.

    Written after one line cost a 4h26m run and 18 of its 21 cases. The
    migration lane gained a seeded random number generator whose seed was
    stored as ``self.seed`` -- and ``seed()`` was already a method on that
    class, the one that writes known content to a fresh volume. From the
    first test onward every case that created a volume died on

        TypeError: 'int' object is not callable

    and the only three that "passed" were the negative cases, which make
    their volumes with seed=False and so never called it. A green-looking
    3/21 that actually meant the suite never ran.

    Python gives no warning for this: an instance attribute simply hides
    the class's method from that point on, and the failure surfaces
    wherever the method is next called, which can be a different file.
    The cost of asking is one AST walk.

    Methods are collected from the class itself and from any base in the
    same file, plus the two shared bases every lane inherits, since
    shadowing an INHERITED method is the easier mistake to make -- the
    name is not visible in the file doing the shadowing.
    """
    import ast as _ast

    print("=" * 68)
    print("no instance attribute shadows a method of the same class")
    print("=" * 68)

    roots = [os.path.join("e2e_tests"), os.path.join("stress_test"),
             os.path.join("load_tests")]
    if not any(os.path.isdir(r) for r in roots):
        roots = [os.path.join("e2e", r) for r in
                 ("e2e_tests", "stress_test", "load_tests")]

    # The bases nearly everything inherits. Shadowing one of their methods
    # is invisible in the file that does it.
    shared = {}
    for rel in (os.path.join("e2e_tests", "cluster_test_base.py"),
                os.path.join("e2e", "e2e_tests", "cluster_test_base.py"),
                os.path.join("e2e_tests", "migration", "migration_base.py"),
                os.path.join("e2e", "e2e_tests", "migration",
                             "migration_base.py")):
        if not os.path.exists(rel):
            continue
        try:
            tree = _ast.parse(open(rel, encoding="utf-8").read())
        except SyntaxError:
            continue
        for node in _ast.walk(tree):
            if isinstance(node, _ast.ClassDef):
                shared[node.name] = {b.name for b in node.body
                                     if isinstance(b, (_ast.FunctionDef,
                                                       _ast.AsyncFunctionDef))}

    bad = []
    for root in roots:
        if not os.path.isdir(root):
            continue
        for dirpath, _dirs, files in os.walk(root):
            for fn in sorted(files):
                if not fn.endswith(".py"):
                    continue
                path = os.path.join(dirpath, fn)
                try:
                    tree = _ast.parse(open(path, encoding="utf-8").read())
                except SyntaxError:
                    continue
                local = {}
                for node in _ast.walk(tree):
                    if isinstance(node, _ast.ClassDef):
                        local[node.name] = {
                            b.name for b in node.body
                            if isinstance(b, (_ast.FunctionDef,
                                              _ast.AsyncFunctionDef))}
                for node in _ast.walk(tree):
                    if not isinstance(node, _ast.ClassDef):
                        continue
                    methods = set(local.get(node.name, ()))
                    for base in node.bases:
                        name = getattr(base, "id", None) or getattr(
                            base, "attr", None)
                        methods |= set(local.get(name, ()))
                        methods |= set(shared.get(name, ()))
                    if not methods:
                        continue
                    for sub in _ast.walk(node):
                        if not isinstance(sub, (_ast.Assign, _ast.AnnAssign,
                                                _ast.AugAssign)):
                            continue
                        targets = (sub.targets if isinstance(sub, _ast.Assign)
                                   else [sub.target])
                        for tgt in targets:
                            if (isinstance(tgt, _ast.Attribute)
                                    and isinstance(tgt.value, _ast.Name)
                                    and tgt.value.id == "self"
                                    and tgt.attr in methods):
                                bad.append((path, getattr(sub, "lineno", 0),
                                            node.name, tgt.attr))

    if bad:
        for path, line, cls, attr in sorted(set(bad)):
            try:
                rel = os.path.relpath(path)
            except ValueError:
                rel = path
            print(f"  {rel}:{line}")
            print(f"      {cls}.{attr} is a METHOD, and this assigns an "
                  f"attribute over it")
            print(f"      every later self.{attr}(...) raises TypeError "
                  f"'... object is not callable'")
        print()
        print(f"  {len(set(bad))} shadowed method(s). Rename the attribute.")
        print()
        return False
    print("  none")
    print()
    return True


def check_no_lossy_error_truncation():
    """No bare ``[:n]`` slice on text that could be a product error.

    Twice in two days this lane destroyed the only copy of the thing
    that explained a failure. The migration poller cut its error at 200
    characters, which landed exactly on "(taking the migration back
    also failed: ...)", so the more serious half of both rollback
    failures is the half that is gone. await_batch logged no error at
    all, so a tree failing its SECOND migration -- the shape SFAM-2820
    describes -- survives only as "batch status=failed phase=migrating
    members=3".

    The cluster is destroyed when a run ends. A truncated product error
    is not a smaller clue than a whole one, it is no clue, and getting
    it back costs another seven-hour run.

    MigrationTestBase.detail() exists for this: it logs the full text
    and returns a short form pointing at it. Slices on names, ids and
    sample lists are fine; this only matches captured output.
    """
    print("=" * 68)
    print("no product error is truncated without being logged in full")
    print("=" * 68)

    rx = re.compile(
        r"(?:str\(\s*(?:exc|err|e|out|combined|last|stderr|stdout)\s*\)"
        r"|\b(?:out|err|combined|last|output|stderr|stdout)\b"
        r"(?:\s*\+\s*\w+)?(?:\.strip\(\))?)\[:\d+\]")

    base = os.path.join(E2E, "e2e_tests", "migration")
    bad = []
    if os.path.isdir(base):
        for name in sorted(os.listdir(base)):
            if not name.endswith(".py"):
                continue
            path = os.path.join(base, name)
            with open(path, encoding="utf-8") as fh:
                for n, line in enumerate(fh, 1):
                    if rx.search(line):
                        bad.append("    e2e_tests/migration/%s:%d" % (name, n))
                        bad.append("        %s" % line.strip()[:120])
    if bad:
        for row in bad:
            print(row)
        print()
        print("    Use self.detail(x) rather than x[:n]: it logs the whole")
        print("    text and returns a short form that points at it, so the")
        print("    message stays readable and nothing is destroyed.")
        return False
    print("    none")
    return True


def check_params_the_helper_refuses_to_default():
    """A parameter that looks optional but whose helper refuses it empty.

    `check_helper_kwargs` catches keywords a helper does not have. This
    catches the opposite: a keyword it DOES have, declared `= None` so every
    call site compiles, whose body then raises when it is not supplied.

        def create_backup_restore(self, ..., target_pool: str | None = None):
            if not target_pool:
                raise ValueError("target_pool is required by ...")

    Nothing is wrong until the line runs, and then it is wrong everywhere at
    once: 44 of 48 call sites omitted target_pool, which is 16 classes of the
    2026-10-17 backup lane failing after ten hours on a ValueError raised
    before a single restore was attempted.

    The signature cannot be trusted here -- `= None` says optional and the
    body says otherwise -- so the body is what is read.
    """
    import ast

    print("=" * 68)
    print("parameters a helper declares optional and then refuses")
    print("=" * 68)

    HELPER_DIR = os.path.join("e2e", "utils")
    HELPERS = [os.path.join("e2e", "utils", "k8s_utils.py"),
               os.path.join("e2e", "utils", "sbcli_utils.py"),
               os.path.join("e2e", "utils", "ssh_utils.py")]

    def refused_params(fn):
        """Params this function raises on when falsy or None."""
        out = set()
        names = {a.arg for a in
                 fn.args.args + fn.args.posonlyargs + fn.args.kwonlyargs}
        # Only top-level guards. A nested one states an either/or --
        # delete_snapshot's "if not snap_id: if not snap_name: raise" means
        # supply one of the two, not both, and flagging each separately
        # reports every correct call site.
        for node in fn.body:
            if not isinstance(node, ast.If):
                continue
            # The guard must do nothing but refuse. A body that also
            # loops, branches or assigns is a branch SELECTOR, not a
            # refusal: wait_for_health_status's "if not device_id:" picks
            # the node path and raises only on timeout at the end of it.
            if not any(isinstance(s, ast.Raise) for s in node.body):
                continue
            if not all(isinstance(s, (ast.Raise, ast.Expr, ast.Pass))
                       for s in node.body):
                continue
            t = node.test
            # if not x:
            if isinstance(t, ast.UnaryOp) and isinstance(t.op, ast.Not) \
                    and isinstance(t.operand, ast.Name) \
                    and t.operand.id in names:
                out.add(t.operand.id)
            # if x is None:
            elif isinstance(t, ast.Compare) and isinstance(t.left, ast.Name) \
                    and t.left.id in names \
                    and len(t.ops) == 1 and isinstance(t.ops[0], ast.Is) \
                    and isinstance(t.comparators[0], ast.Constant) \
                    and t.comparators[0].value is None:
                out.add(t.left.id)
        # A parameter the function reassigns before guarding is not the
        # caller's input by then. delete_snapshot resolves snap_id from
        # snap_name and then raises "Snapshot not found" if the LOOKUP came
        # back empty -- a runtime outcome, not a missing argument.
        assigned = set()
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        assigned.add(tgt.id)
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign))                     and isinstance(node.target, ast.Name):
                assigned.add(node.target.id)
        out -= assigned

        # only those that are actually defaulted -- a plain required
        # positional is already enforced by Python
        defaulted = set()
        args = fn.args.args[1:]                      # drop self
        for a, d in zip(args[len(args) - len(fn.args.defaults):],
                        fn.args.defaults):
            if isinstance(d, ast.Constant) and d.value is None:
                defaulted.add(a.arg)
        for a, d in zip(fn.args.kwonlyargs, fn.args.kw_defaults):
            if isinstance(d, ast.Constant) and d.value is None:
                defaulted.add(a.arg)
        return out & defaulted

    # method name -> {param: "Class.method"} it refuses
    refuses, order = {}, {}
    for path in HELPERS:
        if not os.path.exists(path):
            continue
        tree = ast.parse(open(path, encoding="utf-8").read())
        for cls in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            for fn in [n for n in cls.body
                       if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
                bad = refused_params(fn)
                if bad:
                    refuses.setdefault(fn.name, {}).update(
                        {p: f"{cls.name}.{fn.name}" for p in bad})
                    order[fn.name] = [a.arg for a in fn.args.args[1:]]

    if not refuses:
        print("    no helper refuses a parameter it defaults -- nothing to check")
        return True
    for meth, params in sorted(refuses.items()):
        print(f"    {meth}: {', '.join(sorted(params))}")

    bad = []
    for root, _dirs, files in os.walk("e2e"):
        if any(p in root for p in ("__pycache__", ".agents")):
            continue
        # The helpers' own modules are excluded: a helper calling a sibling
        # supplies what it means to internally, and a bare name like
        # `connect` there is as likely to be paramiko's as ours.
        if os.path.normpath(root).startswith(os.path.normpath(HELPER_DIR)):
            continue
        for fname in files:
            if not fname.endswith(".py"):
                continue
            path = os.path.join(root, fname)
            try:
                tree = ast.parse(open(path, encoding="utf-8").read())
            except SyntaxError:
                continue
            # Each call is judged inside its enclosing function, because
            # the common way to break this is not omitting the argument --
            # it is FORWARDING one that is itself defaulted to None:
            #
            #     def _restore_backup(self, ..., pool_name=None):
            #         k8s.create_backup_restore(..., target_pool=pool_name)
            #
            # Every call site reads as supplying the parameter and 44 of 48
            # supplied None. Presence alone proves nothing.
            def scan(fn_node, nullable):
                for node in ast.iter_child_nodes(fn_node):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        inner = set(nullable)
                        a = node.args
                        pos = a.args + a.posonlyargs
                        for arg, dflt in zip(pos[len(pos) - len(a.defaults):],
                                             a.defaults):
                            if isinstance(dflt, ast.Constant)                                     and dflt.value is None:
                                inner.add(arg.arg)
                            else:
                                inner.discard(arg.arg)
                        for arg, dflt in zip(a.kwonlyargs, a.kw_defaults):
                            if isinstance(dflt, ast.Constant)                                     and dflt.value is None:
                                inner.add(arg.arg)
                            else:
                                inner.discard(arg.arg)
                        scan(node, inner)
                        continue
                    if isinstance(node, ast.Call)                             and isinstance(node.func, ast.Attribute)                             and node.func.attr in refuses:
                        check_call(node, nullable)
                    scan(node, nullable)

            def check_call(node, nullable, path=path):
                if any(k.arg is None for k in node.keywords):
                    return                   # **kwargs -- cannot tell
                by_kw = {k.arg: k.value for k in node.keywords if k.arg}
                names = order.get(node.func.attr, [])
                positional = set(names[:len(node.args)])
                for param, owner in refuses[node.func.attr].items():
                    if param not in by_kw and param not in positional:
                        bad.append(
                            f"        {path}:{node.lineno}  "
                            f"{node.func.attr}(...) omits '{param}', which "
                            f"{owner} raises on")
                        continue
                    val = by_kw.get(param)
                    if isinstance(val, ast.Name) and val.id in nullable:
                        bad.append(
                            f"        {path}:{node.lineno}  "
                            f"{node.func.attr}(...) passes {param}="
                            f"{val.id}, which defaults to None, and "
                            f"{owner} raises on empty")

            scan(tree, set())
    if bad:
        for row in sorted(set(bad)):
            print(row)
        print()
        print("    Pass a real value, or give the helper a real default.")
        print("    `= None` plus a raise is a required argument that every")
        print("    call site compiles against and no call site satisfies.")
        return False
    print("    every call site supplies them")
    return True


def main():
    ok = True
    ok &= run("CLI invocations match simplyblock_cli/cli.py",
              [os.path.join("e2e", "scripts", "check_cli_usage.py")])
    ok &= run("every group class is nameable individually",
              [os.path.join("e2e", "scripts", "check_test_registry.py")])
    ok &= check_lane_creates_a_pool()
    ok &= check_no_naive_error_checks()
    ok &= check_seed_writes_to_a_real_mount()
    ok &= check_helper_kwargs()
    ok &= check_params_the_helper_refuses_to_default()
    ok &= check_no_attribute_shadows_a_method()
    ok &= check_no_lossy_error_truncation()
    ok &= check_bringup_document_against_crd()

    print("=" * 68)
    if ok:
        print("PREFLIGHT PASSED -- nothing a cluster was needed to find")
        print()
        print("Still unproven by this: anything that needs a real cluster.")
        print("Run the smallest case next, not the whole lane.")
        return 0
    print("PREFLIGHT FAILED -- fix the above before spending lab time")
    return 1


if __name__ == "__main__":
    sys.exit(main())
