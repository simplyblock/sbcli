#!/usr/bin/env python3
"""Check every CLI invocation in the test tree against the real cli.py.

Written after an entire 19-class lane failed on its first line because
``volume add`` was called with ``--pool`` -- a flag that does not exist. The
pool is the third POSITIONAL argument. argparse rejected it, every test
raised in its first few seconds, and the run still took nine hours because
each failure was followed by a full teardown and diagnostic collection.

Four invented things went in together, all of which this would have caught
in under a second:

    volume add --pool X                 -> pool is positional
    volume add --replication-policy P   -> no such flag; attach afterwards
    volume add --subsystem S            -> no such flag; use --namespaced
    cluster replication-policy-add ... T -> --target is a required FLAG
    cluster replication-policy-add --snapshot-retention N -> it is --keep

Run it before pushing a lane, or from CI:

    python e2e/scripts/check_cli_usage.py
    python e2e/scripts/check_cli_usage.py --cli ../simplyblock_cli/cli.py

What it does NOT do: understand shell quoting, f-string interpolation of a
whole command, or verbs built up across several lines. It reports what it can
parse and says how many it skipped, because a checker that silently ignores
half its input is worse than no checker.
"""
import argparse
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_CLI = HERE.parent.parent / "simplyblock_cli" / "cli.py"
DEFAULT_ROOTS = [HERE.parent / "e2e_tests", HERE.parent / "load_tests",
                 HERE.parent / "stress_test"]

#: Nouns the CLI groups subcommands under, including the aliases.
NOUNS = ("storage-node", "sn", "cluster", "volume", "lvol", "control-plane",
         "cp", "mgmt", "storage-pool", "pool", "snapshot",
         "consistency-group", "cg", "backup", "qos", "db-backup")


def parse_cli(path):
    """{(noun, verb): {flags}} from the init_<noun>__<verb> definitions."""
    src = path.read_text(encoding="utf-8")
    spec, cur = {}, None
    for line in src.splitlines():
        m = re.match(r"\s*def init_([a-z_]+?)__([a-z_0-9]+)\(", line)
        if m:
            noun = m.group(1).replace("_", "-")
            verb = m.group(2).replace("_", "-")
            cur = (noun, verb)
            spec.setdefault(cur, set())
            continue
        if cur:
            if re.match(r"\s*def ", line):
                cur = None
                continue
            for f in re.findall(r"""['"](--[a-z0-9-]+)['"]""", line):
                spec[cur].add(f)
    return spec


def iter_invocations(roots):
    """Yield (file, lineno, noun, verb, [flags]) for every CLI call found."""
    pat = re.compile(
        r"""base_cmd\}\s*(?P<pre>(?:-d\s+|--dev\s+)*)(?P<rest>[^"']*)""")
    for root in roots:
        if not root.exists():
            continue
        for p in sorted(root.rglob("*.py")):
            for i, line in enumerate(p.read_text(encoding="utf-8",
                                                 errors="replace").splitlines(), 1):
                m = pat.search(line)
                if not m:
                    continue
                rest = m.group("rest").strip()
                toks = rest.split()
                if len(toks) < 2:
                    yield p, i, None, None, []        # unparseable
                    continue
                noun, verb = toks[0], toks[1]
                if noun not in NOUNS or verb.startswith(("-", "{")):
                    yield p, i, None, None, []
                    continue
                flags = [t for t in toks[2:] if t.startswith("--")]
                yield p, i, noun, verb, flags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cli", type=Path, default=DEFAULT_CLI)
    ap.add_argument("--root", type=Path, action="append", default=None)
    args = ap.parse_args()

    if not args.cli.exists():
        print(f"cannot read the CLI at {args.cli}; pass --cli", file=sys.stderr)
        return 2
    spec = parse_cli(args.cli)
    print(f"parsed {len(spec)} subcommands from {args.cli}")

    roots = args.root or DEFAULT_ROOTS
    bad, checked, skipped = [], 0, 0
    for p, line, noun, verb, flags in iter_invocations(roots):
        if noun is None:
            skipped += 1
            continue
        key = (noun, verb)
        # The CLI defines these under one noun and aliases the other, so try
        # the common aliases before calling a verb unknown.
        alias = {"volume": "lvol", "lvol": "volume", "sn": "storage-node",
                 "storage-node": "sn", "pool": "storage-pool",
                 "storage-pool": "pool", "cg": "consistency-group",
                 "consistency-group": "cg", "cp": "control-plane"}
        if key not in spec and (alias.get(noun), verb) in spec:
            key = (alias[noun], verb)
        if key not in spec:
            bad.append((p, line, f"unknown subcommand: {noun} {verb}"))
            continue
        checked += 1
        for f in flags:
            if f not in spec[key]:
                bad.append((p, line,
                            f"{noun} {verb}: {f} is not a flag of this "
                            f"subcommand (it has: "
                            f"{' '.join(sorted(spec[key])) or 'no flags'})"))

    print(f"checked {checked} invocation(s); {skipped} not statically "
          f"parseable (interpolated verb or command built across lines)")
    if bad:
        print(f"\n{len(bad)} problem(s):\n")
        for p, line, msg in bad:
            try:
                rel = p.relative_to(Path.cwd())
            except ValueError:
                rel = p
            print(f"  {rel}:{line}\n      {msg}")
        return 1
    print("no invented flags or verbs found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
