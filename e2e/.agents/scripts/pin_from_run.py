#!/usr/bin/env python3
"""Pin the repo maps to the code a specific CI run actually executed.

Why this exists
---------------
Every moving part in an e2e run can be on a different ref: the automation on a
feature branch, the product image on main or a release tag, SPDK on 26.3, the
operator on a fix branch. Debugging that run against whatever happens to be
checked out locally produces answers that are *precisely wrong* -- a log line
resolves to a real file:line in code that never ran, which is worse than not
finding it at all, because it looks like an answer.

So: read the versions a run used, write them to repo-maps/pins.json, and let
repo_map.py rebuild each map from its pinned ref.

Usage
-----
    # from a CI run (needs the run-versions artifact, see below)
    python3 .agents/scripts/pin_from_run.py --run 12345678 --build

    # from a versions JSON you already have (artifact, Slack paste, local file)
    python3 .agents/scripts/pin_from_run.py --from-file run-versions.json

    # by hand, when you already know what ran
    python3 .agents/scripts/pin_from_run.py --set spdk=26.3 --set sbcli=main

    # back to your own checkouts
    python3 .agents/scripts/pin_from_run.py --clear

Where the versions come from
----------------------------
workflow_dispatch inputs are NOT exposed by the GitHub REST API for a run, so
none of this can be derived from a run id alone. The e2e workflows therefore
write a `run-versions` artifact holding the inputs verbatim, and --run
downloads it. A run from before that step existed has no artifact, and --run
says so rather than guessing.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, UTC

HERE = os.path.dirname(os.path.abspath(__file__))
AGENTS_DIR = os.path.dirname(HERE)
OUT_DIR = os.path.join(AGENTS_DIR, "repo-maps")
PINS = os.path.join(OUT_DIR, "pins.json")

# run-versions field -> map name.
#
# sbcli is pinned to the product image, not to the automation branch: the
# automation is the tree you are sitting in and the file tools already reach
# it, while the product is the part you cannot see and the part that ran.
FIELD_TO_REPO = {
    "simplyblock_image": "sbcli",
    "spdk_image": "spdk",
    "operator_repo_branch": "operator",
}


def tag_to_ref(value):
    """Turn an image reference into a git ref, as far as that is honest.

    `simplyblock/spdk:main-latest` -> `main`, `26.3` -> `26.3`.

    These images carry no provenance label (there is no
    org.opencontainers.image.revision anywhere in the build), so this is
    convention, not fact: the tag names a branch, and that branch's tip today
    is not necessarily the commit the image was built from. The raw value is
    kept in the pin file so the inference is always auditable.
    """
    if not value:
        return ""
    ref = value.rsplit(":", 1)[-1] if ":" in value else value
    ref = re.sub(r"-latest$", "", ref)
    return ref.strip()


def versions_from_run(run_id):
    """Download and parse the run-versions artifact for a run."""
    tmp = tempfile.mkdtemp(prefix="pin-from-run-")
    try:
        try:
            p = subprocess.run(
                ["gh", "run", "download", str(run_id), "-n", "run-versions",
                 "-D", tmp],
                capture_output=True, text=True, timeout=180)
        except FileNotFoundError:
            sys.exit("gh CLI not found. Install it, or use --from-file/--set.")
        if p.returncode != 0:
            sys.exit(
                "no 'run-versions' artifact on run %s.\n%s\n\n"
                "That run predates the artifact, or it failed before the step "
                "that writes it. Read the versions off the run summary and "
                "pass them with --set instead." % (run_id, p.stderr.strip()))
        for dirpath, _, files in os.walk(tmp):
            for fn in sorted(files):
                if fn.endswith(".json"):
                    with open(os.path.join(dirpath, fn), encoding="utf-8") as fh:
                        return json.load(fh)
        sys.exit("run-versions artifact on %s contained no JSON" % run_id)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def to_pins(versions):
    """Map a run-versions document onto {repo: {ref, from}} plus caveats."""
    pins, notes = {}, []
    for field, repo in FIELD_TO_REPO.items():
        raw = (versions.get(field) or "").strip()
        if not raw:
            notes.append("%s: no %s in the run, left unpinned" % (repo, field))
            continue
        ref = tag_to_ref(raw)
        if not ref:
            notes.append("%s: could not derive a ref from %r" % (repo, raw))
            continue
        pins[repo] = {"ref": ref, "from": "%s=%s" % (field, raw)}

    # ultra ships inside the SPDK container and no workflow input names its
    # ref, so there is nothing to derive. Saying so is the honest move: quietly
    # copying the spdk ref would look authoritative and be a guess.
    if "ultra" not in pins:
        notes.append("ultra: no input names its ref (it is built into the SPDK "
                     "image) -- pin it by hand with --set ultra=<branch>")
    return pins, notes


def write_pins(pins, source, run_meta=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    doc = {
        "source": source,
        "captured": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pins": pins,
    }
    if run_meta:
        doc["run"] = run_meta
    with open(PINS, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    return doc


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--run", help="GitHub Actions run id or URL")
    g.add_argument("--from-file", help="a run-versions JSON on disk")
    g.add_argument("--clear", action="store_true",
                   help="remove pins and go back to the working trees")
    ap.add_argument("--set", action="append", default=[], metavar="NAME=REF",
                    help="pin one map by hand; wins over derived values")
    ap.add_argument("--build", action="store_true",
                    help="rebuild the maps once the pins are written")
    args = ap.parse_args()

    if args.clear:
        if os.path.exists(PINS):
            os.remove(PINS)
            print("removed %s" % PINS)
        else:
            print("no pins to clear")
        # Removing the pins does not un-build the maps. Saying so matters:
        # otherwise the next lookup silently uses pinned content under an
        # unpinned banner, which is the confusion this tool exists to remove.
        print("maps still hold the pinned content until you rebuild:")
        print("  python .agents/scripts/repo_map.py")
        return 0

    versions, source, run_meta = {}, "manual (--set)", None
    if args.run:
        run_id = str(args.run).rstrip("/").rsplit("/", 1)[-1]
        versions = versions_from_run(run_id)
        source = "github run %s" % run_id
        run_meta = {k: versions.get(k) for k in
                    ("run_id", "run_url", "workflow", "automation_branch",
                     "automation_sha", "cluster_id") if versions.get(k)}
    elif args.from_file:
        with open(args.from_file, encoding="utf-8") as fh:
            versions = json.load(fh)
        source = "file %s" % os.path.basename(args.from_file)

    pins, notes = to_pins(versions) if versions else ({}, [])

    for item in args.set:
        if "=" not in item:
            sys.exit("--set needs NAME=REF, got %r" % item)
        k, v = item.split("=", 1)
        pins[k.strip()] = {"ref": v.strip(), "from": "--set"}

    if not pins:
        sys.exit("nothing to pin -- pass --run, --from-file or --set")

    doc = write_pins(pins, source, run_meta)
    print("pins written to %s  (source: %s)" % (PINS, doc["source"]))
    for name in sorted(pins):
        print("  %-10s %-24s %s" % (name, pins[name]["ref"],
                                    pins[name].get("from", "")))
    for n in notes:
        print("  note: %s" % n)

    if args.build:
        print()
        return subprocess.call([sys.executable,
                                os.path.join(HERE, "repo_map.py")])

    print("\nrebuild the maps with:")
    print("  python .agents/scripts/repo_map.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
