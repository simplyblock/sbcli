#!/usr/bin/env python3
"""SessionStart hook: tell the agent which external repo maps exist, and whether
they are stale.

What this does and does not do
------------------------------
It does NOT reduce token usage by itself. The saving comes from the map files:
grepping `.agents/repo-maps/<name>.messages.tsv` answers "where does this log
line come from" in one call, instead of walking somebody else's 10,000-file
tree. All this hook does is make sure the agent knows the maps are there and
whether they still match the checked-out commit, because a map nobody mentions
gets ignored and a stale map is worse than none.

It is deliberately silent when there is no config, so a developer who has not
set any of this up sees nothing.

Wired in e2e/.claude/settings.json as a SessionStart hook.
"""

import json
import os
import subprocess
import sys

# Resolve everything from this file's own location rather than from
# CLAUDE_PROJECT_DIR. The hook lives under e2e/, but the project root may be
# either the repo root or e2e/ depending on how the session was opened, and a
# hook that only works from one of those is a hook that silently does nothing
# half the time.
AGENTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Print paths relative to wherever the session actually opened, so the hint is
# copy-pasteable whether the root is the repo or e2e/.
try:
    REL = os.path.relpath(AGENTS, os.getcwd())
except ValueError:          # different drive on Windows
    REL = AGENTS
CONFIG = os.path.join(AGENTS, "repo-map.config.json")
MAPS = os.path.join(AGENTS, "repo-maps")
PINS = os.path.join(MAPS, "pins.json")
SCRIPT = os.path.join(AGENTS, "scripts", "repo_map.py")


def interpreter():
    """The python name that actually runs here, for the copy-paste hint.

    On Windows a bare `python3` is usually the Microsoft Store alias stub: it
    is on PATH, exits 49, and runs nothing. Printing it in a hint sends the
    reader to a command that fails for reasons that have nothing to do with
    what they were debugging.
    """
    try:
        ok = subprocess.run(["python3", "-c", ""], capture_output=True,
                            timeout=10).returncode == 0
    except Exception:                                 # noqa: BLE001
        ok = False
    return "python3" if ok else "python"


def ref_candidates(ref):
    """Spellings to try. Image tags do not spell branch names: SPDK tags an
    image `26.3` while the branch is `R26.3`."""
    out = [ref, "origin/" + ref]
    if not ref.startswith("R"):
        out += ["R" + ref, "origin/R" + ref]
    if ref.startswith("v"):
        out += [ref[1:], "origin/" + ref[1:]]
    return out


def near_refs(path, ref):
    """Branches whose name contains the ref, so an unresolvable pin points at
    what does exist instead of just failing."""
    try:
        out = subprocess.run(["git", "branch", "-a", "--list", "*%s*" % ref],
                             cwd=path, capture_output=True, text=True,
                             timeout=15)
        names = [ln.strip().lstrip("* ").replace("remotes/", "")
                 for ln in out.stdout.splitlines() if ln.strip()]
        return names[:3]
    except Exception:                                 # noqa: BLE001
        return []


def rev(path, ref):
    """Short sha for a ref, trying the usual variants. Empty if unresolvable."""
    for cand in ref_candidates(ref):
        try:
            out = subprocess.run(["git", "rev-parse", "--short", cand],
                                 cwd=path, capture_output=True, text=True,
                                 timeout=15)
            if out.returncode == 0 and out.stdout.strip():
                return out.stdout.strip()
        except Exception:                             # noqa: BLE001
            pass
    return ""


def load_pins():
    """Pinned refs, written by pin_from_run.py or by hand. Never raises."""
    if not os.path.exists(PINS):
        return {}, {}
    try:
        with open(PINS, encoding="utf-8") as fh:
            doc = json.load(fh)
        return doc.get("pins", {}) or {}, doc
    except Exception:                                 # noqa: BLE001
        return {}, {}


def short_head(path):
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             cwd=path, capture_output=True, text=True,
                             timeout=15)
        return out.stdout.strip()
    except Exception:                                 # noqa: BLE001
        return ""


def main():
    # No config means this developer has not set up external repos. Say nothing.
    if not os.path.exists(CONFIG):
        return 0

    try:
        with open(CONFIG, encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as exc:                          # noqa: BLE001
        print(f"[repo-maps] config at {CONFIG} is unreadable: {exc}")
        return 0

    repos = cfg.get("repos") or {}
    if not repos:
        return 0

    pins, pindoc = load_pins()

    current, stale, missing, absent = [], [], [], []
    for name, rcfg in repos.items():
        path = rcfg.get("path", "")
        if not os.path.isdir(path):
            absent.append(f"{name} (path not on this machine: {path})")
            continue
        map_md = os.path.join(MAPS, f"{name}.md")
        if not os.path.exists(map_md):
            missing.append(name)
            continue
        # A pinned map must be judged against its pin, not against whatever
        # the developer happens to have checked out. Judging it against HEAD
        # is how a correctly pinned map gets reported STALE and rebuilt into
        # the wrong code, which is the exact failure this is meant to stop.
        ref = (pins.get(name) or {}).get("ref", "")
        want = rev(path, ref) if ref else short_head(path)
        label = f"{name}@{ref}" if ref else name
        recorded = ""
        try:
            with open(map_md, encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("head:"):
                        recorded = line.strip()
                        break
        except OSError:
            missing.append(name)
            continue
        if ref and not want:
            near = near_refs(path, ref)
            hint = (f"did you mean {', '.join(near)}?" if near
                    else f"git -C {path} fetch --all")
            stale.append(f"{label} (pinned ref does not resolve -- {hint})")
        elif want and want not in recorded:
            stale.append(f"{label} (map at {recorded.replace('head: ', '')}, "
                         f"want {want})")
        else:
            current.append(label)

    if not (current or stale or missing or absent):
        return 0

    print("[repo-maps] External repo indexes for cross-repo analysis.")
    print("[repo-maps] GREP these, do not read them:")
    print(f"[repo-maps]   {REL}{os.sep}repo-maps{os.sep}<name>.messages.tsv"
          f"   message -> file:line")
    print(f"[repo-maps]   {REL}{os.sep}repo-maps{os.sep}<name>.symbols.tsv"
          f"    symbol  -> file:line")
    if current:
        print(f"[repo-maps] current: {', '.join(sorted(current))}")
    if stale:
        print(f"[repo-maps] STALE, results may point at the wrong lines: "
              f"{'; '.join(stale)}")
    if missing:
        print(f"[repo-maps] no map yet: {', '.join(sorted(missing))}")
    if absent:
        print(f"[repo-maps] configured but not present here: {'; '.join(absent)}")
    if pins:
        src = pindoc.get("source") or "pins.json"
        print(f"[repo-maps] PINNED to the code a run used, from {src}. "
              f"Maps are NOT your working tree.")
    if stale or missing:
        print(f"[repo-maps] refresh with: {interpreter()} "
              f"{os.path.join(REL, 'scripts', 'repo_map.py')}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:                          # noqa: BLE001
        # A hook that breaks a session is worse than a hook that says nothing.
        print(f"[repo-maps] hook error (ignored): {type(exc).__name__}: {exc}")
        sys.exit(0)
