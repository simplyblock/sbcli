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
REL = os.path.join("e2e", ".agents")
CONFIG = os.path.join(AGENTS, "repo-map.config.json")
MAPS = os.path.join(AGENTS, "repo-maps")
SCRIPT = os.path.join(AGENTS, "scripts", "repo_map.py")


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
        head = short_head(path)
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
        if head and head not in recorded:
            stale.append(f"{name} (map at {recorded.replace('head: ', '')}, "
                         f"repo at {head})")
        else:
            current.append(name)

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
    if stale or missing:
        print(f"[repo-maps] refresh with: python3 "
              f"{os.path.join(REL, 'scripts', 'repo_map.py')}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:                          # noqa: BLE001
        # A hook that breaks a session is worse than a hook that says nothing.
        print(f"[repo-maps] hook error (ignored): {type(exc).__name__}: {exc}")
        sys.exit(0)
