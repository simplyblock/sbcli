#!/usr/bin/env python3
"""Agent-neutral guard hook: gate use of the dev docker-compose stack.

`tests/AGENTS.md` mandates that integration tests run through
`tox run -e integration`, which provisions FoundationDB via testcontainers.
The `docker-compose-dev.yml` stack (and reusing its FDB instance) is only to be
touched when a human explicitly instructs it — yet agents reflexively reach for
`docker compose -f docker-compose-dev.yml ...` to "check what's already
running". This hook turns that silent reflex into an explicit confirmation.

It reads a pre-tool-execution payload as JSON on stdin and, for shell commands
that reference `docker-compose-dev.yml`, asks the runner to confirm before
proceeding. It is deliberately runner-neutral:

- Input contract: ``{"tool_name": "...", "tool_input": {"command": "..."}}``.
  Anything that doesn't look like a shell command touching the dev compose file
  is passed through untouched.
- Output contract (Claude Code ``PreToolUse``): a JSON decision on stdout that
  asks for confirmation. The process always exits 0 so Claude Code uses the JSON
  decision rather than treating a non-zero exit as a hard block. Other agent
  runners can wire this same script to their own pre-exec hook and read the same
  stdout JSON.

Keep the matching logic here (the source of truth); per-runner config should
only *reference* this script, mirroring how `.agents/skills/` is the shared
source and tool-specific dirs are thin pointers.
"""
import json
import re
import sys

# Match the dev compose file by name, however it is referenced on the command
# line (-f docker-compose-dev.yml, a path prefix, .yaml, etc.).
_PATTERN = re.compile(r"docker-compose-dev\.ya?ml")

_REASON = (
    "This command uses the dev docker-compose stack (docker-compose-dev.yml). "
    "Per tests/AGENTS.md, integration tests must run via `tox run -e integration` "
    "(testcontainers provisions FoundationDB automatically); the dev compose "
    "stack and reusing its FDB instance are only for explicit, human-instructed "
    "use. Approve only if you were explicitly told to use compose."
)


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0  # Unparseable input is not ours to police.

    if payload.get("tool_name") != "Bash":
        return 0

    command = (payload.get("tool_input") or {}).get("command", "") or ""
    if not _PATTERN.search(command):
        return 0

    # Claude Code reads this JSON and turns the call into a user confirmation.
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": _REASON,
        }
    }))
    # Always exit 0: the stdout JSON carries the "ask" decision. A non-zero exit
    # would be read as a hard block instead of a confirmation prompt.
    return 0


if __name__ == "__main__":
    sys.exit(main())
