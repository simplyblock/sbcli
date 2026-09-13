# Agent tooling for the e2e suite

Everything here is scoped to `e2e/`. Nothing outside this folder is touched, and
in particular the repo-root `.claude/settings.json` and `.agents/` are tracked
files shared with everyone else and are deliberately left alone.

## Why

RCAs in `sbcli-rca-archive` start from a single log line, for example

    lvol.c:2942:lvs_update_on_failover_cpl: *ERROR*: Forcing application shutdown via abort

and then stall, because the SPDK / ultra / operator sources are not part of this
repo. Analysis falls back on inference. With the sources checked out and indexed,
that same line resolves in one grep:

    $ grep -i "Forcing application shutdown" .agents/repo-maps/spdk.messages.tsv
    Forcing application shutdown via abort.    lib/lvol/lvol.c:2945

That one lookup closed an open question in
`k8s_rapid_spdk_abort_rca_20260911.md` that had been marked "inference, cannot
confirm" for two days.

## Layout

    .agents/scripts/repo_map.py              the indexer (pure Python, no deps)
    .agents/hooks/check-repo-maps.py         SessionStart hook: existence + staleness
    .agents/repo-map.config.example.json     committed template
    .agents/repo-map.config.json             YOUR paths (gitignored)
    .agents/repo-maps/                       generated indexes (gitignored)
    ../.claude/settings.json                 wires the hook (e2e-scoped)

## Use

    cp .agents/repo-map.config.example.json .agents/repo-map.config.json
    # edit the paths, then
    python3 .agents/scripts/repo_map.py           # index everything
    python3 .agents/scripts/repo_map.py --check   # staleness only

Each repo produces three files:

| file | size | how to use |
|---|---|---|
| `<name>.md` | ~1 KB | read it, it is the orientation page |
| `<name>.messages.tsv` | 300 KB - 1 MB | **grep only** |
| `<name>.symbols.tsv` | 100 - 660 KB | **grep only** |

The TSVs are large on purpose. Reading one costs more context than grepping the
original repo, which would defeat the point. Grep them:

    grep -i "unable to read stripe" .agents/repo-maps/ultra.messages.tsv
    grep -P "^lvs_update_on_failover_cpl\t" .agents/repo-maps/spdk.symbols.tsv

Format placeholders are normalised, so a message written as
`f"port fence held {elapsed:.3f}s"` is indexed as `port fence held {}s`. Grep the
stable words, not the values.

## Notes

- The indexer is pure Python by design. `rg` on this machine is a Git Bash alias,
  not a binary, so `shutil.which("rg")` returns None and any hook depending on it
  would fail silently.
- The hook does not save tokens by itself. The index files do. The hook only
  reports that they exist and whether they match the checked-out commit, because
  a map nobody mentions gets ignored and a stale map is worse than none.
- The hook is silent when there is no config, so this costs nothing for anyone
  who has not set it up.
