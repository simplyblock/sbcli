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
    .agents/scripts/pin_from_run.py          pin the maps to a CI run's refs
    .agents/repo-maps/pins.json              which ref each map is built from (gitignored)
    ../.claude/settings.json                 wires the hook (e2e-scoped)
    ../../.claude/settings.json              same hook, for repo-root sessions

## Use

    cp .agents/repo-map.config.example.json .agents/repo-map.config.json
    cp ../e2e/.claude/settings.local.example.json ../e2e/.claude/settings.local.json
    # edit the paths in both, then
    python3 .agents/scripts/repo_map.py           # index everything
    python3 .agents/scripts/repo_map.py --check   # staleness only

## Pinning to the code a run actually used

Every moving part can be on a different ref: the automation on a feature
branch, the product image on `main`, SPDK on `R26.3`, the operator on a fix
branch. A map built from your own checkout is then *confidently wrong* - a log
line resolves to a real `file:line` in code the run never executed, which is
worse than not resolving it at all, because it looks like an answer.

Measured, not hypothetical: `Forcing application shutdown via abort` is
`lib/lvol/lvol.c:2945` on SPDK `master` and `:2942` on `R26.3`.

    # pin to a CI run (needs its run-versions artifact) and rebuild
    python3 .agents/scripts/pin_from_run.py --run 12345678 --build

    # or by hand, when you already know what ran
    python3 .agents/scripts/pin_from_run.py --set spdk=R26.3 --set sbcli=main
    python3 .agents/scripts/repo_map.py

    # back to your own checkouts (then rebuild)
    python3 .agents/scripts/pin_from_run.py --clear

Add `--keep` to park each pinned checkout under `.agents/worktrees/<name>/`,
so the pinned source is *readable* and not merely indexed. This matters more
than it sounds: the map gives the right `file:line` for the pinned commit, but
opening that file still lands in whatever is checked out, and at the same line
number another branch usually holds a different, plausible statement. SPDK
`lvol.c:2942` is `Forcing application shutdown via abort` on `R26.3` and
`Cannot update lvolstore on failover` on `master` - both real lines from the
same failover chain, which is how a confident and wrong RCA gets written.

Precedence is `--ref` > `pins.json` > a `ref` key in the repo config > the
working tree. Pinned builds use a detached `git worktree`, so your own checkout
and any uncommitted work are never touched. While pins are in effect the
SessionStart hook says so on every session, and judges staleness against the
pin rather than against your `HEAD`.

Image tags do not spell branch names - SPDK tags an image `26.3` while the
branch is `R26.3` - so ref lookup also tries `origin/<ref>` and an `R` prefix,
and an unresolvable pin suggests branches whose names contain it.

### Where the refs come from

`.github/workflows/k8s-native-e2e.yaml` writes a `run-versions` artifact
holding the dispatch inputs verbatim, plus the resolved operator SHA, and
prints the same table to the run summary. `--run` downloads that artifact;
workflow_dispatch inputs are not exposed by the REST API for a finished run, so
there is no way to derive them from a run id alone. The same values are handed
to the test process as env vars, so the Slack summary names the code that
failed.

One honest gap: these images carry no `org.opencontainers.image.revision`
label, so a tag identifies a *branch*, and that branch's tip today is not
necessarily the commit the image was built from. `pins.json` keeps the raw tag
next to the derived ref so the inference stays auditable. Labelling the image
builds with the commit SHA would close this properly.

`ultra` has no workflow input at all - it is built into the SPDK image - so
`pin_from_run.py` leaves it unpinned and says so rather than guessing.

Both files are gitignored, because absolute paths are per-developer.

`settings.local.json` grants file-tool access (Read / Grep / Glob) to the
indexed repos via `permissions.additionalDirectories`. Without it the maps still
resolve a message to `file:line`, but reading the source needs Bash. It also
needs to list the repo root and the RCA archive: with `e2e/` as the project
root, `simplyblock_core` and `sbcli-rca-archive` are both outside it. The
setting applies from the next session, not the current one.

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

## Indexed repos

| name | what it gives you |
|---|---|
| `spdk` | the abort and blobstore paths every SPDK RCA starts from |
| `ultra` | the distrib layer (`DISTRIBD Unable to read stripe`) |
| `operator` | the CSI driver, which owns `ctrl_loss_tmo` / `keep-alive-tmo` / `fast_io_fail` |
| `sbcli` | `simplyblock_core`, `simplyblock_web`, `simplyblock_cli` |

`sbcli` matters specifically when the session is opened with `e2e/` as its root:
the product code then sits outside the project and is not reachable with the
file tools. `e2e` itself is excluded from that index via `exclude_dirs`, because
it is the root and is already directly readable.

## Notes

- The indexer is pure Python by design. `rg` on this machine is a Git Bash alias,
  not a binary, so `shutil.which("rg")` returns None and any hook depending on it
  would fail silently.
- The hook does not save tokens by itself. The index files do. The hook only
  reports that they exist and whether they match the checked-out commit, because
  a map nobody mentions gets ignored and a stale map is worse than none.
- The hook is silent when there is no config, so this costs nothing for anyone
  who has not set it up.
- `max_symbols` / `max_log_strings` default to 100000. They used to default to
  4000/3000, which silently truncated the whole-repo sbcli index at 3000 of
  9761 messages and made `port fence held` un-findable minutes after it had
  worked. Truncation is now reported loudly in the build output. If you ever see
  `[TRUNCATED`, raise the limit: lookups will be missing entries.
