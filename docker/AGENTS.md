# AGENTS.md — container images

`Dockerfile` is a multi-stage build (`base-os` → `python-deps` → `runtime`) and
`docker-bake.hcl` is the single source of truth for targets, tags, platforms and cache
policy. Both CI and local builds go through bake, so a local build produces the same image
references CI pushes. Never add tags or platforms to a workflow — put them in the bake file.

```bash
docker buildx bake -f docker/docker-bake.hcl --print          # resolve config, don't build
docker buildx bake -f docker/docker-bake.hcl --check           # lint the Dockerfile
docker buildx bake -f docker/docker-bake.hcl \
    -f docker/docker-bake.local.hcl controlplane --load        # build locally, host arch only
```

Targets: `controlplane` (the published image) and `scan` (single-platform, `--load`ed by
`security.yml`). `.github/workflows/build-images.yml` builds one architecture per native
runner, pushes by digest, and assembles the manifest lists with `docker buildx imagetools
create`.

## CACHE_KEY

The expensive `base-os` stage (OS packages plus the per-arch FoundationDB, parted, libnvme
and nvme-cli RPMs) is bounded by a `CACHE_KEY` build arg holding a truncated timestamp — a
weekly bucket computed by `weekstamp()` in the bake file. Changing it evicts `base-os` and
everything below, so a rebuild picks up current RHEL errata. This replaces the separately
published `simplyblock/simplyblock:base_image` tag and the cron that refreshed it.

**`CACHE_KEY` must be referenced by a `RUN` in `base-os`.** BuildKit only invalidates a layer
for build args the layer actually uses; merely declaring the `ARG` busts nothing. There is a
`RUN echo "base-os cache key: ${CACHE_KEY}"` for exactly this reason — do not remove it.

CI computes the value once in `build-images.yml`'s `prepare` job rather than letting the bake
default apply per leg: `timestamp()` is evaluated per invocation, so a matrix run straddling a
bucket boundary would build its two architectures from different package sets. If you change
`weekstamp()`, change the bash equivalent in `prepare` to match.

Force a cold rebuild with `CACHE_KEY=$(date +%s) docker buildx bake ...`, or by dispatching
`build-images.yml` with the `cache_key` input — that is also the remedy when the daily
`security.yml` scan reports a vulnerability already fixed upstream.

## Constraints when editing the Dockerfile

- **No venv, and keep the source tree at `/app`.** Services run `python3 <path>` from `/app`
  (`simplyblock_core/scripts/docker-compose-swarm.yml`) and the storage node starts via
  `sudo -E python3` (`storage_node_ops.py`); sudoers' `secure_path` resets `PATH` even under
  `-E`, so a venv interpreter would never be found. The console scripts are also load-bearing
  — `services/tasks_cluster_status.py` shells out to `$SIMPLY_BLOCK_COMMAND_NAME`.
- **`env_var` must stay beside the package.** `constants.py`'s `get_config_var` opens
  `f"{SCRIPT_PATH}/env_var"`, and `release.yml` / `python-publish.yml` `sed` that file. It
  reaches the installed package through a `[tool.setuptools.package-data]` glob, not by
  accident — see the packaging section of the root `AGENTS.md`.
- **Do not remove the `dnf mark install` before the `dnf remove`.** UBI ships several of the
  requested packages as *dependencies*, and installing an already-present package does not
  change its install reason, so dnf still considers them autoremovable. Without the marks,
  removing `python3-urllib3` cascades into `dmidecode`, `which`, `psmisc` and `iproute`. That
  is not hypothetical: it is why the old `base_image` and the product image built from it have
  no `dmidecode`, which several code paths shell out to for the system UUID.
- **The `python3-*` removals are required.** `pylock.toml` installs `urllib3`, `requests`,
  `idna`, `charset-normalizer`, `six` and `python-dateutil` from PyPI; pip cannot uninstall an
  RPM-provided distribution (no `RECORD` file) and fails the build, and merely shadowing them
  would leave stale versions visible to the RPM-database scanners in `security.yml`. If a
  future UBI adds another such duplicate, the build fails loudly with `Cannot uninstall <pkg>`
  — add it to the removal list rather than reaching for `--ignore-installed`. A pinned
  dependency set is what makes this fail rather than pass silently: unpinned requirements were
  satisfied by the RPM copies, so pip never attempted an uninstall and the application ran
  against RPM versions that pip believed it managed.
- **Dependencies come from `pylock.toml`, and pip is pinned because of it.** The lock is a
  PEP 751 file generated from `[project.dependencies]` by `tox run -e lock`. The `lock` job in
  `build-images.yml` recompiles it and gates `build` on the result — this workflow is the
  lock's only consumer, so a stale one means a pushed image built from a dependency set nobody
  declared, not a lint failure. Hashes are part of the format, so there is no `--require-hashes` to
  remember. pip's PEP 751 reader is still marked experimental in its own `--help`, which is
  why the bootstrap pins `pip==` and asserts support with a `grep` rather than letting a
  `CACHE_KEY` rollover move the reader unnoticed. The application itself is a second,
  `--no-deps` install: a local directory has no hash and cannot appear in a lock.
- **`FDB_VERSION` and the `foundationdb` binding must share a series.** The series fixes the
  API version `db_controller.py` requests (`KVD_DB_VERSION = 730`), so 7.3.63 bindings on a
  7.3.69 client library are fine and 7.4 anything is not. `python-deps` asserts it at build
  time; the FoundationDB server in the compose files and `tests/integration/conftest.py`
  tracks the same series and cannot be checked here.
- **No `RUN --mount=type=cache`.** Cache mounts are not exported to the `gha` or `registry`
  cache backends, so on a fresh CI runner they are never populated.
- Per-arch logic uses `TARGETARCH` (not `TARGETPLATFORM`, which misses variants like
  `linux/arm64/v8`) and `set -eux`, because the previous `;`-joined form let a failed RPM
  download still produce a green build.

## Known issue

The image runs **Python 3.12**, not 3.13: `dnf install python3.13` adds an alternative
interpreter while `python3` stays 3.12, so every `command: python3 ...` and every pip install
targets 3.12 while CI tests on 3.13. The `python3 --version` line in `python-deps` exists to
keep this visible. Fixing it touches every Swarm compose command and is not yet done.

## Unrelated images

`Dockerfile.alpine-tools` and `Dockerfile.helm-deployer` share no base image, package manager,
stage or cache with the control plane, and keep their own manually-triggered workflows. Do not
fold them into `docker-bake.hcl`. Both are also unpinned (`alpine/helm:latest`, kubectl
resolved from `stable.txt`, talosctl `latest`) and so not reproducible; `helm-deployer` has no
references anywhere in this repository and arguably belongs with `simplyblock/helm-charts`.
