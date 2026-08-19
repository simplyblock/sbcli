# AGENTS.md — container images

Everything in `docker/`. The control-plane image is `Dockerfile`; `Dockerfile.alpine-tools` and
`Dockerfile.helm-deployer` are unrelated (see the last section).

## Building

All paths below are relative to the sbcli repository root; run the commands from there.

```bash
# Local build, host architecture only.
docker buildx build -f docker/Dockerfile -t simplyblock:dev --load .

# What CI and build_image.sh publish: both architectures, pushed.
./build_image.sh
```

`build_image.sh` reads `simplyblock_core/env_var` for the tag and credentials and defaults
`PLATFORMS` to `linux/amd64,linux/arm64`. The publishing workflows are
`.github/workflows/docker-image.yml` (Docker Hub + public ECR), `docker-image-2.yml` and
`docker-image-quay.yml`; `security.yml` builds a single-arch image and scans it with Grype and
Trivy.

Useful checks against a built image:

```bash
docker run --rm IMG python3 -c 'import sys; print(sys.version, sys.prefix, sys._is_gil_enabled())'
docker run --rm IMG sudo -E python3 -c 'import sys; print(sys.prefix)'   # must be /opt/venv
docker run --rm IMG sbctl --version
docker run --rm IMG simplyblock-service --help
```

## Stages and caching

`Dockerfile` is a single multi-stage build (BuildKit required) with three stages:

| Stage | Contents |
|-------|----------|
| `base` | UBI 10 plus the runtime OS packages: FoundationDB client, `nvme-cli`, `parted`, `iscsi-initiator-utils`, ssh, systemd; creates the `simplyblock` user and the sudoers drop-in |
| `builder` | `base` + `uv`; installs the 3.14t interpreter into `/opt/python` and syncs `/opt/venv` from `uv.lock` |
| `runtime` | `base` + `/opt/python` and `/opt/venv` copied from `builder` + the sources at `/app` |

Layer order is what makes rebuilds cheap: OS packages change rarely, `pyproject.toml`/`uv.lock`
occasionally, sources on every commit. Only the layers after the changed input are rebuilt, and
CI carries the rest between runs through the buildx GitHub Actions cache (`cache-from`/`cache-to`
in `.github/workflows/docker-image*.yml`). There is deliberately no separate pre-built base image.

Because the `base` stage pins packages by name only, that cache would otherwise serve the same
`dnf` resolution forever and never pick up a security update. The `CACHE_KEY` build arg exists to
break it: every workflow passes `date -u +%G-W%V` (ISO year and week), so the OS-package layers
are rebuilt from scratch once a week and an upstream fix reaches the image within seven days.
`build_image.sh` passes the same value. To force a refresh out of band, pass any other value:

```bash
CACHE_KEY=$(date -u +%s) ./build_image.sh
docker buildx build --build-arg CACHE_KEY=refresh -f docker/Dockerfile .
```

`.dockerignore` (at the repository root, not here) keeps `tests/`, `.github/`, `docs/`, `assets/`
and the tooling caches out of the build context, so editing any of them does not invalidate the
image's source layer. It also excludes virtualenvs, which would otherwise smuggle an outdated
`pip`/`setuptools` into the image for the scanners to find.

`security.yml` deliberately uses the *same* weekly key rather than a fresh one — the scan has to
report on the packages that are actually shipped. Rebuilding it against fresher packages would
let the scan go green while the published image still carries the vulnerability.

## Constraints when editing the Dockerfile

- **Do not remove the `secure_path` line from `/etc/sudoers.d/simplyblock`.** Several entry points
  start as `sudo -E python3 ...`: the storage-node API (`storage_node_ops.py`), the SPDK proxy
  (`simplyblock_web/api/internal/storage_node/docker.py`,
  `simplyblock_web/templates/storage_deploy_spdk.yaml.j2`) and `node_configure.py` (from the
  operator's init script, `kubernetes/operator/internal/utils/storage_nodeset_ds.go`). UBI's
  `/etc/sudoers` sets `Defaults secure_path = /sbin:/bin:/usr/sbin:/usr/bin`, which replaces
  `PATH` **even with `-E`**. Measured on `ubi10/ubi:10.2`, with `/opt/venv/bin` first on `PATH`:

  | invocation | without the drop-in | with it |
  |---|---|---|
  | `python3` | venv | venv |
  | `sudo -E python3` | **`/bin/python3`** | venv |
  | `sudo python3` | **`/bin/python3`** | venv |
  | `su simplyblock -c "sudo -E python3"` | **`/bin/python3`** | venv |
  | `sudo -E sh -c "python3"` | **`/bin/python3`** | venv |
  | `sh -c "eval sudo -E python3 …"` (operator shape) | **`/bin/python3`** | venv |
  | `sudo /bin/python3`, `sudo dnf` | unaffected | unaffected |

  Without it those services silently run the platform interpreter, which has none of the
  dependencies. This is why an earlier attempt at a venv image was abandoned rather than fixed.

- **Keep the source tree at `/app`, and keep the install editable — for now.** The Helm charts
  (`kubernetes/helm-charts/.../controlplane_deploy.yaml`, `.../storage-node.yaml`), the CSI chart,
  `simplyblock_core/scripts/docker-compose-swarm.yml` and `cluster_ops.py` all name source files
  (`python3 simplyblock_core/services/<name>.py`), and Swarm services created by an older control
  plane keep their original command string across an image upgrade. Those paths resolve only
  because `/app` *is* the installed package.

  This is compatibility scaffolding with a defined exit. The packaging half is already done —
  `uv sync --no-editable` installs cleanly and every data file (`env_var`, `scripts/**`,
  `templates/**`, dashboards) resolves from `site-packages`, so nothing in the wheel blocks the
  switch. The only thing left is the consumers: once they all use the `simplyblock-service` /
  `sbctl` entry points, the change is `uv sync --locked --no-editable` in the builder plus
  dropping `COPY . /app` from the runtime stage. Remaining offenders:

  ```bash
  # from the sbcli repository root, not from docker/
  grep -rnE 'python3? +simplyblock_(core|web)/[a-z_/]+\.py' \
    ../kubernetes simplyblock_core/scripts/docker-compose-swarm.yml simplyblock_core/cluster_ops.py
  ```

- **`python3` in `base` is for `dnf`, not the application.** dnf and subscription-manager are
  written in Python; removing it breaks the package manager. Nothing the application needs is
  installed into it — `/opt/venv` is entirely separate, which is also why the RPM-vs-pip
  collisions the pre-venv image had to work around no longer exist.

- **No compiler toolchain, no `python3-pip`.** Every dependency resolves to a `cp314t` wheel and
  `foundationdb` is a pure-Python sdist, so nothing needs to compile. If a future dependency has
  no free-threaded wheel the build fails loudly — that is the intended signal, not a reason to add
  `gcc` back. `uv` creates the venv without `pip`/`setuptools`/`wheel`, so there is nothing to
  uninstall and no vendored pip bundle for the image scan to flag.

- **No `RUN --mount=type=cache`.** Cache mounts are not exported to the `gha` or `registry` cache
  backends, so on a fresh CI runner they are never populated.

- **The interpreter must be copied along with the venv.** A uv-managed venv symlinks its `python`
  into `UV_PYTHON_INSTALL_DIR` and records that absolute path in `pyvenv.cfg`, so the runtime
  stage copies both `/opt/python` and `/opt/venv`, to identical paths.

- **The image runs with the GIL disabled.** `PYTHON_GIL=1` in the container environment re-enables
  it on the same image — the rollback lever if a free-threading regression shows up in the field.

## Unrelated images

`Dockerfile.alpine-tools` and `Dockerfile.helm-deployer` share no base image, package manager,
stage or cache with the control plane, and have their own manually-triggered workflows. Do not
fold them into this build. Both are also unpinned (`alpine/helm:latest`, kubectl resolved from
`stable.txt`, talosctl `latest`) and so not reproducible.
