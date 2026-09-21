# AGENTS.md — e2e

End-to-end harness. Drives a **real deployed cluster** over SSH and the REST API and asserts on
what the system does. Unlike `tests/`, nothing here runs in-process against the control plane:
this tree is a *client* of the system under test, never a part of it.

## The black-box invariant

**Never import `simplyblock_core`, `simplyblock_cli` or `simplyblock_web` from harness code.**

The harness and the system under test are different machines running independently selected
versions:

| what | selected by |
|------|-------------|
| harness source (this tree) | the workflow ref (`github.ref_name`) |
| `sbctl` on the storage / mgmt nodes | the `SBCLI_BRANCH` workflow input |
| control-plane containers | the `SIMPLY_BLOCK_DOCKER_IMAGE` input |

An import binds an assertion to the *runner's* copy of the code, which need not be the version
deployed on the cluster. That does not fail loudly — the test keeps running and asserts the wrong
value, which is the worst possible outcome for a test. It also only works by accident: it needs
`sbctl` installed in the harness environment, which the PEP 723 script environments deliberately
do not provide.

When a test needs a product constant, **mirror it** with a comment naming the upstream definition
and stating that the two must move together. `e2e_tests/test_object_limits.py` is the reference
example. Mirroring is weaker coupling than an import, but it is *visible* — a reviewer changing
`simplyblock_core/constants.py` can grep for the counterpart.

The one exception is code inside a string that is shipped to a node and executed there (the
upgrade migration scripts in `e2e_tests/upgrade_tests/`). That runs in the node's interpreter
against the deployed package, so it is by definition the right version.

## Execution model

Every entry point is a [PEP 723](https://peps.python.org/pep-0723/) script:

```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [...]
# ///
```

uv provisions the interpreter and the dependencies on first run. There is **no
`requirements.txt`** and no shared virtualenv — do not reintroduce either.

The one entry point that is not Python is `lab_lock.sh`, which reserves the lab hosts a CI run
touches. It is shell because its payload is a snippet executed *on* the locked hosts over ssh,
which may have nothing installed on them beyond coreutils — the hosts are what the workflow
wipes. See `.github/actions/lab-lock/README.md`.

- Invoke scripts directly (`./e2e.py`), never `python3 e2e.py` — the latter silently bypasses the
  declared environment and picks up whatever the host interpreter happens to have.
- CI only needs uv on `PATH`; workflows install it with `python3 -m pip install --upgrade pip uv`
  and add `uv.find_uv_bin()`'s directory to `$GITHUB_PATH`.
- `#!/usr/bin/env -S uv run --script` needs the executable bit. A new entry point must be added
  with mode `100755`.
- Keep each script's `dependencies` matching its actual import closure. A script's environment is
  isolated, so a package another script declares is not available here.
- Comments inside the metadata block need a doubled hash (`# # note`): PEP 723 strips one `# `,
  and the remainder must be valid TOML.

### `requests>=2.34.0`

Pinned as a floor wherever `requests` is used, deliberately. Releases before 2.34.0 silently
collapsed duplicate slashes in URL paths, which masked malformed base-URL joins in the API
clients; 2.34.0 stopped ([psf/requests#7315](https://github.com/psf/requests/pull/7315)). Holding
the floor keeps a join bug failing immediately instead of being hidden again. `SbcliUtils`
normalises its base URL for the same reason — endpoint paths are rooted (`/lvol`, `/mgmtnode`), so
the base must not carry a trailing slash.
