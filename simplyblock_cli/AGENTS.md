# AGENTS.md — simplyblock_cli

CLI package for `sbctl`. Entry point: `simplyblock_cli.cli:main`.

## Code Generation

`cli.py` is **auto-generated** from `cli-reference.yaml` — never edit it directly. After changing commands or options in the YAML, regenerate:

```bash
./simplyblock_cli/scripts/generate.sh
```

The generator (`scripts/cli-wrapper-gen.py`) reads the YAML and writes `cli.py`. CI verifies `cli.py` is up-to-date.

## Key Files

- `cli-reference.yaml` — Declarative definition of all CLI commands, subcommands, and arguments
- `cli-reference-schema.yaml` — JSON Schema for the reference file (linked for IDE validation)
- `clibase.py` — Implementation of all command handlers

## CLI Reference Rules

- **Positional arguments** (no `--`/`-` prefix) are always required.
- **Optional arguments** (`--`/`-` prefix) are optional unless marked `required: true`.
- **Function naming**: The generator maps commands to handler functions in `clibase.py` as `<command>__<subcommand>`. Hyphens in command names become underscores (e.g., `storage-node list` → `storage_node__list`).
- **Secret arguments**: Use `type: secret` in `cli-reference.yaml` for any argument that carries a password, token, or key. The generator maps this to `SecretStr` (from `pydantic`) as the argparse type converter, so the value is wrapped at parse time and masked in `repr(vars(args))`. Handler code in `clibase.py` receives a `SecretStr` and should pass it through to the API without unwrapping.

## Adding a CLI Command

1. Add the command/subcommand definition in `cli-reference.yaml`.
2. Run `./simplyblock_cli/scripts/generate.sh`.
3. Implement the handler function in `clibase.py` following the naming convention above.
