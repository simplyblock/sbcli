# Simplyblock Control Plane Tooling

![](assets/simplyblock-logo.svg)

Simplyblock is a Kubernetes-native, distributed block storage solution. It was designed from the ground up to be
high-performance, NVMe over Fabrics end-to-end, highly available, fault-tolerant, and scalable.

Simplyblock is developed and maintained by [Simplyblock](https://simplyblock.io).

## Documentation

The full simplyblock documentation can be found at [docs.simplyblock.io](https://docs.simplyblock.io).

## Installing the Command Line Interface

The `sbctl` tooling can be installed using our pypi package [sbctl](https://pypi.org/project/sbctl/).

```bash
pip install --upgrade sbctl
```

## Repository Components

This repository contains the source code for the Simplyblock Control Plane core services and the command line interface
(`sbctl`).

Technology stack:
- Python
- Docker
- Kubernetes
- FoundationDB
- FastAPI (API v2)
- Flask (API v1)
- OpenAPI (Swagger)

The repository is split into the following components:

### Simplyblock Core
The `simplyblock_core/` directory contains the core logic and controllers for a simplyblock control plane cluster.

### Simplyblock CLI
The `simplyblock_cli` directory contains the `sbctl` command line interface code. A full list of the available command
line interface commands and options can be found in the [sbctl reference](https://docs.simplyblock.io/latest/reference/cli/).

Commands and options in the command line interface are implemented using an automatic generator and a YAML definitions
file (`simplyblock_cli/sbctl/cli-reference.yaml`). For well-conformity of the YAML file, a schema definition is
provided in `simplyblock_cli/sbctl/cli-reference-schema.json` and linked on top of the reference for immediate use
in Visual Studio Code and IntelliJ-based platforms.

If commands or options are added, updated, or removed, the YAML file, the generator must be executed to update the
startup code of `sbctl`.

```bash
./simplyblock_cli/scripts/generate.sh
```

Executing this command updates `simplyblock_cli/cli.py` according to the reference file.

#### Reference Notes

There are a few implicit rules in the reference file:

- **Positional Argument:** Arguments without leading hyphens (`--` or  `-`) are considered positional arguments which are always required.
- **Optional Argument:** Arguments with leading hyphens (`--` or  `-`) are considered optional arguments except marked as `required: true`.
- **Naming Convention Commands:** When commands and subcommands are added, the generator creates the function names according to the following naming convention `<command>__<subcommand>` which has to be implemented in `clibase.py`. If the command or subcommand contains a hyphen in the name, it is exchanged for an underscore.

### Simplyblock Web API

The `simplyblock_web` directory contains the source code for the simplyblock control plane API. A full list of the
available API endpoints and options can be found in the [API reference](https://docs.simplyblock.io/latest/reference/api/).

## Notes on Local Development

### FoundationDB
Simplyblock's control plane uses [FoundationDB](https://www.foundationdb.org/), which means it requires a FDB client
library (libfdb_c.dylib) for the Python bindings to interact with the database.

Depending on the OS architecture, the appropriate version must be installed from their official
[github repo](https://github.com/apple/foundationdb).

```bash
wget https://github.com/apple/foundationdb/releases/download/7.3.3/FoundationDB-7.3.3_arm64.pkg
```

### Docker Compose

A local development environment can be spun up using the Docker compose file: `docker-compose-dev.yml`.

```bash
sudo docker compose -f docker-compose-dev.yml up --build -d
```

## License

Copyright (c) 2021-2025 Simplyblock GmbH. All rights reserved.
