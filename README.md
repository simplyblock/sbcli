
# SBCLI (SimplyBlock CLI)

Simplyblock provides a feature-rich CLI (command line interface) to deploy SimplyBlock Cluster and to manage the SimplyBlock Control plane

## Install

The CLI can be installed using pypi package [sbctl](https://pypi.org/project/sbctl/)

```
pip install --upgrade sbctl
```

# Components

## Simply Block Core
Contains core logic and controllers for the simplyblock cluster

## Simply Block CLI
The list of all the available CLI options can be here [CLI docs](./simplyblock_cli/README.md)

## SimplyBlock Web API

Web app that provides API to manage the cluster. More about this in [README.md](./simplyblock_web/README.md)


### local development

FoundationDB requires a client library (libfdb_c.dylib) for the Python bindings to interact with the database.
Depending on the OS architecture, please install the appropriate version from the official github repo

```
wget https://github.com/apple/foundationdb/releases/download/7.3.3/FoundationDB-7.3.3_arm64.pkg
```
setup the code on a management node and the webApp code can be developed by building the `docker-compose-dev.yml` file.

```
sudo docker compose -f docker-compose-dev.yml up --build -d
```
