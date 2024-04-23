
# Simply Block
[![Docker Image Build](https://github.com/simplyblock-io/sbcli/actions/workflows/docker-image.yml/badge.svg)](https://github.com/simplyblock-io/sbcli/actions/workflows/docker-image.yml)

[![Python Unit Testing](https://github.com/simplyblock-io/sbcli/actions/workflows/python-testing.yml/badge.svg)](https://github.com/simplyblock-io/sbcli/actions/workflows/python-testing.yml)

 
## Install
Add the package repo from AWS CodeArtifact using [awscli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

```bash
aws codeartifact login --tool pip --repository sbcli --domain simplyblock --domain-owner 565979732541 --region eu-west-1
```
Install package
```bash
pip install --extra-index-url https://pypi.org/simple sbcli-dev
```

# Components 

## Simply Block Core
Contains core logic and controllers for the simplyblock cluster

## Simply Block CLI
Please see this document 
[README.md](../main/simplyblock_cli/README.md)


## Simply Block Web API
Please see this document 
[README.md](../main/simplyblock_web/README.md)



