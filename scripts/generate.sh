#!/usr/bin/env bash

PYTHON="$(command -v python)"
if [[ "${PYTHON}" == "" ]]; then
  PYTHON="$(comamnd -v python3)"
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
${PYTHON} "${SCRIPT_DIR}/cli-wrapper-gen.py" "${SCRIPT_DIR}/.."
