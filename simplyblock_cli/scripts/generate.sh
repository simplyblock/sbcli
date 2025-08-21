#!/usr/bin/env bash

PYTHON="$(command -v python)"
if [[ "${PYTHON}" == "" ]]; then
  PYTHON="$(command -v python3)"
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
${PYTHON} -m pip --quiet install -r ${SCRIPT_DIR}/requirements.txt
${PYTHON} "${SCRIPT_DIR}/cli-wrapper-gen.py" "${SCRIPT_DIR}/.."
