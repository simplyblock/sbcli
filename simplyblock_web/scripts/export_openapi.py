"""Writes the v2 API's OpenAPI schema to the path given as the first argument.

Importing `simplyblock_web.app` is safe with no live FoundationDB or k8s
infrastructure: `DBController()` degrades silently when no cluster file is
present (simplyblock_core/db_controller.py), and every DB/k8s access in the v2
routers happens at request time, not at import time.
"""
import json
import sys

from simplyblock_web.app import app


def main() -> None:
    with open(sys.argv[1], 'w') as f:
        json.dump(app.openapi(), f, indent=2, sort_keys=True)
        f.write('\n')


if __name__ == '__main__':
    main()
