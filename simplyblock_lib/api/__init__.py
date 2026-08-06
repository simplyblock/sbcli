# coding=utf-8
"""FastAPI scaffolding shared by simplyblock web services.

Kept import-light at package level: importing ``simplyblock_lib.api`` must not
pull in fastapi/starlette — import the submodules explicitly.
"""
