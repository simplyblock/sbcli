#!/usr/bin/env python
# coding=utf-8
import logging

from simplyblock_cli.cli import CLIWrapper

logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)


if __name__ == '__main__':
    cli = CLIWrapper()
    cli.run()
