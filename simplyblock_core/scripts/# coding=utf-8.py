# coding=utf-8
import logging

import time
import sys


# Import the GELF logger
from graypy import GELFUDPHandler
from logging_loki import LokiHandler

# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFUDPHandler('0.0.0.0', 12201)
loki_handler = LokiHandler(url="http://loki:3100/loki/api/v1/push", tags={"application": "simplyblk"},version="1")
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.addHandler(loki_handler)
logger.setLevel(logging.DEBUG)

### script to test connection once connection is ascertain
# get DB controller


logger.info("Starting capacity monitoring service...")
while True:

    logger.info(f"Checking cluster: israel")

    logger.error("Cluster capacity record not found!")
     

