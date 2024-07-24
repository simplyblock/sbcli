import requests
import time
import os
import sys
import logging


from simplyblock_core import constants

# Import the GELF logger
from graypy import GELFUDPHandler
from logging_loki import LokiHandler

# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFUDPHandler('0.0.0.0', constants.GELF_PORT)
loki_handler = LokiHandler(url="http://0.0.0.0:3100/loki/api/v1/push", tags={"application": "simplyblk"},version="1")
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.addHandler(loki_handler)
logger.setLevel(logging.DEBUG)

deletion_interval = os.getenv('LOG_DELETION_INTERVAL', '24h')

delete_query = f'''
{{
  "query": {{
    "range": {{
      "timestamp": {{
        "lt": "now-{deletion_interval}"
      }}
    }}
  }}
}}
'''

while True:
    logger.info('Deleting gray log entries...')
    try:
        headers = {'Content-Type':  "application/json"}
        response = requests.post("http://opensearch:9200/graylog_*/_delete_by_query",
                                 data=delete_query, headers=headers)
        logger.info(f"response.status_code: {response.status_code}")
        logger.info(f"response.text: {response.text}")
        if response.status_code != 200:
            logger.error("Failed to delete logs.")
        else:
            logger.info("Logs deleted successfully.")
    except Exception as e:
        logger.error("Failed to delete logs.")
        logger.error(e)

    time.sleep(constants.GRAYLOG_CHECK_INTERVAL_SEC)
