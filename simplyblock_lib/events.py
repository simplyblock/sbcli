# coding=utf-8
"""Level-mirrored event logging.

Event records are persisted by the caller (they are DB models); the generic
part is mapping an event's severity to the right Python-logger method so every
stored event is also visible in the service log / log shipping.
"""

import logging

# Severity names as stored on event records (EventObj.event_level).
LEVEL_DEBUG = "Debug"
LEVEL_INFO = "Info"
LEVEL_WARN = "Warning"
LEVEL_ERROR = "Error"
LEVEL_CRITICAL = "Critical"

_LEVEL_TO_LOGGING = {
    LEVEL_DEBUG: logging.DEBUG,
    LEVEL_INFO: logging.INFO,
    LEVEL_WARN: logging.WARNING,
    LEVEL_ERROR: logging.ERROR,
    LEVEL_CRITICAL: logging.CRITICAL,
}


def log_at_level(logger, event_level, message):
    """Mirror an event ``message`` to ``logger`` at the logging level matching
    the event severity name. Unknown severities log at INFO."""
    logger.log(_LEVEL_TO_LOGGING.get(event_level, logging.INFO), message)
