import logging.config

from .log import LOGGING

name = 'redis_streams_task_queue'

logging.config.dictConfig(LOGGING)

__all__ = [
    'exceptions',
    'queue.py',
    'worker'
]
