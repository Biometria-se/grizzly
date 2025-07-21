"""Logic for how logs are handled in grizzly and locust.

This is not used, but kept for reference.
"""

from __future__ import annotations

import logging
from logging.handlers import QueueHandler, QueueListener
from re import sub
from socket import gethostname
from time import time
from typing import TYPE_CHECKING

from gevent.queue import Full, Queue

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types import StrDict

logger = logging.getLogger('grizzly.log')


def setup_logging(loglevel: str | int, logfile: str | None = None, maxsize: int = 10000) -> None:
    if isinstance(loglevel, str):
        loglevel = loglevel.upper()

    hostname = sub(r'\..*', '', gethostname())

    config: StrDict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': f'[%(asctime)s] {hostname}/%(levelname)s/%(name)s: %(message)s',
            },
            'plain': {
                'format': '%(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
            'console_plain': {
                'class': 'logging.StreamHandler',
                'formatter': 'plain',
            },
            'log_reader': {'class': 'locust.log.LogReader', 'formatter': 'default'},
            'queue_listener': {
                'class': 'grizzly.log.DiscardingQueueHandler',
                'listener': 'grizzly.log.AutoStartQueueListener',
                'handlers': ['console', 'log_reader'],
                'queue': {
                    '()': 'gevent.queue.Queue',
                    'maxsize': maxsize,
                },
            },
            'queue_listener_plain': {
                'class': 'grizzly.log.DiscardingQueueHandler',
                'listener': 'grizzly.log.AutoStartQueueListener',
                'handlers': ['console_plain'],
                'queue': {
                    '()': 'gevent.queue.Queue',
                    'maxsize': maxsize,
                },
            },
        },
        'loggers': {
            'locust': {
                'handlers': ['queue_listener'],
                'level': loglevel,
                'propagate': False,
            },
            'locust.stats_logger': {
                'handlers': ['queue_listener_plain'],
                'level': 'INFO',
                'propagate': False,
            },
        },
        'root': {
            'handlers': ['queue_listener'],
            'level': loglevel,
        },
    }

    if logfile:
        # if a file has been specified add a file logging handler and set
        # the locust and root loggers to use it
        config['handlers']['file'] = {
            'class': 'logging.FileHandler',
            'filename': logfile,
            'formatter': 'default',
        }
        config['loggers']['locust']['handlers'] = ['file', 'queue_listener']
        config['root']['handlers'] = ['file', 'queue_listener']

    logging.config.dictConfig(config)


class AutoStartQueueListener(QueueListener):
    def __init__(self, queue: Queue, *handlers: logging.Handler, respect_handler_level: bool = False) -> None:
        super().__init__(queue, *handlers, respect_handler_level=respect_handler_level)

        self.start()


class DiscardingQueueHandler(QueueHandler):
    def __init__(self, queue: Queue) -> None:
        super().__init__(queue)

        self._discarding_started: float | None = None
        self._discarding_count: int = 0

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            super().enqueue(record)

            if self._discarding_started is not None:
                delta = time() - self._discarding_started
                logger.warning('Discarded %d log messages during %.2f seconds due to excessive logging resulting in log queue being full', self._discarding_count, delta)

            self._discarding_started = None
            self._discarding_count = 0
        except Full:
            if self._discarding_started is None:
                self._discarding_started = time()

            self._discarding_count += 1
