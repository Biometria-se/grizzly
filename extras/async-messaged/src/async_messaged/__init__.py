"""Core async-message functionality."""

from __future__ import annotations

import logging
import sys
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from platform import node as hostname
from threading import Event
from time import monotonic as time
from typing import Any, TypedDict, final

from grizzly_common.transformer import JsonBytesEncoder

__all__: list[str] = []


def _get_log_dir() -> Path:
    grizzly_context_root = environ.get('GRIZZLY_CONTEXT_ROOT', None)
    log_dir_path = environ.get('GRIZZLY_LOG_DIR', None)
    if grizzly_context_root is None:
        message = 'GRIZZLY_CONTEXT_ROOT environment variable is not set'
        raise ValueError(message)

    log_dir_root = Path(grizzly_context_root) / 'logs'
    if log_dir_path is not None:
        log_dir_root = log_dir_root / log_dir_path

    log_dir_root.mkdir(parents=True, exist_ok=True)

    return log_dir_root


def configure_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]

    try:
        log_file = _get_log_dir() / f'async-messaged.{hostname()}.{datetime.now().strftime("%Y%m%dT%H%M%S%f")}.log'
        handlers.append(logging.FileHandler(log_file))
    except ValueError:
        pass

    level = environ.get('GRIZZLY_EXTRAS_LOGLEVEL', 'INFO')

    logging.basicConfig(
        level=level,
        format='[%(asctime)s] %(levelname)-5s: %(name)s: %(message)s',
        handlers=handlers,
    )

    # silence library loggers
    for logger_name in ['azure']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)


configure_logging()


AsyncMessageMetadata = dict[str, Any] | None
AsyncMessagePayload = Any


class AsyncMessageContext(TypedDict, total=False):
    url: str
    queue_manager: str
    connection: str
    channel: str
    username: str | None
    password: str | None
    tenant: str | None
    key_file: str | None
    cert_label: str | None
    ssl_cipher: str | None
    message_wait: int | None
    endpoint: str
    content_type: str | None
    heartbeat_interval: int | None
    header_type: str | None
    metadata: dict[str, str] | None
    consume: bool
    unique: bool
    verbose: bool
    forward: bool


class AsyncMessageRequest(TypedDict, total=False):
    request_id: str
    action: str
    worker: str | None
    client: int
    context: AsyncMessageContext
    payload: AsyncMessagePayload


class AsyncMessageResponse(TypedDict, total=False):
    request_id: str
    success: bool
    worker: str
    message: str | None
    payload: AsyncMessagePayload
    metadata: AsyncMessageMetadata
    response_length: int
    response_time: int
    action: str


class AsyncMessageError(Exception):
    pass


class AsyncMessageHandler(ABC):
    worker: str
    message_wait: int | None
    logger: logging.Logger
    _event: Event

    def __init__(self, worker: str, event: Event | None) -> None:
        self.worker = worker
        self.message_wait = None
        self.logger = logging.getLogger(f'handler::{self.__class__.__name__}::{worker}')
        self._event = event if event is not None else Event()

        # silence loggers
        for logger_name in ['uamqp', 'uamqp.c_uamqp', 'urllib3.connectionpool']:
            logging.getLogger(logger_name).setLevel(logging.ERROR)

    @abstractmethod
    def get_handler(self, action: str) -> AsyncMessageRequestHandler | None:
        message = f'{self.__class__.__name__}: get_handler is not implemented'
        raise NotImplementedError(message)  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        message = f'{self.__class__.__name__}: close is not implemented'
        raise NotImplementedError(message)  # pragma: no cover

    @final
    def handle(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        start_time = time()
        action = request.get('action', None)
        request_id = request.get('request_id', None)

        try:
            if action is None:
                message = 'no action in request'
                raise RuntimeError(message)

            request_handler = self.get_handler(action)
            self.logger.debug(
                'handling %s, request_id=%s, request=\n%s',
                action,
                request_id,
                jsondumps(request, indent=2, cls=JsonBytesEncoder),
            )

            response: AsyncMessageResponse

            if request_handler is None:
                message = f'no implementation for {action}'
                raise AsyncMessageError(message)

            response = request_handler(self, request)
            response['success'] = True
            if self._event.is_set():
                response.update({'success': False, 'message': 'abort'})
        except Exception as e:
            response = {
                'success': False,
                'message': f'{action or "UNKNOWN"}: {e.__class__.__name__}="{e!s}"',
            }
            self.logger.exception(
                '%s: request_id=%s, %s',
                action or 'UNKNOWN',
                request_id,
                e.__class__.__name__,
            )
        finally:
            total_time = int((time() - start_time) * 1000)
            response.update(
                {
                    'worker': self.worker,
                    'response_time': total_time,
                },
            )

            if response.get('action') is None:
                response.update({'action': str(action)})

            if not self._event.is_set():
                self.logger.debug(
                    'handled %s for, request_id=%s, response=\n%s',
                    action,
                    request_id,
                    jsondumps(response, indent=2, cls=JsonBytesEncoder),
                )

        return response


AsyncMessageRequestHandler = Callable[[AsyncMessageHandler, AsyncMessageRequest], AsyncMessageResponse]
InferredAsyncMessageRequestHandler = Callable[[Any, AsyncMessageRequest], AsyncMessageResponse]


LRU_READY = '\x01'
SPLITTER_FRAME = b''


def register(handlers: dict[str, AsyncMessageRequestHandler], action: str, *actions: str) -> Callable[[InferredAsyncMessageRequestHandler], InferredAsyncMessageRequestHandler]:
    def decorator(
        func: InferredAsyncMessageRequestHandler,
    ) -> InferredAsyncMessageRequestHandler:
        for a in (action, *actions):
            if a in handlers:
                continue

            handlers.update({a: func})

        return func

    return decorator
