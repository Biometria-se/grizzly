"""Core async-message functionality."""
from __future__ import annotations

import logging
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from platform import node as hostname
from time import monotonic as time
from typing import Any, Callable, Dict, List, Optional, TypedDict, final

from grizzly_extras.transformer import JsonBytesEncoder

__all__: List[str] = []

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
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stderr)]

    try:
        log_file = _get_log_dir() / f'async-messaged.{hostname()}.{datetime.now().strftime("%Y%m%dT%H%M%S%f")}.log'
        handlers.append(logging.FileHandler(log_file))
    except ValueError:
        pass

    level = logging.getLevelName(environ.get('GRIZZLY_EXTRAS_LOGLEVEL', 'INFO'))

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


AsyncMessageMetadata = Optional[Dict[str, Any]]
AsyncMessagePayload = Optional[Any]


class AsyncMessageContext(TypedDict, total=False):
    url: str
    queue_manager: str
    connection: str
    channel: str
    username: Optional[str]
    password: Optional[str]
    tenant: Optional[str]
    key_file: Optional[str]
    cert_label: Optional[str]
    ssl_cipher: Optional[str]
    message_wait: Optional[int]
    endpoint: str
    content_type: Optional[str]
    heartbeat_interval: Optional[int]
    header_type: Optional[str]
    metadata: Optional[Dict[str, str]]
    consume: bool


class AsyncMessageRequest(TypedDict, total=False):
    action: str
    worker: Optional[str]
    client: int
    context: AsyncMessageContext
    payload: AsyncMessagePayload


class AsyncMessageResponse(TypedDict, total=False):
    success: bool
    worker: str
    message: Optional[str]
    payload: AsyncMessagePayload
    metadata: AsyncMessageMetadata
    response_length: int
    response_time: int


class AsyncMessageError(Exception):
    pass


class AsyncMessageAbort(Exception):  # noqa: N818
    pass


class AsyncMessageHandler(ABC):
    worker: str
    message_wait: Optional[int]
    logger: logging.Logger

    def __init__(self, worker: str) -> None:
        self.worker = worker
        self.message_wait = None
        self.logger = logging.getLogger(f'handler::{self.__class__.__name__}::{worker}')

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

    @abstractmethod
    def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
        message = f'{self.__class__.__name__}: get_handler is not implemented'
        raise NotImplementedError(message)  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        message = f'{self.__class__.__name__}: close is not implemented'
        raise NotImplementedError(message)  # pragma: no cover

    @final
    def handle(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        start_time = time()

        try:
            action = request.get('action', None)
            if action is None:
                message = 'no action in request'
                raise RuntimeError(message)

            request_handler = self.get_handler(action)
            self.logger.debug('handling %s, request=\n%s', action, jsondumps(request, indent=2, cls=JsonBytesEncoder))

            response: AsyncMessageResponse

            if request_handler is None:
                message = f'no implementation for {action}'
                raise AsyncMessageError(message)

            response = request_handler(self, request)
            response['success'] = True
        except Exception as e:
            response = {
                'success': False,
                'message': f'{action or "UNKNOWN"}: {e.__class__.__name__}="{e!s}"',
            }
            self.logger.exception('%s: %s="%s"', action or 'UNKNOWN', e.__class__.__name__, str(e))  # noqa: TRY401
        finally:
            total_time = int((time() - start_time) * 1000)
            response.update({
                'worker': self.worker,
                'response_time': total_time,
            })

            self.logger.debug('handled %s, response=\n%s', action, jsondumps(response, indent=2, cls=JsonBytesEncoder))

        return response


AsyncMessageRequestHandler = Callable[[AsyncMessageHandler, AsyncMessageRequest], AsyncMessageResponse]
InferredAsyncMessageRequestHandler = Callable[[Any, AsyncMessageRequest], AsyncMessageResponse]


LRU_READY = '\x01'
SPLITTER_FRAME = b''


def register(handlers: Dict[str, AsyncMessageRequestHandler], action: str, *actions: str) -> Callable[[InferredAsyncMessageRequestHandler], InferredAsyncMessageRequestHandler]:
    def decorator(func: InferredAsyncMessageRequestHandler) -> InferredAsyncMessageRequestHandler:
        for a in (action, *actions):
            if a in handlers:
                continue

            handlers.update({a: func})

        return func

    return decorator
