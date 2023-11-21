"""Core async-message functionality."""
from __future__ import annotations

import logging
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from io import StringIO
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from platform import node as hostname
from threading import Lock
from time import monotonic as time
from time import perf_counter, sleep
from typing import Any, Callable, Dict, List, Optional, TypedDict, cast, final

import zmq.green as zmq
from zmq.error import Again as ZMQAgain

from grizzly_extras.transformer import JsonBytesEncoder

__all__: List[str] = []


logger = logging.getLogger(__name__)


AsyncMessageMetadata = Optional[Dict[str, Any]]
AsyncMessagePayload = Optional[Any]


class ThreadLogger:
    _logger: logging.Logger
    _lock: Lock = Lock()

    _destination: Optional[str] = None

    def __init__(self, name: str) -> None:
        with self._lock:
            logger = logging.getLogger(name)
            log_format = '[%(asctime)s] %(levelname)-5s: %(name)s: %(message)s'
            formatter = logging.Formatter(log_format)
            level = logging.getLevelName(environ.get('GRIZZLY_EXTRAS_LOGLEVEL', 'INFO'))
            logger.setLevel(level)
            logger.handlers = []
            stdout_handler = logging.StreamHandler(sys.stderr)
            stdout_handler.setFormatter(formatter)
            logger.addHandler(stdout_handler)

            root_logger = logging.getLogger()
            root_logger.setLevel(logging.NOTSET)  # root logger needs to have lower or equal log level
            root_logger.handlers = []
            root_logger.addHandler(logging.StreamHandler(StringIO()))  # disable messages from root logger

            grizzly_context_root = environ.get('GRIZZLY_CONTEXT_ROOT', None)

            if grizzly_context_root is not None:
                if ThreadLogger._destination is None:
                    ThreadLogger._destination = f'async-messaged.{hostname()}.{datetime.now().strftime("%Y%m%dT%H%M%S%f")}.log'
                file_handler = logging.FileHandler(Path(grizzly_context_root) / 'logs' / ThreadLogger._destination)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

            self._logger = logger

    def _log(self, level: int, message: str, *args: Any, exc_info: Optional[bool] = False, **kwargs: Any) -> None:
        with self._lock:
            self._logger.log(level, message, *args, exc_info=exc_info, **kwargs)

    def debug(self, message: str, *args: Any) -> None:
        self._log(logging.DEBUG, message, *args)

    def info(self, message: str, *args: Any) -> None:
        self._log(logging.INFO, message, *args)

    def error(self, message: str, *, exc_info: Optional[bool] = False) -> None:
        self._log(logging.ERROR, message, exc_info=exc_info)

    def warning(self, message: str, *args: Any) -> None:
        self._log(logging.WARNING, message, *args)

    def exception(self, message: str, *args: Any, exc_info: bool = True, **kwargs: Any) -> None:
        self._log(logging.ERROR, message, *args, exc_info=exc_info, **kwargs)


class AsyncMessageContext(TypedDict, total=False):
    url: str
    queue_manager: str
    connection: str
    channel: str
    username: Optional[str]
    password: Optional[str]
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
    logger: ThreadLogger

    def __init__(self, worker: str) -> None:
        self.worker = worker
        self.message_wait = None
        self.logger = ThreadLogger(f'handler::{self.__class__.__name__}::{worker}')

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


def async_message_request(client: zmq.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    try:
        client.send_json(request)

        response: Optional[AsyncMessageResponse] = None

        while True:
            start = perf_counter()
            try:
                response = cast(Optional[AsyncMessageResponse], client.recv_json(flags=zmq.NOBLOCK))
                break
            except ZMQAgain:
                sleep(0.1)
            delta = perf_counter() - start
            if delta > 1.0:
                logger.debug('async_message_request::recv_json took %f seconds', delta)

        if response is None:
            msg = 'no response'
            raise AsyncMessageError(msg)

        message = response.get('message', None)

        if not response['success']:
            raise AsyncMessageError(message)

    except Exception as e:
        if not isinstance(e, (AsyncMessageError, AsyncMessageAbort)):
            logger.exception('failed to send request=%r', request)
        raise
    else:
        return response
