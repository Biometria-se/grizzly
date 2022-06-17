import logging
import sys

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, TypedDict, Callable, List, cast
from os import environ, path
from platform import node as hostname
from json import dumps as jsondumps
from time import monotonic as time
from io import StringIO
from threading import Lock
from datetime import datetime

from grizzly_extras.transformer import JsonBytesEncoder

__all__: List[str] = []


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
                file_handler = logging.FileHandler(path.join(grizzly_context_root, 'logs', ThreadLogger._destination))
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

            self._logger = logger

    def _log(self, level: int, message: str, exc_info: Optional[bool] = False) -> None:
        with self._lock:
            self._logger.log(level, message, exc_info=exc_info)

    def debug(self, message: str) -> None:
        self._log(logging.DEBUG, message)

    def info(self, message: str) -> None:
        self._log(logging.INFO, message)

    def error(self, message: str, exc_info: Optional[bool] = False) -> None:
        self._log(logging.ERROR, message, exc_info=exc_info)

    def warning(self, message: str) -> None:
        self._log(logging.WARNING, message)


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


class AsyncMessageRequest(TypedDict, total=False):
    action: str
    worker: Optional[str]
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


class AsyncMessageHandler(ABC):
    worker: str
    message_wait: Optional[int]
    logger: ThreadLogger

    def __init__(self, worker: str) -> None:
        self.worker = worker
        self.message_wait = None
        self.logger = ThreadLogger(f'handler::{worker}')

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

    @abstractmethod
    def get_handler(self, action: str) -> Optional['AsyncMessageRequestHandler']:
        raise NotImplementedError(f'{self.__class__.__name__}: get_handler is not implemented')

    def handle(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        action = request['action']
        request_handler = self.get_handler(action)
        self.logger.debug(f'handling {action}, request=\n{jsondumps(request, indent=2, cls=JsonBytesEncoder)}')

        response: AsyncMessageResponse

        start_time = time()

        try:
            if request_handler is None:
                raise AsyncMessageError(f'no implementation for {action}')

            response = request_handler(self, request)
            response['success'] = True
        except Exception as e:
            response = {
                'success': False,
                'message': f'{action}: {e.__class__.__name__}="{str(e)}"',
            }
            self.logger.error(f'{action}: {e.__class__.__name__}="{str(e)}"', exc_info=True)
        finally:
            total_time = int((time() - start_time) * 1000)
            response.update({
                'worker': self.worker,
                'response_time': total_time,
            })

            self.logger.debug(f'handled {action}, response=\n{jsondumps(response, indent=2, cls=JsonBytesEncoder)}')

            return response


AsyncMessageRequestHandler = Callable[[AsyncMessageHandler, AsyncMessageRequest], AsyncMessageResponse]
InferredAsyncMessageRequestHandler = Callable[[Any, AsyncMessageRequest], AsyncMessageResponse]


LRU_READY = '\x01'
SPLITTER_FRAME = ''.encode()


def register(handlers: Dict[str, AsyncMessageRequestHandler], action: str, *actions: str) -> Callable[[InferredAsyncMessageRequestHandler], InferredAsyncMessageRequestHandler]:
    def decorator(func: InferredAsyncMessageRequestHandler) -> InferredAsyncMessageRequestHandler:
        # mypy: type AsyncServiceBus (that inherits AsyncMessageHandler) is not of type
        # AsyncMessageHandler or Union[AsyncServiceBus, AsyncMessageQueue]
        # this is a workaround of python/mypys, sometimes, stupid type handling...
        typed_func = cast(AsyncMessageRequestHandler, func)
        for a in (action, *actions):
            if a in handlers:
                continue

            handlers.update({a: typed_func})

        return func

    return decorator
