import logging
import sys

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, TypedDict, Callable, cast
from os import environ, path
from platform import node as hostname
from json import dumps as jsondumps
from time import monotonic as time
from io import StringIO

from grizzly_extras.transformer import JsonBytesEncoder

__all__ = [
    'AsyncMessageContext',
    'AsyncMessageMetadata',
    'AsyncMessagePayload',
    'AsyncMessageRequest',
    'AsyncMessageResponse',
    'AsyncMessageError',
]


AsyncMessageMetadata = Optional[Dict[str, Any]]
AsyncMessagePayload = Optional[Any]

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

    def __init__(self, worker: str) -> None:
        self.worker = worker
        self.message_wait = None

    @abstractmethod
    def get_handler(self, action: str) -> Optional['AsyncMessageRequestHandler']:
        raise NotImplementedError(f'{self.__class__.__name__}: get_handler is not implemented')

    def handle(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        action = request['action']
        request_handler = self.get_handler(action)
        logger.debug(f'{self.worker}: handling {action}')
        logger.debug(f'{self.worker}: {jsondumps(request, indent=2, cls=JsonBytesEncoder)}')

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
            logger.error(f'{self.worker}: {action}: {e.__class__.__name__}="{str(e)}"', exc_info=True)
        finally:
            total_time = int((time() - start_time) * 1000)
            response.update({
                'worker': self.worker,
                'response_time': total_time,
            })

            logger.debug(f'{self.worker}: handled {action}')
            logger.debug(f'{self.worker}: {jsondumps(response, indent=2, cls=JsonBytesEncoder)}')

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

def configure_logger(name: str) -> logging.Logger:
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

    if level < logging.INFO:
        file_name = f'async-messaged.{hostname()}.log'
        file_handler = logging.FileHandler(path.join(environ.get('GRIZZLY_CONTEXT_ROOT', '.'), 'logs', file_name))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.info(f'level={logging.getLevelName(level)}')

    return logger

logger = configure_logger(__name__)
