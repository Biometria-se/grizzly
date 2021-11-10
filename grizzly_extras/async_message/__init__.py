import logging

from abc import ABC
from typing import Optional, Dict, Any, TypedDict, Callable, cast
from os import environ, path
from platform import node as hostname
from json import JSONEncoder


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


class JsonBytesEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            try:
                return o.decode('utf-8')
            except:
                return o.decode('latin-1')

        return JSONEncoder.default(self, o)


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
    queue: str


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

    def __init__(self, worker: str) -> None:
        self.worker = worker


LRU_READY = '\x01'
SPLITTER_FRAME = ''.encode()

log_format = '[%(asctime)s] %(levelname)-5s: %(name)s: %(message)s'

logger = logging.getLogger(__name__)
level = logging.getLevelName(environ.get('GRIZZLY_EXTRAS_LOGLEVEL', 'INFO'))
logging.basicConfig(format=log_format, level=level)

logger.info(f'level: {logging.getLevelName(level)}')

if level < logging.INFO:
    formatter = logging.Formatter(log_format)
    file_name = f'async-messaged.{hostname()}.log'
    file_handler = logging.FileHandler(path.join(environ.get('GRIZZLY_CONTEXT_ROOT', '.'), 'logs', file_name))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
