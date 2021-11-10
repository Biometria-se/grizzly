import logging

logger = logging.getLogger(__name__)


class FileRequests:
    pass


class HttpRequests:
    pass


from .response_event import ResponseEvent
from .request_logger import RequestLogger
from .response_handler import ResponseHandler
from .context_variables import ContextVariables

__all__ = [
    'ResponseEvent',
    'RequestLogger',
    'ResponseHandler',
    'ContextVariables',
]
