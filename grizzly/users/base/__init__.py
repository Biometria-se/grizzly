import logging

logger = logging.getLogger(__name__)


class FileRequests:
    pass


class HttpRequests:
    pass


from .response_event import ResponseEvent
from .request_logger import RequestLogger
from .response_handler import ResponseHandler
from .grizzly_user import GrizzlyUser

__all__ = [
    'ResponseEvent',
    'RequestLogger',
    'ResponseHandler',
    'GrizzlyUser',
]
