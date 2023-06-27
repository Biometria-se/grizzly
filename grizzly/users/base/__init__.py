from abc import abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types import GrizzlyResponse
    from grizzly.tasks import RequestTask


class FileRequests:
    pass


class HttpRequests:
    pass


class AsyncRequests:
    @abstractmethod
    def async_request_impl(self, request: 'RequestTask') -> 'GrizzlyResponse':
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented async_request')  # pragma: no cover


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
