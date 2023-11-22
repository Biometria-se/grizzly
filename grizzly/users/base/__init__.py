"""Base module for all load user related stuff."""
from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse


class FileRequests:
    pass


class HttpRequests:
    pass


class AsyncRequests:
    @abstractmethod
    def async_request_impl(self, request: 'RequestTask') -> 'GrizzlyResponse':
        message = f'{self.__class__.__name__} has not implemented async_request'
        raise NotImplementedError(message)  # pragma: no cover


from .grizzly_user import GrizzlyUser, GrizzlyUserMeta, grizzlycontext

__all__ = [
    'GrizzlyUser',
    'GrizzlyUserMeta',
    'grizzlycontext',
]
