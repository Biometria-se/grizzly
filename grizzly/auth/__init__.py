from __future__ import annotations

from typing import Any, Dict, Tuple, Protocol, TypedDict, Optional, Type, Literal, Union, cast, runtime_checkable, TYPE_CHECKING
from functools import wraps
from enum import Enum
from time import time
from importlib import import_module
from urllib.parse import urlparse
from abc import ABCMeta

from grizzly.types import WrappedFunc
from grizzly.types.locust import Environment
from grizzly.utils import safe_del, merge_dicts


if TYPE_CHECKING:
    from grizzly.scenarios import GrizzlyScenario


class AuthMethod(Enum):
    NONE = 1
    CLIENT = 2
    USER = 3


class GrizzlyAuthHttpContextUser(TypedDict):
    username: Optional[str]
    password: Optional[str]
    redirect_uri: Optional[str]


class GrizzlyAuthHttpContextClient(TypedDict):
    id: Optional[str]
    secret: Optional[str]
    resource: Optional[str]


class GrizzlyAuthHttpContext(TypedDict):
    client: Optional[GrizzlyAuthHttpContextClient]
    user: Optional[GrizzlyAuthHttpContextUser]
    provider: Optional[str]
    refresh_time: int


class GrizzlyHttpContext(TypedDict):
    verify_certificates: bool
    metadata: Optional[Dict[str, str]]
    auth: Optional[GrizzlyAuthHttpContext]


class GrizzlyHttpAuthClient(metaclass=ABCMeta):
    host: str
    environment: Environment
    headers: Dict[str, str]
    _context: GrizzlyHttpContext
    session_started: Optional[float]
    parent: Optional['GrizzlyScenario']

    def add_metadata(self, key: str, value: str) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        cast(dict, self._context['metadata']).update({key: value})


class refresh_token:
    impl: Type[RefreshToken]
    render: bool

    def __init__(self, impl: Union[Type[RefreshToken], str], render: bool = False) -> None:
        if isinstance(impl, str):
            if impl.count('.') > 1:
                module_name, class_name = impl.rsplit('.', 1)
            else:
                module_name = RefreshToken.__module__
                class_name = impl

            module = import_module(module_name)
            dynamic_impl = getattr(module, class_name)
            assert issubclass(dynamic_impl, RefreshToken), f'{module_name}.{class_name} is not a subclass of {RefreshToken.__module__}.{RefreshToken.__name__}'
            impl = dynamic_impl

        self.impl = cast(Type[RefreshToken], impl)
        self.render = render

    def __call__(self, func: WrappedFunc) -> WrappedFunc:
        def render(client: GrizzlyHttpAuthClient) -> None:
            if client.parent is None:
                return

            host = client.parent.render(client.host)
            parsed = urlparse(host)
            client.host = f'{parsed.scheme}://{parsed.netloc}'

            client_context = client.parent.user._context.get(parsed.netloc, None)

            # we have a host specific context that we should merge into current context
            if client_context is not None:
                client._context = cast(GrizzlyHttpContext, merge_dicts(cast(dict, client._context), cast(dict, client_context)))

        @wraps(func)
        def refresh_token(client: GrizzlyHttpAuthClient, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            if self.render:
                render(client)

            auth_context = client._context.get('auth', None)

            if auth_context is not None:
                auth_client = auth_context.get('client', None)
                auth_user = auth_context.get('user', None)

                use_auth_client = (
                    auth_client is not None
                    and auth_client.get('id', None) is not None
                    and auth_client.get('secret', None) is not None
                    and auth_context.get('provider', None) is not None
                )
                use_auth_user = (
                    auth_client is not None
                    and auth_user is not None
                    and auth_client.get('id', None) is not None
                    and auth_user.get('username', None) is not None
                    and auth_user.get('password', None) is not None
                    and auth_user.get('redirect_uri', None) is not None
                    and auth_context.get('provider', None) is not None
                )

                if use_auth_client:
                    auth_method = AuthMethod.CLIENT
                elif use_auth_user:
                    auth_method = AuthMethod.USER
                else:
                    auth_method = AuthMethod.NONE

                if auth_method is not AuthMethod.NONE and client.session_started is not None:
                    session_now = time()
                    session_duration = session_now - client.session_started

                    # refresh token if session has been alive for at least refresh_time
                    if session_duration >= auth_context.get('refresh_time', 3000) or client.headers.get('Authorization', None) is None:
                        token = self.impl.get_token(client, auth_method)
                        client.session_started = time()
                        client.headers.update({'Authorization': f'Bearer {token}'})
                else:
                    safe_del(client.headers, 'Authorization')

            return func(client, *args, **kwargs)

        return cast(WrappedFunc, refresh_token)


@runtime_checkable
class RefreshToken(Protocol):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> str:
        raise NotImplementedError(f'{cls.__class__.__name__} has not implemented "get_token"')


from .aad import AAD

__all__ = [
    'AAD',
]