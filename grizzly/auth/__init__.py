from __future__ import annotations

from typing import Any, Dict, Tuple, TypedDict, Optional, Type, Literal, Union, Protocol, TypeVar, Callable, cast, TYPE_CHECKING
from functools import wraps
from enum import Enum
from time import time
from importlib import import_module
from urllib.parse import urlparse
from abc import ABCMeta

from grizzly.types import GrizzlyResponse
from grizzly.types.locust import Environment
from grizzly.utils import safe_del, merge_dicts

try:
    from typing import ParamSpec
except:
    from typing_extensions import ParamSpec  # type: ignore[assignment]


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


class AuthMethod(Enum):
    NONE = 1
    CLIENT = 2
    USER = 3


class AuthType(Enum):
    HEADER = 1
    COOKIE = 2


class GrizzlyAuthHttpContextUser(TypedDict):
    username: Optional[str]
    password: Optional[str]
    redirect_uri: Optional[str]
    initialize_uri: Optional[str]


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
    cookies: Dict[str, str]
    _context: GrizzlyHttpContext
    session_started: Optional[float]

    def add_metadata(self, key: str, value: str) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        cast(dict, self._context['metadata']).update({key: value})


P = ParamSpec('P')
F = TypeVar('F', bound=Callable[..., GrizzlyResponse])


class AuthenticatableFunc(Protocol[P]):
    def __call__(self, parent: 'GrizzlyScenario', *args: P.args, **kwargs: P.kwargs) -> GrizzlyResponse:
        ...

    def __get__(self, *args: Any, **kwargs: Any) -> Any:
        ...


class refresh_token:
    impl: Type[RefreshToken]

    def __init__(self, impl: Union[Type[RefreshToken], str]) -> None:
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

    def __call__(self, func: F) -> AuthenticatableFunc[P]:
        def render(parent: GrizzlyScenario, client: GrizzlyHttpAuthClient) -> None:
            host = parent.render(client.host)
            parsed = urlparse(host)
            client.host = f'{parsed.scheme}://{parsed.netloc}'

            client_context = parent.user._context.get(parsed.netloc, None)

            # we have a host specific context that we should merge into current context
            if client_context is not None:
                client._context = cast(GrizzlyHttpContext, merge_dicts(cast(dict, client._context), cast(dict, client_context)))

        @wraps(func)
        def refresh_token(client: GrizzlyHttpAuthClient, parent: GrizzlyScenario, *args: P.args, **kwargs: P.kwargs) -> GrizzlyResponse:
            if client._context.get('auth', None) is None:
                render(parent, client)

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
                    auth_user is not None
                    and auth_user.get('username', None) is not None
                    and auth_user.get('password', None) is not None
                    and (
                        (
                            auth_user.get('redirect_uri', None) is not None
                            and auth_context.get('provider', None) is not None
                            and auth_client is not None
                            and auth_client.get('id', None) is not None
                        )
                        or auth_user.get('initialize_uri', None) is not None
                    )
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
                    if session_duration >= auth_context.get('refresh_time', 3000) or (client.headers.get('Authorization', None) is None and client.cookies == {}):
                        auth_type, secret = self.impl.get_token(parent, client, auth_method)
                        client.session_started = time()
                        if auth_type == AuthType.HEADER:
                            client.headers.update({'Authorization': f'Bearer {secret}'})
                        else:
                            name, value = secret.split('=', 1)
                            client.cookies.update({name: value})
                else:
                    safe_del(client.headers, 'Authorization')
                    safe_del(client.headers, 'Cookie')

            bound = func.__get__(client, client.__class__)

            return cast(GrizzlyResponse, bound(parent, *args, **kwargs))

        return cast(AuthenticatableFunc[P], refresh_token)


class RefreshToken(metaclass=ABCMeta):
    @classmethod
    def get_token(cls, parent: GrizzlyScenario, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> Tuple[AuthType, str]:
        if auth_method == AuthMethod.CLIENT:
            return cls.get_oauth_token(parent, client)
        else:
            return cls.get_oauth_authorization(parent, client)

    @classmethod
    def get_oauth_authorization(cls, parent: GrizzlyScenario, client: GrizzlyHttpAuthClient) -> Tuple[AuthType, str]:
        raise NotImplementedError(f'{cls.__name__} has not implemented "get_oauth_authorization"')  # pragma: no cover

    @classmethod
    def get_oauth_token(cls, parent: GrizzlyScenario, client: GrizzlyHttpAuthClient, pkcs: Optional[Tuple[str, str]] = None) -> Tuple[AuthType, str]:
        raise NotImplementedError(f'{cls.__name__} has not implemented "get_oauth_token"')  # pragma: no cover


from .aad import AAD

__all__ = [
    'AAD',
]
