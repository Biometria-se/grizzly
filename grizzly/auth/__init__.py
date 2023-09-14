from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, Type, Literal, Union, TypeVar, Callable, Generic, cast, TYPE_CHECKING
from enum import Enum
from time import time
from importlib import import_module
from urllib.parse import urlparse
from abc import ABCMeta
from functools import wraps

from grizzly.types import GrizzlyResponse
from grizzly.types.locust import Environment
from grizzly.utils import safe_del, merge_dicts

try:
    from typing import ParamSpec
except:
    from typing_extensions import ParamSpec  # type: ignore[assignment]


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario


class AuthMethod(Enum):
    NONE = 1
    CLIENT = 2
    USER = 3


class AuthType(Enum):
    HEADER = 1
    COOKIE = 2


P = ParamSpec('P')


class GrizzlyHttpAuthClient(Generic[P], metaclass=ABCMeta):
    host: str
    environment: Environment
    headers: Dict[str, str]
    cookies: Dict[str, str]
    _context: Dict[str, Any] = {
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': None,
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': None,
    }
    session_started: Optional[float]
    grizzly: GrizzlyContext
    _scenario: GrizzlyContextScenario

    def add_metadata(self, key: str, value: str) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        cast(dict, self._context['metadata']).update({key: value})


AuthenticatableFunc = TypeVar('AuthenticatableFunc', bound=Callable[..., GrizzlyResponse])


class refresh_token(Generic[P]):
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

    def __call__(self, func: AuthenticatableFunc) -> AuthenticatableFunc:
        def render(client: GrizzlyHttpAuthClient) -> None:
            variables = cast(Dict[str, Any], client._context.get('variables', {}))
            host = client.grizzly.state.jinja2.from_string(client.host).render(**variables)
            parsed = urlparse(host)

            client.host = f'{parsed.scheme}://{parsed.netloc}'

            client_context = client._context.get(parsed.netloc, None)

            # we have a host specific context that we should merge into current context
            if client_context is not None:
                client._context = merge_dicts(client._context, cast(dict, client_context))

        @wraps(func)
        def refresh_token(client: GrizzlyHttpAuthClient, *args: P.args, **kwargs: P.kwargs) -> GrizzlyResponse:
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
                        auth_type, secret = self.impl.get_token(client, auth_method)
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

            return cast(GrizzlyResponse, bound(*args, **kwargs))

        return cast(AuthenticatableFunc, refresh_token)


class RefreshToken(metaclass=ABCMeta):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> Tuple[AuthType, str]:
        if auth_method == AuthMethod.CLIENT:
            return cls.get_oauth_token(client)
        else:
            return cls.get_oauth_authorization(client)

    @classmethod
    def get_oauth_authorization(cls, client: GrizzlyHttpAuthClient) -> Tuple[AuthType, str]:
        raise NotImplementedError(f'{cls.__name__} has not implemented "get_oauth_authorization"')  # pragma: no cover

    @classmethod
    def get_oauth_token(cls, client: GrizzlyHttpAuthClient, pkcs: Optional[Tuple[str, str]] = None) -> Tuple[AuthType, str]:
        raise NotImplementedError(f'{cls.__name__} has not implemented "get_oauth_token"')  # pragma: no cover


from .aad import AAD

__all__ = [
    'AAD',
]
