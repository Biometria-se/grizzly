"""@anchor pydoc:grizzly.auth
Core logic for handling different implementations for authorization.
"""
from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from enum import Enum
from functools import wraps
from importlib import import_module
from time import time
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Generic, Literal, Optional, Set, Tuple, Type, TypeVar, Union, cast
from urllib.parse import urlparse

from grizzly.tasks import RequestTask
from grizzly.types import GrizzlyResponse
from grizzly.utils import merge_dicts, safe_del

try:
    from typing import ParamSpec
except:
    from typing_extensions import ParamSpec  # type: ignore[assignment]


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario
    from grizzly.types.locust import Environment


class AuthMethod(Enum):
    NONE = 1
    CLIENT = 2
    USER = 3


class AuthType(Enum):
    HEADER = 1
    COOKIE = 2


P = ParamSpec('P')


logger = logging.getLogger(__name__)


class GrizzlyHttpAuthClient(Generic[P], metaclass=ABCMeta):
    host: str
    environment: Environment
    metadata: Dict[str, Any]
    cookies: Dict[str, str]
    __context__: ClassVar[Dict[str, Any]] = {
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
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }
    session_started: Optional[float]
    grizzly: GrizzlyContext
    _scenario: GrizzlyContextScenario
    _context: Dict[str, Any]

    def add_metadata(self, key: str, value: str) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        cast(dict, self._context['metadata']).update({key: value})

    @property
    def __context_change_history__(self) -> Set[str]:
        return cast(Set[str], self._context['__context_change_history__'])

    @property
    def __cached_auth__(self) -> Dict[str, str]:
        # clients might not cache auth tokens, let's set it to an empty dict
        # which could be refered to later, without updating client implementation
        if '__cached_auth__' not in self._context:
            self._context['__cached_auth__'] = {}
        return cast(Dict[str, str], self._context['__cached_auth__'])


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

    def __call__(self, func: AuthenticatableFunc) -> AuthenticatableFunc:  # noqa: PLR0915
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
                    request: Optional[RequestTask] = None
                    # look for RequestTask in args, we need to set metadata on it
                    for arg in args:
                        if isinstance(arg, RequestTask):
                            request = arg
                            break

                    session_now = time()
                    session_duration = session_now - client.session_started

                    time_for_refresh = session_duration >= auth_context.get('refresh_time', 3000)

                    # refresh token if session has been alive for at least refresh_time
                    authorization_token = client.metadata.get('Authorization', None)
                    if time_for_refresh or (authorization_token is None and client.cookies == {}):
                        refreshed_for = auth_user.get('username', '<unknown username>') if auth_method == AuthMethod.USER else auth_client.get('id', '<unknown client id>')

                        if time_for_refresh:
                            # if credentials are switched after one of the cached has timeout,
                            # it will be None, and hence it will be refreshed next time it is used
                            client.session_started = time()
                            client.__cached_auth__.clear()
                            logger.info(
                                '%s/%d %s needs refresh (%f >= %s), reset session started and clear cache',
                                client.__class__.__name__, id(client), refreshed_for, session_duration, auth_context.get('refresh_time', '3000'),
                            )

                        auth_type, secret = self.impl.get_token(client, auth_method)

                        if auth_type == AuthType.HEADER:
                            header = {'Authorization': f'Bearer {secret}'}
                            client.metadata.update(header)
                            if request is not None:
                                request.metadata.update(header)
                        else:
                            name, value = secret.split('=', 1)
                            client.cookies.update({name: value})

                        logger.info('%s/%d updated token at %f for %s', client.__class__.__name__, id(client), session_now, refreshed_for)
                    elif authorization_token is not None and request is not None:  # update current request with active authorization token
                        request.metadata.update({'Authorization': authorization_token})
                else:
                    safe_del(client.metadata, 'Authorization')
                    safe_del(client.metadata, 'Cookie')

            bound = func.__get__(client, client.__class__)

            return cast(GrizzlyResponse, bound(*args, **kwargs))

        return cast(AuthenticatableFunc, refresh_token)


class RefreshToken(metaclass=ABCMeta):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> Tuple[AuthType, str]:
        if auth_method == AuthMethod.CLIENT:
            return cls.get_oauth_token(client)

        return cls.get_oauth_authorization(client)

    @classmethod
    @abstractmethod
    def get_oauth_authorization(cls, client: GrizzlyHttpAuthClient) -> Tuple[AuthType, str]:  # pragma: no cover
        message = f'{cls.__name__} has not implemented "get_oauth_authorization"'
        raise NotImplementedError(message)

    @classmethod
    @abstractmethod
    def get_oauth_token(cls, client: GrizzlyHttpAuthClient, pkcs: Optional[Tuple[str, str]] = None) -> Tuple[AuthType, str]:  # pragma: no cover
        message = f'{cls.__name__} has not implemented "get_oauth_token"'
        raise NotImplementedError(message)


from .aad import AAD

__all__ = [
    'AAD',
]
