from __future__ import annotations

from typing import Any, Dict, Tuple, Protocol, TypedDict, Optional, Type, Literal, cast
from functools import wraps
from enum import Enum
from time import time

from grizzly.types import WrappedFunc
from grizzly.types.locust import Environment
from grizzly.utils import safe_del


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


class GrizzlyHttpAuthClient(Protocol):
    host: str
    environment: Environment
    headers: Dict[str, str]
    _context: GrizzlyHttpContext
    session_started: Optional[float]


class refresh_token:
    impl: Type[RefreshToken]

    def __init__(self, impl: Type[RefreshToken]) -> None:
        self.impl = impl

    def __call__(self, func: WrappedFunc) -> WrappedFunc:
        @wraps(func)
        def refresh_token(client: GrizzlyHttpAuthClient, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
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


class RefreshToken(Protocol):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> str:
        raise NotImplementedError(f'{cls.__class__.__name__} has not implemented "get_token"')


from .aad import AAD

__all__ = [
    'AAD',
]
