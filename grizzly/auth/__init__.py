"""@anchor pydoc:grizzly.auth
Core logic for handling different implementations for authorization.
"""
from __future__ import annotations

import logging
from abc import ABCMeta
from datetime import datetime, timezone
from functools import wraps
from importlib import import_module
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Generic, Optional, Set, Type, TypeVar, Union, cast
from urllib.parse import urlparse

from azure.core.credentials import AccessToken

from grizzly.tasks import RequestTask
from grizzly.types import GrizzlyResponse
from grizzly.types.locust import StopUser
from grizzly.utils import merge_dicts
from grizzly_extras.azure.aad import AuthMethod, AuthType, AzureAadCredential

try:
    from typing import ParamSpec
except:
    from typing_extensions import ParamSpec  # type: ignore[assignment]


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario
    from grizzly.types.locust import Environment

P = ParamSpec('P')

logger = logging.getLogger(__name__)


class GrizzlyHttpAuthClient(Generic[P], metaclass=ABCMeta):
    logger: logging.Logger
    host: str
    environment: Environment
    credential: Optional[AzureAadCredential] = None
    metadata: Dict[str, Any]
    cookies: Dict[str, str]
    __context__: ClassVar[Dict[str, Any]] = {
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
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
    def __cached_auth__(self) -> Dict[str, AzureAadCredential]:
        # clients might not cache auth tokens, let's set it to an empty dict
        # which could be refered to later, without updating client implementation
        if '__cached_auth__' not in self._context:
            self._context['__cached_auth__'] = {}
        return cast(Dict[str, AzureAadCredential], self._context['__cached_auth__'])


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
        @wraps(func)
        def refresh_token(client: GrizzlyHttpAuthClient, *args: P.args, **kwargs: P.kwargs) -> GrizzlyResponse:
            # make sure the client has a credential instance, if it is needed
            self.impl.initialize(client)

            if client.credential is not None and client.credential.auth_method is not AuthMethod.NONE:
                request: Optional[RequestTask] = None
                # look for RequestTask in args, we need to set metadata on it
                for arg in args:
                    if isinstance(arg, RequestTask):
                        request = arg
                        break

                # refresh token if session has been alive for at least refresh_time
                authorization_token = client.metadata.get('Authorization', None)
                exception: Optional[Exception] = None
                start_time = perf_counter()
                refreshed = False
                action_triggered = False
                action_for = (client.credential.username or '<unknown username>') if client.credential.auth_method == AuthMethod.USER else client.credential.client_id

                try:
                    access_token = client.credential.get_token()
                    refreshed = client.credential.refreshed
                    action_triggered = refreshed or (authorization_token is None and client.cookies == {})

                    if action_triggered:
                        if refreshed:
                            # if credentials are switched after one of the cached has timeout,
                            # it will be None, and hence it will be refreshed next time it is used
                            client.__cached_auth__.clear()
                            action_name = 'refreshed'
                        else:
                            action_name = 'claimed'

                        next_refresh = datetime.fromtimestamp(access_token.expires_on, tz=timezone.utc).astimezone(tz=None)

                        logger.info(
                            '%s/%d %s %s %s token until %s',
                            client.__class__.__name__,
                            id(client),
                            action_for,
                            action_name,
                            client.credential.auth_method.name.lower(),
                            next_refresh,
                        )

                    # always make sure client and request has the right token
                    if client.credential.auth_type == AuthType.HEADER:  # add token bearer to headers
                        header = {'Authorization': f'Bearer {access_token.token}'}
                        client.metadata.update(header)
                        if request is not None:
                            request.metadata.update(header)
                    else:  # add token to cookies
                        client.cookies.update({client.credential.COOKIE_NAME: access_token.token})

                except Exception as e:
                    exception = e
                    client.logger.exception('failed to get token')
                finally:
                    if action_triggered or exception is not None:
                        scenario_index = client._scenario.identifier
                        request_meta = {
                            'request_type': 'AUTH',
                            'response_time': int((perf_counter() - start_time) * 1000),
                            'name': f'{scenario_index} {self.impl.__name__} OAuth2 {client.credential.auth_method.name.lower()} token: {action_for}',
                            'context': client._context,
                            'response': None,
                            'exception': exception,
                            'response_length': 0,
                        }

                        client.environment.events.request.fire(**request_meta)

                    if exception is not None:
                        raise StopUser from exception

            bound = func.__get__(client, client.__class__)

            return cast(GrizzlyResponse, bound(*args, **kwargs))

        return cast(AuthenticatableFunc, refresh_token)


def render(client: GrizzlyHttpAuthClient) -> None:
    variables = cast(Dict[str, Any], client._context.get('variables', {}))
    host = client.grizzly.state.jinja2.from_string(client.host).render(**variables)
    parsed = urlparse(host)

    client.host = f'{parsed.scheme}://{parsed.netloc}'

    client_context = client._context.get(parsed.netloc, None)

    # we have a host specific context that we should merge into current context
    if client_context is not None:
        client._context = merge_dicts(client._context, cast(dict, client_context))


class RefreshToken(metaclass=ABCMeta):
    __TOKEN_CREDENTIAL_TYPE__: ClassVar[Type[AzureAadCredential]]

    @classmethod
    def initialize(cls, client: GrizzlyHttpAuthClient) -> None:
        render(client)

        auth_context = client._context.get('auth', None)

        if auth_context is not None:
            auth_client = auth_context.get('client', {})
            auth_user = auth_context.get('user', {})

            username = auth_user.get('username', None)
            password = auth_user.get('password', None) or auth_client.get('secret', None)
            client_id = auth_client.get('id', None)

            # nothing has changed, use existing crendential
            if client.credential is not None and (
                client.credential.username == username
                and client.credential.password == password
                and client.credential.auth_method != AuthMethod.NONE
            ):
                return

            tenant = auth_context.get('tenant', None) or auth_context.get('provider', None)
            initialize_uri = auth_user.get('initialize_uri', None)
            redirect_uri = auth_user.get('redirect_uri', None)

            use_auth_client = (
                client_id is not None
                and auth_client.get('secret', None) is not None
                and tenant is not None
            )
            use_auth_user = (
                username is not None
                and password is not None
                and (
                    (
                        redirect_uri is not None
                        and tenant is not None
                        and client_id is not None
                    )
                    or initialize_uri is not None
                )
            )

            if use_auth_client:
                auth_method = AuthMethod.CLIENT
                password = auth_client.get('secret', None)
            elif use_auth_user:
                auth_method = AuthMethod.USER
                initialize_uri = auth_user.get('initialize_uri', None)
            else:
                auth_method = AuthMethod.NONE

            client.credential = cls.__TOKEN_CREDENTIAL_TYPE__(
                username,
                password,
                tenant,
                auth_method,
                host=client.host,
                client_id=client_id,
                redirect=redirect_uri,
                initialize=initialize_uri,
            )


from .aad import AAD

__all__ = [
    'AAD',
    'AccessToken',
]
