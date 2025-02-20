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
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Generic, TypeVar, Union, cast
from urllib.parse import urlparse
from uuid import uuid4

from azure.core.credentials import AccessToken
from gevent.event import AsyncResult
from gevent.lock import Semaphore

from grizzly.scenarios import GrizzlyScenario
from grizzly.types import GrizzlyResponse
from grizzly.types.locust import MasterRunner, StopUser
from grizzly.users import GrizzlyUser
from grizzly.utils import ModuleLoader, merge_dicts
from grizzly_extras.azure.aad import AuthMethod, AuthType, AzureAadCredential

try:
    from typing import ParamSpec
except:
    from typing_extensions import ParamSpec  # type: ignore[assignment]


if TYPE_CHECKING:  # pragma: no cover
    from locust.rpc.protocol import Message

    from grizzly.context import GrizzlyContext, GrizzlyContextScenario
    from grizzly.tasks import RequestTask
    from grizzly.testdata import GrizzlyVariables
    from grizzly.types.locust import Environment

P = ParamSpec('P')

logger = logging.getLogger(__name__)


class GrizzlyHttpAuthClient(Generic[P], metaclass=ABCMeta):
    logger: logging.Logger
    host: str
    environment: Environment
    credential: AzureAadCredential | None = None
    metadata: dict[str, Any]
    cookies: dict[str, str]
    __context__: ClassVar[dict[str, Any]] = {
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
    variables: GrizzlyVariables
    session_started: float | None
    grizzly: GrizzlyContext
    _scenario: GrizzlyContextScenario
    _context: dict[str, Any]

    def add_metadata(self, key: str, value: str) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        cast(dict, self._context['metadata']).update({key: value})

    @property
    def __context_change_history__(self) -> set[str]:
        return cast(set[str], self._context['__context_change_history__'])

    @property
    def __cached_auth__(self) -> dict[str, AzureAadCredential]:
        # clients might not cache auth tokens, let's set it to an empty dict
        # which could be refered to later, without updating client implementation
        if '__cached_auth__' not in self._context:
            self._context['__cached_auth__'] = {}
        return cast(dict[str, AzureAadCredential], self._context['__cached_auth__'])


AuthenticatableFunc = TypeVar('AuthenticatableFunc', bound=Callable[..., GrizzlyResponse])


class refresh_token(Generic[P]):
    impl: type[RefreshToken]

    def __init__(self, impl: Union[type[RefreshToken], str]) -> None:
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

        self.impl = cast(type[RefreshToken], impl)

    def __call__(self, func: AuthenticatableFunc) -> AuthenticatableFunc:  # noqa: PLR0915
        @wraps(func)
        def refresh_token(client: GrizzlyHttpAuthClient, arg: Union[RequestTask, GrizzlyScenario], *args: P.args, **kwargs: P.kwargs) -> GrizzlyResponse:  # noqa: PLR0915, PLR0912
            request: RequestTask | None = None

            # make sure the client has a credential instance, if it is needed
            if isinstance(arg, GrizzlyScenario):
                request = None
                user = arg.user
            else:
                request = arg
                user = cast(GrizzlyUser, client)

            self.impl.initialize(client, user)

            if client.credential is not None and client.credential.auth_method is not AuthMethod.NONE:
                # refresh token if session has been alive for at least refresh_time
                authorization_token = client.metadata.get('Authorization', None)
                exception: Exception | None = None
                start_time = perf_counter()
                refreshed = False
                action_triggered = False
                action_for = (client.credential.username or '<unknown username>') if client.credential.auth_method == AuthMethod.USER else client.credential.client_id

                try:
                    now = datetime.now(tz=timezone.utc).timestamp()

                    if (authorization_token is None and client.cookies == {}) or client.credential._access_token is None or client.credential._access_token.expires_on <= now:
                        access_token, refreshed = RefreshTokenDistributor.get_token(client)
                        client.credential._access_token = access_token
                        action_triggered = refreshed or (authorization_token is None and client.cookies == {})
                    else:
                        access_token = client.credential._access_token
                        refreshed = False
                        action_triggered = False

                    client.credential._refreshed = refreshed

                    logger.debug('%s refereshed=%r, action_triggered=%r', user.logger.name, refreshed, action_triggered)

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

            return cast(GrizzlyResponse, bound(arg, *args, **kwargs))

        return cast(AuthenticatableFunc, refresh_token)


def render(client: GrizzlyHttpAuthClient, user: GrizzlyUser) -> None:
    host = user.render(client.host)
    parsed = urlparse(host)

    client.host = f'{parsed.scheme}://{parsed.netloc}'

    client_context = client._context.get(parsed.netloc, None)

    # we have a host specific context that we should merge into current context
    if client_context is not None:
        client._context = merge_dicts(client._context, cast(dict, client_context))


class RefreshTokenDistributor:
    _responses: ClassVar[dict[int, AsyncResult]] = {}
    _credentials: ClassVar[dict[int, AzureAadCredential]] = {}

    semaphore: ClassVar[Semaphore] = Semaphore()
    semaphores: ClassVar[dict[int, Semaphore]] = {}

    @classmethod
    def handle_response(cls, environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG003
        logger.debug('got consume_token: %r', msg.data)
        uid = msg.data['uid']
        response = msg.data['response']

        cls._responses[uid].set(response)


    @classmethod
    def handle_request(cls, environment: Environment, msg: Message, **_kwargs: Any) -> None:
        logger.debug('got produce_token: %r', msg.data)
        cid = msg.data['cid']  # (worker) client id
        uid = msg.data['uid']  # user id (user instance)
        rid = msg.data['rid']  # request id
        request = msg.data['request']
        key = hash(frozenset(request.items()))

        with cls.semaphore:
            if key not in cls.semaphores:
                cls.semaphores.update({key: Semaphore()})

        with cls.semaphores[key]:
            try:
                response: dict[str, Any]

                if key not in cls._credentials:
                    auth_method = AuthMethod.from_string(request['auth_method'])

                    default_module_name, class_name = request['class_name'].rsplit('.', 1)
                    credentials_class_type = ModuleLoader[AzureAadCredential].load(default_module_name, class_name)
                    credentials = credentials_class_type(
                        request['username'],
                        request['password'],
                        request['tenant'],
                        auth_method,
                        host=request['host'],
                        client_id=request['client_id'],
                        redirect=request['redirect'],
                        initialize=request['initialize'],
                        otp_secret=request['otp_secret'],
                        scope=request['scope'],
                    )
                    cls._credentials.update({key: credentials})
                    logger.debug('created %s for %d', class_name, key)

                access_token = cls._credentials[key].access_token
                refreshed = cls._credentials[key]._refreshed

                action_name = 'refreshed' if refreshed else 'claimed'

                logger.debug('%s token for %d', action_name, key)

                response = {
                    'token': access_token.token,
                    'expires_on': access_token.expires_on,
                    'refreshed': refreshed,
                }
            except Exception as e:
                response = {
                    'error': f'{e.__class__.__name__}: {e!s}',
                }
                logger.exception('failed to get token')

        assert environment.runner is not None

        environment.runner.send_message('consume_token', {'uid': uid, 'rid': rid, 'response': response}, client_id=cid)


    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient) -> tuple[AccessToken, bool]:
        uid = id(client)
        rid = str(uuid4())

        if uid in cls._responses:
            logger.warning('greenlet %d, user %s is already waiting for testdata', uid, client.logger.name)

        assert client.credential is not None

        module_name = client.credential.__class__.__module__
        class_name = client.credential.__class__.__name__

        request: dict[str, Any] = {
            'class_name': f'{module_name}.{class_name}',
            'username': client.credential.username,
            'password': client.credential.password,
            'tenant': client.credential.tenant,
            'auth_method': client.credential.auth_method.name,
            'host': client.credential.host,
            'client_id': client.credential.client_id,
            'redirect': client.credential.redirect,
            'initialize': client.credential.initialize,
            'otp_secret': client.credential.otp_secret,
            'scope': client.credential.scope,
        }

        cls._responses.update({uid: AsyncResult()})

        assert not isinstance(client.grizzly.state.locust, MasterRunner)

        logger.debug('%s is asking for a token', client.logger.name)

        client.grizzly.state.locust.send_message('produce_token', {'uid': uid, 'cid': client.grizzly.state.locust.client_id, 'rid': rid, 'request': request})

        try:
            response = cast(dict[str, Any], cls._responses[uid].get(timeout=10.0))
            error = response.get('error', None)

            if error is None:
                return AccessToken(response['token'], response['expires_on']), cast(bool, response['refreshed'])

            raise RuntimeError(error)
        finally:
            del cls._responses[uid]


class RefreshToken(metaclass=ABCMeta):
    __TOKEN_CREDENTIAL_TYPE__: ClassVar[type[AzureAadCredential]]

    @classmethod
    def initialize(cls, client: GrizzlyHttpAuthClient, user: GrizzlyUser) -> None:
        render(client, user)

        auth_context = client._context.get('auth', None)

        if auth_context is not None:
            auth_client: dict[str, Any] = auth_context.get('client', {})
            auth_user: dict[str, Any] = auth_context.get('user', {})

            username: str | None = auth_user.get('username')
            password: str | None = auth_user.get('password') or auth_client.get('secret')
            otp_secret: str | None = auth_user.get('otp_secret')
            client_id: str | None = auth_client.get('id')

            # nothing has changed, use existing crendential
            if client.credential is not None and (
                client.credential.username == username
                and client.credential.password == password
                and client.credential.auth_method != AuthMethod.NONE
            ):
                return

            tenant = auth_context.get('tenant', None) or auth_context.get('provider', None)
            initialize_uri = auth_user.get('initialize_uri')
            redirect_uri = auth_user.get('redirect_uri')

            use_auth_client = (
                client_id is not None
                and auth_client.get('secret') is not None
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
                password = auth_client.get('secret')
            elif use_auth_user:
                auth_method = AuthMethod.USER
                initialize_uri = auth_user.get('initialize_uri')
            else:
                auth_method = AuthMethod.NONE


            parsed = urlparse(client_id)
            scope: str | None = None
            if auth_method != AuthMethod.NONE and client_id is not None and parsed.scheme in ['http', 'https']:
                scope = f'{client_id}/user_impersonation'

            client.credential = cls.__TOKEN_CREDENTIAL_TYPE__(
                username,
                password,
                tenant,
                auth_method,
                host=client.host,
                client_id=client_id,
                redirect=redirect_uri,
                initialize=initialize_uri,
                otp_secret=otp_secret,
                scope=scope,
            )


from .aad import AAD

__all__ = [
    'AAD',
    'AccessToken',
]
