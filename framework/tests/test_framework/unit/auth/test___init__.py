"""Unit tests of grizzly.auth."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import call
from uuid import uuid4

import pytest
from gevent.event import AsyncResult
from gevent.lock import Semaphore
from grizzly.auth import AccessToken, GrizzlyHttpAuthClient, RefreshToken, RefreshTokenDistributor, refresh_token
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod
from grizzly.users import RestApiUser
from grizzly.utils import has_template, safe_del
from grizzly_common.azure.aad import AuthMethod, AuthType, AzureAadCredential
from locust.rpc.protocol import Message

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types.locust import LocalRunner, WorkerRunner

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class DummyAuthCredential(AzureAadCredential):
    def __init__(
        self,
        username: str | None,
        password: str,
        tenant: str,
        auth_method: AuthMethod,
        /,
        host: str,
        redirect: str | None,
        initialize: str | None,
        otp_secret: str | None = None,
        scope: str | None = None,
        client_id: str = '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
    ) -> None:
        self.username = username
        self.password = password
        self.tenant = tenant
        self.auth_method = auth_method
        self.host = host
        self.redirect = redirect
        self.initialize = initialize
        self.otp_secret = otp_secret
        self.scope = scope
        self.client_id = client_id
        self.auth_type = AuthType.HEADER if self.initialize is None else AuthType.COOKIE

        self._refreshed = False
        self._access_token = None
        self._token_payload = None

    def get_token(self, *scopes: str, claims: str | None = None, tenant_id: str | None = None, **_kwargs: Any) -> AccessToken:  # noqa: ARG002
        now = datetime.now(tz=timezone.utc).timestamp()

        if self._access_token is None or self._access_token.expires_on <= now:
            self._refreshed = self._access_token is not None and self._access_token.expires_on <= now
            self._access_token = AccessToken('dummy', expires_on=int(now + 3600))

        return self._access_token

    def get_oauth_authorization(self, *scopes: str, claims: str | None = None, tenant_id: str | None = None) -> AccessToken:  # noqa: ARG002
        return cast('AccessToken', self._access_token)

    def get_oauth_token(self, *, code: str | None = None, verifier: str | None = None, resource: str | None = None, tenant_id: str | None = None) -> AccessToken:  # noqa: ARG002
        return cast('AccessToken', self._access_token)


class DummyAuth(RefreshToken):
    __TOKEN_CREDENTIAL_TYPE__ = DummyAuthCredential


class NotAnAuth:
    pass


class TestGrizzlyHttpAuthClient:
    def test_add_metadata(self) -> None:
        class HttpAuth(GrizzlyHttpAuthClient):
            def __init__(self) -> None:
                self._context = {
                    'verify_certificates': True,
                    'metadata': None,
                    'auth': None,
                }

        client = HttpAuth()

        assert client._context.get('metadata', {}) is None

        client.add_metadata('foo', 'bar')

        assert client._context.get('metadata', None) == {'foo': 'bar'}


def test_refresh_token_client(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    decorator: refresh_token[DummyAuth] = refresh_token(DummyAuth)
    get_token_mock = mocker.patch(
        'grizzly.auth.RefreshTokenDistributor.get_token',
        side_effect=[
            (AccessToken('dummy', (int(datetime.now(tz=timezone.utc).timestamp()) + 3600)), False),
        ],
    )

    auth_context = parent.user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    def request(_: RestApiUser, request: RequestTask, *_args: Any, **_kwargs: Any) -> GrizzlyResponse:  # noqa: ARG001
        return None, None

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
    parent.user._scenario.tasks.clear()
    parent.user._scenario.tasks.add(request_task)

    # no auth, no parent
    original_auth_context = cast('dict', parent.user._context['auth']).copy()
    parent.user._context['auth'] = None

    parent.user.request(request_task)

    get_token_mock.assert_not_called()
    assert parent.user._context['auth'] is None

    parent.user._context['auth'] = original_auth_context

    auth_client_context = auth_context['client']
    auth_context = parent.user.context()['auth']

    # no authentication for api, request will be called which raises NotRefreshed
    assert auth_context['provider'] is None
    assert auth_client_context['id'] is None
    assert auth_client_context['secret'] is None
    assert auth_client_context['resource'] is None

    parent.user.request(request_task)

    get_token_mock.assert_not_called()

    parent.user._context.get('auth', {}).get('client', {}).update({'id': 'asdf', 'secret': 'asdf'})
    parent.user._context.get('auth', {}).update({'refresh_time': 3000, 'tenant': 'example.com'})

    # session is fresh, but no token set (first call)
    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)

    assert parent.user.credential == SOME(AzureAadCredential, auth_method=AuthMethod.CLIENT)
    assert parent.user.credential._access_token is not None
    get_token_mock.assert_called_once_with(parent.user)
    get_token_mock.reset_mock()
    assert len(caplog.messages) == 1
    assert 'asdf claimed client token until' in caplog.messages[-1]
    caplog.clear()

    assert parent.user.metadata['Authorization'] == 'Bearer dummy'

    # token is fresh and set, no refresh
    old_access_token = parent.user.credential._access_token
    assert old_access_token is not None

    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)

    get_token_mock.assert_not_called()
    get_token_mock.reset_mock()

    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert old_access_token is parent.user.credential._access_token
    assert caplog.messages == []

    # authorization is set, but it is time to refresh token
    get_token_mock.side_effect = [
        (AccessToken('dummy', int(datetime.now(tz=timezone.utc).timestamp())), True),
    ]
    old_access_token = AccessToken('dummy', expires_on=int(datetime.now(tz=timezone.utc).timestamp() - 3600))
    parent.user.credential._access_token = old_access_token

    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)

    get_token_mock.assert_called_once_with(parent.user)
    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert old_access_token is not parent.user.credential._access_token
    assert old_access_token.expires_on < parent.user.credential._access_token.expires_on
    assert len(caplog.messages) == 1
    assert 'asdf refreshed client token until' in caplog.messages[-1]


def test_refresh_token_user(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    with pytest.raises(AssertionError, match='test_framework.unit.auth.test___init__.NotAnAuth is not a subclass of grizzly.auth.RefreshToken'):
        refresh_token('test_framework.unit.auth.test___init__.NotAnAuth')

    # use string instead of class
    refresh_token('AAD')

    decorator: refresh_token[DummyAuth] = refresh_token('test_framework.unit.auth.test___init__.DummyAuth')
    get_token_mock = mocker.patch(
        'grizzly.auth.RefreshTokenDistributor.get_token',
        side_effect=[
            (AccessToken('dummy', (int(datetime.now(tz=timezone.utc).timestamp()) + 3600)), False),
        ],
    )

    auth_context = parent.user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    def request(_: RestApiUser, *_args: Any, **_kwargs: Any) -> GrizzlyResponse:
        return None, None

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    auth_user_context = auth_context['user']
    request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
    parent.user._scenario.tasks.clear()
    parent.user._scenario.tasks.add(request_task)

    # no authentication for api, request will be called which raises NotRefreshed
    assert auth_user_context['username'] is None
    assert auth_user_context['password'] is None
    assert auth_user_context['redirect_uri'] is None
    assert auth_context['tenant'] is None

    parent.user.request(request_task)
    get_token_mock.assert_not_called()

    auth_context['client']['id'] = 'asdf'
    auth_user_context.update(
        {
            'username': 'bob@example.com',
            'password': 'HemligaArne',
            'redirect_uri': '/authenticated',
        },
    )

    # AuthType.NONE since not tenant nor provider is set
    parent.user.request(request_task)
    get_token_mock.assert_not_called()

    safe_del(parent.user.metadata, 'Authorization')

    auth_context.update(
        {
            'refresh_time': 3000,
            'tenant': 'example.com',
        },
    )

    # session is fresh, but no token set (first call)
    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user)
    get_token_mock.reset_mock()

    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert parent.user.credential == SOME(AzureAadCredential, auth_method=AuthMethod.USER)
    assert parent.user.credential._access_token is not None
    assert len(caplog.messages) == 1
    assert 'bob@example.com claimed user token until' in caplog.messages[-1]
    caplog.clear()

    # token is fresh and set, no refresh
    old_access_token = parent.user.credential._access_token
    assert old_access_token is not None

    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)

    get_token_mock.assert_not_called()
    get_token_mock.reset_mock()
    assert caplog.messages == []
    assert old_access_token is parent.user.credential._access_token

    # authorization is set, but it is time to refresh token
    get_token_mock.side_effect = [
        (AccessToken('dummy', int(datetime.now(tz=timezone.utc).timestamp())), True),
    ]
    old_access_token = AccessToken('dummy', expires_on=int(datetime.now(tz=timezone.utc).timestamp() - 3600))
    parent.user.credential._access_token = old_access_token

    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user)
    get_token_mock.reset_mock()

    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert old_access_token is not parent.user.credential._access_token
    assert old_access_token.expires_on < parent.user.credential._access_token.expires_on
    assert len(caplog.messages) == 1
    assert 'bob@example.com refreshed user token until' in caplog.messages[-1]

    # change user -> new credential/access token
    get_token_mock.side_effect = [
        (AccessToken('dummy', int(datetime.now(tz=timezone.utc).timestamp())), True),
    ]
    old_access_token = parent.user.credential._access_token
    parent.user.add_context({'auth': {'user': {'username': 'alice@example.com', 'password': 'foobar'}}})
    assert 'Authorization' not in parent.user.metadata
    assert getattr(parent.user, 'credential', 'foo') is None
    auth_context = parent.user._context.get('auth', None)
    assert auth_context is not None
    assert auth_context.get('user', None) == {
        'username': 'alice@example.com',
        'password': 'foobar',
        'otp_secret': None,
        'redirect_uri': '/authenticated',
        'initialize_uri': None,
    }

    # new user in context, needs to get a new token
    get_token_mock.side_effect = [
        (AccessToken('dummy', int(datetime.now(tz=timezone.utc).timestamp())), False),
    ]
    parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user)
    assert parent.user.metadata.get('Authorization', None) == 'Bearer dummy'
    cached_auth_values = list(parent.user.__cached_auth__.values())
    assert len(cached_auth_values) == 1
    assert cached_auth_values[0]._access_token == SOME(AccessToken, token=old_access_token.token, expires_on=old_access_token.expires_on)


@pytest.mark.parametrize('host', ['www.example.com', '{{ test_host }}'])
def test_refresh_token_user_render(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, host: str) -> None:
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    decorator: refresh_token[DummyAuth] = refresh_token(DummyAuth)
    get_token_mock = mocker.patch(
        'grizzly.auth.RefreshTokenDistributor.get_token',
        side_effect=[
            (AccessToken('dummy', (int(datetime.now(tz=timezone.utc).timestamp()) + 3600)), False),
        ],
    )

    def get(_: HttpClientTask, parent: GrizzlyScenario, *_args: Any, **_kwargs: Any) -> GrizzlyResponse:  # noqa: ARG001
        return None, None

    mocker.patch(
        'grizzly.tasks.clients.http.HttpClientTask.request_from',
        decorator(get),
    )

    rendered_host = 'www.example.net' if has_template(host) else host

    grizzly = grizzly_fixture.grizzly
    parent.user._scenario.variables.update({'foobar': 'none', 'test_host': f'http://{rendered_host}'})
    parent.user.variables.update({'foobar': 'none', 'test_host': f'http://{rendered_host}'})

    http_client_task = type('TestHttpClientTask', (HttpClientTask,), {'__scenario__': grizzly.scenario})

    # no auth in context
    client = http_client_task(RequestDirection.FROM, f'http://{host}/blob/file.txt', 'test', payload_variable='foobar')
    parent.user.variables.update({'test_host': f'http://{rendered_host}'})

    client.on_start(parent)

    if rendered_host == host:
        assert client.host == f'http://{host}/blob/file.txt'
    else:  # assumed that variables contains scheme
        assert client.host == f'{host}/blob/file.txt'

    client.request_from(parent)

    get_token_mock.assert_not_called()
    assert client._context == {'verify_certificates': True, 'metadata': None, 'auth': None, 'host': ''}
    assert client.host == f'http://{rendered_host}'

    get_token_mock.reset_mock()

    # auth in context
    cast('dict', client._context).update(
        {
            rendered_host: {
                'auth': {
                    'client': {
                        'id': 'foobar',
                    },
                    'user': {
                        'username': 'bob',
                        'password': 'password',
                        'redirect_uri': '/authenticated',
                    },
                    'provider': 'https://login.example.com/oauth2',
                },
            },
        },
    )

    client.request_from(parent)

    get_token_mock.assert_called_once_with(client)

    actual_context = cast('dict', client._context.copy())
    safe_del(actual_context, rendered_host)

    assert actual_context == {
        'verify_certificates': True,
        'metadata': None,
        'auth': {
            'client': {
                'id': 'foobar',
            },
            'user': {
                'username': 'bob',
                'password': 'password',
                'redirect_uri': '/authenticated',
            },
            'provider': 'https://login.example.com/oauth2',
        },
        'host': '',
    }
    assert client.metadata == {'Authorization': 'Bearer dummy', 'x-grizzly-user': ANY(str)}


class TestRefreshTokenDistributor:
    def test_handle_response(self, grizzly_fixture: GrizzlyFixture) -> None:
        environment = grizzly_fixture.behave.locust.environment

        uid = id('foo')
        msg = Message(
            'consume_token',
            {
                'uid': uid,
                'response': {
                    'foo': 'bar',
                    'hello': 'world',
                },
            },
            'worker-1',
        )
        result = AsyncResult()

        try:
            assert RefreshTokenDistributor._credentials == {}
            assert RefreshTokenDistributor._responses == {}
            RefreshTokenDistributor._responses.update({uid: result})

            RefreshTokenDistributor.handle_response(environment, msg, foo='bar')

            assert RefreshTokenDistributor._responses == {uid: result}
            assert RefreshTokenDistributor._responses[uid].get(timeout=1.0) == {
                'foo': 'bar',
                'hello': 'world',
            }
            assert RefreshTokenDistributor._credentials == {}
        finally:
            RefreshTokenDistributor._responses.clear()

    def test_handle_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture()

        expires_on = int(datetime.now(tz=timezone.utc).timestamp())
        environment = grizzly_fixture.behave.locust.environment
        send_message_mock = mocker.patch.object(environment.runner, 'send_message', return_value=None)
        module_loader_mock = mocker.patch('grizzly.auth.ModuleLoader.load', return_value=AzureAadCredential)
        access_token1 = AccessToken('dummy', expires_on)
        access_token_mock = mocker.patch('grizzly_common.azure.aad.AzureAadCredential.access_token', new_callable=mocker.PropertyMock, return_value=access_token1)

        uid = id(parent.user)
        cid = 'worker-1'
        rid = str(uuid4())
        client_id = str(uuid4())

        request = {
            'class_name': 'grizzly_common.azure.aad.AzureAadCredential',
            'auth_method': 'USER',
            'username': 'foo@example.com',
            'password': 's3cr3+',
            'tenant': 'example.com',
            'host': 'foo.example.com',
            'client_id': client_id,
            'redirect': 'https://foo.example.com/login-callback',
            'initialize': None,
            'otp_secret': None,
            'scope': None,
        }

        msg = Message(
            'produce_token',
            {
                'cid': cid,
                'uid': uid,
                'rid': rid,
                'request': request,
            },
            'worker-1',
        )

        key1 = hash(frozenset(request.items()))

        try:
            assert RefreshTokenDistributor._credentials == {}
            assert RefreshTokenDistributor._responses == {}
            assert RefreshTokenDistributor.semaphore == ANY(Semaphore)
            assert RefreshTokenDistributor.semaphores == {}

            # new credential
            RefreshTokenDistributor.handle_request(environment, msg, foo='bar')

            module_loader_mock.assert_called_once_with('grizzly_common.azure.aad', 'AzureAadCredential')
            module_loader_mock.reset_mock()

            send_message_mock.assert_called_once_with(
                'consume_token',
                {
                    'uid': uid,
                    'rid': rid,
                    'response': {
                        'token': access_token1.token,
                        'expires_on': access_token1.expires_on,
                        'refreshed': False,
                    },
                },
                client_id=cid,
            )
            send_message_mock.reset_mock()

            assert RefreshTokenDistributor._responses == {}
            assert RefreshTokenDistributor._credentials == {
                key1: SOME(
                    AzureAadCredential,
                    username='foo@example.com',
                    password='s3cr3+',
                    tenant='example.com',
                    host='foo.example.com',
                    client_id=client_id,
                    redirect='https://foo.example.com/login-callback',
                    initialize=None,
                    otp_secret=None,
                    scope=None,
                ),
            }
            assert RefreshTokenDistributor.semaphores == {key1: ANY(Semaphore)}

            # use cached credential
            RefreshTokenDistributor.handle_request(environment, msg, foo='bar')
            module_loader_mock.assert_not_called()

            send_message_mock.assert_called_once_with(
                'consume_token',
                {
                    'uid': uid,
                    'rid': rid,
                    'response': {
                        'token': access_token1.token,
                        'expires_on': access_token1.expires_on,
                        'refreshed': False,
                    },
                },
                client_id=cid,
            )
            send_message_mock.reset_mock()

            assert RefreshTokenDistributor._responses == {}
            assert RefreshTokenDistributor._credentials == {
                key1: SOME(
                    AzureAadCredential,
                    username='foo@example.com',
                    password='s3cr3+',
                    tenant='example.com',
                    host='foo.example.com',
                    client_id=client_id,
                    redirect='https://foo.example.com/login-callback',
                    initialize=None,
                    otp_secret=None,
                    scope=None,
                ),
            }
            assert RefreshTokenDistributor.semaphores == {key1: ANY(Semaphore)}

            # another credential
            request.update({'username': 'bar@example.com', 'password': 'qwerty', 'otp_secret': 'aaaa'})
            key2 = hash(frozenset(request.items()))
            access_token2 = AccessToken('bar-dummy', expires_on)
            access_token_mock.return_value = access_token2

            RefreshTokenDistributor.handle_request(environment, msg, foo='bar')

            module_loader_mock.assert_called_once_with('grizzly_common.azure.aad', 'AzureAadCredential')
            module_loader_mock.reset_mock()

            send_message_mock.assert_called_once_with(
                'consume_token',
                {
                    'uid': uid,
                    'rid': rid,
                    'response': {
                        'token': access_token2.token,
                        'expires_on': access_token2.expires_on,
                        'refreshed': False,
                    },
                },
                client_id=cid,
            )
            send_message_mock.reset_mock()

            assert RefreshTokenDistributor._responses == {}
            assert RefreshTokenDistributor._credentials == {
                key1: SOME(
                    AzureAadCredential,
                    username='foo@example.com',
                    password='s3cr3+',
                    tenant='example.com',
                    host='foo.example.com',
                    client_id=client_id,
                    redirect='https://foo.example.com/login-callback',
                    initialize=None,
                    otp_secret=None,
                    scope=None,
                ),
                key2: SOME(
                    AzureAadCredential,
                    username='bar@example.com',
                    password='qwerty',
                    tenant='example.com',
                    host='foo.example.com',
                    client_id=client_id,
                    redirect='https://foo.example.com/login-callback',
                    initialize=None,
                    otp_secret='aaaa',
                    scope=None,
                ),
            }
            assert RefreshTokenDistributor.semaphores == {key1: ANY(Semaphore), key2: ANY(Semaphore)}

            # error
            access_token_mock.side_effect = [RuntimeError('failed to get token')]

            RefreshTokenDistributor.handle_request(environment, msg, foo='bar')

            send_message_mock.assert_called_once_with(
                'consume_token',
                {
                    'uid': uid,
                    'rid': rid,
                    'response': {
                        'error': 'RuntimeError: failed to get token',
                    },
                },
                client_id=cid,
            )
            send_message_mock.reset_mock()
            module_loader_mock.assert_not_called()

            assert RefreshTokenDistributor._responses == {}
            assert RefreshTokenDistributor._credentials == {
                key1: SOME(
                    AzureAadCredential,
                    username='foo@example.com',
                    password='s3cr3+',
                    tenant='example.com',
                    host='foo.example.com',
                    client_id=client_id,
                    redirect='https://foo.example.com/login-callback',
                    initialize=None,
                    otp_secret=None,
                    scope=None,
                ),
                key2: SOME(
                    AzureAadCredential,
                    username='bar@example.com',
                    password='qwerty',
                    tenant='example.com',
                    host='foo.example.com',
                    client_id=client_id,
                    redirect='https://foo.example.com/login-callback',
                    initialize=None,
                    otp_secret='aaaa',
                    scope=None,
                ),
            }
            assert RefreshTokenDistributor.semaphores == {key1: ANY(Semaphore), key2: ANY(Semaphore)}

        finally:
            RefreshTokenDistributor._credentials.clear()
            RefreshTokenDistributor.semaphores.clear()

    def test_get_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert parent is not None
        assert isinstance(parent.user, RestApiUser)

        grizzly = grizzly_fixture.grizzly

        grizzly.state.locust.register_message('consume_token', RefreshTokenDistributor.handle_response, concurrent=True)
        grizzly.state.locust.register_message('produce_token', RefreshTokenDistributor.handle_request, concurrent=True)

        send_message_mock = mocker.spy(grizzly.state.locust, 'send_message')

        client_id = str(uuid4())

        parent.user.credential = AzureAadCredential(
            'foo@example.com',
            'qwerty',
            'example.com',
            AuthMethod.USER,
            host='example.com',
            client_id=client_id,
            redirect='https://example.com/login-callback',
            initialize=None,
            otp_secret='aaaa',
            scope=None,
        )

        expires_on = int(datetime.now(tz=timezone.utc).timestamp())
        access_token = AccessToken('dummy', expires_on)
        access_token_mock = mocker.patch('grizzly_common.azure.aad.AzureAadCredential.access_token', new_callable=mocker.PropertyMock, return_value=access_token)

        # get token OK
        assert RefreshTokenDistributor.get_token(parent.user) == (SOME(AccessToken, token=access_token.token, expires_on=access_token.expires_on), False)

        assert send_message_mock.call_count == 2

        assert send_message_mock.call_args_list[0] == call(
            'produce_token',
            {
                'uid': id(parent.user),
                'cid': parent.user.grizzly.state.locust.client_id,  # type: ignore[union-attr]
                'rid': ANY(str),
                'request': {
                    'class_name': 'grizzly_common.azure.aad.AzureAadCredential',
                    'username': parent.user.credential.username,
                    'password': parent.user.credential.password,
                    'tenant': parent.user.credential.tenant,
                    'auth_method': parent.user.credential.auth_method.name,
                    'host': parent.user.credential.host,
                    'client_id': parent.user.credential.client_id,
                    'redirect': parent.user.credential.redirect,
                    'initialize': parent.user.credential.initialize,
                    'otp_secret': parent.user.credential.otp_secret,
                    'scope': parent.user.credential.scope,
                },
            },
        )
        assert send_message_mock.call_args_list[1] == call(
            'consume_token',
            {
                'uid': id(parent.user),
                'rid': ANY(str),
                'response': {
                    'token': 'dummy',
                    'expires_on': expires_on,
                    'refreshed': False,
                },
            },
            client_id=cast('LocalRunner | WorkerRunner', parent.user.grizzly.state.locust).client_id,
        )
        send_message_mock.reset_mock()

        # get token ERROR
        access_token_mock.side_effect = [RuntimeError('failed to get token')]

        with pytest.raises(RuntimeError, match='RuntimeError: failed to get token'):
            RefreshTokenDistributor.get_token(parent.user)

        assert send_message_mock.call_count == 2

        assert send_message_mock.call_args_list[0] == call(
            'produce_token',
            {
                'uid': id(parent.user),
                'cid': parent.user.grizzly.state.locust.client_id,  # type: ignore[union-attr]
                'rid': ANY(str),
                'request': {
                    'class_name': 'grizzly_common.azure.aad.AzureAadCredential',
                    'username': parent.user.credential.username,
                    'password': parent.user.credential.password,
                    'tenant': parent.user.credential.tenant,
                    'auth_method': parent.user.credential.auth_method.name,
                    'host': parent.user.credential.host,
                    'client_id': parent.user.credential.client_id,
                    'redirect': parent.user.credential.redirect,
                    'initialize': parent.user.credential.initialize,
                    'otp_secret': parent.user.credential.otp_secret,
                    'scope': parent.user.credential.scope,
                },
            },
        )
        assert send_message_mock.call_args_list[1] == call(
            'consume_token',
            {
                'uid': id(parent.user),
                'rid': ANY(str),
                'response': {
                    'error': 'RuntimeError: failed to get token',
                },
            },
            client_id=cast('LocalRunner | WorkerRunner', parent.user.grizzly.state.locust).client_id,
        )
        send_message_mock.reset_mock()
