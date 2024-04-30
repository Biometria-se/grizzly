"""Unit tests of grizzly.auth."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, cast
from unittest.mock import ANY

import pytest

from grizzly.auth import AccessToken, GrizzlyHttpAuthClient, RefreshToken, refresh_token
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod
from grizzly.users import RestApiUser
from grizzly.utils import has_template, safe_del
from grizzly_extras.azure.aad import AuthMethod, AuthType, AzureAadCredential
from tests.helpers import SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture

    from grizzly.scenarios import GrizzlyScenario
    from tests.fixtures import GrizzlyFixture


class DummyAuthCredential(AzureAadCredential):
    def __init__(  # noqa: PLR0913
            self,
            username: Optional[str],
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

        return cast(AccessToken, self._access_token)

    def get_oauth_authorization(self, *scopes: str, claims: str | None = None, tenant_id: str | None = None) -> AccessToken:  # noqa: ARG002
        return cast(AccessToken, self._access_token)

    def get_oauth_token(self, *, code: str | None = None, verifier: str | None = None, resource: str | None = None, tenant_id: str | None = None) -> AccessToken:  # noqa: ARG002
        return cast(AccessToken, self._access_token)


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
    get_token_mock = mocker.spy(DummyAuthCredential, 'get_token')

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
    original_auth_context = cast(dict, parent.user._context['auth']).copy()
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
    get_token_mock.assert_called_once_with(parent.user.credential)
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

    get_token_mock.assert_called_once_with(parent.user.credential)
    get_token_mock.reset_mock()

    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert old_access_token is parent.user.credential._access_token
    assert caplog.messages == []

    # authorization is set, but it is time to refresh token
    old_access_token = AccessToken('dummy', expires_on=int(datetime.now(tz=timezone.utc).timestamp() - 3600))
    parent.user.credential._access_token = old_access_token

    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)

    get_token_mock.assert_called_once_with(parent.user.credential)
    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert old_access_token is not parent.user.credential._access_token
    assert old_access_token.expires_on < parent.user.credential._access_token.expires_on
    assert len(caplog.messages) == 1
    assert 'asdf refreshed client token until' in caplog.messages[-1]


def test_refresh_token_user(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    with pytest.raises(AssertionError, match='tests.unit.test_grizzly.auth.test___init__.NotAnAuth is not a subclass of grizzly.auth.RefreshToken'):
        refresh_token('tests.unit.test_grizzly.auth.test___init__.NotAnAuth')

    # use string instead of class
    refresh_token('AAD')

    decorator: refresh_token[DummyAuth] = refresh_token('tests.unit.test_grizzly.auth.test___init__.DummyAuth')
    get_token_mock = mocker.spy(DummyAuthCredential, 'get_token')

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
    auth_user_context.update({
        'username': 'bob@example.com',
        'password': 'HemligaArne',
        'redirect_uri': '/authenticated',
    })

    # AuthType.NONE since not tenant nor provider is set
    parent.user.request(request_task)
    get_token_mock.assert_not_called()

    safe_del(parent.user.metadata, 'Authorization')

    auth_context.update({
        'refresh_time': 3000,
        'tenant': 'example.com',
    })

    # session is fresh, but no token set (first call)
    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user.credential)
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

    get_token_mock.assert_called_once_with(parent.user.credential)
    get_token_mock.reset_mock()
    assert caplog.messages == []
    assert old_access_token is parent.user.credential._access_token

    # authorization is set, but it is time to refresh token
    old_access_token = AccessToken('dummy', expires_on=int(datetime.now(tz=timezone.utc).timestamp() - 3600))
    parent.user.credential._access_token = old_access_token

    with caplog.at_level(logging.INFO):
        parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user.credential)
    get_token_mock.reset_mock()

    assert parent.user.metadata['Authorization'] == 'Bearer dummy'
    assert old_access_token is not parent.user.credential._access_token
    assert old_access_token.expires_on < parent.user.credential._access_token.expires_on
    assert len(caplog.messages) == 1
    assert 'bob@example.com refreshed user token until' in caplog.messages[-1]

    # change user -> new credential/access token
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
    parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user.credential)
    assert parent.user.metadata.get('Authorization', None) == 'Bearer dummy'
    cached_auth_values = list(parent.user.__cached_auth__.values())
    assert len(cached_auth_values) == 1
    assert cached_auth_values[0]._access_token == SOME(AccessToken, token=old_access_token.token, expires_on=old_access_token.expires_on)


@pytest.mark.parametrize('host', ['www.example.com', '{{ test_host }}'])
def test_refresh_token_user_render(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, host: str) -> None:
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    decorator: refresh_token[DummyAuth] = refresh_token(DummyAuth)
    get_token_mock = mocker.spy(DummyAuthCredential, 'get_token')

    def get(_: HttpClientTask, parent: GrizzlyScenario, *_args: Any, **_kwargs: Any) -> GrizzlyResponse:  # noqa: ARG001
        return None, None

    mocker.patch(
        'grizzly.tasks.clients.http.HttpClientTask.get',
        decorator(get),
    )

    rendered_host = 'www.example.net' if has_template(host) else host

    grizzly = grizzly_fixture.grizzly
    grizzly.state.variables.update({'foobar': 'none', 'test_host': f'http://{rendered_host}'})

    http_client_task = type('TestHttpClientTask', (HttpClientTask,), {'__scenario__': grizzly.scenario})

    # no auth in context
    client = http_client_task(RequestDirection.FROM, f'http://{host}/blob/file.txt', 'test', payload_variable='foobar')
    cast(dict, client._context).update({'variables': {'test_host': f'http://{rendered_host}'}})
    client.on_start(parent)

    if rendered_host == host:
        assert client.host == f'http://{host}/blob/file.txt'
    else:  # assumed that variables contains scheme
        assert client.host == f'{host}/blob/file.txt'

    client.get(parent)

    get_token_mock.assert_not_called()
    assert client._context == {'verify_certificates': True, 'metadata': None, 'auth': None, 'host': '', 'variables': {'test_host': f'http://{rendered_host}'}}
    assert client.host == f'http://{rendered_host}'

    get_token_mock.reset_mock()

    # auth in context
    cast(dict, client._context).update({
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
    })

    client.get(parent)

    get_token_mock.assert_called_once_with(client.credential)

    actual_context = cast(dict, client._context.copy())
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
        'variables': {
            'test_host': f'http://{rendered_host}',
        },
    }
    assert client.metadata == {'Authorization': 'Bearer dummy', 'x-grizzly-user': ANY}
