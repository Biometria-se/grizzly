"""Unit tests of grizzly.auth."""
from __future__ import annotations

from time import time
from typing import TYPE_CHECKING, Any, Literal, Tuple, cast
from unittest.mock import ANY

import pytest

from grizzly.auth import AuthMethod, AuthType, GrizzlyHttpAuthClient, RefreshToken, refresh_token
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod
from grizzly.users import RestApiUser
from grizzly.utils import has_template, safe_del

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from grizzly.scenarios import GrizzlyScenario
    from tests.fixtures import GrizzlyFixture


class DummyAuth(RefreshToken):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> Tuple[AuthType, str]:  # noqa: ARG003
        return AuthType.HEADER, 'dummy'


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


def test_refresh_token_client(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    decorator: refresh_token[DummyAuth] = refresh_token(DummyAuth)
    get_token_mock = mocker.spy(DummyAuth, 'get_token')

    auth_context = parent.user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    def request(_: RestApiUser, request: RequestTask, *_args: Any, **_kwargs: Any) -> GrizzlyResponse:  # noqa: ARG001
        return None, None

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    try:
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

        auth_client_context['id'] = 'asdf'
        auth_client_context['secret'] = 'asdf'  # noqa: S105
        auth_context['refresh_time'] = 3000
        auth_context['provider'] = 'http://login.example.com/oauth2'

        # session has not started
        parent.user.request(request_task)
        get_token_mock.assert_not_called()

        parent.user.session_started = time()
        safe_del(parent.user.metadata, 'Authorization')

        # session is fresh, but no token set (first call)
        parent.user.request(request_task)
        get_token_mock.assert_called_once_with(parent.user, AuthMethod.CLIENT)
        get_token_mock.reset_mock()

        assert parent.user.metadata['Authorization'] == 'Bearer dummy'

        # token is fresh and set, no refresh
        parent.user.session_started = time()
        parent.user.metadata['Authorization'] = 'Bearer dummy'

        parent.user.request(request_task)
        get_token_mock.assert_not_called()

        # authorization is set, but it is time to refresh token
        parent.user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        parent.user.request(request_task)
        get_token_mock.assert_called_once_with(parent.user, AuthMethod.CLIENT)
    finally:
        pass


def test_refresh_token_user(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    with pytest.raises(AssertionError, match='tests.unit.test_grizzly.auth.test___init__.NotAnAuth is not a subclass of grizzly.auth.RefreshToken'):
        refresh_token('tests.unit.test_grizzly.auth.test___init__.NotAnAuth')

    # use string instead of class
    refresh_token('AAD')

    decorator: refresh_token[DummyAuth] = refresh_token('tests.unit.test_grizzly.auth.test___init__.DummyAuth')
    get_token_mock = mocker.spy(DummyAuth, 'get_token')

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
    assert auth_context['provider'] is None

    parent.user.request(request_task)
    get_token_mock.assert_not_called()

    auth_context['client']['id'] = 'asdf'
    auth_user_context.update({
        'username': 'bob@example.com',
        'password': 'HemligaArne',
        'redirect_uri': '/authenticated',
    })
    auth_context.update({
        'refresh_time': 3000,
        'provider': 'https://login.example.com/oauth2',
    })

    # session has not started
    parent.user.request(request_task)
    get_token_mock.assert_not_called()

    parent.user.session_started = time()
    safe_del(parent.user.metadata, 'Authorization')

    # session is fresh, but no token set (first call)
    parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user, AuthMethod.USER)
    get_token_mock.reset_mock()
    assert parent.user.metadata['Authorization'] == 'Bearer dummy'

    # token is fresh and set, no refresh
    parent.user.session_started = time()

    parent.user.request(request_task)
    get_token_mock.assert_not_called()

    # authorization is set, but it is time to refresh token
    parent.user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

    parent.user.request(request_task)
    get_token_mock.assert_called_once_with(parent.user, AuthMethod.USER)
    get_token_mock.reset_mock()

    parent.user.add_context({'auth': {'user': {'username': 'alice@example.com', 'password': 'foobar'}}})
    assert 'Authorization' not in parent.user.metadata
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
    get_token_mock.assert_called_once_with(parent.user, AuthMethod.USER)
    assert parent.user.metadata.get('Authorization', None) == 'Bearer dummy'


@pytest.mark.parametrize('host', ['www.example.com', '{{ test_host }}'])
def test_refresh_token_user_render(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, host: str) -> None:
    parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(parent.user, RestApiUser)

    decorator: refresh_token[DummyAuth] = refresh_token(DummyAuth)
    get_token_mock = mocker.spy(DummyAuth, 'get_token')

    def get(_: HttpClientTask, parent: GrizzlyScenario, *_args: Any, **_kwargs: Any) -> GrizzlyResponse:  # noqa: ARG001
        return None, None

    mocker.patch(
        'grizzly.tasks.clients.http.HttpClientTask.get',
        decorator(get),
    )

    rendered_host = 'www.example.net' if has_template(host) else host

    grizzly = grizzly_fixture.grizzly
    grizzly.state.variables.update({'foobar': 'none', 'test_host': f'http://{rendered_host}'})

    # no auth in context
    HttpClientTask.__scenario__ = grizzly.scenario
    client = HttpClientTask(RequestDirection.FROM, f'http://{host}/blob/file.txt', 'test', payload_variable='foobar')
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

    get_token_mock.assert_called_once_with(client, AuthMethod.USER)

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
