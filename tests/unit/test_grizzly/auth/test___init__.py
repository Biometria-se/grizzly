from typing import Any, Dict, Tuple, Literal, cast
from time import time
from unittest.mock import ANY

import pytest

from pytest_mock import MockerFixture

from grizzly.auth import refresh_token, RefreshToken, AuthMethod, AuthType, GrizzlyHttpAuthClient
from grizzly.users import RestApiUser
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestMethod, RequestDirection
from grizzly.utils import safe_del

from tests.fixtures import GrizzlyFixture


class DummyAuth(RefreshToken):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> Tuple[AuthType, str]:
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
    _, user, scenario = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(user, RestApiUser)
    assert scenario is not None

    decorator = refresh_token(DummyAuth)
    get_token_mock = mocker.spy(DummyAuth, 'get_token')

    auth_context = user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    def request(self: 'RestApiUser', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        return None

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    try:
        request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        user._scenario.tasks.clear()
        user._scenario.tasks.add(request_task)

        # no auth, no parent
        original_auth_context = cast(dict, user._context['auth']).copy()
        user._context['auth'] = None

        user.request(request_task)

        get_token_mock.assert_not_called()
        assert user._context['auth'] is None

        user._context['auth'] = original_auth_context

        auth_client_context = auth_context['client']
        auth_context = user.context()['auth']

        # no authentication for api, request will be called which raises NotRefreshed
        assert auth_context['provider'] is None
        assert auth_client_context['id'] is None
        assert auth_client_context['secret'] is None
        assert auth_client_context['resource'] is None

        user.request(request_task)

        get_token_mock.assert_not_called()

        auth_client_context['id'] = 'asdf'
        auth_client_context['secret'] = 'asdf'
        auth_context['refresh_time'] = 3000
        auth_context['provider'] = 'http://login.example.com/oauth2'

        # session has not started
        user.request(request_task)
        get_token_mock.assert_not_called()

        user.session_started = time()
        safe_del(user.headers, 'Authorization')

        # session is fresh, but no token set (first call)
        user.request(request_task)
        get_token_mock.assert_called_once_with(user, AuthMethod.CLIENT)
        get_token_mock.reset_mock()

        assert user.headers['Authorization'] == 'Bearer dummy'

        # token is fresh and set, no refresh
        user.session_started = time()
        user.headers['Authorization'] = 'Bearer dummy'

        user.request(request_task)
        get_token_mock.assert_not_called()

        # authorization is set, but it is time to refresh token
        user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        user.request(request_task)
        get_token_mock.assert_called_once_with(user, AuthMethod.CLIENT)
    finally:
        pass


def test_refresh_token_user(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    _, user, _ = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(user, RestApiUser)

    with pytest.raises(AssertionError) as ae:
        refresh_token('tests.unit.test_grizzly.auth.test___init__.NotAnAuth')
    assert str(ae.value) == 'tests.unit.test_grizzly.auth.test___init__.NotAnAuth is not a subclass of grizzly.auth.RefreshToken'

    # use string instead of class
    refresh_token('AAD')

    decorator = refresh_token('tests.unit.test_grizzly.auth.test___init__.DummyAuth')
    get_token_mock = mocker.spy(DummyAuth, 'get_token')

    auth_context = user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    def request(self: 'RestApiUser', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        return None

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    try:
        auth_user_context = auth_context['user']
        request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        user._scenario.tasks.clear()
        user._scenario.tasks.add(request_task)

        # no authentication for api, request will be called which raises NotRefreshed
        assert auth_user_context['username'] is None
        assert auth_user_context['password'] is None
        assert auth_user_context['redirect_uri'] is None
        assert auth_context['provider'] is None

        user.request(request_task)
        get_token_mock.assert_not_called()

        auth_context['client']['id'] = 'asdf'
        auth_user_context['username'] = 'bob@example.com'
        auth_user_context['password'] = 'HemligaArne'
        auth_user_context['redirect_uri'] = '/authenticated'
        auth_context['refresh_time'] = 3000
        auth_context['provider'] = 'https://login.example.com/oauth2'

        # session has not started
        user.request(request_task)
        get_token_mock.assert_not_called()

        user.session_started = time()
        safe_del(user.headers, 'Authorization')

        # session is fresh, but no token set (first call)
        user.request(request_task)
        get_token_mock.assert_called_once_with(user, AuthMethod.USER)
        get_token_mock.reset_mock()
        assert user.headers['Authorization'] == 'Bearer dummy'

        # token is fresh and set, no refresh
        user.session_started = time()

        user.request(request_task)
        get_token_mock.assert_not_called()

        # authorization is set, but it is time to refresh token
        user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        user.request(request_task)
        get_token_mock.assert_called_once_with(user, AuthMethod.USER)
        get_token_mock.reset_mock()

        # safe_del(user.headers, 'Authorization')

        user.add_context({'auth': {'user': {'username': 'alice@example.com'}}})
        assert 'Authorization' not in user.headers
        auth_context = user._context.get('auth', None)
        assert auth_context is not None
        assert auth_context.get('user', None) == {
            'username': 'alice@example.com',
            'password': 'HemligaArne',
            'redirect_uri': '/authenticated',
            'initialize_uri': None,
        }

        # new user in context, needs to get a new token
        user.request(request_task)
        get_token_mock.assert_called_once_with(user, AuthMethod.USER)
        assert user.headers.get('Authorization', None) == 'Bearer dummy'
    finally:
        pass


@pytest.mark.parametrize('host', ['www.example.com', '{{ test_host }}'])
def test_refresh_token_user_render(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, host: str) -> None:
    _, user, parent = grizzly_fixture(user_type=RestApiUser)
    assert isinstance(user, RestApiUser)
    assert parent is not None

    decorator = refresh_token(DummyAuth)
    get_token_mock = mocker.spy(DummyAuth, 'get_token')

    def get(self: 'HttpClientTask', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        return None

    mocker.patch(
        'grizzly.tasks.clients.http.HttpClientTask.get',
        decorator(get),
    )

    if '{{' in host and '}}' in host:
        rendered_host = 'www.example.net'
    else:
        rendered_host = host

    grizzly = grizzly_fixture.grizzly
    grizzly.state.variables.update({'foobar': 'none', 'test_host': f'http://{rendered_host}'})
    user._context.update({'variables': {'test_host': f'http://{rendered_host}'}})

    # no auth in context
    client = HttpClientTask(RequestDirection.FROM, f'http://{host}/blob/file.txt', 'test', payload_variable='foobar')
    client.on_start(parent)

    assert client.parent is not None

    if rendered_host == host:
        assert client.host == f'http://{host}/blob/file.txt'
    else:  # assumed that variables contains scheme
        assert client.host == f'{host}/blob/file.txt'

    client.get(parent)

    get_token_mock.assert_not_called()
    assert client._context == {'verify_certificates': True, 'metadata': None, 'auth': None}
    assert client.host == f'http://{rendered_host}'

    get_token_mock.reset_mock()

    # auth in context
    cast(dict, client.parent.user._context).update({
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
    }
    assert client.headers == {'Authorization': 'Bearer dummy', 'x-grizzly-user': ANY}
