from typing import Any, Dict, Tuple, Literal, cast
from time import time

import pytest

from pytest_mock import MockerFixture

from grizzly.auth import refresh_token, RefreshToken, AuthMethod, GrizzlyHttpAuthClient
from grizzly.context import GrizzlyContextScenario
from grizzly.users import RestApiUser
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.utils import safe_del

from tests.fixtures import GrizzlyFixture

RestApiScenarioFixture = Tuple[RestApiUser, GrizzlyContextScenario]


class DummyAuth(RefreshToken):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> str:
        return 'dummy'


@pytest.fixture
def restapi_user(grizzly_fixture: GrizzlyFixture) -> RestApiScenarioFixture:
    scenario = GrizzlyContextScenario(1)
    scenario.name = scenario.description = 'TestScenario'
    scenario.context['host'] = 'test'
    scenario.user.class_name = 'RestApiUser'

    _, user, _ = grizzly_fixture('http://example.net', RestApiUser)

    request = grizzly_fixture.request_task.request

    scenario.tasks.add(request)

    return cast(RestApiUser, user), scenario


def test_refresh_token_client(restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
    [user, scenario] = restapi_user
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
        auth_client_context = auth_context['client']
        request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        scenario.tasks.clear()
        scenario.tasks.add(request_task)

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


def test_refresh_token_user(restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
    [user, scenario] = restapi_user
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
        auth_user_context = auth_context['user']
        request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        scenario.tasks.clear()
        scenario.tasks.add(request_task)

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
        }

        # new user in context, needs to get a new token
        user.request(request_task)
        get_token_mock.assert_called_once_with(user, AuthMethod.USER)
        assert user.headers.get('Authorization', None) == 'Bearer dummy'
    finally:
        pass
