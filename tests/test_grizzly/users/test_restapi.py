from typing import Callable, cast, Dict, Any, Optional, Tuple
from time import time
from enum import Enum
from urllib.parse import urlparse

import pytest
import requests

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from requests.models import Response
from json import dumps as jsondumps
from locust.clients import ResponseContextManager
from locust.exception import StopUser
from locust.event import EventHook
from jinja2 import Template

from grizzly.users.restapi import AuthMethod, RestApiUser, refresh_token
from grizzly.users.meta import RequestLogger, ResponseHandler, ContextVariables
from grizzly.clients import ResponseEventSession
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.task import RequestTask
from grizzly.utils import transform

from ..fixtures import grizzly_context, request_task  # pylint: disable=unused-import
from ..helpers import RequestSilentFailureEvent, RequestEvent, ResultSuccess

import logging

# we are not interested in misleading log messages when unit testing
logging.getLogger().setLevel(logging.CRITICAL)

@pytest.fixture
def restapi_user(grizzly_context: Callable) -> Tuple[RestApiUser, GrizzlyContextScenario]:
    scenario = GrizzlyContextScenario()
    scenario.name = 'TestScenario'
    scenario.context['host'] = 'test'
    scenario.user_class_name = 'RestApiUser'

    _, user, _, [_, _, request] = grizzly_context('http://test.ie', RestApiUser)

    scenario.add_task(request)

    return cast(RestApiUser, user), scenario


def test_refresh_token_client(restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
    [user, scenario] = restapi_user
    decorator = refresh_token()

    auth_context = user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    class NotRefreshed(Exception):
        pass

    class Refreshed(Exception):
        pass

    def request(self: 'RestApiUser', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        raise NotRefreshed()

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    def get_token(self: 'RestApiUser', auth_method: AuthMethod) -> None:
        if auth_method == AuthMethod.CLIENT:
            raise Refreshed()

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.get_token',
        get_token,
    )

    try:
        auth_client_context = auth_context['client']
        request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        scenario.tasks.clear()
        scenario.add_task(request_task)

        # no authentication for api, request will be called which raises NotRefreshed
        assert user._context['auth']['url'] is None
        assert auth_client_context['id'] is None
        assert auth_client_context['secret'] is None
        assert auth_client_context['tenant'] is None
        assert auth_client_context['resource'] is None

        with pytest.raises(NotRefreshed):
            user.request(request_task)

        auth_client_context['id'] = 'asdf'
        auth_client_context['secret'] = 'asdf'
        auth_client_context['tenant'] = 'test.onmicrosoft.com'
        auth_context['refresh_time'] = 3000

        # session has not started
        with pytest.raises(NotRefreshed):
            user.request(request_task)

        user.session_started = time()
        user.headers['Authorization'] = None

        # session is fresh, but no token set (first call)
        with pytest.raises(Refreshed):
            user.request(request_task)

        # token is fresh and set, no refresh
        user.session_stated = time()
        user.headers['Authorization'] = f'Bearer asdf'

        with pytest.raises(NotRefreshed):
            user.request(request_task)

        # authorization is set, but it is time to refresh token
        user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        with pytest.raises(Refreshed):
            user.request(request_task)
    finally:
        pass


def test_refresh_token_user(restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
    [user, scenario] = restapi_user
    decorator = refresh_token()

    auth_context = user.context()['auth']

    assert auth_context['refresh_time'] == 3000

    class NotRefreshed(Exception):
        pass

    class Refreshed(Exception):
        pass

    def request(self: 'RestApiUser', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        raise NotRefreshed()

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.request',
        decorator(request),
    )

    def get_token(self: 'RestApiUser', auth_method: AuthMethod) -> None:
        if auth_method == AuthMethod.USER:
            raise Refreshed()

    mocker.patch(
        'grizzly.users.restapi.RestApiUser.get_token',
        get_token,
    )

    try:
        auth_user_context = auth_context['user']
        request_task = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        scenario.tasks.clear()
        scenario.add_task(request_task)

        # no authentication for api, request will be called which raises NotRefreshed
        assert auth_user_context['username'] is None
        assert auth_user_context['password'] is None
        assert auth_user_context['redirect_uri'] is None
        assert auth_context['url'] is None

        with pytest.raises(NotRefreshed):
            user.request(request_task)

        auth_context['client']['id'] = 'asdf'
        auth_user_context['username'] = 'bob@example.com'
        auth_user_context['password'] = 'HemligaArne'
        auth_user_context['redirect_uri'] = '/authenticated'
        auth_context['refresh_time'] = 3000

        # session has not started
        with pytest.raises(NotRefreshed):
            user.request(request_task)

        user.session_started = time()
        user.headers['Authorization'] = None

        # session is fresh, but no token set (first call)
        with pytest.raises(Refreshed):
            user.request(request_task)

        # token is fresh and set, no refresh
        user.session_stated = time()
        user.headers['Authorization'] = f'Bearer asdf'

        with pytest.raises(NotRefreshed):
            user.request(request_task)

        # authorization is set, but it is time to refresh token
        user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        with pytest.raises(Refreshed):
            user.request(request_task)

        user.add_context({'auth': {'user': {'username': 'alice@example.com'}}})
        assert user.headers['Authorization'] == None
        assert user._context['auth']['user'] == {
            'username': 'alice@example.com',
            'password': 'HemligaArne',
            'redirect_uri': '/authenticated',
        }

        # new user in context, needs to get a new token
        with pytest.raises(Refreshed):
            user.request(request_task)
    finally:
        pass


class TestRestApiUser:
    def test_create(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario]) -> None:
        [user, _] = restapi_user
        assert user is not None
        assert isinstance(user, RestApiUser)
        assert issubclass(user.__class__, RequestLogger)
        assert issubclass(user.__class__, ResponseHandler)
        assert issubclass(user.__class__, ContextVariables)
        assert user.host == 'http://test.ie'
        assert user._context == {
            'variables': {},
            'log_all_requests': False,
            'verify_certificates': True,
            'auth': {
                'refresh_time': 3000,
                'url': None,
                'client': {
                    'id': None,
                    'secret': None,
                    'resource': None,
                    'tenant': None,
                },
                'user': {
                    'username': None,
                    'password': None,
                    'redirect_uri': None,
                }
            }
        }

    def test_on_start(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario]) -> None:
        [user, _] = restapi_user
        assert user.session_started is None

        user.on_start()

        assert user.session_started is not None

    def test_get_token(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
        [user, _] = restapi_user
        class Called(Exception):
            pass

        def mocked_get_client_token(i: RestApiUser) -> None:
            raise Called(AuthMethod.CLIENT)

        mocker.patch(
            'grizzly.users.restapi.RestApiUser.get_client_token',
            mocked_get_client_token,
        )

        def mocked_get_user_token(i: RestApiUser) -> None:
            raise Called(AuthMethod.USER)

        mocker.patch(
            'grizzly.users.restapi.RestApiUser.get_user_token',
            mocked_get_user_token,
        )

        with pytest.raises(Called) as e:
            user.get_token(AuthMethod.CLIENT)
        assert 'CLIENT' in str(e)

        with pytest.raises(Called) as e:
            user.get_token(AuthMethod.USER)
        assert 'USER' in str(e)

        user.get_token(AuthMethod.NONE)

    def test_get_client_token(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
        [user, _] = restapi_user

        def mock_client_post(payload: Dict[str, Any], status_code: int = 200) -> None:
            def client_post(self: ResponseEventSession, method: str, url: str, name: Optional[str] = None, **kwargs: Dict[str, Any]) -> ResponseContextManager:
                response = Response()
                response._content = jsondumps(payload).encode('utf-8')
                response.status_code = status_code
                response_context_manager = ResponseContextManager(response, RequestSilentFailureEvent(False), {})
                response_context_manager.request_meta = {
                    'method': None,
                    'name': name,
                    'response_time': 1.0,
                    'content_size': 1337,
                    'exception': None,
                }

                return response_context_manager

            mocker.patch(
                'grizzly.clients.ResponseEventSession.request',
                client_post,
            )

        user._context['auth']['client'] = {
            'id': 'asdf',
            'secret': 'asdf',
            'resource': 'asdf',
            'tenant': 'test',
        }

        session_started = time()

        assert user.session_started is None

        mock_client_post({'error_description': 'fake error message'}, 400)

        with pytest.raises(StopUser):
            user.get_client_token()

        assert user.session_started is None

        user.session_started = session_started

        mock_client_post({'access_token': 'asdf'}, 200)

        assert user.headers['Authorization'] is None

        with pytest.raises(ResultSuccess):
            user.get_client_token()

        assert user.session_started > session_started
        assert user.headers['Authorization'] == f'Bearer asdf'
        assert user._context['auth']['url'] == 'https://login.microsoftonline.com/test/oauth2/token'

        # no tenant set
        del user._context['auth']['url']
        del user._context['auth']['client']['tenant']

        print(user._context)

        with pytest.raises(ValueError) as ve:
            user.get_client_token()
        assert 'auth.client.tenant and auth.url is not set' in str(ve)

        user._context['auth']['client']['tenant'] = None
        user._context['auth']['url'] = None

        with pytest.raises(ValueError) as ve:
            user.get_client_token()
        assert 'auth.client.tenant and auth.url is not set' in str(ve)


    @pytest.mark.skip(reason='needs credentials, should run explicitly manually')
    def test_get_user_token_real(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
        [user, _] = restapi_user

        user._context = {
            'host': 'https://backend.example.com',
            'auth': {
                'client': {
                    'id': '',
                },
                'user': {
                    'username': '',
                    'password': '',
                    'redirect_uri': 'https://www.example.com/silent',
                },
                'url': None,
            }
        }

        user.get_user_token()

    def test_get_user_token(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
        [user, _] = restapi_user

        def fire(self: EventHook, *, reverse: bool = False, **kwargs: Dict[str, Any]) -> None:
            pass

        mocker.patch(
            'locust.event.EventHook.fire',
            fire,
        )

        class Error(Enum):
            REQUEST_1_NO_DOLLAR_CONFIG = 0
            REQUEST_1_HTTP_STATUS = 1
            REQUEST_2_HTTP_STATUS = 2
            REQUEST_3_HTTP_STATUS = 3
            REQUEST_4_HTTP_STATUS = 4
            REQUEST_2_ERROR_MESSAGE = 5
            REQUEST_1_MISSING_STATE = 6

        def mock_request_session(inject_error: Optional[Error] = None) -> None:
            def request(self: 'requests.Session', method: str, url: str, name: Optional[str] = None, **kwargs: Dict[str, Any]) -> requests.Response:
                response = Response()
                response.status_code = 200
                response.url = url

                if method == 'GET' and url.endswith('/oauth2/authorize'):
                    if inject_error == Error.REQUEST_1_NO_DOLLAR_CONFIG:
                        response._content = ''.encode('utf-8')
                    else:
                        dollar_config_raw = {
                            'hpgact': 1800,
                            'hpgid': 11,
                            'sFT': 'xxxxxxxxxxxxxxxxxxx',
                            'sCtx': 'yyyyyyyyyyyyyyyyyyy',
                            'apiCanary': 'zzzzzzzzzzzzzzzzzz',
                            'canary': 'canary=1:1',
                            'correlationId': 'aa-bb-cc',
                            'sessionId': 'session-a-b-c',
                            'country': 'SE',
                            'urlGetCredentialType': 'https://test.nu/GetCredentialType?mkt=en-US',
                            'urlPost': 'https://test.nu/login',
                        }
                        if inject_error == Error.REQUEST_1_MISSING_STATE:
                            del dollar_config_raw['hpgact']

                        dollar_config = jsondumps(dollar_config_raw)
                        response._content = f'$Config={dollar_config};'.encode('utf-8')

                    if inject_error == Error.REQUEST_1_HTTP_STATUS:
                        response.status_code = 400

                    response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'
                elif method == 'POST' and url.endswith('/GetCredentialType'):
                    data: Dict[str, Any] = {
                        'FlowToken': 'xxxxxxxxxxxxxxxxxxx',
                        'apiCanary': 'zzzzzzzzzzzz',
                    }

                    if inject_error == Error.REQUEST_2_ERROR_MESSAGE:
                        data = {
                            'error': 'an error message'
                        }

                    payload = jsondumps(data)
                    response._content = payload.encode('utf-8')
                    response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'

                    if inject_error == Error.REQUEST_2_HTTP_STATUS:
                        response.status_code = 400
                elif method == 'POST' and url.endswith('/login'):
                    dollar_config = jsondumps({
                        'hpgact': 1800,
                        'hpgid': 11,
                        'sFT': 'xxxxxxxxxxxxxxxxxxx',
                        'sCtx': 'yyyyyyyyyyyyyyyyyyy',
                        'apiCanary': 'zzzzzzzzzzzzzzzzzz',
                        'canary': 'canary=1:1',
                        'correlationId': 'aa-bb-cc',
                        'sessionId': 'session-a-b-c',
                        'country': 'SE',
                        'urlGetCredentialType': 'https://test.nu/GetCredentialType?mkt=en-US',
                        'urlPost': '/kmsi',
                    })
                    response._content = f'$Config={dollar_config};'.encode('utf-8')
                    response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'
                    if inject_error == Error.REQUEST_3_HTTP_STATUS:
                        response.status_code = 400
                elif method == 'POST' and url.endswith('/kmsi'):
                    if inject_error == Error.REQUEST_4_HTTP_STATUS:
                        response.status_code = 200
                    else:
                        response.status_code = 302
                    auth_user_context = user._context['auth']['user']
                    redirect_uri_parsed = urlparse(auth_user_context['redirect_uri'])
                    if len(redirect_uri_parsed.netloc) == 0:
                        redirect_uri = f"{user._context['host']}{auth_user_context['redirect_uri']}"
                    else:
                        redirect_uri = auth_user_context['redirect_uri']
                    response.headers['Location'] = f'{redirect_uri}#id_token=asdf'
                else:
                    response._content = jsondumps({'error_description': 'error'}).encode('utf-8')

                return response

            mocker.patch(
                'requests.Session.request',
                request,
            )


        session_started = time()

        user._context = {
            'host': 'https://backend.example.com',
            'auth': {
                'client': {
                    'id': 'aaaa',
                },
                'user': {
                    'username': 'test-user',
                    'password': 'H3ml1g4Arn3',
                    'redirect_uri': 'http://www.example.com/authenticated',
                },
                'url': None,
            }
        }

        mock_request_session()

        # test when auth.user.username doesn't contain a valid tenant
        with pytest.raises(StopUser):
            user.get_user_token()

        user._context['auth']['user']['username'] = 'test-user@'

        with pytest.raises(StopUser):
            user.get_user_token()

        user._context['auth']['user']['username'] = 'test-user@example.onmicrosoft.com'
        assert user.session_started is None

        # test when login sequence returns bad request
        mock_request_session(Error.REQUEST_1_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        mock_request_session(Error.REQUEST_2_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        mock_request_session(Error.REQUEST_3_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        mock_request_session(Error.REQUEST_4_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        assert user.session_started is None

        # test error handling when login sequence response doesn't contain expected payload
        mock_request_session(Error.REQUEST_1_NO_DOLLAR_CONFIG)
        with pytest.raises(StopUser):
            user.get_user_token()

        mock_request_session(Error.REQUEST_1_MISSING_STATE)
        with pytest.raises(StopUser):
            user.get_user_token()


        mock_request_session(Error.REQUEST_2_ERROR_MESSAGE)
        with pytest.raises(StopUser):
            user.get_user_token()


        # successful login sequence
        mock_request_session()

        user.session_started = session_started
        assert user.headers['Authorization'] is None

        user.get_user_token()

        assert user.session_started > session_started
        assert user.headers['Authorization'] == f'Bearer asdf'
        assert user._context['auth']['url'] == 'https://login.microsoftonline.com/example.onmicrosoft.com/oauth2/authorize'

        # test no host in redirect uri
        user._context['auth']['user']['redirect_uri'] = '/authenticated'

        user.get_user_token()


    def test_get_error_message(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario]) -> None:
        user, _ = restapi_user

        response = Response()
        response._content = ''.encode('utf-8')
        response_context_manager = ResponseContextManager(response, RequestEvent(), {})

        response.status_code = 401
        assert user.get_error_message(response_context_manager) == 'unauthorized'

        response.status_code = 403
        assert user.get_error_message(response_context_manager) == 'forbidden'

        response.status_code = 404
        assert user.get_error_message(response_context_manager) == 'not found'

        response.status_code = 405
        assert user.get_error_message(response_context_manager) == 'unknown'

        response._content = 'just a simple string'.encode('utf-8')
        assert user.get_error_message(response_context_manager) == 'just a simple string'

        response._content = '{"Message": "message\\nproperty\\\\nthat is multiline"}'.encode('utf-8')
        assert user.get_error_message(response_context_manager) == 'message property'

        response._content = '{"error_description": "error description\\r\\nthat is multiline"}'.encode('utf-8')
        assert user.get_error_message(response_context_manager) == 'error description'

        response._content = '{"success": false}'.encode('utf-8')
        assert user.get_error_message(response_context_manager) == '{"success": false}'

    def test_request(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario], mocker: MockerFixture) -> None:
        [user, scenario] = restapi_user

        def mock_client_post(status_code: int) -> None:
            def client_post(self: ResponseEventSession, method: str, url: str, name: str, catch_response: bool, **kwargs: Dict[str, Any]) -> ResponseContextManager:
                response = Response()
                response._content = jsondumps({}).encode('utf-8')
                response.status_code = status_code
                response_context_manager = ResponseContextManager(response, RequestSilentFailureEvent(False), {})
                response_context_manager.request_meta = {
                    'method': None,
                    'name': name,
                    'response_time': 1.0,
                    'content_size': 1337,
                    'exception': None,
                }

                return response_context_manager

            mocker.patch(
                'grizzly.clients.ResponseEventSession.request',
                client_post,
            )

        request = cast(RequestTask, scenario.tasks[-1])

        # missing template variables
        with pytest.raises(StopUser):
            user.request(request)

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 1,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        user.add_context(remote_variables)

        mock_client_post(status_code=400)

        scenario.stop_on_failure = True

        # status_code != 200, stop_on_failure = True
        with pytest.raises(StopUser):
            user.request(request)

        request.response.add_status_code(400)

        with pytest.raises(ResultSuccess):
            user.request(request)

        request.response.add_status_code(-400)
        scenario.stop_on_failure = False

        # status_code != 200, stop_on_failure = False
        user.request(request)

        mock_client_post(status_code=200)

        request.response.add_status_code(-200)
        user.request(request)

        request.response.add_status_code(200)

        # status_code == 200
        with pytest.raises(ResultSuccess):
            user.request(request)

        # incorrect formated [json] payload
        request.source = '{"hello: "world"}'
        request.template = Template(request.source)

        with pytest.raises(StopUser):
            user.request(request)

        # unsupported request method
        request.method = RequestMethod.RECEIVE

        with pytest.raises(NotImplementedError):
            user.request(request)

    def test_add_context(self, restapi_user: Tuple[RestApiUser, GrizzlyContextScenario]) -> None:
        user, _ = restapi_user

        assert 'test_context_variable' not in user._context
        assert user._context['auth']['url'] is None
        assert user._context['auth']['refresh_time'] == 3000

        user.add_context({'test_context_variable': 'value'})

        assert 'test_context_variable' in user._context

        user.add_context({'auth': {'url': 'http://auth.example.org'}})

        assert user._context['auth']['url'] == 'http://auth.example.org'
        assert user._context['auth']['refresh_time'] == 3000

        user.headers['Authorization'] = 'Bearer asdfasdfasdf'

        user.add_context({'auth': {'user': {'username': 'something new'}}})

        assert user.headers['Authorization'] is None
