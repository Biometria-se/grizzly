import logging

from typing import cast, Dict, Any, Optional, Tuple, Callable, Type
from time import time
from enum import Enum
from urllib.parse import urlparse

from locust.clients import ResponseContextManager
from locust.exception import StopUser
from locust.contrib.fasthttp import FastHttpSession, ResponseContextManager as FastResponseContextManager, insecure_ssl_context_factory

import pytest
import requests
import gevent

from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from requests.models import Response
from json import dumps as jsondumps

from grizzly.users.restapi import AuthMethod, RestApiUser, refresh_token
from grizzly.users.base import AsyncRequests, RequestLogger, ResponseHandler, GrizzlyUser
from grizzly.clients import ResponseEventSession
from grizzly.types import GrizzlyResponse, RequestMethod, GrizzlyResponseContextManager
from grizzly.context import GrizzlyContextScenario, GrizzlyContext
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.exceptions import RestartScenario
from grizzly_extras.transformer import TransformerContentType

from ...fixtures import GrizzlyFixture, ResponseContextManagerFixture
from ...helpers import RequestSilentFailureEvent, RequestEvent, ResultSuccess


RestApiScenarioFixture = Tuple[RestApiUser, GrizzlyContextScenario]


@pytest.mark.usefixtures('grizzly_fixture')
@pytest.fixture
def restapi_user(grizzly_fixture: GrizzlyFixture) -> RestApiScenarioFixture:
    scenario = GrizzlyContextScenario(1)
    scenario.name = 'TestScenario'
    scenario.context['host'] = 'test'
    scenario.user.class_name = 'RestApiUser'

    _, user, _ = grizzly_fixture('http://example.net', RestApiUser)

    request = grizzly_fixture.request_task.request

    scenario.tasks.add(request)

    return cast(RestApiUser, user), scenario


def test_refresh_token_client(restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
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
        scenario.tasks.add(request_task)

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
        user.session_started = time()
        user.headers['Authorization'] = 'Bearer asdf'

        with pytest.raises(NotRefreshed):
            user.request(request_task)

        # authorization is set, but it is time to refresh token
        user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        with pytest.raises(Refreshed):
            user.request(request_task)
    finally:
        pass


def test_refresh_token_user(restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
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
        scenario.tasks.add(request_task)

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
        user.session_started = time()
        user.headers['Authorization'] = 'Bearer asdf'

        with pytest.raises(NotRefreshed):
            user.request(request_task)

        # authorization is set, but it is time to refresh token
        user.session_started = time() - (cast(int, auth_context['refresh_time']) + 1)

        with pytest.raises(Refreshed):
            user.request(request_task)

        user.add_context({'auth': {'user': {'username': 'alice@example.com'}}})
        assert user.headers.get('Authorization', '') is None
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
    def test___init__(self, restapi_user: RestApiScenarioFixture) -> None:
        [user, _] = restapi_user
        assert user is not None
        assert isinstance(user, RestApiUser)
        assert issubclass(user.__class__, RequestLogger)
        assert issubclass(user.__class__, ResponseHandler)
        assert issubclass(user.__class__, GrizzlyUser)
        assert issubclass(user.__class__, AsyncRequests)
        assert user.host == 'http://example.net'
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
                },
            },
            'metadata': None,
        }

    def test_on_start(self, restapi_user: RestApiScenarioFixture) -> None:
        [user, _] = restapi_user
        assert user.session_started is None

        user.on_start()

        assert user.session_started is not None

    def test_get_token(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
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

    def test_get_client_token(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
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

        assert user.session_started >= session_started
        assert user.headers['Authorization'] == 'Bearer asdf'
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
    def test_get_user_token_real(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        import logging
        with caplog.at_level(logging.DEBUG):
            [user, scenario] = restapi_user

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
                },
                'verify_certificates': False,
                'metadata': {
                    'Ocp-Apim-Subscription-Key': '',
                }
            }
            user.host = user._context.get('host', '')

            fire = mocker.spy(user.environment.events.request, 'fire')

            user.get_user_token()

            request = RequestTask(RequestMethod.GET, name='test', endpoint='/api/v2/test')
            request.scenario = scenario
            headers, body = user.request(request)
            print(headers)
            print(body)
            print(fire.call_args_list[0])
            assert 0

    def test_get_user_token(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        [user, _] = restapi_user

        fire_spy = mocker.spy(user.environment.events.request, 'fire')

        class Error(Enum):
            REQUEST_1_NO_DOLLAR_CONFIG = 0
            REQUEST_1_HTTP_STATUS = 1
            REQUEST_2_HTTP_STATUS = 2
            REQUEST_3_HTTP_STATUS = 3
            REQUEST_4_HTTP_STATUS = 4
            REQUEST_2_ERROR_MESSAGE = 5
            REQUEST_1_MISSING_STATE = 6
            REQUEST_3_ERROR_MESSAGE = 7
            REQUEST_3_MFA_REQUIRED = 8

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
                    if inject_error == Error.REQUEST_3_ERROR_MESSAGE:
                        dollar_config = jsondumps({
                            'strServiceExceptionMessage': 'failed big time',
                        })
                    elif inject_error == Error.REQUEST_3_MFA_REQUIRED:
                        dollar_config = jsondumps({
                            'arrUserProofs': [{
                                'authMethodId': 'fax',
                                'display': '+46 1234',
                            }]
                        })
                    else:
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

        assert fire_spy.call_count == 1

        user._context['auth']['user']['username'] = 'test-user@'

        with pytest.raises(StopUser):
            user.get_user_token()

        assert fire_spy.call_count == 2

        user._context['auth']['user']['username'] = 'test-user@example.onmicrosoft.com'
        assert user.session_started is None

        # test when login sequence returns bad request
        mock_request_session(Error.REQUEST_1_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        assert fire_spy.call_count == 3

        mock_request_session(Error.REQUEST_2_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        assert fire_spy.call_count == 4

        mock_request_session(Error.REQUEST_3_HTTP_STATUS)

        with pytest.raises(StopUser):
            user.get_user_token()

        assert fire_spy.call_count == 5

        mock_request_session(Error.REQUEST_3_ERROR_MESSAGE)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                user.get_user_token()

        assert fire_spy.call_count == 6
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', '')
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'failed big time'

        assert caplog.messages[-1] == 'failed big time'
        caplog.clear()

        mock_request_session(Error.REQUEST_3_MFA_REQUIRED)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                user.get_user_token()

        expected_error_message = 'test-user@example.onmicrosoft.com requires MFA for login: fax = +46 1234'

        assert fire_spy.call_count == 7
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', '')
        assert isinstance(exception, RuntimeError)
        assert str(exception) == expected_error_message

        assert caplog.messages[-1] == expected_error_message
        caplog.clear()

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
        assert user.headers['Authorization'] == 'Bearer asdf'
        assert user._context['auth']['url'] == 'https://login.microsoftonline.com/example.onmicrosoft.com/oauth2/authorize'

        # test no host in redirect uri
        user._context['auth']['user']['redirect_uri'] = '/authenticated'

        user.get_user_token()

    def test_get_error_message(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
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

        text_mock = mocker.patch('requests.models.Response.text', new_callable=mocker.PropertyMock)
        text_mock.return_value = None
        assert user.get_error_message(response_context_manager) == "unknown response <class 'locust.clients.ResponseContextManager'>"

    def test_async_request(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
        user, scenario = restapi_user
        request = cast(RequestTask, scenario.tasks[-1])

        request_spy = mocker.patch.object(user, '_request')

        assert user._context.get('verify_certificates', None)

        user.async_request(request)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[-1]
        assert len(args) == 2
        assert isinstance(args[0], FastHttpSession)
        assert args[0].environment is user.environment
        assert args[0].base_url == user.host
        assert args[0].user is user
        assert args[0].client.max_retries == 1
        assert args[0].client.clientpool.client_args.get('connection_timeout', None) == 60.0
        assert args[0].client.clientpool.client_args.get('network_timeout', None) == 60.0
        assert args[0].client.clientpool.client_args.get('ssl_context_factory', None) is gevent.ssl.create_default_context  # pylint: disable=no-member
        assert args[1] is request

        user._context['verify_certificates'] = False

        user.async_request(request)

        assert request_spy.call_count == 2
        args, _ = request_spy.call_args_list[-1]
        assert args[0].client.clientpool.client_args.get('ssl_context_factory', None) is insecure_ssl_context_factory

    def test_request(self, restapi_user: RestApiScenarioFixture, mocker: MockerFixture) -> None:
        user, scenario = restapi_user
        request = cast(RequestTask, scenario.tasks[-1])

        request_spy = mocker.patch.object(user, '_request')

        assert user._context.get('verify_certificates', None)

        user.request(request)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[-1]
        assert len(args) == 2
        assert args[0] is user.client
        assert args[1] is request

        user.request(request)

    @pytest.mark.parametrize('request_func', [RestApiUser.request, RestApiUser.async_request])
    def test__request(
        self,
        restapi_user: RestApiScenarioFixture,
        mocker: MockerFixture,
        request_func: Callable[[RestApiUser, RequestTask], GrizzlyResponse],
        response_context_manager_fixture: ResponseContextManagerFixture,
    ) -> None:
        user, scenario = restapi_user

        class ClientRequestMock:
            def __init__(self, status_code: int, user: GrizzlyUser, request_func: Callable[[RestApiUser, RequestTask], GrizzlyResponse]) -> None:
                self.status_code = status_code
                self.user = user
                self.spy = mocker.spy(self, 'request')

                if request_func is RestApiUser.request:
                    namespace = 'grizzly.clients.ResponseEventSession.request'
                else:
                    namespace = 'grizzly.users.restapi.FastHttpSession.request'

                mocker.patch(namespace, self.request)

            def request(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> GrizzlyResponseContextManager:
                cls_rcm = cast(Type[GrizzlyResponseContextManager], ResponseContextManager if request_func is RestApiUser.request else FastResponseContextManager)
                return response_context_manager_fixture(cls_rcm, self.status_code, self.user.environment, response_body={}, **kwargs)  # type: ignore

        request = cast(RequestTask, scenario.tasks[-1])

        # missing template variables
        with pytest.raises(StopUser):
            request_func(user, request)

        remote_variables = {
            'variables': transform(GrizzlyContext(), {
                'AtomicIntegerIncrementer.messageID': 1,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        user.add_context(remote_variables)

        request_mock = ClientRequestMock(status_code=400, user=user, request_func=request_func)

        scenario.failure_exception = StopUser

        # status_code != 200, stop_on_failure = True
        with pytest.raises(StopUser):
            request_func(user, request)

        assert request_mock.spy.call_count == 1

        scenario.failure_exception = RestartScenario

        # status_code != 200, stop_on_failure = True
        with pytest.raises(RestartScenario):
            request_func(user, request)

        assert request_mock.spy.call_count == 2

        request.response.add_status_code(400)

        with pytest.raises(ResultSuccess):
            request_func(user, request)

        assert request_mock.spy.call_count == 3

        request.response.add_status_code(-400)
        scenario.failure_exception = None

        # status_code != 200, stop_on_failure = False
        metadata, payload = request_func(user, request)

        assert request_mock.spy.call_count == 4
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(request.scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{user.host}{request.endpoint}'

        if request_func is RestApiUser.request:
            assert kwargs.get('request', None) is request
            assert kwargs.get('verify', False)

        assert metadata is None
        assert payload == '{}'

        request_mock = ClientRequestMock(status_code=200, user=user, request_func=request_func)

        user._context['verify_certificates'] = False

        request.response.add_status_code(-200)
        request_func(user, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(request.scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{user.host}{request.endpoint}'

        if request_func is RestApiUser.request:
            assert kwargs.get('request', None) is request
            assert not kwargs.get('verify', True)

        request.response.add_status_code(200)

        # status_code == 200
        with pytest.raises(ResultSuccess):
            request_func(user, request)

        assert request_mock.spy.call_count == 2

        # incorrect formated [json] payload
        request.source = '{"hello: "world"}'

        with pytest.raises(StopUser):
            request_func(user, request)

        assert request_mock.spy.call_count == 2

        # unsupported request method
        request.method = RequestMethod.RECEIVE

        with pytest.raises(NotImplementedError):
            request_func(user, request)

        assert request_mock.spy.call_count == 2

        # post XML
        user.host = 'http://localhost:1337'
        request.method = RequestMethod.POST
        request.endpoint = '/'
        request.response.content_type = TransformerContentType.XML
        request_mock = ClientRequestMock(status_code=200, user=user, request_func=request_func)
        request.response.add_status_code(200)
        request.source = '<?xml version="1.0"?><example></example'

        with pytest.raises(ResultSuccess):
            request_func(user, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(request.scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{user.host}{request.endpoint}'
        assert kwargs.get('data', None) == bytes(request.source, 'UTF-8')
        assert 'headers' in kwargs
        assert 'Content-Type' in kwargs['headers']
        assert kwargs['headers']['Content-Type'] == 'application/xml'

        # post multipart
        user.host = 'http://localhost:1337'
        request.method = RequestMethod.POST
        request.endpoint = '/'
        request.arguments = {'multipart_form_data_name': 'input_name', 'multipart_form_data_filename': 'filename'}
        request.response.content_type = TransformerContentType.MULTIPART_FORM_DATA
        request_mock = ClientRequestMock(status_code=200, user=user, request_func=request_func)
        request.response.add_status_code(200)
        request.source = '<?xml version="1.0"?><example></example'

        with pytest.raises(ResultSuccess):
            request_func(user, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(request.scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{user.host}{request.endpoint}'

        # post with metadata
        user.host = 'http://localhost:1337'
        request.method = RequestMethod.POST
        request.endpoint = '/'
        request.arguments = None
        request.metadata = {'my_header': 'value'}
        request.response.content_type = TransformerContentType.JSON
        request_mock = ClientRequestMock(status_code=200, user=user, request_func=request_func)
        request.response.add_status_code(200)
        request.source = '{"alice": 1}'

        with pytest.raises(ResultSuccess):
            request_func(user, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(request.scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{user.host}{request.endpoint}'
        assert 'headers' in kwargs
        assert 'my_header' in kwargs['headers']
        assert kwargs['headers']['my_header'] == 'value'

    def test_add_context(self, restapi_user: RestApiScenarioFixture) -> None:
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
