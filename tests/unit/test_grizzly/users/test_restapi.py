from typing import cast, Dict, Any, Tuple, Callable, Type
from time import time

from locust.clients import ResponseContextManager
from locust.contrib.fasthttp import FastHttpSession, ResponseContextManager as FastResponseContextManager, insecure_ssl_context_factory

import pytest
import gevent

from _pytest.logging import LogCaptureFixture
from requests.models import Response

from grizzly.users.restapi import RestApiUser
from grizzly.users.base import AsyncRequests, RequestLogger, ResponseHandler, GrizzlyUser
from grizzly.types import GrizzlyResponse, RequestMethod, GrizzlyResponseContextManager
from grizzly.types.locust import StopUser
from grizzly.context import GrizzlyContext
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.exceptions import RestartScenario
from grizzly.auth import GrizzlyAuthHttpContext
from grizzly.scenarios import GrizzlyScenario
from grizzly_extras.transformer import TransformerContentType

from tests.fixtures import ResponseContextManagerFixture, MockerFixture, GrizzlyFixture
from tests.helpers import RequestEvent, ResultSuccess


class TestRestApiUser:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser, host='http://example.net')
        assert isinstance(parent.user, RestApiUser)

        assert issubclass(parent.user.__class__, RequestLogger)
        assert issubclass(parent.user.__class__, ResponseHandler)
        assert issubclass(parent.user.__class__, GrizzlyUser)
        assert issubclass(parent.user.__class__, AsyncRequests)
        assert parent.user.host == 'http://example.net'
        assert parent.user._context == {
            'variables': {},
            'log_all_requests': False,
            'verify_certificates': True,
            'auth': {
                'refresh_time': 3000,
                'provider': None,
                'client': {
                    'id': None,
                    'secret': None,
                    'resource': None,
                },
                'user': {
                    'username': None,
                    'password': None,
                    'redirect_uri': None,
                    'initialize_uri': None,
                },
            },
            'metadata': None,
        }
        assert parent.user.headers == {
            'Content-Type': 'application/json',
            'x-grizzly-user': parent.user.__class__.__name__,
        }

        RestApiUser._context['metadata'] = {'foo': 'bar'}

        user = RestApiUser(parent.user.environment)

        assert user.headers.get('foo', None) == 'bar'

    def test_on_start(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)
        assert parent.user.session_started is None

        parent.user.on_start()

        assert parent.user.session_started is not None

    @pytest.mark.skip(reason='needs credentials, should run explicitly manually')
    def test_get_oauth_authorization_real(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        import logging
        with caplog.at_level(logging.DEBUG):
            parent = grizzly_fixture(user_type=RestApiUser)
            assert isinstance(parent.user, RestApiUser)

            parent.user._context = {
                'host': '',
                'auth': {
                    'client': {
                        'id': '',
                    },
                    'user': {
                        'username': '',
                        'password': '',
                        'redirect_uri': '',
                        'response_mode': '',
                    },
                    'provider': '',
                },
                'verify_certificates': False,
                'metadata': {
                    'Ocp-Apim-Subscription-Key': '',
                }
            }
            parent.user.headers.update({
                'Ocp-Apim-Subscription-Key': '',
            })
            parent.user.host = cast(dict, parent.user._context)['host']
            parent.user.session_started = time()

            fire = mocker.spy(parent.user.environment.events.request, 'fire')

            # user.get_oauth_authorization()
            request = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')
            headers, body = parent.user.request(parent, request)
            parent.logger.info(headers)
            parent.logger.info(body)
            parent.logger.info(fire.call_args_list)
            assert 0

    def test_get_error_message(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        response = Response()
        response._content = ''.encode('utf-8')
        response_context_manager = ResponseContextManager(response, RequestEvent(), {})

        response.status_code = 401
        assert parent.user.get_error_message(response_context_manager) == 'unauthorized'

        response.status_code = 403
        assert parent.user.get_error_message(response_context_manager) == 'forbidden'

        response.status_code = 404
        assert parent.user.get_error_message(response_context_manager) == 'not found'

        response.status_code = 405
        assert parent.user.get_error_message(response_context_manager) == 'unknown'

        response._content = 'just a simple string'.encode('utf-8')
        assert parent.user.get_error_message(response_context_manager) == 'just a simple string'

        response._content = '{"Message": "message\\nproperty\\\\nthat is multiline"}'.encode('utf-8')
        assert parent.user.get_error_message(response_context_manager) == 'message property'

        response._content = '{"error_description": "error description\\r\\nthat is multiline"}'.encode('utf-8')
        assert parent.user.get_error_message(response_context_manager) == 'error description'

        response._content = '{"success": false}'.encode('utf-8')
        assert parent.user.get_error_message(response_context_manager) == '{"success": false}'

        text_mock = mocker.patch('requests.models.Response.text', new_callable=mocker.PropertyMock)
        text_mock.return_value = None
        assert parent.user.get_error_message(response_context_manager) == "unknown response <class 'locust.clients.ResponseContextManager'>"

    def test_async_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        request = cast(RequestTask, parent.user._scenario.tasks()[-1])

        request_spy = mocker.patch.object(parent.user, '_request')

        assert parent.user._context.get('verify_certificates', None)

        parent.user.async_request(parent, request)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[-1]
        assert len(args) == 3
        assert args[0] is parent
        assert args[1] is request
        assert isinstance(args[2], FastHttpSession)
        assert args[2].environment is parent.user.environment
        assert args[2].base_url == parent.user.host
        assert args[2].user is parent.user
        assert args[2].client.max_retries == 1
        assert args[2].client.clientpool.client_args.get('connection_timeout', None) == 60.0
        assert args[2].client.clientpool.client_args.get('network_timeout', None) == 60.0
        assert args[2].client.clientpool.client_args.get('ssl_context_factory', None) is gevent.ssl.create_default_context  # pylint: disable=no-member

        parent.user._context['verify_certificates'] = False

        parent.user.async_request(parent, request)

        assert request_spy.call_count == 2
        args, _ = request_spy.call_args_list[-1]
        assert args[2].client.clientpool.client_args.get('ssl_context_factory', None) is insecure_ssl_context_factory

    def test_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        request = cast(RequestTask, parent.user._scenario.tasks()[-1])

        request_spy = mocker.patch.object(parent.user, '_request')

        assert parent.user._context.get('verify_certificates', None)

        parent.user.request(parent, request)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[-1]
        assert len(args) == 3
        assert args[0] is parent
        assert args[1] is request
        assert args[2] is parent.user.client

        parent.user.request(parent, request)

    @pytest.mark.parametrize('request_func', [RestApiUser.request, RestApiUser.async_request])
    def test__request(
        self,
        grizzly_fixture: GrizzlyFixture,
        mocker: MockerFixture,
        request_func: Callable[[RestApiUser, GrizzlyScenario, RequestTask], GrizzlyResponse],
        response_context_manager_fixture: ResponseContextManagerFixture,
    ) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        class ClientRequestMock:
            def __init__(self, status_code: int, user: GrizzlyUser, request_func: Callable[[RestApiUser, GrizzlyScenario, RequestTask], GrizzlyResponse]) -> None:
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

        request = cast(RequestTask, parent.user._scenario.tasks()[-1])

        # missing template variables
        with pytest.raises(StopUser):
            request_func(parent.user, parent, request)

        remote_variables = {
            'variables': transform(GrizzlyContext(), {
                'AtomicIntegerIncrementer.messageID': 1,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        parent.user.add_context(remote_variables)

        request_mock = ClientRequestMock(status_code=400, user=parent.user, request_func=request_func)

        parent.user._scenario.failure_exception = StopUser

        # status_code != 200, stop_on_failure = True
        with pytest.raises(StopUser):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 1

        parent.user._scenario.failure_exception = RestartScenario

        # status_code != 200, stop_on_failure = True
        with pytest.raises(RestartScenario):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 2

        request.response.add_status_code(400)

        with pytest.raises(ResultSuccess):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 3

        request.response.add_status_code(-400)
        parent.user._scenario.failure_exception = None

        # status_code != 200, stop_on_failure = False
        metadata, payload = request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 4
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(parent.user._scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{parent.user.host}{request.endpoint}'

        if request_func is RestApiUser.request:
            assert kwargs.get('request', None) is request
            assert kwargs.get('verify', False)

        assert metadata is None
        assert payload == '{}'

        request_mock = ClientRequestMock(status_code=200, user=parent.user, request_func=request_func)

        parent.user._context['verify_certificates'] = False

        request.response.add_status_code(-200)
        request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(parent.user._scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{parent.user.host}{request.endpoint}'

        if request_func is RestApiUser.request:
            assert kwargs.get('request', None) is request
            assert not kwargs.get('verify', True)

        request.response.add_status_code(200)

        # status_code == 200
        with pytest.raises(ResultSuccess):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 2

        # incorrect formated [json] payload
        request.source = '{"hello: "world"}'

        with pytest.raises(StopUser):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 2

        # unsupported request method
        request.method = RequestMethod.RECEIVE

        with pytest.raises(NotImplementedError):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 2

        # post XML
        parent.user.host = 'http://localhost:1337'
        request.method = RequestMethod.POST
        request.endpoint = '/'
        request.response.content_type = TransformerContentType.XML
        request_mock = ClientRequestMock(status_code=200, user=parent.user, request_func=request_func)
        request.response.add_status_code(200)
        request.source = '<?xml version="1.0"?><example></example'

        with pytest.raises(ResultSuccess):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(parent.user._scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{parent.user.host}{request.endpoint}'
        assert kwargs.get('data', None) == bytes(request.source, 'UTF-8')
        assert 'headers' in kwargs
        assert 'Content-Type' in kwargs['headers']
        assert kwargs['headers']['Content-Type'] == 'application/xml'

        # post multipart
        parent.user.host = 'http://localhost:1337'
        request.method = RequestMethod.POST
        request.endpoint = '/'
        request.arguments = {'multipart_form_data_name': 'input_name', 'multipart_form_data_filename': 'filename'}
        request.response.content_type = TransformerContentType.MULTIPART_FORM_DATA
        request_mock = ClientRequestMock(status_code=200, user=parent.user, request_func=request_func)
        request.response.add_status_code(200)
        request.source = '<?xml version="1.0"?><example></example'

        with pytest.raises(ResultSuccess):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(parent.user._scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{parent.user.host}{request.endpoint}'

        # post with metadata
        parent.user.host = 'http://localhost:1337'
        request.method = RequestMethod.POST
        request.endpoint = '/'
        request.arguments = None
        request.metadata = {'my_header': 'value'}
        request.response.content_type = TransformerContentType.JSON
        request_mock = ClientRequestMock(status_code=200, user=parent.user, request_func=request_func)
        request.response.add_status_code(200)
        request.source = '{"alice": 1}'

        with pytest.raises(ResultSuccess):
            request_func(parent.user, parent, request)

        assert request_mock.spy.call_count == 1
        _, kwargs = request_mock.spy.call_args_list[-1]
        assert kwargs.get('method', None) == request.method.name
        assert kwargs.get('name', '').startswith(parent.user._scenario.identifier)
        assert kwargs.get('catch_response', False)
        assert kwargs.get('url', None) == f'{parent.user.host}{request.endpoint}'
        assert 'headers' in kwargs
        assert 'my_header' in kwargs['headers']
        assert kwargs['headers']['my_header'] == 'value'

    def test_add_context(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)

        assert 'test_context_variable' not in parent.user._context
        assert cast(GrizzlyAuthHttpContext, parent.user._context['auth'])['provider'] is None
        assert cast(GrizzlyAuthHttpContext, parent.user._context['auth'])['refresh_time'] == 3000

        parent.user.add_context({'test_context_variable': 'value'})

        assert 'test_context_variable' in parent.user._context

        parent.user.add_context({'auth': {'provider': 'http://auth.example.org'}})

        assert cast(GrizzlyAuthHttpContext, parent.user._context['auth'])['provider'] == 'http://auth.example.org'
        assert cast(GrizzlyAuthHttpContext, parent.user._context['auth'])['refresh_time'] == 3000

        parent.user.headers['Authorization'] = 'Bearer asdfasdfasdf'

        parent.user.add_context({'auth': {'user': {'username': 'something new'}}})

        assert 'Authorization' not in parent.user.headers
