import json

from typing import cast, Dict, Any, Callable
from time import time
from unittest.mock import ANY

from locust.clients import ResponseContextManager
from locust.contrib.fasthttp import FastHttpSession, insecure_ssl_context_factory

import pytest
import gevent

from _pytest.logging import LogCaptureFixture
from requests.models import Response

from grizzly.users.restapi import RestApiUser
from grizzly.users.base import AsyncRequests, RequestLogger, ResponseHandler, GrizzlyUser
from grizzly.types import GrizzlyResponse, RequestMethod
from grizzly.types.locust import StopUser
from grizzly.context import GrizzlyContext
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly_extras.transformer import TransformerContentType

from tests.fixtures import MockerFixture, GrizzlyFixture
from tests.helpers import RequestEvent


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
                    'otp_secret': None,
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
                        'otp_secret': None,
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
            headers, body = parent.user.request(request)
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

        response.status_code = 400
        assert parent.user.get_error_message(response_context_manager) == 'bad request'

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

        parent.user.async_request_impl(request)

        assert request_spy.call_count == 1
        args, kwargs = request_spy.call_args_list[-1]
        assert kwargs == {}
        assert len(args) == 2
        assert args[0] is request
        assert isinstance(args[1], FastHttpSession)
        assert args[1].environment is parent.user.environment
        assert args[1].base_url == parent.user.host
        assert args[1].user is parent.user
        assert args[1].client.max_retries == 1
        assert args[1].client.clientpool.client_args.get('connection_timeout', None) == 60.0
        assert args[1].client.clientpool.client_args.get('network_timeout', None) == 60.0
        assert args[1].client.clientpool.client_args.get('ssl_context_factory', None) is gevent.ssl.create_default_context  # pylint: disable=no-member

        parent.user._context['verify_certificates'] = False

        parent.user.async_request_impl(request)

        assert request_spy.call_count == 2
        args, kwargs = request_spy.call_args_list[-1]
        assert kwargs == {}
        assert args[1].client.clientpool.client_args.get('ssl_context_factory', None) is insecure_ssl_context_factory

    def test_request_impl(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        request = cast(RequestTask, parent.user._scenario.tasks()[-1])
        request.source = 'hello'

        request_spy = mocker.patch.object(parent.user, '_request')

        assert parent.user._context.get('verify_certificates', None)

        parent.user.request_impl(request)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[-1]
        assert len(args) == 2
        assert args[0] is request
        assert args[1] is parent.user.client

        parent.user.request(request)

    @pytest.mark.parametrize('request_func', [RestApiUser.request_impl, RestApiUser.async_request_impl])
    def test__request(
        self,
        grizzly_fixture: GrizzlyFixture,
        mocker: MockerFixture,
        request_func: Callable[[RestApiUser, RequestTask], GrizzlyResponse],
    ) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        assert parent.user.__class__.__name__ == 'RestApiUser'

        is_async_request = request_func is RestApiUser.async_request_impl

        request_event_spy = mocker.patch.object(parent.user.environment.events.request, 'fire')
        response_magic = mocker.MagicMock()

        if is_async_request:
            request_spy = mocker.patch('locust.contrib.fasthttp.FastHttpSession.request', return_value=response_magic)
        else:
            request_spy = mocker.patch.object(parent.user.client, 'request', return_value=response_magic)

        response_spy = response_magic.__enter__.return_value
        response_spy.request_meta = {}

        request = cast(RequestTask, parent.user._scenario.tasks()[-1])
        request.async_request = is_async_request

        remote_variables = {
            'variables': transform(GrizzlyContext(), {
                'AtomicIntegerIncrementer.messageID': 1,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        parent.user.add_context(remote_variables)
        parent.user.host = 'http://test'

        # incorrect method
        request.method = RequestMethod.SEND

        with pytest.raises(StopUser):
            parent.user.request(request)

        request_spy.assert_not_called()
        request_event_spy.assert_called_once_with(
            request_type='SEND',
            name='001 TestScenario',
            response_time=ANY,
            response_length=0,
            context=parent.user._context,
            exception=ANY,
        )

        _, kwargs = request_event_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert str(exception) == 'SEND is not implemented for RestApiUser'

        request_event_spy.reset_mock()

        # request GET, 200
        response_spy._manual_result = None
        response_spy.status_code = 200
        response_spy.text = '{"foo": "bar"}'
        response_spy.headers = {'x-bar': 'foo'}
        request.method = RequestMethod.GET

        assert parent.user.request(request) == ({'x-bar': 'foo'}, '{"foo": "bar"}')

        expected_parameters: Dict[str, Any] = {
            'headers': parent.user.headers,
        }

        if not is_async_request:
            expected_parameters.update({
                'request': ANY,
                'verify': parent.user._context.get('verify_certificates', True),
            })

        request_spy.assert_called_once_with(
            method='GET',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            cookies=parent.user.cookies,
            **expected_parameters,
        )

        response_spy.success.assert_called_once_with()

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request GET, 400, StopUser, request.metadata populated
        response_spy._manual_result = None
        response_spy.status_code = 400
        response_spy.text = ''
        request.metadata = {'x-foo': 'bar'}
        expected_parameters['headers'].update({'x-foo': 'bar'})

        parent.user._scenario.failure_exception = StopUser

        with pytest.raises(StopUser):
            parent.user.request(request)

        request_spy.assert_called_once_with(
            method='GET',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            cookies=parent.user.cookies,
            **expected_parameters,
        )

        response_spy.success.assert_not_called()
        response_spy.failure.assert_called_once_with(
            '400 not in [200]: bad request'
        )

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request GET, 404, no failure exception
        response_spy.status_code = 404
        response_spy.text = '{"error_description": "borked"}'
        parent.user._scenario.failure_exception = None
        request.metadata = None

        assert parent.user.request(request) == ({'x-bar': 'foo'}, '{"error_description": "borked"}')

        request_spy.assert_called_once_with(
            method='GET',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            cookies=parent.user.cookies,
            **expected_parameters,
        )

        response_spy.success.assert_not_called()
        response_spy.failure.assert_called_once_with(
            '404 not in [200]: borked'
        )

        request_spy.reset_mock()
        response_spy.reset_mock()

        # request POST, 200, json
        request.method = RequestMethod.POST
        response_spy.status_code = 200
        response_spy.text = 'success'

        assert parent.user.request(request) == ({'x-bar': 'foo'}, 'success')

        expected_source = parent.user.render(request).source
        assert expected_source is not None
        expected_source_json = json.loads(expected_source)

        # this is done automagically for requests, but not for grequests
        if not is_async_request:
            response_spy.request_body = expected_source

        assert json.loads(response_spy.request_body) == expected_source_json
        expected_parameters.update({'json': expected_source_json})

        request_spy.assert_called_once_with(
            method='POST',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            cookies=parent.user.cookies,
            **expected_parameters,
        )

        response_spy.success.assert_called_once_with()
        response_spy.failure.assert_not_called()

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request POST, invalid json
        request.source = '{"hello}'
        parent.user._scenario.failure_exception = None  # always stop for this error

        with pytest.raises(StopUser):
            parent.user.request(request)

        request_spy.assert_not_called()
        response_spy.assert_not_called()

        # request PUT, 200, multipart form data
        request.method = RequestMethod.PUT
        del expected_parameters['json']
        request.arguments = {
            'multipart_form_data_name': 'foobar',
            'multipart_form_data_filename': 'foobar.txt',
        }
        request.source = 'foobar'
        request.response.content_type = TransformerContentType.MULTIPART_FORM_DATA
        expected_parameters.update({
            'files': {'foobar': ('foobar.txt', request.source,)}
        })

        assert parent.user.request(request) == ({'x-bar': 'foo'}, 'success')

        request_spy.assert_called_once_with(
            method='PUT',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            cookies=parent.user.cookies,
            **expected_parameters,
        )

        response_spy.success.assert_called_once_with()
        response_spy.failure.assert_not_called()

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request PUT, data
        request.response.content_type = TransformerContentType.XML
        request.source = '<?xml version="1.0" encoding="utf-8"?><hello><foo/></hello>'
        del expected_parameters['files']
        expected_parameters.update({'data': request.source.encode('utf-8')})

        assert parent.user.request(request) == ({'x-bar': 'foo'}, 'success')

        request_spy.assert_called_once_with(
            method='PUT',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            cookies=parent.user.cookies,
            **expected_parameters,
        )

        response_spy.success.assert_called_once_with()
        response_spy.failure.assert_not_called()

        response_spy.reset_mock()
        request_spy.reset_mock()

    def test_add_context(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)

        assert 'test_context_variable' not in parent.user._context
        assert parent.user._context['auth']['provider'] is None
        assert parent.user._context['auth']['refresh_time'] == 3000

        parent.user.add_context({'test_context_variable': 'value'})

        assert 'test_context_variable' in parent.user._context

        parent.user.add_context({'auth': {'provider': 'http://auth.example.org'}})

        assert parent.user._context['auth']['provider'] == 'http://auth.example.org'
        assert parent.user._context['auth']['refresh_time'] == 3000

        parent.user.headers['Authorization'] = 'Bearer asdfasdfasdf'

        parent.user.add_context({'auth': {'user': {'username': 'something new'}}})

        assert 'Authorization' not in parent.user.headers
