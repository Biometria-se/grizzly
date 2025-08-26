"""Unit tests for grizzly.users.restapi."""

from __future__ import annotations

import json
from contextlib import suppress
from hashlib import sha256
from time import time
from typing import TYPE_CHECKING, cast

import gevent
import pytest
from geventhttpclient.client import HTTPClientPool
from grizzly.auth.aad import AAD
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.types import GrizzlyResponse, RequestMethod, StrDict
from grizzly.types.locust import StopUser
from grizzly.users import AsyncRequests, GrizzlyUser, RestApiUser
from grizzly_common.azure.aad import AzureAadCredential
from grizzly_common.transformer import TransformerContentType
from locust.contrib.fasthttp import FastHttpSession, LocustUserAgent, insecure_ssl_context_factory
from locust.exception import ResponseError

from test_framework.helpers import ANY, SOME, create_mocked_fast_response_context_manager

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class TestRestApiUser:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser, host='http://example.net')
        assert isinstance(parent.user, RestApiUser)

        assert issubclass(parent.user.__class__, GrizzlyUser)
        assert issubclass(parent.user.__class__, AsyncRequests)
        assert parent.user.host == 'http://example.net'
        assert parent.user._context == {
            'host': 'http://example.net',
            'log_all_requests': False,
            'verify_certificates': True,
            'timeout': 60,
            'auth': {
                'refresh_time': 3000,
                'provider': None,
                'tenant': None,
                'client': {
                    'id': None,
                    'secret': None,
                    'resource': None,
                    'cert_file': None,
                    'key_file': None,
                },
                'user': {
                    'username': None,
                    'password': None,
                    'otp_secret': None,
                    'redirect_uri': None,
                    'initialize_uri': None,
                },
            },
            'metadata': {
                'Content-Type': 'application/json',
                'x-grizzly-user': parent.user.__class__.__name__,
            },
            '__cached_auth__': {},
            '__context_change_history__': set(),
        }
        assert parent.user.metadata == {
            'Content-Type': 'application/json',
            'x-grizzly-user': parent.user.__class__.__name__,
        }

        parent.user.__class__.__context__['metadata'] = {'foo': 'bar'}

        user = parent.user.__class__(parent.user.environment)

        assert user.metadata.get('foo', None) == 'bar'

    @pytest.mark.skip(reason='needs credentials, should run explicitly manually')
    def test_get_oauth_authorization_real(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        import logging

        with caplog.at_level(logging.DEBUG):
            parent = grizzly_fixture(user_type=RestApiUser, host='')
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
                        'initialize_uri': None,
                        'response_mode': None,
                    },
                    'tenant': '',
                },
                'verify_certificates': True,
                'metadata': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0 OS/10.0.19045',
                },
            }
            parent.user.host = cast('dict', parent.user.__context__)['host']
            parent.user.session_started = time()

            fire = mocker.spy(parent.user.environment.events.request, 'fire')

            request = RequestTask(RequestMethod.GET, name='test', endpoint='')
            headers, body = parent.user.request(request)
            parent.logger.info(headers)
            parent.logger.info(body)
            parent.logger.info(fire.call_args_list)
            assert 0  # noqa: PT015

    def test_get_error_message(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        url = 'https://localhost:1234/api/mocked'

        response_context_manager = create_mocked_fast_response_context_manager(content='', status_code=400, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned bad request'

        response_context_manager = create_mocked_fast_response_context_manager(content='', status_code=401, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned unauthorized'

        response_context_manager = create_mocked_fast_response_context_manager(content='', status_code=403, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned forbidden'

        response_context_manager = create_mocked_fast_response_context_manager(content='', status_code=404, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned not found'

        response_context_manager = create_mocked_fast_response_context_manager(content='', status_code=405, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned method not allowed'

        response_context_manager = create_mocked_fast_response_context_manager(content='', status_code=999, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned unknown'

        response_context_manager = create_mocked_fast_response_context_manager(content='just a simple string', status_code=999, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned "just a simple string"'

        response_context_manager = create_mocked_fast_response_context_manager(content='{"Message": "message\\nproperty\\\\nthat is multiline"}', status_code=999, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned "message property"'

        response_context_manager = create_mocked_fast_response_context_manager(
            content='{"error_description": "error description\\r\\nthat is multiline"}', status_code=999, url=url
        )
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned "error description"'

        response_context_manager = create_mocked_fast_response_context_manager(content='{"success": false}', status_code=999, url=url)
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned "{{"success": false}}"'

        response_context_manager = create_mocked_fast_response_context_manager(
            content="""<html>
    <head>
        <title>  what a bummer </title>
    </head>
    <body>
        <h1>meep</h1>
    </body>
</html>""",
            status_code=999,
            url=url,
        )
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned "what a bummer"'

        text_mock = mocker.patch('locust.contrib.fasthttp.FastResponse.text', new_callable=mocker.PropertyMock)
        text_mock.return_value = None
        assert parent.user._get_error_message(response_context_manager) == f'{url} returned an unknown response'

    def test_async_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        request = cast('RequestTask', parent.user._scenario.tasks()[-1])

        request_spy = mocker.patch.object(parent.user, '_request')

        assert parent.user._context.get('verify_certificates', None)

        parent.user.async_request_impl(request)

        request_spy.assert_called_once_with(
            request,
            SOME(
                FastHttpSession,
                request_event=parent.user.environment.events.request,
                base_url=parent.user.host,
                user=parent.user,
                client=SOME(
                    LocustUserAgent,
                    max_retries=1,
                    clientpool=SOME(
                        HTTPClientPool,
                        client_args=SOME(
                            dict,
                            network_timeout=60.0,
                            ssl_context_factory=gevent.ssl.create_default_context,
                        ),
                    ),
                ),
            ),
        )
        request_spy.reset_mock()

        parent.user._context['verify_certificates'] = False

        parent.user.async_request_impl(request)

        request_spy.assert_called_once_with(
            request,
            SOME(
                FastHttpSession,
                request_event=parent.user.environment.events.request,
                base_url=parent.user.host,
                user=parent.user,
                client=SOME(
                    LocustUserAgent,
                    max_retries=1,
                    clientpool=SOME(
                        HTTPClientPool,
                        client_args=SOME(
                            dict,
                            network_timeout=60.0,
                            ssl_context_factory=insecure_ssl_context_factory,
                        ),
                    ),
                ),
            ),
        )

    def test_request_impl(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        request = cast('RequestTask', parent.user._scenario.tasks()[-1])
        request.source = 'hello'

        request_spy = mocker.patch.object(parent.user, '_request')

        assert parent.user._context.get('verify_certificates', None)

        parent.user.request_impl(request)

        request_spy.assert_called_once_with(
            SOME(RequestTask, name=request.name, endpoint=request.endpoint, source=request.source),
            parent.user.client,
        )
        request_spy.reset_mock()

        parent.user.request(request)

        request_spy.assert_called_once_with(
            SOME(RequestTask, name=f'001 {request.name}', endpoint=request.endpoint, source=request.source),
            parent.user.client,
        )
        request_spy.reset_mock()

    @pytest.mark.parametrize('request_func', [RestApiUser.request_impl, RestApiUser.async_request_impl])
    def test__request(  # noqa: PLR0915
        self,
        grizzly_fixture: GrizzlyFixture,
        mocker: MockerFixture,
        request_func: Callable[[RestApiUser, RequestTask], GrizzlyResponse],
    ) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)
        assert isinstance(parent.user, RestApiUser)

        assert parent.user.__class__.__name__ == f'RestApiUser_{parent.user._scenario.identifier}'

        is_async_request = request_func is RestApiUser.async_request_impl

        request_event_spy = mocker.patch.object(parent.user.environment.events.request, 'fire')
        response_magic = mocker.MagicMock()

        request_spy = mocker.patch('locust.contrib.fasthttp.FastHttpSession.request', return_value=response_magic)

        response_spy = response_magic.__enter__.return_value
        response_spy.request_meta = {}

        request = cast('RequestTask', parent.user._scenario.tasks()[-1])
        request.async_request = is_async_request

        remote_variables = {
            'variables': transform(
                grizzly_fixture.grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 1,
                    'AtomicDate.now': '',
                    'messageID': 137,
                },
            ),
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
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(parent.user),
                **parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=ANY(NotImplementedError, message=f'SEND is not implemented for RestApiUser_{parent.user._scenario.identifier}'),
        )
        request_event_spy.reset_mock()

        # request GET, 200
        response_spy._manual_result = None
        response_spy.status_code = 200
        response_spy.text = '{"foo": "bar"}'
        response_spy.headers = {'x-bar': 'foo'}
        request.method = RequestMethod.GET

        assert parent.user.request(request) == ({'x-bar': 'foo'}, '{"foo": "bar"}')

        expected_parameters: StrDict = {
            'headers': request.metadata,
        }

        request_spy.assert_called_once_with(
            method='GET',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            **expected_parameters,
        )

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request GET, 400, StopUser, request.metadata populated
        response_spy._manual_result = None
        response_spy.status_code = 400
        response_spy.request_meta = {'exception': ResponseError('400 not in [200]: bad request')}
        response_spy.text = ''
        request.metadata = {'x-foo': 'bar'}
        expected_parameters['headers'].update({'x-foo': 'bar'})

        parent.user._scenario.failure_handling.update({None: StopUser})

        with pytest.raises(StopUser):
            parent.user.request(request)

        request_spy.assert_called_once_with(
            method='GET',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            **expected_parameters,
        )

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request GET, 404, no failure exception
        response_spy.status_code = 404
        response_spy.text = '{"error_description": "borked"}'
        response_spy.request_meta = {}

        with suppress(KeyError):
            del parent.user._scenario.failure_handling[None]

        request.metadata = {}
        del expected_parameters['headers']['x-foo']

        assert parent.user.request(request) == ({'x-bar': 'foo'}, '{"error_description": "borked"}')

        request_spy.assert_called_once_with(
            method='GET',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            **expected_parameters,
        )

        request_spy.reset_mock()
        response_spy.reset_mock()

        # request POST, 200, json
        request.method = RequestMethod.POST
        response_spy.status_code = 200
        response_spy.text = 'success'

        assert parent.user.request(request) == ({'x-bar': 'foo'}, 'success')

        expected_source = parent.user.render_request(request).source
        assert expected_source is not None
        expected_source_json = json.loads(expected_source)

        expected_parameters.update({'json': expected_source_json})

        request_spy.assert_called_once_with(
            method='POST',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            **expected_parameters,
        )

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request POST, invalid json
        request.source = '{"hello}'

        with suppress(KeyError):
            del parent.user._scenario.failure_handling[None]  # always stop for this error

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
        expected_parameters.update(
            {
                'files': {'foobar': ('foobar.txt', request.source)},
            },
        )

        assert parent.user.request(request) == ({'x-bar': 'foo'}, 'success')

        expected_parameters['headers'].update({'Content-Type': 'multipart/form-data'})

        request_spy.assert_called_once_with(
            method='PUT',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            **expected_parameters,
        )

        response_spy.reset_mock()
        request_spy.reset_mock()

        # request PUT, data
        request.response.content_type = TransformerContentType.XML
        request.source = '<?xml version="1.0" encoding="utf-8"?><hello><foo/></hello>'
        del expected_parameters['files']
        expected_parameters.update({'data': request.source.encode('utf-8')})
        expected_parameters['headers'].update({'Content-Type': 'application/xml'})

        assert parent.user.request(request) == ({'x-bar': 'foo'}, 'success')

        request_spy.assert_called_once_with(
            method='PUT',
            name='001 TestScenario',
            url='http://test/api/test',
            catch_response=True,
            **expected_parameters,
        )

        response_spy.reset_mock()
        request_spy.reset_mock()

    def test_add_context(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)

        parent.user._context['auth']['user'].update({'username': 'bob', 'password': 'foobar'})

        assert 'test_context_variable' not in parent.user._context
        assert parent.user._context['auth']['provider'] is None
        assert parent.user._context['auth']['refresh_time'] == 3000

        parent.user.add_context({'test_context_variable': 'value'})

        assert 'test_context_variable' in parent.user._context

        parent.user.add_context({'auth': {'provider': 'http://auth.example.org'}})

        assert parent.user._context['auth']['provider'] == 'http://auth.example.org'
        assert parent.user._context['auth']['refresh_time'] == 3000

        AAD.initialize(parent.user, parent.user)

        parent.user.add_context({'auth': {'user': {'password': 'other'}}})

        assert parent.user.__context_change_history__ == {'auth.user.password'}

        parent.user.add_context({'auth': {'user': {'username': 'something new'}}})

        assert parent.user.__context_change_history__ == set()
        expected_cache_key = sha256(b'bob:foobar').hexdigest()

        assert parent.user.__cached_auth__ == {expected_cache_key: SOME(AzureAadCredential, username='bob', password='foobar')}

        assert 'Authorization' not in parent.user.metadata
