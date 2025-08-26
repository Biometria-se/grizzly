"""Unit tests of grizzly.tasks.clients.http."""

from __future__ import annotations

import logging
from base64 import b64encode
from contextlib import suppress
from itertools import cycle
from json import dumps as jsondumps
from json import loads as jsonloads
from os import environ
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.exceptions import RestartScenario
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestDirection
from grizzly.types.locust import ResponseError, StopUser
from grizzly_common.transformer import TransformerContentType
from requests import Response
from requests.structures import CaseInsensitiveDict

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class TestHttpClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenario.variables.update({'test_payload': 'none', 'test_metadata': 'none'})

        HttpClientTask.__scenario__ = grizzly.scenario
        with pytest.raises(ValueError, match='either .* or .* does not exist'):
            HttpClientTask(
                RequestDirection.FROM,
                'http://example.org | timeout=1800, client_cert=hello.crt, client_key=hello.key',
                payload_variable='test_payload',
                metadata_variable='test_metadata',
            )

        task_factory = HttpClientTask(
            RequestDirection.FROM,
            'http://example.org | timeout=1800',
            payload_variable='test_payload',
            metadata_variable='test_metadata',
        )

        assert task_factory.endpoint == 'http://example.org'
        assert task_factory.host == 'http://example.org'
        assert task_factory.timeout == 1800
        assert task_factory.ssl_context_factory is None

    def test_on_start(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        behave = grizzly_fixture.behave.context
        grizzly = cast('GrizzlyContext', behave.grizzly)
        grizzly.scenario.variables.update({'test_payload': 'none', 'test_metadata': 'none'})
        parent.user._context.update({'test': 'was here'})

        HttpClientTask.__scenario__ = grizzly.scenario
        task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')
        task = task_factory()

        task_factory.add_metadata('x-test-header', 'foobar')

        assert getattr(task_factory, 'parent', None) is None
        assert getattr(task_factory, 'environment', None) is None
        assert getattr(task_factory, 'session_started', None) is None
        assert task_factory.metadata == {'x-grizzly-user': ANY(str)}
        assert task_factory._context.get('test', None) is None

        task.on_start(parent)

        assert getattr(task_factory, 'session_started', -1.0) >= 0.0
        assert task_factory.metadata == {'x-grizzly-user': ANY(str), 'x-test-header': 'foobar'}
        assert task_factory._context.get('test', None) == 'was here'

    @pytest.mark.parametrize('log_prefix', [False, True])
    def test_request_from(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, *, log_prefix: bool) -> None:  # noqa: PLR0915
        try:
            if log_prefix:
                environ['GRIZZLY_LOG_DIR'] = 'foobar'

            behave = grizzly_fixture.behave.context
            grizzly = cast('GrizzlyContext', behave.grizzly)
            test_cls = type('HttpClientTestTask', (HttpClientTask,), {'__scenario__': grizzly.scenario})

            with pytest.raises(AssertionError, match='HttpClientTestTask: variable argument is not applicable for direction TO'):
                test_cls(RequestDirection.TO, 'http://example.org', payload_variable='test')

            with pytest.raises(AssertionError, match='HttpClientTestTask: source argument is not applicable for direction FROM'):
                test_cls(RequestDirection.FROM, 'http://example.org', source='test')

            with pytest.raises(AssertionError, match='HttpClientTestTask: variable test has not been initialized'):
                test_cls(RequestDirection.FROM, 'http://example.org', payload_variable='test')

            with pytest.raises(AssertionError, match='HttpClientTestTask: variable test has not been initialized'):
                test_cls(RequestDirection.FROM, 'http://example.org', payload_variable=None, metadata_variable='test')

            response = Response()
            response.url = 'http://example.org'
            response._content = jsondumps({'hello': 'world'}).encode()
            response.status_code = 200

            requests_get_spy = mocker.patch(
                'grizzly.tasks.clients.http.Session.get',
                side_effect=[response, RuntimeError, RuntimeError, RuntimeError, RuntimeError, response, response, response, response],
            )

            grizzly.scenario.variables.update({'test': 'none'})

            with pytest.raises(AssertionError, match='HttpClientTestTask: payload variable is not set, but metadata variable is set'):
                test_cls(RequestDirection.FROM, 'http://example.org', payload_variable=None, metadata_variable='test')

            parent = grizzly_fixture()
            parent.user._context.update({'test': 'was here'})

            request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

            grizzly.scenario.variables.update({'test_payload': 'none', 'test_metadata': 'none'})

            task_factory = test_cls(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')
            assert task_factory.arguments == {}
            assert task_factory.__template_attributes__ == {'endpoint', 'destination', 'source', 'name', 'variable_template'}

            task = task_factory()

            assert callable(task)

            assert task_factory._context.get('test', None) is None

            task_factory.name = 'test-1'
            response.status_code = 400
            response.headers = CaseInsensitiveDict({'x-foo-bar': 'test'})

            task(parent)

            assert task_factory._context.get('test', None) == 'was here'
            requests_get_spy.assert_called_once_with(
                'http://example.org',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
            )
            requests_get_spy.reset_mock()

            request_fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} test-1',
                response_time=ANY(float, int),
                response_length=len(jsondumps({'hello': 'world'}).encode()),
                context=parent.user._context,
                exception=ANY(ResponseError, message='400 not in [200]: http://example.org returned "{"hello": "world"}"'),
            )
            request_fire_spy.reset_mock()

            request_logs = list(task_factory.log_dir.rglob('**/*'))
            assert len(request_logs) == 1
            request_log = request_logs[-1]
            parent_name = 'foobar' if log_prefix else 'logs'
            assert request_log.parent.name == parent_name

            log_entry = jsonloads(request_log.read_text())
            assert log_entry == {
                'stacktrace': ANY(list),
                'request': {
                    'time': ANY(float, int),
                    'url': 'http://example.org',
                    'metadata': ANY(dict),
                    'payload': None,
                },
                'response': {
                    'url': 'http://example.org',
                    'metadata': {'x-foo-bar': 'test'},
                    'payload': ANY(str),
                    'status': 400,
                },
            }

            parent.user.set_variable('test', None)
            response.status_code = 200
            task_factory.name = 'test-2'

            task(parent)

            assert parent.user.variables.get('test', '') is None  # not set
            requests_get_spy.assert_called_once_with(
                'http://example.org',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
            )
            requests_get_spy.reset_mock()

            request_fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} test-2',
                response_time=ANY(int, float),
                response_length=0,
                context=parent.user._context,
                exception=ANY(RuntimeError),
            )
            request_fire_spy.reset_mock()

            assert len(list(task_factory.log_dir.rglob('**/*'))) == 2

            parent.user._scenario.failure_handling.update({None: StopUser})
            task_factory.name = 'test-3'

            with pytest.raises(StopUser):
                task(parent)

            parent.user._scenario.failure_handling.update({None: RestartScenario})
            task_factory.name = 'test-4'

            requests_get_spy.reset_mock()
            request_fire_spy.reset_mock()

            with pytest.raises(RestartScenario):
                task(parent)

            assert parent.user.variables.get('test', '') is None  # not set
            requests_get_spy.assert_called_once_with(
                'http://example.org',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
            )
            requests_get_spy.reset_mock()

            request_fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} test-4',
                response_time=ANY(int, float),
                response_length=0,
                context=parent.user._context,
                exception=ANY(RuntimeError),
            )
            request_fire_spy.reset_mock()
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 4

            with suppress(KeyError):
                del parent.user._scenario.failure_handling[None]

            task_factory = test_cls(RequestDirection.FROM, 'http://example.org', 'http-get', payload_variable='test')
            task = task_factory()
            assert task_factory.arguments == {}
            assert task_factory.content_type == TransformerContentType.UNDEFINED

            task(parent)

            assert parent.user.variables.get('test', '') is None  # not set
            requests_get_spy.assert_called_once_with(
                'http://example.org',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
            )
            requests_get_spy.reset_mock()

            request_fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} http-get',
                response_time=ANY(int, float),
                response_length=0,
                context=parent.user._context,
                exception=ANY(RuntimeError),
            )
            request_fire_spy.reset_mock()
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            grizzly.state.configuration['test.host'] = 'https://example.org'

            task_factory = test_cls(RequestDirection.FROM, 'https://$conf::test.host$/api/test', 'http-env-get', payload_variable='test')
            assert task_factory.verify
            assert task_factory.content_type == TransformerContentType.UNDEFINED
            task = task_factory()
            response.url = 'https://example.org/api/test'
            requests_get_spy.side_effect = cycle([response])

            task(parent)

            requests_get_spy.assert_called_once_with(
                'https://example.org/api/test',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
            )
            requests_get_spy.reset_mock()
            request_fire_spy.assert_called_once()
            request_fire_spy.reset_mock()

            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            task_factory = test_cls(RequestDirection.FROM, 'https://$conf::test.host$/api/test | verify=False, content_type=json', 'http-env-get-1', payload_variable='test')
            task = task_factory()
            assert not task_factory.verify
            assert task_factory.content_type == TransformerContentType.JSON
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            task(parent)

            requests_get_spy.assert_called_once_with(
                'https://example.org/api/test',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}', 'Content-Type': 'application/json'},
            )
            requests_get_spy.reset_mock()
            request_fire_spy.assert_called_once()
            request_fire_spy.reset_mock()

            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            task_factory = test_cls(RequestDirection.FROM, 'https://$conf::test.host$/api/test | verify=True, content_type=json', 'http-env-get-2', payload_variable='test')
            task = task_factory()
            assert task_factory.verify
            assert task_factory.content_type == TransformerContentType.JSON

            parent.user._scenario.context['log_all_requests'] = True
            task_factory._context['metadata'] = {'x-test-header': 'foobar'}

            task.on_start(parent)

            assert task_factory.metadata.get('x-test-header', None) == 'foobar'

            task(parent)

            requests_get_spy.assert_called_once_with(
                'https://example.org/api/test',
                headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}', 'x-test-header': 'foobar', 'Content-Type': 'application/json'},
            )
            requests_get_spy.reset_mock()
            request_fire_spy.assert_called_once()
            request_fire_spy.reset_mock()
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 6

            with pytest.raises(NotImplementedError, match='HttpClientTestTask has not implemented support for step text'):
                test_cls(
                    RequestDirection.FROM,
                    'https://$conf::test.host$/api/test | verify=True, content_type=json',
                    'http-env-get-2',
                    payload_variable='test',
                    text='foobar',
                )
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_LOG_DIR']

    @pytest.mark.skip(reason='needs real credentials, so only used during development')
    def test_request_from_real(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        parent = grizzly_fixture()

        target_host = ''
        endpoint = ''

        test_cls = type(
            'HttpClientTestTask',
            (HttpClientTask,),
            {
                '__scenario__': grizzly.scenario,
            },
        )

        with caplog.at_level(logging.DEBUG):
            task_factory = test_cls(RequestDirection.FROM, f'https://{target_host}/{endpoint}')
            task_factory.host = f'https://{target_host}'
            task_factory.environment = parent.user.environment
            task_factory.metadata.update(
                {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
                },
            )
            task_factory._context = {
                target_host: {
                    'auth': {
                        'client': {
                            'id': '',
                        },
                        'user': {
                            'username': '',
                            'password': '',
                            'redirect_uri': '',
                            'initialize_uri': None,
                        },
                        'tenant': '',
                    },
                },
            }
            task = task_factory()

            headers, payload = task(parent)

        parent.logger.info(headers)
        parent.logger.info(payload)

        assert 0  # noqa: PT015

    def test_request_to(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        grizzly = grizzly_fixture.grizzly

        test_cls = type('HttpClientTestTask', (HttpClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})
        parent = grizzly_fixture()

        response = Response()
        response.url = 'http://example.org'
        response._content = b'foobar'
        response.status_code = 200

        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        requests_request_spy = mocker.patch(
            'grizzly.tasks.clients.http.Session.request',
            return_value=response,
        )

        with pytest.raises(AssertionError, match='source argument is not applicable for direction FROM'):
            test_cls(RequestDirection.FROM, 'http://example.org', source='foobar')

        task_factory = test_cls(RequestDirection.TO, 'http://example.org', 'test-put', source='foobar {{ foo }}!')

        task = task_factory()

        parent.user.set_variable('foo', 'bar')

        assert task(parent) == ({}, 'foobar')

        requests_request_spy.assert_called_once_with(
            'PUT',
            'http://example.org',
            data='foobar bar!',
            headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
        )
        requests_request_spy.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} test-put',
            response_time=ANY(float, int),
            response_length=len(b'foobar'),
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()

        with suppress(KeyError):
            del grizzly.scenario.failure_handling[None]

        response.status_code = 500
        response.headers = CaseInsensitiveDict({'x-foo-bar': 'test'})
        requests_request_spy.return_value = response

        assert task(parent) == ({'x-foo-bar': 'test'}, 'foobar')

        requests_request_spy.assert_called_once_with(
            'PUT',
            'http://example.org',
            data='foobar bar!',
            headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
        )
        requests_request_spy.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} test-put',
            response_time=ANY(float, int),
            response_length=len(b'foobar'),
            context=parent.user._context,
            exception=ANY(ResponseError, message='500 not in [200]: http://example.org returned "foobar"'),
        )
        request_fire_spy.reset_mock()

        task_factory.response.add_status_code(500)

        assert task(parent) == ({'x-foo-bar': 'test'}, 'foobar')

        requests_request_spy.assert_called_once_with(
            'PUT',
            'http://example.org',
            data='foobar bar!',
            headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
        )
        requests_request_spy.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} test-put',
            response_time=ANY(float, int),
            response_length=len(b'foobar'),
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()

        task_factory.response.add_status_code(-500)

        # make sure b64encode filter is loaded
        from grizzly.testdata.filters import b64encode as _b64encode_filter

        _b64encode_filter('dummy')

        task_factory = test_cls(
            RequestDirection.TO,
            'http://example.org',
            'test-put',
            source=jsondumps(
                {
                    'files': [
                        {'bytes': '{{ file_content | b64encode }}', 'name': '{{ file_name }}'},
                    ],
                },
            ),
        )

        task = task_factory()

        parent.user.set_variable('file_name', 'test.json')
        parent.user.set_variable('world', 'foobar')
        parent.user.set_variable('file_content', parent.user.render('<hello>{{ world }}</hello>'))

        response.status_code = 200
        requests_request_spy.return_value = response

        assert task(parent) == ({'x-foo-bar': 'test'}, 'foobar')

        expected_data = jsondumps(
            {
                'files': [
                    {'bytes': b64encode(b'<hello>foobar</hello>').decode(), 'name': 'test.json'},
                ],
            },
        )
        requests_request_spy.assert_called_once_with(
            'PUT',
            'http://example.org',
            data=expected_data,
            headers={'x-grizzly-user': f'HttpClientTestTask::{id(task_factory)}'},
        )
        requests_request_spy.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} test-put',
            response_time=ANY(float, int),
            response_length=len(b'foobar'),
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()
