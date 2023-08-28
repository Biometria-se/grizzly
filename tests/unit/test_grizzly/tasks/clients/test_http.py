from typing import cast
from json import dumps as jsondumps, loads as jsonloads
from unittest.mock import ANY
from itertools import cycle
from os import environ

import pytest

from pytest_mock import MockerFixture
from requests import Response
from requests.structures import CaseInsensitiveDict

from grizzly_extras.transformer import TransformerContentType
from grizzly.context import GrizzlyContext
from grizzly.tasks.clients import HttpClientTask
from grizzly.exceptions import RestartScenario
from grizzly.types import RequestDirection
from grizzly.types.locust import StopUser, CatchResponseError

from tests.fixtures import GrizzlyFixture


class TestHttpClientTask:
    def test_on_start(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        behave = grizzly_fixture.behave.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables.update({'test_payload': 'none', 'test_metadata': 'none'})
        parent.user._context.update({'test': 'was here'})

        HttpClientTask.__scenario__ = grizzly.scenario
        task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')
        task = task_factory()

        task_factory.add_metadata('x-test-header', 'foobar')

        assert getattr(task_factory, 'parent', None) is None
        assert getattr(task_factory, 'environment', None) is None
        assert getattr(task_factory, 'session_started', None) is None
        assert task_factory.headers == {'x-grizzly-user': ANY}
        assert task_factory._context.get('test', None) is None

        task.on_start(parent)

        assert getattr(task_factory, 'session_started', -1.0) >= 0.0
        assert task_factory.headers == {'x-grizzly-user': ANY, 'x-test-header': 'foobar'}
        assert task_factory._context.get('test', None) == 'was here'

    @pytest.mark.parametrize('log_prefix', [False, True,])
    def test_get(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, log_prefix: bool) -> None:
        try:
            if log_prefix:
                environ['GRIZZLY_LOG_DIR'] = 'foobar'

            behave = grizzly_fixture.behave.context
            grizzly = cast(GrizzlyContext, behave.grizzly)

            HttpClientTask.__scenario__ = grizzly.scenario

            with pytest.raises(AttributeError) as ae:
                HttpClientTask(RequestDirection.TO, 'http://example.org', payload_variable='test')
            assert 'HttpClientTask: variable argument is not applicable for direction TO' in str(ae.value)

            with pytest.raises(AttributeError) as ae:
                HttpClientTask(RequestDirection.FROM, 'http://example.org', source='test')
            assert 'HttpClientTask: source argument is not applicable for direction FROM' in str(ae.value)

            with pytest.raises(ValueError) as ve:
                HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test')
            assert 'HttpClientTask: variable test has not been initialized' in str(ve)

            with pytest.raises(ValueError) as ve:
                HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable=None, metadata_variable='test')
            assert 'HttpClientTask: variable test has not been initialized' in str(ve)

            response = Response()
            response.url = 'http://example.org'
            response._content = jsondumps({'hello': 'world'}).encode()
            response.status_code = 200

            requests_get_spy = mocker.patch(
                'grizzly.tasks.clients.http.requests.get',
                side_effect=[response, RuntimeError, RuntimeError, RuntimeError, RuntimeError, response, response, response, response]
            )

            grizzly.state.variables.update({'test': 'none'})

            with pytest.raises(ValueError) as ve:
                HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable=None, metadata_variable='test')
            assert 'HttpClientTask: payload variable is not set, but metadata variable is set' in str(ve)

            parent = grizzly_fixture()
            parent.user._context.update({'test': 'was here'})

            request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

            grizzly.state.variables.update({'test_payload': 'none', 'test_metadata': 'none'})

            task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')
            assert task_factory.arguments == {}
            assert task_factory.__template_attributes__ == {'endpoint', 'destination', 'source', 'name', 'variable_template'}

            task = task_factory()

            assert callable(task)

            assert parent.user._context['variables'].get('test_payload', None) is None
            assert parent.user._context['variables'].get('test_metadata', None) is None
            assert task_factory._context.get('test', None) is None

            task_factory.name = 'test-1'
            response.status_code = 400
            response.headers = CaseInsensitiveDict({'x-foo-bar': 'test'})

            task(parent)

            assert task_factory._context.get('test', None) == 'was here'
            assert parent.user._context['variables'].get('test_payload', None) is None
            assert parent.user._context['variables'].get('test_metadata', None) is None
            assert requests_get_spy.call_count == 1
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'http://example.org'
            assert len(kwargs) == 2
            assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert kwargs.get('cookies', None) == {}

            assert request_fire_spy.call_count == 1
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} test-1'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == len(jsondumps({'hello': 'world'}))
            assert kwargs.get('context', None) is parent.user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, CatchResponseError)
            assert str(exception) == '400 not in [200]: {"hello": "world"}'

            request_logs = list(task_factory.log_dir.rglob('**/*'))
            assert len(request_logs) == 1
            request_log = request_logs[-1]
            parent_name = 'foobar' if log_prefix else 'logs'
            assert request_log.parent.name == parent_name

            log_entry = jsonloads(request_log.read_text())
            assert log_entry.get('request', {}).get('time', None) >= 0.0
            assert log_entry.get('request', {}).get('url', None) == 'http://example.org'
            assert log_entry.get('request', {}).get('metadata', None) is not None
            assert log_entry.get('request', {}).get('payload', '') is None
            assert log_entry.get('response', {}).get('url', None) == 'http://example.org'
            assert log_entry.get('response', {}).get('metadata', None) == {'x-foo-bar': 'test'}
            assert log_entry.get('response', {}).get('payload', None) is not None
            assert log_entry.get('response', {}).get('status', None) == 400

            parent.user._context['variables']['test'] = None
            response.status_code = 200
            task_factory.name = 'test-2'

            task(parent)

            assert parent.user._context['variables'].get('test', '') is None  # not set
            assert requests_get_spy.call_count == 2
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'http://example.org'
            assert len(kwargs) == 2
            assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert kwargs.get('cookies', None) == {}

            assert request_fire_spy.call_count == 2
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} test-2'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == 0
            assert kwargs.get('context', None) is parent.user._context
            assert isinstance(kwargs.get('exception', None), RuntimeError)
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 2

            parent.user._scenario.failure_exception = StopUser
            task_factory.name = 'test-3'

            with pytest.raises(StopUser):
                task(parent)

            parent.user._scenario.failure_exception = RestartScenario
            task_factory.name = 'test-4'

            with pytest.raises(RestartScenario):
                task(parent)

            assert parent.user._context['variables'].get('test', '') is None  # not set
            assert requests_get_spy.call_count == 4
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'http://example.org'
            assert len(kwargs) == 2
            assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert kwargs.get('cookies', None) == {}

            assert request_fire_spy.call_count == 4
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} test-4'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == 0
            assert kwargs.get('context', None) is parent.user._context
            assert isinstance(kwargs.get('exception', None), RuntimeError)
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 4

            parent.user._scenario.failure_exception = None

            task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', 'http-get', payload_variable='test')
            task = task_factory()
            assert task_factory.arguments == {}
            assert task_factory.content_type == TransformerContentType.UNDEFINED

            task(parent)

            assert parent.user._context['variables'].get('test', '') is None  # not set
            assert requests_get_spy.call_count == 5
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'http://example.org'
            assert len(kwargs) == 2
            assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert kwargs.get('cookies', None) == {}

            assert request_fire_spy.call_count == 5
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} http-get'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == 0
            assert kwargs.get('context', None) is parent.user._context
            assert isinstance(kwargs.get('exception', None), RuntimeError)
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            grizzly.state.configuration['test.host'] = 'https://example.org'

            task_factory = HttpClientTask(RequestDirection.FROM, 'https://$conf::test.host$/api/test', 'http-env-get', payload_variable='test')
            assert task_factory.arguments == {'verify': True}
            assert task_factory.content_type == TransformerContentType.UNDEFINED
            task = task_factory()
            response.url = 'https://example.org/api/test'
            requests_get_spy.side_effect = cycle([response])

            task(parent)

            assert requests_get_spy.call_count == 6
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'https://example.org/api/test'
            assert len(kwargs) == 3
            assert kwargs.get('verify', None)
            assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert kwargs.get('cookies', None) == {}

            print(list(task_factory.log_dir.rglob('**/*')))

            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            task_factory = HttpClientTask(RequestDirection.FROM, 'https://$conf::test.host$/api/test | verify=False, content_type=json', 'http-env-get-1', payload_variable='test')
            task = task_factory()
            assert task_factory.arguments == {'verify': False}
            assert task_factory.content_type == TransformerContentType.JSON
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            task(parent)

            assert requests_get_spy.call_count == 7
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'https://example.org/api/test'
            assert len(kwargs) == 3
            assert not kwargs.get('verify', None)
            assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert kwargs.get('cookies', None) == {}
            assert request_fire_spy.call_count == 7
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

            task_factory = HttpClientTask(RequestDirection.FROM, 'https://$conf::test.host$/api/test | verify=True, content_type=json', 'http-env-get-2', payload_variable='test')
            task = task_factory()
            assert task_factory.arguments == {'verify': True}
            assert task_factory.content_type == TransformerContentType.JSON

            parent.user._scenario.context['log_all_requests'] = True
            task_factory._context['metadata'] = {'x-test-header': 'foobar'}

            task.on_start(parent)

            assert task_factory.headers.get('x-test-header', None) == 'foobar'

            task(parent)

            assert requests_get_spy.call_count == 8
            args, kwargs = requests_get_spy.call_args_list[-1]
            assert args[0] == 'https://example.org/api/test'
            assert len(kwargs) == 3
            assert kwargs.get('verify', None)
            headers = kwargs.get('headers', {})
            assert headers.get('x-grizzly-user', None).startswith('HttpClientTask::')
            assert headers.get('x-test-header', None) == 'foobar'
            assert kwargs.get('cookies', None) == {}
            assert request_fire_spy.call_count == 8
            args, kwargs = request_fire_spy.call_args_list[-1]
            assert len(list(task_factory.log_dir.rglob('**/*'))) == 6

            with pytest.raises(NotImplementedError) as nie:
                HttpClientTask(
                    RequestDirection.FROM,
                    'https://$conf::test.host$/api/test | verify=True, content_type=json',
                    'http-env-get-2',
                    payload_variable='test',
                    text='foobar',
                )
            assert str(nie.value) == 'HttpClientTask has not implemented support for step text'
        finally:
            try:
                del environ['GRIZZLY_LOG_DIR']
            except:
                pass

    def test_put(self, grizzly_fixture: GrizzlyFixture) -> None:
        HttpClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        task_factory = HttpClientTask(RequestDirection.TO, 'http://put.example.org', source='')
        task = task_factory()

        parent = grizzly_fixture()
        parent.user._context.update({'test': 'was here'})

        assert task_factory._context.get('test', None) is None

        with pytest.raises(NotImplementedError) as nie:
            task(parent)
        assert 'HttpClientTask has not implemented PUT' in str(nie.value)
        assert task_factory._context.get('test', None) == 'was here'
