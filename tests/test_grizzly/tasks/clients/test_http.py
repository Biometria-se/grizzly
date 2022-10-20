from typing import cast
from json import dumps as jsondumps, loads as jsonloads

import pytest

from pytest_mock import MockerFixture
from locust.exception import StopUser
from requests import Response

from grizzly_extras.transformer import TransformerContentType
from grizzly.context import GrizzlyContext
from grizzly.tasks.clients import HttpClientTask
from grizzly.exceptions import RestartScenario
from grizzly.types import RequestDirection

from ....fixtures import GrizzlyFixture


class TestHttpClientTask:
    def test_get(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        behave = grizzly_fixture.behave
        grizzly = cast(GrizzlyContext, behave.grizzly)

        with pytest.raises(AttributeError) as ae:
            HttpClientTask(RequestDirection.TO, 'http://example.org', variable='test')
        assert 'HttpClientTask: variable argument is not applicable for direction TO' in str(ae.value)

        with pytest.raises(AttributeError) as ae:
            HttpClientTask(RequestDirection.FROM, 'http://example.org', source='test')
        assert 'HttpClientTask: source argument is not applicable for direction FROM' in str(ae.value)

        with pytest.raises(ValueError) as ve:
            HttpClientTask(RequestDirection.FROM, 'http://example.org', variable='test')
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

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        request_fire_spy = mocker.spy(scenario.user.environment.events.request, 'fire')

        task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', variable='test')
        assert task_factory.arguments == {}

        task = task_factory()

        assert callable(task)

        assert scenario.user._context['variables'].get('test', None) is None

        task_factory.name = 'test-1'
        response.status_code = 400

        task(scenario)

        assert scenario.user._context['variables'].get('test', '') == jsondumps({'hello': 'world'})
        assert requests_get_spy.call_count == 1
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'
        assert len(kwargs) == 1
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'CLTSK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} test-1'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == len(jsondumps({'hello': 'world'}))
        assert kwargs.get('context', None) is scenario.user._context
        assert kwargs.get('exception', '') is None
        request_logs = list(task_factory.log_dir.rglob('**/*'))
        assert len(request_logs) == 1
        request_log = request_logs[-1]
        log_entry = jsonloads(request_log.read_text())
        assert log_entry.get('request', {}).get('time', None) >= 0.0
        assert log_entry.get('request', {}).get('url', None) == 'http://example.org'
        assert log_entry.get('request', {}).get('metadata', None) is not None
        assert log_entry.get('request', {}).get('payload', '') is None
        assert log_entry.get('response', {}).get('url', None) == 'http://example.org'
        assert log_entry.get('response', {}).get('metadata', None) == {}
        assert log_entry.get('response', {}).get('payload', None) is not None
        assert log_entry.get('response', {}).get('status', None) == 400

        scenario.user._context['variables']['test'] = None
        response.status_code = 200
        task_factory.name = 'test-2'

        task(scenario)

        assert scenario.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 2
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'
        assert len(kwargs) == 1
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')

        assert request_fire_spy.call_count == 2
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'CLTSK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} test-2'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is scenario.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 2

        scenario.user._scenario.failure_exception = StopUser
        task_factory.name = 'test-3'

        with pytest.raises(StopUser):
            task(scenario)

        scenario.user._scenario.failure_exception = RestartScenario
        task_factory.name = 'test-4'

        with pytest.raises(RestartScenario):
            task(scenario)

        assert scenario.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 4
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'
        assert len(kwargs) == 1
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')

        assert request_fire_spy.call_count == 4
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'CLTSK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} test-4'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is scenario.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 4

        scenario.user._scenario.failure_exception = None

        task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', 'http-get', variable='test')
        task = task_factory()
        assert task_factory.arguments == {}
        assert task_factory.content_type == TransformerContentType.UNDEFINED

        task(scenario)

        assert scenario.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 5
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'
        assert len(kwargs) == 1
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')

        assert request_fire_spy.call_count == 5
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'CLTSK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} http-get'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is scenario.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

        grizzly.state.configuration['test.host'] = 'https://example.org'

        task_factory = HttpClientTask(RequestDirection.FROM, 'https://$conf::test.host$/api/test', 'http-env-get', variable='test')
        assert task_factory.arguments == {'verify': True}
        assert task_factory.content_type == TransformerContentType.UNDEFINED
        task = task_factory()

        task(scenario)

        assert requests_get_spy.call_count == 6
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'https://example.org/api/test'
        assert len(kwargs) == 2
        assert kwargs.get('verify', None)
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

        task_factory = HttpClientTask(RequestDirection.FROM, 'https://$conf::test.host$/api/test | verify=False, content_type=json', 'http-env-get-1', variable='test')
        task = task_factory()
        assert task_factory.arguments == {'verify': False}
        assert task_factory.content_type == TransformerContentType.JSON
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

        task(scenario)

        assert requests_get_spy.call_count == 7
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'https://example.org/api/test'
        assert len(kwargs) == 2
        assert not kwargs.get('verify', None)
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
        assert request_fire_spy.call_count == 7
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 5

        task_factory = HttpClientTask(RequestDirection.FROM, 'https://$conf::test.host$/api/test | verify=True, content_type=json', 'http-env-get-2', variable='test')
        task = task_factory()
        assert task_factory.arguments == {'verify': True}
        assert task_factory.content_type == TransformerContentType.JSON

        scenario.user._scenario.context['log_all_requests'] = True

        task(scenario)

        assert requests_get_spy.call_count == 8
        args, kwargs = requests_get_spy.call_args_list[-1]
        assert args[0] == 'https://example.org/api/test'
        assert len(kwargs) == 2
        assert kwargs.get('verify', None)
        assert kwargs.get('headers', {}).get('x-grizzly-user', None).startswith('HttpClientTask::')
        assert request_fire_spy.call_count == 8
        args, kwargs = request_fire_spy.call_args_list[-1]
        assert len(list(task_factory.log_dir.rglob('**/*'))) == 6

    def test_put(self, grizzly_fixture: GrizzlyFixture) -> None:
        task_factory = HttpClientTask(RequestDirection.TO, 'http://put.example.org', source='')
        task = task_factory()

        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        with pytest.raises(NotImplementedError) as nie:
            task(scenario)
        assert 'HttpClientTask has not implemented PUT' in str(nie.value)
