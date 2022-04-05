from typing import cast
from json import dumps as jsondumps

import pytest

from pytest_mock import MockerFixture
from locust.exception import StopUser
from requests import Response

from grizzly.context import GrizzlyContext
from grizzly.tasks.clients import HttpClientTask
from grizzly.exceptions import RestartScenario
from grizzly.types import RequestDirection

from ...fixtures import GrizzlyFixture


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

        requests_get_spy = mocker.patch(
            'grizzly.tasks.clients.http.requests.get',
            side_effect=[response, RuntimeError, RuntimeError, RuntimeError]
        )

        grizzly.state.variables.update({'test': 'none'})

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        request_fire_spy = mocker.spy(scenario.user.environment.events.request, 'fire')

        task = HttpClientTask(RequestDirection.FROM, 'http://example.org', variable='test')

        implementation = task.implementation()

        assert callable(implementation)

        assert scenario.user._context['variables'].get('test', None) is None

        implementation(scenario)

        assert scenario.user._context['variables'].get('test', '') == jsondumps({'hello': 'world'})
        assert requests_get_spy.call_count == 1
        args, _ = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TASK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} HttpClientTask->test'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == len(jsondumps({'hello': 'world'}))
        assert kwargs.get('context', None) is scenario.user._context
        assert kwargs.get('exception', '') is None

        scenario.user._context['variables']['test'] = None

        implementation(scenario)

        assert scenario.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 2
        args, _ = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'

        assert request_fire_spy.call_count == 2
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TASK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} HttpClientTask->test'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is scenario.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)

        scenario.user._scenario.failure_exception = StopUser

        with pytest.raises(StopUser):
            implementation(scenario)

        scenario.user._scenario.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            implementation(scenario)

        assert scenario.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 4
        args, _ = requests_get_spy.call_args_list[-1]
        assert args[0] == 'http://example.org'

        assert request_fire_spy.call_count == 4
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TASK'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} HttpClientTask->test'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is scenario.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)

    def test_put(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        task = HttpClientTask(RequestDirection.TO, 'http://put.example.org', source='')
        implementation = task.implementation()

        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        with pytest.raises(NotImplementedError) as nie:
            implementation(scenario)
        assert 'HttpClientTask has not implemented PUT' in str(nie.value)
