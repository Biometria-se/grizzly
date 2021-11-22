from typing import Callable, cast
from json import dumps as jsondumps
import pytest

from pytest_mock import mocker, MockerFixture  # pylint: disable=unused-import
from locust.exception import StopUser
from requests import Response
from behave.runner import Context

from grizzly.context import GrizzlyContext
from grizzly.task.getter import HttpGetTask

from ...fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import


class TestHttpGetTask:
    @pytest.mark.usefixtures('behave_context', 'grizzly_context')
    def test(self, mocker: MockerFixture, behave_context: Context, grizzly_context: Callable) -> None:
        grizzly = cast(GrizzlyContext, behave_context.grizzly)

        with pytest.raises(ValueError) as ve:
            HttpGetTask(endpoint='http://example.org', variable='test')
        assert 'HttpGetTask: variable test has not been initialized' in str(ve)

        response = Response()
        response.url = 'http://example.org'
        response._content = jsondumps({'hello': 'world'}).encode()

        requests_get_spy = mocker.patch(
            'grizzly.task.getter.http.requests.get',
            side_effect=[response, RuntimeError, RuntimeError]
        )

        grizzly.state.variables.update({'test': 'none'})

        _, _, tasks, _ = grizzly_context()

        request_fire_spy = mocker.spy(tasks.user.environment.events.request, 'fire')

        task = HttpGetTask(endpoint='http://example.org', variable='test')

        implementation = task.implementation()

        assert callable(implementation)

        assert tasks.user._context['variables'].get('test', None) is None

        implementation(tasks)

        assert tasks.user._context['variables'].get('test', '') == jsondumps({'hello': 'world'})
        assert requests_get_spy.call_count == 1
        args, _ = requests_get_spy.call_args_list[0]
        assert args[0] == 'http://example.org'

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[0]
        assert kwargs.get('request_type', None) == 'TASK'
        assert kwargs.get('name', None) == f'{tasks.user._scenario.identifier} HttpGetTask->test'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == len(jsondumps({'hello': 'world'}))
        assert kwargs.get('context', None) is tasks.user._context
        assert kwargs.get('exception', '') is None

        tasks.user._context['variables']['test'] = None

        implementation(tasks)

        assert tasks.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 2
        args, _ = requests_get_spy.call_args_list[0]
        assert args[0] == 'http://example.org'

        assert request_fire_spy.call_count == 2
        _, kwargs = request_fire_spy.call_args_list[1]
        assert kwargs.get('request_type', None) == 'TASK'
        assert kwargs.get('name', None) == f'{tasks.user._scenario.identifier} HttpGetTask->test'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is tasks.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)

        tasks.user._scenario.stop_on_failure = True

        with pytest.raises(StopUser):
            implementation(tasks)

        assert tasks.user._context['variables'].get('test', '') is None  # not set
        assert requests_get_spy.call_count == 3
        args, _ = requests_get_spy.call_args_list[0]
        assert args[0] == 'http://example.org'

        assert request_fire_spy.call_count == 3
        _, kwargs = request_fire_spy.call_args_list[2]
        assert kwargs.get('request_type', None) == 'TASK'
        assert kwargs.get('name', None) == f'{tasks.user._scenario.identifier} HttpGetTask->test'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length') == 0
        assert kwargs.get('context', None) is tasks.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)
