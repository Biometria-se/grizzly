import logging
from os import environ
from unittest.mock import MagicMock
from typing import List, cast

import pytest

from gevent import Greenlet
from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from locust.exception import StopUser

from grizzly.tasks import RequestTask, AsyncRequestGroupTask, LogMessageTask
from grizzly.types import RequestMethod
from grizzly.users import RestApiUser
from grizzly.scenarios import IteratorScenario
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario

from ...fixtures import GrizzlyFixture


class TestAsyncRequestGroup:
    def test__init__(self) -> None:
        task_factory = AsyncRequestGroupTask(name='test')

        assert isinstance(task_factory.tasks, list)
        assert len(task_factory.tasks) == 0
        assert task_factory.name == 'test'

    def test_add(self) -> None:
        task_factory = AsyncRequestGroupTask(name='test')
        requests = cast(List[RequestTask], task_factory.tasks)
        assert len(requests) == 0

        task_factory.add(RequestTask(RequestMethod.GET, name='test', endpoint='/api/test'))

        assert len(requests) == 1
        assert requests[-1].name == 'test:test'

        with pytest.raises(ValueError) as ve:
            task_factory.add(LogMessageTask(message='hello world'))
        assert str(ve.value) == 'AsyncRequestGroupTask only accepts RequestTask tasks, not LogMessageTask'

    @pytest.mark.parametrize('affix', [True, False])
    def test_get_templates(self, affix: bool) -> None:
        task_factory = AsyncRequestGroupTask(name='async-{{ name }}')
        assert len(task_factory.tasks) == 0

        name_template = 'test-'
        if affix:
            name_template += '{{ name }}-'

        task_factory.add(RequestTask(RequestMethod.GET, name=f'{name_template}-1', endpoint='/api/test'))
        task_factory.add(RequestTask(RequestMethod.GET, name=f'{name_template}-2', endpoint='/api/test'))
        task_factory.add(RequestTask(RequestMethod.GET, name=f'{name_template}-3', endpoint='/api/test'))

        assert len(task_factory.tasks) == 3
        assert sorted(task_factory.get_templates()) == sorted([
            'async-{{ name }}',
            f'async-{{{{ name }}}}:{name_template}-1',
            f'async-{{{{ name }}}}:{name_template}-2',
            f'async-{{{{ name }}}}:{name_template}-3',
        ])

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        scenario_context = GrizzlyContextScenario(1)
        scenario_context.name = scenario_context.description = 'test scenario'

        task_factory = AsyncRequestGroupTask(name='test-async-group', scenario=scenario_context)
        requests = cast(List[RequestTask], task_factory.tasks)
        task = task_factory()

        with pytest.raises(NotImplementedError) as nie:
            task(scenario)
        assert str(nie.value) == 'TestUser does not inherit AsyncRequests'

        _, _, scenario = grizzly_fixture(user_type=RestApiUser)

        assert scenario is not None

        joinall_mock = mocker.patch('grizzly.tasks.async_group.gevent.joinall', return_value=None)
        requests_event_mock = mocker.spy(scenario.user.environment.events.request, 'fire')

        # no requests in group
        task(scenario)

        assert joinall_mock.call_count == 1
        args, _ = joinall_mock.call_args_list[-1]
        assert args[0] == []

        assert requests_event_mock.call_count == 1
        _, kwargs = requests_event_mock.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'ASYNC'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} {task_factory.name} (0)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is scenario.user._context
        assert kwargs.get('exception', '') is None

        spawn_mock = mocker.patch('grizzly.tasks.async_group.gevent.spawn', return_value=MagicMock(spec=Greenlet))
        settrace_mock = MagicMock()
        spawn_mock.return_value.settrace = settrace_mock

        # check that greenlets are spawned for each request
        spawn_mock.return_value.get.return_value = ({}, 'hello world!')

        task_factory.add(RequestTask(RequestMethod.POST, name='test-post', endpoint='/api/post'))
        task_factory.add(RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/get'))

        assert len(requests) == 2
        assert requests[-1].name == 'test-async-group:test-get'

        task(scenario)

        assert spawn_mock.call_count == len(task_factory.tasks)
        args, _ = spawn_mock.call_args_list[0]
        assert args[1] is task_factory.tasks[0]
        args, _ = spawn_mock.call_args_list[1]
        assert args[1] is task_factory.tasks[1]
        assert settrace_mock.call_count == 0

        assert joinall_mock.call_count == 2
        args, _ = joinall_mock.call_args_list[-1]
        assert len(args[0]) == len(task_factory.tasks)

        assert requests_event_mock.call_count == 2
        _, kwargs = requests_event_mock.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'ASYNC'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} {task_factory.name} (2)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == len('hello world!') * 2
        assert kwargs.get('context', None) is scenario.user._context
        assert kwargs.get('exception', '') is None

        # exception in one of the requests
        spawn_mock.return_value.get.side_effect = [RuntimeError, ({}, 'foo bar',)]

        task(scenario)

        assert requests_event_mock.call_count == 3
        _, kwargs = requests_event_mock.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'ASYNC'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} {task_factory.name} (2)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == len('foo bar')
        assert kwargs.get('context', None) is scenario.user._context
        assert isinstance(kwargs.get('exception', None), RuntimeError)

        # exception before spawning greenlets, with a scenario failure exception
        joinall_mock.side_effect = [RuntimeError]
        scenario_context.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            task(scenario)

        # with greenlet trace method
        joinall_mock.side_effect = [RuntimeError]
        spawn_mock.reset_mock()
        try:
            environ['GEVENT_MONITOR_THREAD_ENABLE'] = 'true'
            scenario_context.failure_exception = StopUser

            with caplog.at_level(logging.DEBUG):
                with pytest.raises(StopUser):
                    task(scenario)

            spawn_mock.call_count == 2
            assert settrace_mock.call_count == 2
        finally:
            del environ['GEVENT_MONITOR_THREAD_ENABLE']

    @pytest.mark.skip(reason='needs a webservice that sleeps')
    def test___call___real(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, user, scenario = grizzly_fixture(host='http://host.docker.internal:8002', user_type=RestApiUser, scenario_type=IteratorScenario)

        assert scenario is not None
        assert user is not None

        request_spy = mocker.spy(user.environment.events.request, 'fire')

        assert user.host == 'http://host.docker.internal:8002'

        context_scenario = GrizzlyContextScenario(1)
        context_scenario.name = context_scenario.description = 'test scenario'

        task_factory = AsyncRequestGroupTask(name='test', scenario=context_scenario)

        task_factory.add(RequestTask(RequestMethod.GET, name='sleep-2', endpoint='/api/sleep/2', scenario=context_scenario))
        task_factory.add(RequestTask(RequestMethod.GET, name='sleep-6', endpoint='/api/sleep/6', scenario=context_scenario))
        task_factory.add(RequestTask(RequestMethod.GET, name='sleep-1', endpoint='/api/sleep/1', scenario=context_scenario))
        # task_factory.add(RequestTask(RequestMethod.POST, name='post-echo', endpoint='/api/echo', source='{"foo": "bar"}', scenario=context_scenario))

        assert len(task_factory.tasks) == 3

        task = task_factory()
        caplog.handler.formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

        with caplog.at_level(logging.DEBUG):
            task(scenario)

        with open('output.log', 'w+') as fd:
            fd.write(caplog.text)
        assert request_spy.call_count == 4
