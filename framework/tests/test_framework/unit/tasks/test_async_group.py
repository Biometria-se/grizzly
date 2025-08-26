"""Unit tests for grizzly.tasks.async_group."""

from __future__ import annotations

import logging
from os import environ
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock

import pytest
from gevent import Greenlet
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario
from grizzly.scenarios import IteratorScenario
from grizzly.tasks import AsyncRequestGroupTask, LogMessageTask, RequestTask
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from grizzly.users import RestApiUser

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class TestAsyncRequestGroup:
    def test__init__(self) -> None:
        task_factory = AsyncRequestGroupTask(name='test')

        assert isinstance(task_factory.tasks, list)
        assert len(task_factory.tasks) == 0
        assert task_factory.name == 'test'
        assert task_factory.__template_attributes__ == {'name', 'tasks'}

    def test_add(self) -> None:
        task_factory = AsyncRequestGroupTask(name='test')
        requests = cast('list[RequestTask]', task_factory.tasks)
        assert len(requests) == 0

        task_factory.add(RequestTask(RequestMethod.GET, name='test', endpoint='/api/test'))

        assert len(requests) == 1
        assert requests[-1].name == 'test:test'

        with pytest.raises(TypeError, match='AsyncRequestGroupTask only accepts RequestTask tasks, not LogMessageTask'):
            task_factory.add(LogMessageTask(message='hello world'))

    @pytest.mark.parametrize('affix', [True, False])
    def test_get_templates(self, *, affix: bool) -> None:
        task_factory = AsyncRequestGroupTask(name='async-{{ name }}')
        assert len(task_factory.tasks) == 0

        name_template = 'test-'
        if affix:
            name_template += '{{ name }}-'

        task_factory.add(RequestTask(RequestMethod.GET, name=f'{name_template}-1', endpoint='/api/test'))
        task_factory.add(RequestTask(RequestMethod.GET, name=f'{name_template}-2', endpoint='/api/test'))
        task_factory.add(RequestTask(RequestMethod.GET, name=f'{name_template}-3', endpoint='/api/test'))

        assert len(task_factory.tasks) == 3
        assert sorted(task_factory.get_templates()) == sorted(
            [
                'async-{{ name }}',
                f'async-{{{{ name }}}}:{name_template}-1',
                f'async-{{{{ name }}}}:{name_template}-2',
                f'async-{{{{ name }}}}:{name_template}-3',
            ],
        )

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture()

        task_factory = AsyncRequestGroupTask(name='test-async-group')
        requests = cast('list[RequestTask]', task_factory.tasks)
        task = task_factory()

        with pytest.raises(NotImplementedError, match='test_framework.helpers.TestUser_001 does not inherit AsyncRequests'):
            task(parent)

        parent = grizzly_fixture(user_type=RestApiUser)

        joinall_mock = mocker.patch('grizzly.tasks.async_group.gevent.joinall', return_value=None)
        requests_event_mock = mocker.spy(parent.user.environment.events.request, 'fire')

        # no requests in group
        task(parent)

        joinall_mock.assert_called_once_with([])
        joinall_mock.reset_mock()

        requests_event_mock.assert_called_once_with(
            request_type='ASYNC',
            name=f'{parent.user._scenario.identifier} {task_factory.name} (0)',
            response_time=ANY(int),
            response_length=0,
            context=parent.user._context,
            exception=None,
        )
        requests_event_mock.reset_mock()

        spawn_mock = mocker.patch('grizzly.tasks.async_group.gevent.spawn', return_value=MagicMock(spec=Greenlet))
        settrace_mock = MagicMock()
        spawn_mock.return_value.settrace = settrace_mock

        # check that greenlets are spawned for each request
        spawn_mock.return_value.get.return_value = ({}, 'hello world!')

        task_factory.add(RequestTask(RequestMethod.POST, name='test-post', endpoint='/api/post'))
        task_factory.add(RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/get'))

        assert len(requests) == 2
        assert requests[-1].name == 'test-async-group:test-get'

        task(parent)

        assert spawn_mock.call_count == len(task_factory.tasks)
        args, kwargs = spawn_mock.call_args_list[0]
        assert kwargs == {}
        assert args == (parent.user.request, task_factory.tasks[0])
        args, kwargs = spawn_mock.call_args_list[1]
        assert kwargs == {}
        assert args == (parent.user.request, task_factory.tasks[1])
        spawn_mock.reset_mock()
        settrace_mock.assert_not_called()

        assert joinall_mock.call_count == 1
        args, kwargs = joinall_mock.call_args_list[-1]
        assert kwargs == {}
        assert len(args[0]) == len(task_factory.tasks)
        joinall_mock.reset_mock()

        requests_event_mock.assert_called_once_with(
            request_type='ASYNC',
            name=f'{parent.user._scenario.identifier} {task_factory.name} (2)',
            response_time=ANY(int),
            response_length=len('hello world!') * 2,
            context=parent.user._context,
            exception=None,
        )
        requests_event_mock.reset_mock()

        # exception in one of the requests
        spawn_mock.return_value.get.side_effect = [RuntimeError, ({}, 'foo bar')]

        task(parent)

        requests_event_mock.assert_called_once_with(
            request_type='ASYNC',
            name=f'{parent.user._scenario.identifier} {task_factory.name} (2)',
            response_time=ANY(int),
            response_length=len('foo bar'),
            context=parent.user._context,
            exception=ANY(RuntimeError),
        )
        requests_event_mock.reset_mock()

        # exception before spawning greenlets, with a scenario failure exception
        joinall_mock.side_effect = [RuntimeError]
        parent.user._scenario.failure_handling.update({None: RestartScenario})

        with pytest.raises(RestartScenario):
            task(parent)

        # with greenlet trace method
        joinall_mock.side_effect = [RuntimeError]
        spawn_mock.reset_mock()
        try:
            environ['GEVENT_MONITOR_THREAD_ENABLE'] = 'true'
            parent.user._scenario.failure_handling.update({None: StopUser})

            with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
                task(parent)

            assert spawn_mock.call_count == 2
            assert settrace_mock.call_count == 2
            spawn_mock.reset_mock()
            settrace_mock.reset_mock()
        finally:
            del environ['GEVENT_MONITOR_THREAD_ENABLE']

    @pytest.mark.skip(reason='needs a webservice that sleeps')
    def test___call___real(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture(host='http://host.docker.internal:8002', user_type=RestApiUser, scenario_type=IteratorScenario)

        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        assert parent.user.host == 'http://host.docker.internal:8002'

        context_scenario = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('test scenario'), grizzly=grizzly_fixture.grizzly)
        parent.user._scenario = context_scenario

        task_factory = AsyncRequestGroupTask(name='test')

        task_factory.add(RequestTask(RequestMethod.GET, name='sleep-2', endpoint='/api/sleep/2'))
        task_factory.add(RequestTask(RequestMethod.GET, name='sleep-6', endpoint='/api/sleep/6'))
        task_factory.add(RequestTask(RequestMethod.GET, name='sleep-1', endpoint='/api/sleep/1'))

        assert len(task_factory.tasks) == 3

        task = task_factory()
        caplog.handler.formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

        with caplog.at_level(logging.DEBUG):
            task(parent)

        assert request_spy.call_count == 4
        request_spy.reset_mock()
