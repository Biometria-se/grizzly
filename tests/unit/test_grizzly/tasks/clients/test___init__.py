import logging

import pytest

from _pytest.logging import LogCaptureFixture

from grizzly.scenarios import GrizzlyScenario, IteratorScenario
from grizzly.tasks.clients import ClientTask, client
from grizzly.types import GrizzlyResponse, RequestDirection
from grizzly.exceptions import StopUser, RestartScenario

from tests.fixtures import GrizzlyFixture, MockerFixture


def test_task_failing(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:

    @client('test')
    class TestTask(ClientTask):
        __scenario__ = grizzly_fixture.grizzly.scenario

        def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
            with self.action(parent):
                raise RuntimeError('failed get')

        def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
            return None, 'put'

    parent = grizzly_fixture(scenario_type=IteratorScenario)

    assert isinstance(parent, IteratorScenario)

    parent.user._context.update({'test': 'was here'})

    task_factory = TestTask(RequestDirection.FROM, 'test://foo.bar', 'dummy-stuff')

    task = task_factory()

    assert task_factory._context.get('test', None) is None
    parent.user._scenario.failure_exception = StopUser

    with pytest.raises(StopUser):
        task(parent)

    assert task_factory._context.get('test', None) == 'was here'

    parent.user._scenario.failure_exception = RestartScenario
    parent.user._context.update({'test': 'is here', 'foo': 'bar'})

    assert task_factory._context.get('foo', None) is None

    with pytest.raises(RestartScenario):
        task(parent)

    assert parent.user._context.get('test', None) == 'is here'
    assert parent.user._context.get('foo', None) == 'bar'

    parent.user._scenario.failure_exception = None

    task(parent)

    log_error_mock = mocker.patch.object(parent.stats, 'log_error')
    mocker.patch.object(parent, 'on_start', return_value=None)
    mocker.patch.object(parent, 'wait', side_effect=[NotImplementedError, NotImplementedError])
    parent.user.environment.catch_exceptions = True
    parent.user._scenario.failure_exception = RestartScenario

    parent.tasks.clear()
    parent._task_queue.clear()
    parent._task_queue.append(task)
    parent.task_count = 1

    with pytest.raises(NotImplementedError):
        with caplog.at_level(logging.INFO):
            parent.run()

    log_error_mock.assert_called_once_with(None)

    assert 'restarting scenario' in '\n'.join(caplog.messages)
