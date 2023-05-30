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
        def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
            with self.action(parent):
                raise RuntimeError('failed get')

        def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
            return None, 'put'

    parent = grizzly_fixture(scenario_type=IteratorScenario)

    assert isinstance(parent, IteratorScenario)

    task_factory = TestTask(RequestDirection.FROM, 'test://foo.bar', 'dummy-stuff')

    task = task_factory()

    parent.user._scenario.failure_exception = StopUser

    with pytest.raises(StopUser):
        task(parent)

    parent.user._scenario.failure_exception = RestartScenario

    with pytest.raises(RestartScenario):
        task(parent)

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
