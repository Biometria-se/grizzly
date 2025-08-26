"""Unit tests of grizzly.tasks.clients."""

from __future__ import annotations

import logging
from contextlib import suppress
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from grizzly.exceptions import RestartScenario, StopUser
from grizzly.scenarios import GrizzlyScenario, IteratorScenario
from grizzly.tasks.clients import ClientTask, client
from grizzly.types import GrizzlyResponse, RequestDirection

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


@pytest.mark.parametrize('log_prefix', [False, True])
def test_task_failing(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture, *, log_prefix: bool) -> None:
    try:

        @client('test')
        class TestClientTask(ClientTask):
            __scenario__ = grizzly_fixture.grizzly.scenario

            def request_from(self, parent: GrizzlyScenario) -> GrizzlyResponse:
                with self.action(parent):
                    message = 'failed get'
                    raise RuntimeError(message)

            def request_to(self, _: GrizzlyScenario) -> GrizzlyResponse:
                return None, 'put'

        if log_prefix:
            environ['GRIZZLY_LOG_DIR'] = 'foobar'

        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        parent.user._context.update({'test': 'was here'})

        task_factory = TestClientTask(RequestDirection.FROM, 'test://foo.bar', 'dummy-stuff')

        assert (Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs').exists()

        if log_prefix:
            assert (Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs' / 'foobar').exists()
        else:
            assert not (Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs' / 'foobar').exists()

        task = task_factory()

        assert task_factory._context.get('test', None) is None
        parent.user._scenario.failure_handling.update({None: StopUser})

        with pytest.raises(StopUser):
            task(parent)

        assert task_factory._context.get('test', None) == 'was here'

        parent.user._scenario.failure_handling.update({None: RestartScenario})
        parent.user._context.update({'test': 'is here', 'foo': 'bar'})

        assert task_factory._context.get('foo', None) is None

        with pytest.raises(RestartScenario):
            task(parent)

        assert parent.user._context.get('test', None) == 'is here'
        assert parent.user._context.get('foo', None) == 'bar'

        del parent.user._scenario.failure_handling[None]

        task(parent)

        log_error_mock = mocker.patch.object(parent.stats, 'log_error')
        mocker.patch.object(parent, 'on_start', return_value=None)
        mocker.patch.object(parent, 'wait', side_effect=[NotImplementedError, NotImplementedError])
        parent.user.environment.catch_exceptions = True
        parent.user._scenario.failure_handling.update({None: RestartScenario})

        parent.tasks.clear()
        parent._task_queue.clear()
        parent._task_queue.append(task)
        parent.__class__.task_count = 1

        with pytest.raises(NotImplementedError), caplog.at_level(logging.INFO):
            parent.run()

        log_error_mock.assert_called_once_with(None)

        assert 'restarting scenario' in '\n'.join(caplog.messages)
    finally:
        with suppress(KeyError):
            del environ['GRIZZLY_LOG_DIR']
