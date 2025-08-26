"""Unit tests of grizzly.steps.scenario.tasks.async_timer."""

from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly.steps import step_task_async_timer_start, step_task_async_timer_stop
from grizzly.tasks import AsyncTimerTask

from test_framework.helpers import SOME

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture


def test_step_task_async_timer_start(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    grizzly.scenario.tasks.clear()

    step_task_async_timer_start(behave, 'timer-1', 'foobar', '1')

    task_factory = grizzly.scenario.tasks()[-1]

    assert isinstance(task_factory, AsyncTimerTask)
    assert task_factory == SOME(AsyncTimerTask, tname='timer-1', tid='foobar', version='1', action='start')


def test_step_task_async_timer_stop(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    grizzly.scenario.tasks.clear()

    step_task_async_timer_stop(behave, 'timer-1', 'foobar', '1')

    task_factory = grizzly.scenario.tasks()[-1]

    assert isinstance(task_factory, AsyncTimerTask)
    assert task_factory == SOME(AsyncTimerTask, tname='timer-1', tid='foobar', version='1', action='stop')
