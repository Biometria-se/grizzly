"""Unit tests of grizzly.steps.scenario.tasks.timer."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.steps import step_task_timer_start, step_task_timer_stop
from grizzly.tasks import TimerTask

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_timer_start_and_stop(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.tasks.tmp.timers == {}

    with pytest.raises(AssertionError, match='timer with name test-timer-1 has not been defined'):
        step_task_timer_stop(behave, 'test-timer-1')

    step_task_timer_start(behave, 'test-timer-1')

    timer = grizzly.scenario.tasks.tmp.timers.get('test-timer-1', None)
    assert isinstance(timer, TimerTask)
    assert timer.name == 'test-timer-1'
    assert grizzly.scenario.tasks()[-1] is timer

    with pytest.raises(AssertionError, match='timer with name test-timer-1 has already been defined'):
        step_task_timer_start(behave, 'test-timer-1')

    step_task_timer_stop(behave, 'test-timer-1')

    assert grizzly.scenario.tasks.tmp.timers == {
        'test-timer-1': None,
    }

    assert grizzly.scenario.tasks()[-2] is grizzly.scenario.tasks()[-1]
