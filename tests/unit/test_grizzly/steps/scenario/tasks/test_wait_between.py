"""Unit tests of grizzly.steps.scenario.tasks.wait_between."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps import step_task_wait_between_constant, step_task_wait_between_random
from grizzly.tasks import WaitBetweenTask

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_wait_between_random(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    step_task_wait_between_random(behave, 1.4, 1.7)

    assert len(grizzly.scenario.tasks()) == 1

    task = cast(WaitBetweenTask, grizzly.scenario.tasks()[-1])
    assert task.min_time == 1.4
    assert task.max_time == 1.7

    step_task_wait_between_random(behave, 30, 20)

    assert len(grizzly.scenario.tasks()) == 2

    task = cast(WaitBetweenTask, grizzly.scenario.tasks()[-1])
    assert task.min_time == 20
    assert task.max_time == 30


def test_step_task_wait_between_constant(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    step_task_wait_between_constant(behave, 10)

    assert len(grizzly.scenario.tasks()) == 1

    task = cast(WaitBetweenTask, grizzly.scenario.tasks()[-1])
    assert task.min_time == 10
    assert task.max_time is None
