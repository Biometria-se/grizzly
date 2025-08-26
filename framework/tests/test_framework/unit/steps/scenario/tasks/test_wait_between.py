"""Unit tests of grizzly.steps.scenario.tasks.wait_between."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps import step_task_wait_between_constant, step_task_wait_between_random

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import WaitBetweenTask

    from test_framework.fixtures import BehaveFixture


def test_step_task_wait_between_random(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    step_task_wait_between_random(behave, '1.4', '1.7')

    assert len(grizzly.scenario.tasks()) == 1

    task = cast('WaitBetweenTask', grizzly.scenario.tasks()[-1])
    assert task.min_time == '1.4'
    assert task.max_time == '1.7'

    grizzly.state.configuration.update({'foo.bar': '20'})

    step_task_wait_between_random(behave, '30', '$conf::foo.bar$')

    assert len(grizzly.scenario.tasks()) == 2

    task = cast('WaitBetweenTask', grizzly.scenario.tasks()[-1])
    assert task.min_time == '30'
    assert task.max_time == '20'

    step_task_wait_between_random(behave, '{{ min_wait_time }}', '$conf::foo.bar$')

    assert len(grizzly.scenario.tasks()) == 3

    task = cast('WaitBetweenTask', grizzly.scenario.tasks()[-1])
    assert task.min_time == '{{ min_wait_time }}'
    assert task.max_time == '20'
    assert task.get_templates() == ['{{ min_wait_time }}']


def test_step_task_wait_between_constant(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    step_task_wait_between_constant(behave, '10')

    assert len(grizzly.scenario.tasks()) == 1

    task = cast('WaitBetweenTask', grizzly.scenario.tasks()[-1])
    assert task.min_time == '10'
    assert task.max_time is None

    grizzly.state.configuration.update({'foo.bar': '10'})

    step_task_wait_between_constant(behave, '$conf::foo.bar$')

    assert len(grizzly.scenario.tasks()) == 2

    task = cast('WaitBetweenTask', grizzly.scenario.tasks()[-1])
    assert task.min_time == '10'
    assert task.max_time is None

    step_task_wait_between_constant(behave, '{{ wait_time }}')

    assert len(grizzly.scenario.tasks()) == 3

    task = cast('WaitBetweenTask', grizzly.scenario.tasks()[-1])
    assert task.min_time == '{{ wait_time }}'
    assert task.max_time is None
    assert task.get_templates() == ['{{ wait_time }}']
