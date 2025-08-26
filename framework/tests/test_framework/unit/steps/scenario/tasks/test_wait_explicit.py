"""Unit tests of grizzly.steps.scenario.tasks.wait_explicit."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps import step_task_wait_explicit_static
from grizzly.tasks import ExplicitWaitTask

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test_step_task_wait_explicit_static(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_wait_explicit_static(behave, '-1.0')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='wait time cannot be less than 0.0 seconds')]}
    delattr(behave, 'exceptions')

    step_task_wait_explicit_static(behave, 'foobar')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"foobar" is not a template nor a float')]}
    delattr(behave, 'exceptions')

    step_task_wait_explicit_static(behave, '1.337')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, ExplicitWaitTask)
    assert task.time_expression == '1.337'

    grizzly.scenario.variables['wait_time'] = '126'

    step_task_wait_explicit_static(behave, '{{ wait_time }}')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, ExplicitWaitTask)
    assert task.time_expression == '{{ wait_time }}'
