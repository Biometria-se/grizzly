"""Unit tests of grizzly.steps.scenario.tasks.wait_explicit."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import step_task_wait_explicit
from grizzly.tasks import ExplicitWaitTask

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_wait_explicit(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError, match='wait time cannot be less than 0.0 seconds'):
        step_task_wait_explicit(behave, '-1.0')

    with pytest.raises(AssertionError, match='"foobar" is not a template nor a float'):
        step_task_wait_explicit(behave, 'foobar')

    step_task_wait_explicit(behave, '1.337')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, ExplicitWaitTask)
    assert task.time_expression == '1.337'

    grizzly.state.variables['wait_time'] = '126'

    step_task_wait_explicit(behave, '{{ wait_time }}')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, ExplicitWaitTask)
    assert task.time_expression == '{{ wait_time }}'
