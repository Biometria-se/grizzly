"""Unit tests of grizzly.steps.scenario.tasks.log_message."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.context import GrizzlyContext
from grizzly.steps import step_task_log_message
from grizzly.tasks import LogMessageTask

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_log_message(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    step_task_log_message(behave, 'hello {{ world }}')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, LogMessageTask)
    assert task.message == 'hello {{ world }}'
