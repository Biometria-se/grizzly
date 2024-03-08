"""Unit tests of grizzly.steps.scenario.tasks.write_file."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.context import GrizzlyContext
from grizzly.steps import step_task_write_file
from grizzly.tasks import WriteFileTask

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_write_file(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    step_task_write_file(behave, '{{ hello }}', 'output/output.txt')

    assert len(grizzly.scenario.tasks()) == 1

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, WriteFileTask)
    assert task.file_name == 'output/output.txt'
    assert task.content == '{{ hello }}'
