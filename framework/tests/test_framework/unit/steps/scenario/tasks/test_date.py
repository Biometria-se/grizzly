"""Unit tests of grizzly.steps.scenario.tasks.date."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps import step_task_date_parse
from grizzly.tasks import DateTask

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test_step_task_date_parse(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_date_parse(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable date_variable has not been initialized')]}

    grizzly.scenario.variables['date_variable'] = 'none'

    step_task_date_parse(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')

    assert len(grizzly.scenario.tasks()) == 1

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, DateTask)
    assert task.value == '{{ datetime.now() }}'
    assert task.variable == 'date_variable'
    assert task.arguments.get('offset') == '1D'
    templates = task.get_templates()
    assert len(templates) == 1
    assert templates[0] == '{{ datetime.now() }}'
