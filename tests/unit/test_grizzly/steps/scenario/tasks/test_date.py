from typing import cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.tasks import DateTask
from grizzly.steps import step_task_date
from tests.fixtures import BehaveFixture


def test_step_task_date(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')
    assert 'variable date_variable has not been initialized' in str(ae)

    grizzly.state.variables['date_variable'] = 'none'

    step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')

    assert len(grizzly.scenario.tasks()) == 1

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, DateTask)
    assert task.value == '{{ datetime.now() }}'
    assert task.variable == 'date_variable'
    assert task.arguments.get('offset') == '1D'
    templates = task.get_templates()
    assert len(templates) == 1
    assert templates[0] == '{{ datetime.now() }}'
