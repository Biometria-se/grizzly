from typing import cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.tasks import ExplicitWaitTask
from grizzly.steps import step_task_wait_explicit

from tests.fixtures import BehaveFixture


def test_step_task_wait_explicit(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_wait_explicit(behave, '-1.0')
    assert str(ae.value) == 'wait time cannot be less than 0.0 seconds'

    with pytest.raises(AssertionError) as ae:
        step_task_wait_explicit(behave, 'foobar')
    assert str(ae.value) == '"foobar" is not a template nor a float'

    step_task_wait_explicit(behave, '1.337')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, ExplicitWaitTask)
    assert task.time_expression == '1.337'

    grizzly.state.variables['wait_time'] = '126'

    step_task_wait_explicit(behave, '{{ wait_time }}')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, ExplicitWaitTask)
    assert task.time_expression == '{{ wait_time }}'
