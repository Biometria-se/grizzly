import pytest

from grizzly.types import RequestDirection, RequestMethod
from grizzly.steps import step_task_async_group_start, step_task_async_group_close, step_task_request_text_with_name_endpoint
from tests.fixtures import BehaveFixture


def test_step_task_async_group_start(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'async_group', '') is None

    step_task_async_group_start(behave, 'async-test-1')

    assert grizzly.scenario.tasks.tmp.async_group is not None
    assert grizzly.scenario.tasks.tmp.async_group.name == 'async-test-1'

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_start(behave, 'async-test-2')
    assert str(ae.value) == 'async request group "async-test-1" has not been closed'


def test_step_task_async_group_end(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0
    assert getattr(grizzly.scenario.tasks.tmp, 'async_group', '') is None

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_close(behave)
    assert str(ae.value) == 'no async request group is open'

    step_task_async_group_start(behave, 'async-test-1')

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_close(behave)
    assert str(ae.value) == 'there are no requests in async group "async-test-1"'
    assert grizzly.scenario.tasks.tmp.async_group is not None

    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test', direction=RequestDirection.FROM, endpoint='/api/test')
    assert len(grizzly.scenario.tasks) == 0  # OK here

    step_task_async_group_close(behave)

    assert len(grizzly.scenario.tasks()) == 1
    assert grizzly.scenario.tasks.tmp.async_group is None
