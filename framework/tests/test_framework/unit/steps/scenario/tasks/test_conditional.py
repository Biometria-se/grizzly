"""Unit tests of grizzly.steps.scenario.tasks.conditional."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps import (
    step_task_async_group_close,
    step_task_async_group_open,
    step_task_conditional_else,
    step_task_conditional_end,
    step_task_conditional_if,
    step_task_log_message_print,
    step_task_request_text_with_name_endpoint,
    step_task_wait_between_constant,
)
from grizzly.types import RequestDirection, RequestMethod

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import ConditionalTask

    from test_framework.fixtures import BehaveFixture


def test_step_task_conditional_if(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.scenario.tasks.tmp, 'conditional', '') is None

    step_task_conditional_if(behave, '{{ value | int == 10 }}', 'conditional-1')

    assert grizzly.scenario.tasks.tmp.conditional is not None
    assert grizzly.scenario.tasks.tmp.conditional.name == 'conditional-1'
    assert grizzly.scenario.tasks.tmp.conditional._pointer
    assert grizzly.scenario.tasks.tmp.conditional.tasks == {True: []}

    step_task_wait_between_constant(behave, '1.4')
    step_task_log_message_print(behave, 'hello world')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-get', RequestDirection.FROM, '/api/test')
    step_task_async_group_open(behave, 'async-group')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-async-get-1', RequestDirection.FROM, '/api/test')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-async-get-2', RequestDirection.FROM, '/api/test')
    step_task_async_group_close(behave)

    assert list(grizzly.scenario.tasks.tmp.conditional.tasks.keys()) == [True]
    assert len(grizzly.scenario.tasks.tmp.conditional.tasks[True]) == 4

    step_task_conditional_if(behave, '{{ value | int == 20 }}', 'conditional-2')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='cannot create a new conditional while "conditional-1" is still open')]}


def test_step_task_conditional_else(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.scenario.tasks.tmp, 'conditional', '') is None

    step_task_conditional_else(behave)
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='there are no open conditional, you need to create one first')]}
    delattr(behave, 'exceptions')

    # run test to create prerequisites
    test_step_task_conditional_if(behave_fixture)

    assert grizzly.scenario.tasks.tmp.conditional is not None

    step_task_conditional_else(behave)

    assert grizzly.scenario.tasks.tmp.conditional.tasks.get(False, None) == []

    step_task_wait_between_constant(behave, '3.7')
    step_task_log_message_print(behave, 'foo bar')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-async-get-3', RequestDirection.FROM, '/api/test')

    assert list(grizzly.scenario.tasks.tmp.conditional.tasks.keys()) == [True, False]
    assert len(grizzly.scenario.tasks.tmp.conditional.tasks[True]) == 4
    assert len(grizzly.scenario.tasks.tmp.conditional.tasks[False]) == 3


def test_step_task_conditional_end(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'conditional', '') is None

    step_task_conditional_end(behave)
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='there are no open conditional, you need to create one before closing it')]}
    delattr(behave, 'exceptions')

    test_step_task_conditional_else(behave_fixture)

    assert grizzly.scenario.tasks.tmp.conditional is not None

    step_task_conditional_end(behave)

    assert len(grizzly.scenario.tasks()) == 1
    conditional = cast('ConditionalTask', grizzly.scenario.tasks()[-1])

    assert conditional.name == 'conditional-1'
    assert conditional.condition == '{{ value | int == 10 }}'
    assert list(conditional.tasks.keys()) == [True, False]
    assert len(conditional.tasks[True]) == 4
    assert len(conditional.tasks[False]) == 3
