"""Unit tests of grizzly.steps.scenario.tasks.loop."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.steps import (
    step_setup_variable_value,
    step_task_conditional_end,
    step_task_conditional_if,
    step_task_loop_end,
    step_task_loop_start,
    step_task_request_text_with_name_endpoint,
)
from grizzly.types import RequestDirection, RequestMethod

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_loop(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'loop', '') is None

    with pytest.raises(ValueError, match='LoopTask: foobar has not been initialized'):
        step_task_loop_start(behave, '["hello", "world"]', 'foobar', 'test-loop')

    step_setup_variable_value(behave, 'foobar', 'none')

    step_task_loop_start(behave, '["hello", "world"]', 'foobar', 'test-loop')

    assert grizzly.scenario.tasks.tmp.loop is not None
    assert len(grizzly.scenario.tasks()) == 0
    assert len(grizzly.scenario.tasks) == 0  # OK here

    assert grizzly.scenario.tasks.tmp.loop.name == 'test-loop'
    assert grizzly.scenario.tasks.tmp.loop.values == '["hello", "world"]'
    assert grizzly.scenario.tasks.tmp.loop.variable == 'foobar'
    assert len(grizzly.scenario.tasks.tmp.loop.tasks) == 0

    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-get-1', RequestDirection.FROM, '/api/test/1')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-get-2', RequestDirection.FROM, '/api/test/2')

    step_task_conditional_if(behave, '{{ value | int > 0 }}', 'test-conditional')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-cond-get-1', RequestDirection.FROM, '/api/test/1')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-cond-get-2', RequestDirection.FROM, '/api/test/2')
    step_task_conditional_end(behave)

    assert len(grizzly.scenario.tasks.tmp.loop.tasks) == 3
    assert len(grizzly.scenario.tasks()) == 3

    step_task_loop_end(behave)

    assert len(grizzly.scenario.tasks()) == 1
    assert getattr(grizzly.scenario.tasks.tmp, 'loop', '') is None

    with pytest.raises(AssertionError, match='there are no open loop, you need to create one before closing it'):
        step_task_loop_end(behave)
