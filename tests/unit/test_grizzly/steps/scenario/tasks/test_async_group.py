"""Unit tests of grizzly.steps.scenario.tasks.async_group."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.steps import step_task_async_group_close, step_task_async_group_start, step_task_request_text_with_name_endpoint
from grizzly.types import RequestDirection, RequestMethod

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_async_group_start(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'async_group', '') is None

    step_task_async_group_start(behave, 'async-test-1')

    assert grizzly.scenario.tasks.tmp.async_group is not None
    assert grizzly.scenario.tasks.tmp.async_group.name == 'async-test-1'

    with pytest.raises(AssertionError, match='async request group "async-test-1" has not been closed'):
        step_task_async_group_start(behave, 'async-test-2')


def test_step_task_async_group_end(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0
    assert getattr(grizzly.scenario.tasks.tmp, 'async_group', '') is None

    with pytest.raises(AssertionError, match='no async request group is open'):
        step_task_async_group_close(behave)

    step_task_async_group_start(behave, 'async-test-1')

    with pytest.raises(AssertionError, match='there are no requests in async group "async-test-1"'):
        step_task_async_group_close(behave)
    assert grizzly.scenario.tasks.tmp.async_group is not None

    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test', direction=RequestDirection.FROM, endpoint='/api/test')
    assert len(grizzly.scenario.tasks) == 0  # OK here

    step_task_async_group_close(behave)

    assert len(grizzly.scenario.tasks()) == 1
    assert grizzly.scenario.tasks.tmp.async_group is None
