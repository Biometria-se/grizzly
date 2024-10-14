"""Unit tests of grizzly.exceptions."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.exceptions import RestartScenario, RetryTask, StopUser, failure_handler

if TYPE_CHECKING:
    from tests.fixtures import GrizzlyFixture


def test_failure_handler(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly = grizzly_fixture.grizzly

    scenario = grizzly.scenario

    assert scenario.failure_handling == {}

    scenario.failure_handling.update({
        None: StopUser,
        '504 gateway timeout': RetryTask,
    })

    failure_handler(None, scenario)

    with pytest.raises(StopUser):
        failure_handler(RuntimeError('foobar'), scenario)

    with pytest.raises(RetryTask):
        failure_handler(RuntimeError('504 gateway timeout'), scenario)

    del scenario.failure_handling[None]

    failure_handler(RuntimeError('foobar'), scenario)

    with pytest.raises(RetryTask):
        failure_handler(RuntimeError('504 gateway timeout'), scenario)

    scenario.failure_handling.update({AttributeError: RestartScenario})

    with pytest.raises(StopUser):
        failure_handler(AttributeError('foobaz'), scenario)

    scenario.failure_handling.update({MemoryError: RestartScenario})

    with pytest.raises(RestartScenario):
        failure_handler(MemoryError('0% free'), scenario)
