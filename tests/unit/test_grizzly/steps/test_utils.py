"""Unit tests of grizzly.tests.utils."""
from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly.steps.utils import step_utils_fail
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_utils_fail(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    assert behave.exceptions == {}

    step_utils_fail(behave)

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='manually failed')]}
