"""Unit tests of grizzly.tests.utils."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.steps.utils import step_utils_fail

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_utils_fail(behave_fixture: BehaveFixture) -> None:
    with pytest.raises(AssertionError):
        step_utils_fail(behave_fixture.context)
