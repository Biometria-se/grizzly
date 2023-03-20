import pytest

from grizzly.steps.utils import step_utils_fail

from tests.fixtures import BehaveFixture


def test_step_utils_fail(behave_fixture: BehaveFixture) -> None:
    with pytest.raises(AssertionError):
        step_utils_fail(behave_fixture.context)
