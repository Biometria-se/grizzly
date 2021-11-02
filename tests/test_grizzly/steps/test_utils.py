import pytest

from behave.runner import Context

from grizzly.steps.utils import step_utils_fail

from ..fixtures import behave_context, locust_environment  # pylint: disable=unused-import


@pytest.mark.usefixtures('behave_context')
def test_step_utils_fail(behave_context: Context) -> None:
    with pytest.raises(AssertionError):
        step_utils_fail(behave_context)
