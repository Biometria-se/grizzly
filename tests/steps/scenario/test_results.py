import pytest

from behave.runner import Context

from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ...fixtures import behave_context


@pytest.mark.usefixtures('behave_context')
def test_step_results_fail_ratio(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert context_locust.scenario.validation.fail_ratio is None
    assert not context_locust.scenario.should_validate()

    step_results_fail_ratio(behave_context, 10)

    assert context_locust.scenario.validation.fail_ratio == 0.1
    assert context_locust.scenario.should_validate()


@pytest.mark.usefixtures('behave_context')
def test_step_results_avg_response_time(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert context_locust.scenario.validation.avg_response_time is None
    assert not context_locust.scenario.should_validate()

    step_results_avg_response_time(behave_context, 200)

    assert context_locust.scenario.validation.avg_response_time == 200
    assert context_locust.scenario.should_validate()


@pytest.mark.usefixtures('behave_context')
def test_step_results_response_time_percentile(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert context_locust.scenario.validation.response_time_percentile is None
    assert not context_locust.scenario.should_validate()

    step_results_response_time_percentile(behave_context, 95, 800)

    assert context_locust.scenario.validation is not None
    assert context_locust.scenario.validation.response_time_percentile.percentile == 0.95
    assert context_locust.scenario.validation.response_time_percentile.response_time == 800
    assert context_locust.scenario.should_validate()
