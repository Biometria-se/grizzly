from typing import cast

import pytest

from behave.runner import Context

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ...fixtures import behave_context, locust_environment  # pylint: disable=unused-import


@pytest.mark.usefixtures('behave_context')
def test_step_results_fail_ratio(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert grizzly.scenario.validation.fail_ratio is None
    assert not grizzly.scenario.should_validate()

    step_results_fail_ratio(behave_context, 10)

    assert grizzly.scenario.validation.fail_ratio == 0.1
    assert grizzly.scenario.should_validate()


@pytest.mark.usefixtures('behave_context')
def test_step_results_avg_response_time(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert grizzly.scenario.validation.avg_response_time is None
    assert not grizzly.scenario.should_validate()

    step_results_avg_response_time(behave_context, 200)

    assert grizzly.scenario.validation.avg_response_time == 200
    assert grizzly.scenario.should_validate()


@pytest.mark.usefixtures('behave_context')
def test_step_results_response_time_percentile(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert grizzly.scenario.validation.response_time_percentile is None
    assert not grizzly.scenario.should_validate()

    step_results_response_time_percentile(behave_context, 95, 800)

    response_time_percentile = grizzly.scenario.validation.response_time_percentile
    assert response_time_percentile != None
    assert getattr(response_time_percentile, 'percentile', None) == 0.95
    assert getattr(response_time_percentile, 'response_time', None) == 800
    assert grizzly.scenario.should_validate()
