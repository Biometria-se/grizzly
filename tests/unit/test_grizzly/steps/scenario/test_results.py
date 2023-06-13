from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from tests.fixtures import BehaveFixture


def test_step_results_fail_ratio(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.validation.fail_ratio is None
    assert not grizzly.scenario.should_validate()

    step_results_fail_ratio(behave, 10)

    assert grizzly.scenario.validation.fail_ratio == 0.1
    assert grizzly.scenario.should_validate()


def test_step_results_avg_response_time(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.validation.avg_response_time is None
    assert not grizzly.scenario.should_validate()

    step_results_avg_response_time(behave, 200)

    assert grizzly.scenario.validation.avg_response_time == 200
    assert grizzly.scenario.should_validate()


def test_step_results_response_time_percentile(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.validation.response_time_percentile is None
    assert not grizzly.scenario.should_validate()

    step_results_response_time_percentile(behave, 95, 800)

    assert getattr(grizzly.scenario.validation, 'response_time_percentile', None) is not None
    response_time_percentile = grizzly.scenario.validation.response_time_percentile
    assert getattr(response_time_percentile, 'percentile', None) == 0.95
    assert getattr(response_time_percentile, 'response_time', None) == 800
    assert grizzly.scenario.should_validate()
