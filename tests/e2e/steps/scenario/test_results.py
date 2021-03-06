from typing import cast

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ....fixtures import BehaveContextFixture


def test_e2e_step_results_fail_ratio(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_fail_ratio(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.validation.fail_ratio == 96 / 100.0, f'{grizzly.scenario.validation.fail_ratio} != 0.96'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_fail_ratio)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'When fail ratio is greater than "96"% fail scenario',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_results_avg_response_time(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_avg_response_time(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.validation.avg_response_time == 200, f'{grizzly.scenario.validation.avg_response_time} != 200'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_avg_response_time)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'When average response time is greater than "200" milliseconds fail scenario',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_results_response_time_percentile(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_response_time_percentile(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.validation.response_time_percentile is not None
        assert grizzly.scenario.validation.response_time_percentile.response_time == 333, f'{grizzly.scenario.validation.response_time_percentile.response_time} != 333'
        assert grizzly.scenario.validation.response_time_percentile.percentile == 91 / 100.0, f'{grizzly.scenario.validation.response_time_percentile.percentile} != 0.91'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_response_time_percentile)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'When response time percentile "91"% is greater than "333" milliseconds fail scenario',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0
