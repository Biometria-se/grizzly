from typing import cast

from grizzly.types.behave import Context
from grizzly.context import GrizzlyContext

from tests.fixtures import End2EndFixture


def test_e2e_step_results_fail_ratio(e2e_fixture: End2EndFixture) -> None:
    def validate_fail_ratio(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.validation.fail_ratio == 96 / 100.0, f'{grizzly.scenario.validation.fail_ratio} != 0.96'

    e2e_fixture.add_validator(validate_fail_ratio)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'When fail ratio is greater than "96"% fail scenario',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_results_avg_response_time(e2e_fixture: End2EndFixture) -> None:
    def validate_avg_response_time(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.validation.avg_response_time == 500, f'{grizzly.scenario.validation.avg_response_time} != 500'

    e2e_fixture.add_validator(validate_avg_response_time)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'When average response time is greater than "500" milliseconds fail scenario',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_results_response_time_percentile(e2e_fixture: End2EndFixture) -> None:
    def validate_response_time_percentile(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.validation.response_time_percentile is not None
        assert grizzly.scenario.validation.response_time_percentile.response_time == 333, f'{grizzly.scenario.validation.response_time_percentile.response_time} != 333'
        assert grizzly.scenario.validation.response_time_percentile.percentile == 91 / 100.0, f'{grizzly.scenario.validation.response_time_percentile.percentile} != 0.91'

    e2e_fixture.add_validator(validate_response_time_percentile)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'When response time percentile "91"% is greater than "333" milliseconds fail scenario',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0
