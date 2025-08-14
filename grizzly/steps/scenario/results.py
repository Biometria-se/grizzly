"""Module contains step implementations that validates the total response results for all [tasks][grizzly.tasks]
in a scenario, based on locust statistics (response time and failures).
"""

from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext, GrizzlyContextScenarioResponseTimePercentile
from grizzly.types.behave import Context, when


@when('fail ratio is greater than "{fail_ratio:d}"% fail scenario')
def step_results_fail_ratio(context: Context, fail_ratio: int) -> None:
    """Set how many percentages of requests that are allowed to fail before the whole scenario will be set as failed.

    Default behavior is not to validate the result for a scenario based on failed requests.

    Example:
    ```gherkin
    When fail ratio is greater than "8"% fail scenario
    ```

    Args:
        fail_ratio (int): percentage of requests that are allowed to fail

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    assert grizzly.scenario.failure_handling.get(None, None) is None, (
        f"cannot use step 'fail ratio is greater than \"{fail_ratio}\" fail scenario' together with 'on failure' steps"
    )
    grizzly.scenario.validation.fail_ratio = fail_ratio / 100.0


@when('average response time is greater than "{avg_response_time:d}" milliseconds fail scenario')
def step_results_average_response_time(context: Context, avg_response_time: int) -> None:
    """Set the average response time (milliseconds) that all requests in a scenario must be below
    for it to pass.

    Default behavior is not to validate the result for a scenario based on average response time.

    Example:
    ```gherkin
    When average response time is greater than "200" milliseconds fail scenario
    ```

    Args:
        avg_response_time (int): allowed average response time in milliseconds

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.validation.avg_response_time = avg_response_time


@when('response time percentile "{percentile:d}"% is greater than "{response_time:d}" milliseconds fail scenario')
def step_results_response_time_percentile(context: Context, percentile: float, response_time: int) -> None:
    """Set the response time that a specified percentile of the requests needs to be below for the scenario to pass.

    Default behavior is not to validate the result for a scenario based on percetile response times.

    Example:
    ```gherkin
    When response time percentile "95"% is greater than "200" milliseconds fail scenario
    ```

    Args:
        percentile (int): percentile to validate (1-100)
        response_time (int): response time in milliseconds

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.validation.response_time_percentile = GrizzlyContextScenarioResponseTimePercentile(
        response_time=response_time,
        percentile=percentile / 100.0,
    )
