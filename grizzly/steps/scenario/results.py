'''
@anchor pydoc:grizzly.steps.scenario.results Results
This module contains step implementations that validates the total response results for all {@pylink grizzly.tasks} in a scenario, based on
locust statistics like response time and failures.
'''
from typing import cast

from behave.runner import Context
from behave import when  # pylint: disable=no-name-in-module

from ...context import GrizzlyContext, GrizzlyContextScenarioResponseTimePercentile


@when(u'fail ratio is greater than "{fail_ratio:d}"% fail scenario')
def step_results_fail_ratio(context: Context, fail_ratio: int) -> None:
    '''Sets how many percentages of requests that are allowed to fail before the whole scenario will be set as failed.

    This step cannot be used in combination with:

    * {@pylink grizzly.steps.scenario.setup.step_setup_stop_user_on_failure}

    * {@pylink grizzly.steps.scenario.setup.step_setup_restart_scenario_on_failure}

    Default behavior is not to validate the result for a scenario based on failed requests.

    Example:

    ``` gherkin
    When fail ratio is greater than "8"% fail scenario
    ```

    Args:
        fail_ratio (int): percentage of requests that are allowed to fail
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    assert grizzly.scenario.failure_exception is None, f"cannot use step 'fail ratio is greater than \"{fail_ratio}\" fail scenario' togheter with 'on failure' steps"
    grizzly.scenario.validation.fail_ratio = fail_ratio / 100.0


@when(u'average response time is greater than "{avg_response_time:d}" milliseconds fail scenario')
def step_results_avg_response_time(context: Context, avg_response_time: int) -> None:
    '''Sets the average response time (milliseconds) that all requests in a scenario must be below
    for it to pass.

    Default behavior is not to validate the result for a scenario based on average response time.

    Example:

    ``` gherkin
    When average response time is greater than "200" milliseconds fail scenario
    ```

    Args:
        avg_response_time (int): allowed average response time in milliseconds
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.validation.avg_response_time = avg_response_time


@when(u'response time percentile "{percentile:d}"% is greater than "{response_time:d}" milliseconds fail scenario')
def step_results_response_time_percentile(context: Context, percentile: float, response_time: int) -> None:
    '''Sets the response time that a specified percentile of the requests needs to be below for the scenario to pass.

    Default behavior is not to validate the result for a scenario based on percetile response times.

    Example:

    ``` gherkin
    When response time percentile "95"% is greater than "200" milliseconds fail scenario
    ```

    Args:
        percentile (int): percentile to validate (1-100)
        response_time (int): response time in milliseconds
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.validation.response_time_percentile = GrizzlyContextScenarioResponseTimePercentile(
        response_time=response_time,
        percentile=percentile / 100.0,
    )
