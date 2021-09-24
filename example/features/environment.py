from typing import Any, Dict, Tuple, cast

from behave.runner import Context
from behave.model import Scenario

# pylint: disable=unused-import
from grizzly.environment import (
    before_feature,
    after_feature,
    before_scenario as grizzly_before_scenario,
    after_scenario,
    before_step,
)
from grizzly.context import LocustContext

def before_scenario(
    context: Context, scenario: Scenario, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]
) -> None:
    '''
    Overloads before_scenario from grizzly, to set "stop_on_failure" for all scenarios.
    This would be the same as having the following step for all Scenario in a Feature:

    ```gherking
    And stop on first on first failure
    ```
    '''
    grizzly_before_scenario(context, scenario, *args, **kwargs)

    context_locust = cast(LocustContext, context.locust)
    context_locust.scenario.stop_on_failure = True
