# pyright: reportMissingImports=false
from typing import Any, Dict, Tuple, cast

from behave.runner import Context
from behave.model import Scenario

# pylint: disable=unused-import
from grizzly.behave import (  # noqa: F401
    before_feature,
    after_feature,
    before_scenario as grizzly_before_scenario,
    after_scenario,
    before_step,
    after_step,
)
from grizzly.context import GrizzlyContext
from grizzly.exceptions import StopUser
from grizzly.testdata.utils import templatingfilter


@templatingfilter
def touppercase(value: str) -> str:
    return value.upper()


def before_scenario(
    context: Context, scenario: Scenario, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]
) -> None:
    '''
    Overloads before_scenario from grizzly, to set "stop_on_failure" for all scenarios.
    This would be the same as having the following step for all Scenario in a Feature:

    ``` gherkin
    And stop on first on first failure
    ```
    '''
    grizzly_before_scenario(context, scenario, *args, **kwargs)

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.failure_exception = StopUser
