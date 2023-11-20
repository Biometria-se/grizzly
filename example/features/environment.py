# noqa: INP001, D100
# pyright: reportMissingImports=false
from typing import Any, cast

from behave.model import Scenario
from behave.runner import Context

from grizzly.behave import (  # noqa: F401
    after_feature,
    after_scenario,
    after_step,
    before_feature,
    before_step,
)
from grizzly.behave import (
    before_scenario as grizzly_before_scenario,
)
from grizzly.context import GrizzlyContext
from grizzly.exceptions import StopUser
from grizzly.testdata.utils import templatingfilter


@templatingfilter
def touppercase(value: str) -> str:
    return value.upper()


def before_scenario(
    context: Context, scenario: Scenario, *args: Any, **kwargs: Any,
) -> None:
    """Overload before_scenario from grizzly.
    To set "stop_on_failure" for all scenarios. This would be the same as having the following step for all Scenario in a Feature.

    ```gherkin
    And stop on first on first failure
    ```
    """
    grizzly_before_scenario(context, scenario, *args, **kwargs)

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.failure_exception = StopUser
