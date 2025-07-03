# noqa: INP001, D100
# pyright: reportMissingImports=false
from typing import TYPE_CHECKING, Any, cast

from behave.model import Scenario
from behave.runner import Context

from grizzly.behave import (
    after_feature,
    after_scenario,
    after_step,
    before_feature,
    before_step,
)
from grizzly.behave import (
    before_scenario as grizzly_before_scenario,
)
from grizzly.exceptions import StopUser
from grizzly.testdata.filters import templatingfilter

if TYPE_CHECKING:
    from grizzly.context import GrizzlyContext


@templatingfilter
def touppercase(value: str) -> str:
    return value.upper()


def before_scenario(
    context: Context,
    scenario: Scenario,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Overload before_scenario from grizzly.
    To set "stop_on_failure" for all scenarios. This would be the same as having the following step for all Scenario in a Feature.

    ```gherkin
    And stop on first on first failure
    ```
    """
    grizzly_before_scenario(context, scenario, *args, **kwargs)

    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.failure_handling.update({None: StopUser})
