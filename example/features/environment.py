from typing import Any, Dict, Tuple, cast

from behave.runner import Context
from behave.model import Scenario

# pylint: disable=unused-import
from grizzly.environment import (  # noqa: F401
    before_feature as grizzly_before_feature,
    after_feature,
    before_scenario as grizzly_before_scenario,
    after_scenario,
    before_step,
)
from grizzly.context import GrizzlyContext
from grizzly.types import MessageDirection
from locust.exception import StopUser

from steps.custom import callback_server_client  # pylint: disable=import-error


def before_feature(context: Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    '''
    Overloads before_feature from grizzly, to register custom message types and their callbacks,
    which must be done before after_feature is called
    '''
    grizzly_before_feature(context, *args, **kwargs)

    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.setup.locust.messages.register(MessageDirection.SERVER_CLIENT, 'server_client', callback_server_client)


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
