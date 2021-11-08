'''This module contains step implementations that describes a user.'''
from typing import cast

from behave.runner import Context
from behave import given  # pylint: disable=no-name-in-module

from ...context import GrizzlyContext
from ...testdata.utils import resolve_variable


@given(u'a user of type "{user_class_name}" with weight "{weight_value}" load testing "{host}"')
def step_user_type_with_weight(context: Context, user_class_name: str, weight_value: str, host: str) -> None:
    '''Set which type of user the scenario should use and which host is the target,
    together with weight for the user (how much this user should spawn, relative to others).

    ```gherkin
    Given a user of type "RestApi" with weight "2" load testing "..."
    Given a user of type "MessageQueue" with weight "1" load testing "..."
    Given a user of type "ServiceBus" with weight "1" load testing "..."
    Given a user of type "BlobStorage" with weight "4" load testing "..."
    ```

    Args:
        user_class_name (str): name of an implementation in `grizzly.users`, with or without `User`
        weight_value (str): weight value for the user, default is '1' (see http://docs.locust.io/en/stable/writing-a-locustfile.html#weight-attribute)
        host (str): an URL for the target host, format depends on which `user_class_name` (see `grizzly.users`)
    '''
    if not user_class_name.endswith('User'):
        user_class_name = f'{user_class_name}User'

    grizzly = cast(GrizzlyContext, context.grizzly)
    weight = int(round(float(resolve_variable(grizzly, weight_value)), 0))

    assert weight > 0, f'weight value {weight_value} resolved to {weight}, which is not valid'

    grizzly.scenario.user.class_name = user_class_name
    grizzly.scenario.user.weight = weight
    grizzly.scenario.context['host'] = resolve_variable(grizzly, host)

@given(u'a user of type "{user_class_name}" load testing "{host}"')
def step_user_type(context: Context, user_class_name: str, host: str) -> None:
    '''Set which type of user the scenario should use and which host is the target.

    ```gherkin
    Given a user of type "RestApi" load testing "http://api.google.com"
    Given a user of type "MessageQueue" load testing "mq://mqm:secret@mq.example.com/?QueueManager=QMGR01&Channel=Channel01"
    Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789="
    Given a user of type "BlobStorage" load testing "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=examplestorage;AccountKey=xxxyyyyzzz=="
    ```

    Args:
        user_class_name (str): name of an implementation in `grizzly.users`, with or without `User`
        host (str): an URL for the target host, format depends on which `user_class_name` (see `grizzly.users`)
    '''
    if not user_class_name.endswith('User'):
        user_class_name = f'{user_class_name}User'

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.user.class_name = user_class_name
    grizzly.scenario.context['host'] = resolve_variable(grizzly, host)
