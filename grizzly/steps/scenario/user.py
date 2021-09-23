'''This module contains step implementations that describes a user.'''
from typing import cast

from behave.runner import Context
from behave import given  # pylint: disable=no-name-in-module

from ...context import LocustContext
from ...utils import resolve_variable


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

    context_locust = cast(LocustContext, context.locust)
    context_locust.scenario.user_class_name = user_class_name
    context_locust.scenario.context['host'] = resolve_variable(context_locust, host)
