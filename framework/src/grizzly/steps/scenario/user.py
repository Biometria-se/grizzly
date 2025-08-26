"""Module contains step implementations that describes a [load user][grizzly.users]."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from grizzly.locust import FixedUsersDispatcher
from grizzly.testdata.utils import resolve_variable
from grizzly.types.behave import Context, given
from grizzly.utils import has_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


def _setup_user(context: Context, user_class_name: str, host: str, *, weight: str | None = None, user_count: str | None = None, tag: str | None = None) -> None:
    grizzly = cast('GrizzlyContext', context.grizzly)

    if not user_class_name.endswith('User'):
        user_class_name = f'{user_class_name}User'

    grizzly.scenario.user.class_name = user_class_name
    grizzly.scenario.context['host'] = resolve_variable(grizzly.scenario, host)

    if user_count is not None and weight is not None:
        message = 'cannot combine fixed user count with user weights'
        raise AssertionError(message)

    # use using weights
    if weight is not None:
        weight_value = int(round(float(resolve_variable(grizzly.scenario, weight)), 0))

        assert weight_value > 0, f'weight value {weight} resolved to {weight_value}, which is not valid'
        grizzly.scenario.user.weight = weight_value

    # use fixed user count
    if user_count is not None:
        if (grizzly.setup.dispatcher_class is not None and grizzly.setup.dispatcher_class != FixedUsersDispatcher) or grizzly.setup.user_count is not None:
            message = 'this step cannot be used in combination with step `step_shapes_user_count`'
            raise AssertionError(message)

        grizzly.setup.dispatcher_class = FixedUsersDispatcher

        assert user_count[0] != '$', 'this expression does not support $conf or $env variables'
        user_count_value = max(int(round(float(resolve_variable(grizzly.scenario, user_count)), 0)), 1)

        if has_template(user_count):
            grizzly.scenario.orphan_templates.append(user_count)

        assert user_count_value > 0, f'{user_count} resolved to {user_count_value} users, which is not valid'

        grizzly.scenario.user.fixed_count = user_count_value
        grizzly.scenario.user.sticky_tag = tag


@given('"{user_count}" {grammar:UserGramaticalNumber} of type "{user_class_name}" with tag "{tag}" load testing "{host}"')
def step_user_type_with_count_and_tag(context: Context, user_count: str, user_class_name: str, tag: str, host: str, **_kwargs: Any) -> None:
    """Set which type of [load user][grizzly.users] the scenario should use, which `host` is the target, how many users that should be spawned
    and an associated tag.

    Users with the same `tag` value will run on the same set of workers, which will be exclusive for users with the same tag value.

    Example:
    ```gherkin
    Given "1" user of type "RestApi" with tag "foo" load testing "..."
    Given "10" users of type "MessageQueue" with tag "foo" load testing "..."
    Given "5" users of type "ServiceBus" with tag "bar" load testing "..."
    Given "1" user of type "BlobStorage" with tag "bar" load testing "..."
    ```

    Args:
        user_count (int): number of users locust should create
        user_class_name (str): name of an implementation of [load user][grizzly.users], with or without `User`-suffix
        tag (str): unique string to "stick" user types to a set of exclusive workers
        host (str): URL for the target host, format depends on which [load user][grizzly.users] is specified

    """
    _setup_user(context, user_class_name, host, user_count=user_count, tag=tag)


@given('"{user_count}" {grammar:UserGramaticalNumber} of type "{user_class_name}" load testing "{host}"')
def step_user_type_with_count(context: Context, user_count: str, user_class_name: str, host: str, **_kwargs: Any) -> None:
    """Set which type of [load user][grizzly.users] the scenario should use, which `host` is the target and how many users that should be spawned.

    Example:
    ```gherkin
    Given "1" user of type "RestApi" load testing "..."
    Given "10" users of type "MessageQueue" load testing "..."
    Given "5" users of type "ServiceBus" load testing "..."
    Given "1" user of type "BlobStorage" load testing "..."
    ```

    Args:
        user_count (int): number of users locust should create
        user_class_name (str): name of an implementation of [load user][grizzly.users], with or without `User`-suffix
        host (str): an URL for the target host, format depends on which [load user][grizzly.users] is specified

    """
    _setup_user(context, user_class_name, host, user_count=user_count)


@given('a user of type "{user_class_name}" with weight "{weight_value}" load testing "{host}"')
def step_user_type_with_weight(context: Context, user_class_name: str, weight_value: str, host: str) -> None:
    """Set which type of [load user][grizzly.users] the scenario should use and which `host` is the target,
    together with `weight` of the user (how many instances of this user should spawn relative to others).

    Example:
    ```gherkin
    Given a user of type "RestApi" with weight "2" load testing "..."
    Given a user of type "MessageQueue" with weight "1" load testing "..."
    Given a user of type "ServiceBus" with weight "1" load testing "..."
    Given a user of type "BlobStorage" with weight "4" load testing "..."
    ```

    Args:
        user_class_name (str): name of an implementation of [load user][grizzly.users], with or without `User`-suffix
        weight_value (int): weight value for the user, default is `1` (see [writing a locustfile](http://docs.locust.io/en/stable/writing-a-locustfile.html#weight-attribute))
        host (str): an URL for the target host, format depends on which [load user][grizzly.users] is specified

    """
    _setup_user(context, user_class_name, host, weight=weight_value)


@given('a user of type "{user_class_name}" load testing "{host}"')
def step_user_type(context: Context, user_class_name: str, host: str) -> None:
    """Set which type of [load user][grizzly.users] the scenario should use and which `host` is the target.

    Example:
    ```gherkin
    Given a user of type "RestApi" load testing "http://api.example.com"
    Given a user of type "MessageQueue" load testing "mq://mqm:secret@mq.example.com/?QueueManager=QMGR01&Channel=Channel01"
    Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789="
    Given a user of type "BlobStorage" load testing "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=examplestorage;AccountKey=xxxyyyyzzz=="
    ```

    Args:
        user_class_name (str): name of an implementation of [load user][grizzly.users], with or without `User`-suffix
        host (str): an URL for the target host, format depends on which [load user][grizzly.users] is specified

    """
    _setup_user(context, user_class_name, host)
