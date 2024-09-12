"""@anchor pydoc:grizzly.steps.scenario.tasks.keystore Keystore
This module contains step implementations for the {@pylink grizzly.tasks.keystore} task.
"""
from __future__ import annotations

from json import JSONDecodeError
from json import loads as jsonloads
from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import KeystoreTask
from grizzly.types.behave import Context, then


@then('get "{key}" from keystore and save in variable "{variable}", with default value "{default_value}"')
def step_task_keystore_get_default(context: Context, key: str, variable: str, default_value: str) -> None:
    """Get a value for `key` using the {@pylink grizzly.tasks.keystore} task.

    If `key` does not exist in the keystore `default_value` will be used and will be set in the keystore to
    next iteration. `default_value` must be JSON serializable (string values must be single-quoted).

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "none"
    Then get "foobar_key" from keystore and save in variable "foobar", with default value "{'hello': 'world'}"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    if "'" in default_value:
        default_value = default_value.replace("'", '"')

    try:
        grizzly.scenario.tasks.add(KeystoreTask(key, 'get', variable, jsonloads(default_value)))
    except JSONDecodeError as e:
        message = f'"{default_value}" is not valid JSON'
        raise AssertionError(message) from e


@then('get "{key}" from keystore and save in variable "{variable}"')
def step_task_keystore_get(context: Context, key: str, variable: str) -> None:
    """Get a value for `key` using the {@pylink grizzly.tasks.keystore} task.

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "none"
    Then get "foobar_key" from keystore and save in variable "foobar"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'get', variable))


@then('set "{key}" in keystore with value "{value}"')
def step_task_keystore_set(context: Context, key: str, value: str) -> None:
    """Set a value for `key` using the {@pylink grizzly.tasks.keystore} task.

    `value` must be JSON serializable (string values must be single-quoted).

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "{'hello': 'world'}"
    Then set "foobar_key" in keystore with value "{{ foobar }}"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    if "'" in value:
        value = value.replace("'", '"')

    try:
        grizzly.scenario.tasks.add(KeystoreTask(key, 'set', jsonloads(value)))
    except JSONDecodeError as e:
        message = f'"{value}" is not valid JSON'
        raise AssertionError(message) from e


@then('increment "{key}" in keystore and save in variable "{variable}"')
def step_task_keystore_inc_default_step(context: Context, key: str, variable: str) -> None:
    """Increment the integer value for `key` (with step `1`) using the {@pylink grizzly.tasks.keystore} task.

    If there is no value for `key` incrementing will start from 0. The new value is saved in `variable`.

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    Given value for variable "counter" is "none"
    Then increment "counter_key" in keystore and save in variable "counter"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'inc', variable))


@then('pop "{key}" from keystore and save in variable "{variable}"')
def step_task_keystore_pop(context: Context, key: str, variable: str) -> None:
    """Pop a value for `key` using the {@pylink grizzly.tasks.keystore} task.

    This task will block the scenario until there is a value in the keystore for key `key`, in other words
    it can be used to share and synchronize different scenarios.

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    Scenario: push
        And value for variable "foobar" is "none"
        Then push "foobar_key" in keystore with value "foobar"

    Scenario: pop
        And value for variable "foobar" is "none"
        Then pop "foobar_key" from keystore and save in variable "foobar"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'pop', variable))


@then('push "{key}" in keystore with value "{value}"')
def step_task_keystore_push(context: Context, key: str, value: str) -> None:
    """Push a value for `key` using the {@pylink grizzly.tasks.keystore} task.

    `value` must be JSON serializable (string values must be single-quoted).

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    Scenario: push
        And value for variable "foobar" is "none"
        Then push "foobar_key" in keystore with value "foobar"

    Scenario: pop
        And value for variable "foobar" is "none"
        Then pop "foobar_key" from keystore and save in variable "foobar"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    if "'" in value:
        value = value.replace("'", '"')

    try:
        grizzly.scenario.tasks.add(KeystoreTask(key, 'push', jsonloads(value)))
    except JSONDecodeError as e:
        message = f'"{value}" is not valid JSON'
        raise AssertionError(message) from e


@then('remove "{key}" from keystore')
def step_task_keystore_del(context: Context, key: str) -> None:
    """Remove `key` using the {@pylink grizzly.tasks.keystore} task.

    See {@pylink grizzly.tasks.keystore} task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "hello"
    Then set "foobar_key" in keystore with value "{{ foobar }}"
    ...
    Then remove "foobar_key" from keystore
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'del', None))
