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
    Then get "foobar" from keystore and save in variable "foobar", with default value "{'hello': 'world'}"
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
    Then get "foobar" from keystore and save in variable "foobar"
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
    Then set "foobar" in keystore with value "{{ foobar }}"
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
