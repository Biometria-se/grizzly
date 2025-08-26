"""Module contains step implementations for the [Keystore][grizzly.tasks.keystore] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import KeystoreTask
from grizzly.types.behave import Context, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('get "{key}" from keystore and save in variable "{variable}", with default value "{default_value}"')
def step_task_keystore_get_default(context: Context, key: str, variable: str, default_value: str) -> None:
    """Get a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    If `key` does not exist in the keystore `default_value` will be used and will be set in the keystore to
    next iteration. `default_value` must be JSON serializable (string values must be single-quoted).

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "none"
    Then get "foobar_key" from keystore and save in variable "foobar", with default value "{'hello': 'world'}"
    ```

    Args:
        key (str): name of key
        variable (str): name of initialized variable to save keystore value in
        default_value (str): default value to use if value is missing in keystore

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'get', variable, default_value))


@then('get and remove "{key}" from keystore and save in variable "{variable}"')
def step_task_keystore_get_remove(context: Context, key: str, variable: str) -> None:
    """Get a value for `key` using the [Keystore][grizzly.tasks.keystore] task, then remove entry.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "none"
    Then get and remove "foobar_key" from keystore and save in variable "foobar"
    ```

    Args:
        key (str): name of key
        variable (str): name of initialized variable to save keystore value in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'get_del', variable))


@then('get "{key}" from keystore and save in variable "{variable}"')
def step_task_keystore_get(context: Context, key: str, variable: str) -> None:
    """Get a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "none"
    Then get "foobar_key" from keystore and save in variable "foobar"
    ```

    Args:
        key (str): name of key
        variable (str): name of initialized variable to save keystore value in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'get', variable))


@then('set "{key}" in keystore with value "{value}"')
def step_task_keystore_set(context: Context, key: str, value: str) -> None:
    """Set a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    `value` must be JSON serializable (string values must be single-quoted).

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "{'hello': 'world'}"
    Then set "foobar_key" in keystore with value "{{ foobar }} | render=True"
    ```

    Args:
        key (str): name of key
        value (str): JSON serializable value to save in keystore

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'set', value))


@then('set "{key}" in keystore with value')
def step_task_keystore_set_text(context: Context, key: str) -> None:
    """Set a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    `value` is specified via step text, and must be JSON serializable.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "{'hello': 'world'}"
    Then set "foobar_key" in keystore with value
    \"\"\"
    {{ foobar }} | render=True
    \"\"\"
    ```

    Args:
        key (str): name of key

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    assert context.text is not None, 'this step requires a value in the step text'
    grizzly.scenario.tasks.add(KeystoreTask(key, 'set', context.text))


@then('increment "{key}" in keystore and save in variable "{variable}"')
def step_task_keystore_increment_default_with_step(context: Context, key: str, variable: str) -> None:
    """Increment the integer value for `key` (with step `1`) using the [Keystore][grizzly.tasks.keystore] task.

    If there is no value for `key` incrementing will start from 0. The new value is saved in `variable`.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    Given value for variable "counter" is "none"
    Then increment "counter_key" in keystore and save in variable "counter"
    ```

    Args:
        key (str): name of key
        variable (str): name of initialized variable to save keystore value in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'inc', variable))


@then('decrement "{key}" in keystore and save in variable "{variable}"')
def step_task_keystore_decrement_default_with_step(context: Context, key: str, variable: str) -> None:
    """Decrement the integer value for `key` (with step `1`) using the [Keystore][grizzly.tasks.keystore] task.

    If there is no value for `key` decrementing will start from 0. The new value is saved in `variable`.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    Given value for variable "counter" is "none"
    Then decrement "counter_key" in keystore and save in variable "counter"
    ```

    Args:
        key (str): name of key
        variable (str): name of initialized variable to save keystore value in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'dec', variable))


@then('pop "{key}" from keystore and save in variable "{variable}"')
def step_task_keystore_pop(context: Context, key: str, variable: str) -> None:
    """Pop a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    This task will block the scenario until there is a value in the keystore for key `key`, in other words
    it can be used to share and synchronize different scenarios.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    Scenario: push
        And value for variable "foobar" is "none"
        Then push "foobar_key" in keystore with value "foobar"

    Scenario: pop
        And value for variable "foobar" is "none"
        Then pop "foobar_key" from keystore and save in variable "foobar"
    ```

    Args:
        key (str): name of key
        variable (str): name of initialized variable to save keystore value in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'pop', variable))


@then('push "{key}" in keystore with value "{value}"')
def step_task_keystore_push(context: Context, key: str, value: str) -> None:
    """Push a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    `value` must be JSON serializable.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    Scenario: push
        And value for variable "foobar" is "none"
        Then push "foobar_key" in keystore with value "foobar"

    Scenario: pop
        And value for variable "foobar" is "none"
        Then pop "foobar_key" from keystore and save in variable "foobar"
    ```

    Args:
        key (str): name of key
        value (str): JSON serializable value to push to keystore

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'push', value))


@then('push "{key}" in keystore with value')
def step_task_keystore_push_text(context: Context, key: str) -> None:
    """Push a value for `key` using the [Keystore][grizzly.tasks.keystore] task.

    `value` is specified via step text, and must be JSON serializable.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    Scenario: push
        And value for variable "foobar" is "none"
        Then push "foobar_key" in keystore with value
          \"\"\"
          {
            "id": 1,
            "name": test
          }
          \"\"\"

    Scenario: pop
        And value for variable "foobar" is "none"
        Then pop "foobar_key" from keystore and save in variable "foobar"
    ```

    Args:
        key (str): name of key

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    assert context.text is not None, 'this step requires a value in the step text'
    grizzly.scenario.tasks.add(KeystoreTask(key, 'push', context.text))


@then('remove "{key}" from keystore')
def step_task_keystore_remove(context: Context, key: str) -> None:
    """Remove `key` using the [Keystore][grizzly.tasks.keystore] task.

    See [Keystore][grizzly.tasks.keystore] task documentation for more information.

    Example:
    ```gherkin
    And value for variable "foobar" is "hello"
    Then set "foobar_key" in keystore with value "{{ foobar }}"
    ...
    Then remove "foobar_key" from keystore
    ```

    Args:
        key (str): name of key to remove

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(KeystoreTask(key, 'del', None))
