"""Tasks sets and gets values from a distributed keystore. This makes is possible to share values between scenarios.

Retreived (get) values are rendered before setting the variable.
Stored (set) values are not rendered, so it is possible to store templates.


## Step implementations

* [Get][grizzly.steps.scenario.tasks.keystore.step_task_keystore_get]

* [Get default][grizzly.steps.scenario.tasks.keystore.step_task_keystore_get_default]

* [Get remove][grizzly.steps.scenario.tasks.keystore.step_task_keystore_get_remove]

* [Set][grizzly.steps.scenario.tasks.keystore.step_task_keystore_set]

* [Set text][grizzly.steps.scenario.tasks.keystore.step_task_keystore_set_text]

* [Increment default with step][grizzly.steps.scenario.tasks.keystore.step_task_keystore_increment_default_with_step]

* [Decrement default with step][grizzly.steps.scenario.tasks.keystore.step_task_keystore_decrement_default_with_step]

* [Pop][grizzly.steps.scenario.tasks.keystore.step_task_keystore_pop]

* [Push][grizzly.steps.scenario.tasks.keystore.step_task_keystore_push]

* [Push text][grizzly.steps.scenario.tasks.keystore.step_task_keystore_push_text]

* [Remove][grizzly.steps.scenario.tasks.keystore.step_task_keystore_remove]

## Statistics

This task only has request statistics entry, of type `KEYS`, if a key (without `default_value`) that does not have a value set is retrieved.

## Arguments

| Name             | Type         | Description                                                                                                                       | Default    |
| ---------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `key`            | `str`        | name of key in keystore                                                                                                           | _required_ |
| `action`         | `Action`     | literal `set` or `get`                                                                                                            | _required_ |
| `action_context` | `str | Any`  | when `action` is `get` it must be a `str` (variable name), for `set` any goes (as long as it is json serializable and not `None`) | _required_ |
| `default_value`  | `Any | None` | used when `action` is `get` and `key` does not exist in the keystore                                                              | _required_ |

Values for `set` and `push` operations are not rendered by default, they will be pushed as is. By using argument `render`, it is possible to change this behaviour, e.g.:

```gherkin
Given value of variable "identification" is "foobar"
Then push "processed" in keystore with value "{{ identification }} | render=True"
```
"""

from __future__ import annotations

from json import JSONDecodeError
from json import dumps as jsondumps
from json import loads as jsonloads
from typing import TYPE_CHECKING, Any, Literal, cast, get_args

from grizzly_common.arguments import parse_arguments, split_value
from grizzly_common.text import has_separator

from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.utils import resolve_variable

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import StrDict

Action = Literal['get', 'get_del', 'set', 'inc', 'dec', 'push', 'pop', 'del']


@template('action_context', 'key')
class KeystoreTask(GrizzlyTask):
    key: str
    action: Action
    action_context: str | Any | None
    default_value: Any | None

    arguments: StrDict

    def __init__(self, key: str, action: Action, action_context: str | None, default_value: str | None = None) -> None:
        super().__init__(timeout=None)

        self.key = key
        self.action = action
        self.action_context = action_context
        self.default_value = self.json_serialize(default_value)
        self.arguments = {}

        if self.action_context is not None and isinstance(self.action_context, str) and has_separator('|', self.action_context):
            self.action_context, value_arguments = split_value(self.action_context)
            arguments = parse_arguments(value_arguments, unquote=True)
            for k, v in arguments.items():
                rendered_value = resolve_variable(self.grizzly.scenario, v)
                self.arguments.update({k: rendered_value})

        if has_separator('|', self.key):
            self.key, key_arguments = split_value(self.key)
            arguments = parse_arguments(key_arguments, unquote=True)
            for k, v in arguments.items():
                rendered_value = resolve_variable(self.grizzly.scenario, v)
                self.arguments.update({k: rendered_value})

        assert self.action in get_args(Action), f'"{self.action}" is not a valid action'

        if self.action in ['get', 'get_del', 'inc', 'dec', 'pop']:
            assert isinstance(self.action_context, str), f'action context for "{self.action}" must be a string'
            assert action_context in self.grizzly.scenario.variables, f'variable "{action_context}" has not been initialized'
        elif self.action in ['set', 'push']:
            assert self.action_context is not None, f'action context for "{self.action}" must be declared'
            self.action_context = self.json_serialize(self.action_context)
        elif self.action in ['del']:
            assert self.action_context is None, f'action context for "{self.action}" cannot be declared'
        else:  # pragma: no cover
            pass

    @classmethod
    def json_serialize(cls, value: str | None) -> Any | None:
        serialized_value: Any

        if value is None:
            return value

        serialized_value = GrizzlyVariables.guess_datatype(value)

        if not isinstance(serialized_value, str):
            return serialized_value

        if "'" in serialized_value:
            serialized_value = serialized_value.replace("'", '"')

        if not any(quote in serialized_value for quote in ['"', "'"]):
            serialized_value = f'"{serialized_value}"'

        try:
            serialized_value = jsonloads(serialized_value)
        except JSONDecodeError as e:
            message = f'"{serialized_value}" is not valid JSON'
            raise AssertionError(message) from e
        else:
            return serialized_value

    def __call__(self) -> grizzlytask:  # noqa: C901, PLR0915
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:  # noqa: PLR0912, PLR0915
            key = parent.user.render(self.key)

            def render(value: Any) -> Any:
                transform = not isinstance(value, str)
                template = jsondumps(value) if transform else value
                rendered_value = parent.user.render(template)

                return jsonloads(rendered_value) if transform else rendered_value

            try:
                if self.action in ['get', 'get_del']:
                    value = parent.consumer.keystore_get(key, remove=(self.action == 'get_del'))

                    if value is None and self.default_value is not None:
                        parent.consumer.keystore_set(key, self.default_value)
                        value = cast('Any', self.default_value)

                    if value is not None and self.action_context is not None:
                        parent.user.set_variable(self.action_context, render(value))
                    else:
                        message = f'key {key} does not exist in keystore'
                        raise RuntimeError(message)
                elif self.action in ['inc', 'dec']:
                    value = parent.consumer.keystore_inc(key, step=1) if self.action == 'inc' else parent.consumer.keystore_dec(key, step=1)

                    if value is not None and self.action_context is not None:
                        parent.user.set_variable(self.action_context, render(value))
                    else:
                        message = f'key {key} does not exist in keystore'
                        raise RuntimeError(message)
                elif self.action == 'set':
                    value = render(self.action_context) if self.arguments.get('render', False) else self.action_context
                    parent.consumer.keystore_set(key, value)
                elif self.action == 'push':
                    value = render(self.action_context) if self.arguments.get('render', False) else self.action_context
                    parent.consumer.keystore_push(key, value)
                elif self.action == 'pop':
                    wait = int(self.arguments.get('wait', '-1'))
                    poll_interval = float(self.arguments.get('interval', '1.0'))
                    value = parent.consumer.keystore_pop(key, wait=wait, poll_interval=poll_interval)
                    if value is not None and self.action_context is not None:
                        parent.user.set_variable(self.action_context, render(value))
                elif self.action == 'del':
                    parent.consumer.keystore_del(key)
                else:  # pragma: no cover
                    pass
            except Exception as e:
                error_message = str(e)
                if '::' in key:
                    """Last suffix (which is prefixed with '::') is considered a unique identifier"""
                    ambigous_key, _ = key.rsplit('::', 1)
                    ambigous_key = f'{ambigous_key}::{{{{ id }}}}'
                else:
                    ambigous_key = key

                if self.action not in ['pop'] or (self.action in ['pop'] and not isinstance(e, RuntimeError)):
                    parent.user.logger.exception('keystore action %s failed: %s', self.action, error_message)

                parent.user.environment.events.request.fire(
                    request_type='KEYS',
                    name=f'{parent.user._scenario.identifier} {ambigous_key}',
                    response_time=0,
                    response_length=1,
                    context=parent.user._context,
                    exception=e,
                )

                parent.user.failure_handler(e, task=self)

        return task
