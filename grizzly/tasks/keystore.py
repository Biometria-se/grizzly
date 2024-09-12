"""@anchor pydoc:grizzly.tasks.keystore Keystore task
This tasks sets and gets values from a distributed keystore. This makes is possible to share values between scenarios.

Retreived (get) values are rendered before setting the variable.
Stored (set) values are not rendered, so it is possible to store templates.


## Step implementations

* {@pylink grizzly.steps.scenario.tasks.keystore.step_task_keystore_get}

* {@pylink grizzly.steps.scenario.tasks.keystore.step_task_keystore_get_default}

* {@pylink grizzly.steps.scenario.tasks.keystore.step_task_keystore_set}

* {@pylink grizzly.steps.scenario.tasks.keystore.step_task_keystore_inc_default_step}

## Statistics

This task only has request statistics entry, of type `KEYS`, if a key (without `default_value`) that does not have a value set is retrieved.

## Arguments

* `key` _str_: name of key in keystore

* `action` _Action_: literal `set` or `get`

* `action_context` _str | Any_: when `action` is `get` it must be a `str` (variable name), for `set` any goes (as long as it is json serializable and not `None`)

* `default_value` _Any | None_: used when `action` is `get` and `key` does not exist in the keystore
"""
from __future__ import annotations

from json import dumps as jsondumps
from json import loads as jsonloads
from typing import TYPE_CHECKING, Any, Literal, cast, get_args

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario

Action = Literal['get', 'set', 'inc', 'push', 'pop', 'del']


@template('action_context', 'key')
class KeystoreTask(GrizzlyTask):
    key: str
    action: Action
    action_context: str | Any | None
    default_value: Any | None

    def __init__(self, key: str, action: Action, action_context: str | Any, default_value: Any | None = None) -> None:
        super().__init__()

        self.key = key
        self.action = action
        self.action_context = action_context
        self.default_value = default_value

        assert self.action in get_args(Action), f'"{self.action}" is not a valid action'

        if self.action in ['get', 'inc', 'pop']:
            assert isinstance(self.action_context, str), f'action context for "{self.action}" must be a string'
            assert action_context in self.grizzly.scenario.variables, f'variable "{action_context}" has not been initialized'
        elif self.action in ['set', 'push']:
            assert self.action_context is not None, f'action context for "{self.action}" must be declared'
        elif self.action in ['del']:
            assert self.action_context is None, f'action context for "{self.action}" cannot be declared'
        else:  # pragma: no cover
            pass

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:  # noqa: PLR0912
            key = parent.user.render(self.key)

            try:
                if self.action == 'get':
                    value = parent.consumer.keystore_get(key)

                    if value is None and self.default_value is not None:
                        parent.consumer.keystore_set(key, self.default_value)
                        value = cast(Any, self.default_value)

                    if value is not None and self.action_context is not None:
                        parent.user.set_variable(self.action_context, jsonloads(parent.user.render(jsondumps(value))))
                    else:
                        message = f'key {key} does not exist in keystore'
                        raise RuntimeError(message)
                elif self.action == 'inc':
                    value = parent.consumer.keystore_inc(key, step=1)

                    if value is not None and self.action_context is not None:
                        parent.user.set_variable(self.action_context, jsonloads(parent.user.render(jsondumps(value))))
                    else:
                        message = f'key {key} does not exist in keystore'
                        raise RuntimeError(message)
                elif self.action == 'set':
                    # do not render set values, might want it to be a template
                    parent.consumer.keystore_set(key, self.action_context)
                elif self.action == 'push':
                    parent.consumer.keystore_push(key, self.action_context)
                elif self.action == 'pop':
                    value = parent.consumer.keystore_pop(key)
                    if value is not None and self.action_context is not None:
                        parent.user.set_variable(self.action_context, jsonloads(parent.user.render(jsondumps(value))))
                elif self.action == 'del':
                    parent.consumer.keystore_del(key)
                else:  # pragma: no cover
                    pass
            except Exception as e:
                parent.user.logger.exception('keystore action %s failed', self.action)
                parent.user.environment.events.request.fire(
                    request_type='KEYS',
                    name=f'{parent.user._scenario.identifier} {key}',
                    response_time=0,
                    response_length=1,
                    context=parent.user._context,
                    exception=e,
                )

                if parent.user._scenario.failure_exception is not None:
                    raise parent.user._scenario.failure_exception from e

        return task
