"""
@anchor pydoc:grizzly.tasks.keystore Keystore task
This tasks sets and gets values from a distributed keystore. This makes is possible to share values between scenarios.

Retreived (get) values are rendered before setting the variable.
Stored (set) values are not rendered, so it is possible to store templates.

The whole keystore is persistent, so anything stored will be loaded the next time the scenario runs.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_keystore_get}

* {@pylink grizzly.steps.scenario.tasks.step_task_keystore_get_default}

* {@pylink grizzly.steps.scenario.tasks.step_task_keystore_set}

## Statistics

This task only has request statistics entry, of type `KEYS`, if a key (without `default_value`) that does not have a value set is retrieved.

## Arguments

* `key` _str_: name of key in keystore

* `action` _Action_: literal `set` or `get`

* `action_context` _str | Any_: when `action` is `get` it must be a `str` (variable name), for `set` any goes (as long as it is json serializable and not `None`)

* `default_value` _Any (Optional)_: used when `action` is `get` and `key` does not exist in the keystore
"""
from __future__ import annotations
from typing import Any, Literal, Optional, Union, TYPE_CHECKING, cast
from json import loads as jsonloads, dumps as jsondumps

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario

Action = Literal['get', 'set']


@template('action_context')
class KeystoreTask(GrizzlyTask):
    key: str
    action: Action
    action_context: Union[str, Optional[Any]]
    default_value: Optional[Any]

    def __init__(self, key: str, action: Action, action_context: Union[str, Any], default_value: Optional[Any] = None) -> None:
        super().__init__()

        self.key = key
        self.action = action
        self.action_context = action_context
        self.default_value = default_value

        if self.action == 'get':
            assert isinstance(self.action_context, str), 'action context for get must be a string'
            assert action_context in self.grizzly.state.variables, f'{action_context} has not been initialized'
        elif self.action == 'set':
            assert self.action_context is not None, 'action context for set cannot be None'
        else:
            raise AssertionError(f'{self.action} is not a valid action')

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            try:
                if self.action == 'get':
                    value = parent.consumer.keystore_get(self.key)

                    if value is None and self.default_value is not None:
                        parent.consumer.keystore_set(self.key, self.default_value)
                        value = cast(Any, self.default_value)

                    if value is not None:
                        parent.user._context['variables'][self.action_context] = jsonloads(parent.render(jsondumps(value)))
                    else:
                        raise RuntimeError(f'key {self.key} does not exist in keystore')
                elif self.action == 'set':
                    # do not render set values, might want it to be a template
                    parent.consumer.keystore_set(self.key, self.action_context)
                else:  # pragma: no cover
                    pass
            except Exception as e:
                parent.user.logger.error(str(e))
                parent.user.environment.events.request.fire(
                    request_type='KEYS',
                    name=f'{parent.user._scenario.identifier} {self.key}',
                    response_time=0,
                    response_length=1,
                    context=parent.user._context,
                    exception=e,
                )

                if parent.user._scenario.failure_exception is not None:
                    raise parent.user._scenario.failure_exception()

        return task
