"""Unique incremented integer each time it is accessed.

Useful to generate unique ID for each request.

## Format

The first value of an integer that is going to be used.

## Arguments

| Name      | Type   | Description                                                 | Default |
| --------- | ------ | ----------------------------------------------------------- | ------- |
| `step`    | `int`  | how much the value should increment each time               | `1`     |
| `persist` | `bool` | if the initial value should be persist and loaded from file | `False` |

## Example

```gherkin title="example.feature"
And value for variable "AtomicIntegerIncrementer.unique_id" is "100 | step=10"
And value for variable "AtomicIntegerIncrementer.persistent" is "10 | step=5, persist=True"
```

This can then be used in a template:

```json
{
    "id": {{ AtomicIntegerIncrementer.unique_id }}
}
```

Values of `AtomicIntegerIncrementer.unique_id`, per run and iteration:

1. Run

    1. `100`

    2. `110`

    3. `120`

    4. ...

2. Run

    1. `100`

    2. `110`

    3. `120`

    4. ...

Values of `AtomicIntegerIncrementer.persistent`, per run and iteration, for 4 iterations:

1. Run (`features/persistent/example.json` missing)

    1. `5`

    2. `15`

    3. `20`

    4. `25`

2. Run (`features/persistent/example.json` created by Run 1, due to `persistent=True`), initial
value `25 | step=5, persist=True` will be read from the file and override what is written in `example.feature`

    1. `30`

    2. `35`

    3. `40`

    4. `45`
"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, ClassVar, cast

from grizzly_common.arguments import parse_arguments, split_value
from grizzly_common.text import has_separator

from grizzly.types import StrDict, bool_type

from . import AtomicVariable, AtomicVariablePersist

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario


def atomicintegerincrementer__base_type__(value: str | int) -> str:
    if isinstance(value, int):
        return str(value)

    if has_separator('|', value):
        try:
            initial_value, incrementer_arguments = split_value(value)
            initial_value = str(int(float(initial_value)))
        except ValueError as e:
            message = f'AtomicIntegerIncrementer: "{value}" is not a valid initial value'
            raise ValueError(message) from e

        try:
            arguments = parse_arguments(incrementer_arguments)
        except ValueError as e:
            message = f'AtomicIntegerIncrementer: {e!s}'
            raise ValueError(message) from e

        if 'step' not in arguments:
            message = f'AtomicIntegerIncrementer: step is not specified: "{value}"'
            raise ValueError(message)

        for argument in arguments:
            if argument not in AtomicIntegerIncrementer.arguments:
                message = f'AtomicIntegerIncrementer: argument {argument} is not allowed'
                raise ValueError(message)

            AtomicIntegerIncrementer.arguments[argument](arguments[argument])

        value = f'{initial_value} | {incrementer_arguments}'
    else:
        try:
            value = str(int(float(value.strip())))
        except ValueError as e:
            message = f'AtomicIntegerIncrementer: "{value}" is not a valid initial value'
            raise ValueError(message) from e

    return value


class AtomicIntegerIncrementer(AtomicVariable[int], AtomicVariablePersist):
    __base_type__ = atomicintegerincrementer__base_type__

    __initialized: bool = False
    _steps: dict
    arguments: ClassVar[StrDict] = {'step': int, 'persist': bool_type}

    def __init__(self, *, scenario: GrizzlyContextScenario, variable: str, value: str | int, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            safe_value = self.__class__.__base_type__(value)

            if has_separator('|', safe_value):
                incrementer_value, incrementer_arguments = split_value(safe_value)
                initial_value = incrementer_value
                arguments = parse_arguments(incrementer_arguments)
                step = int(arguments['step'])
                state = self.__class__.arguments['persist'](arguments['persist']) if 'persist' in arguments else False
            else:
                initial_value = safe_value
                step = 1
                state = False

            super().__init__(scenario=scenario, variable=variable, value=int(initial_value), outer_lock=True)

            if self.__initialized:
                if variable not in self._steps:
                    self._steps[variable] = {'step': step, 'persist': state}

                return

            self._steps = {variable: {'step': step, 'persist': state}}
            self.__initialized = True

    def generate_initial_value(self, variable: str) -> str:
        """Generate next, persistent, initialization value."""
        persist = self._steps.get(variable, {}).get('persist', False)

        if not persist:
            message = f'{self.__class__.__name__}.{variable} should not be persisted'
            raise ValueError(message)

        value = self.__getitem__(variable)
        arguments = ', '.join([f'{key}={value}' for key, value in self._steps[variable].items()])

        return f'{value} | {arguments}'

    @classmethod
    def clear(cls: type[AtomicIntegerIncrementer]) -> None:
        super().clear()

        instances = cls._instances.get(cls, {})
        for scenario in instances:
            instance = cast('AtomicIntegerIncrementer', cls.get(scenario))
            variables = list(instance._steps.keys())
            for variable in variables:
                del instance._steps[variable]

    def __getitem__(self, variable: str) -> int | None:
        with self.semaphore():
            value = self._get_value(variable)

            if value is not None:
                self._values[variable] = value + self._steps[variable]['step']

            return value

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            with suppress(KeyError):
                del self._steps[variable]

            super().__delitem__(variable)
