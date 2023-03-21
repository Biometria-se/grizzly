'''
@anchor pydoc:grizzly.testdata.variables.integer_incrementer Integer Incrementer
This variable provides an unique integer each time it is accessed.

Useful to generate unique ID for each request.

## Format

The first value of an integer that is going to be used.

## Arguments

* `step` _int_, (optional) - how much the value should increment each time (default `1`)

* `persist` _bool_, (optional) - if the initial value should be persist and loaded from file (default `False`)

## Example

``` gherkin title="example.feature"
And value for variable "AtomicIntegerIncrementer.unique_id" is "100 | step=10"
And value for variable "AtomicIntegerIncrementer.persistent" is "10 | step=5, persist=True"
```

This can then be used in a template:

``` json
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

Values of `AtomicIntegerIncrementer.persistent`, per run and iteration:

1. Run (`features/persistent/example.json` missing)

    1. `5`

    2. `15`

    3. `20`

    4. ...

2. Run (`features/persistent/example.json` created by Run 1, due to `persistent=True`), initial
value `35 | step=5, persist=True` will be read from the file and override what is written in `example.feature`

    1. `25`

    2. `30`

    3. `35`

    4. ...
'''
from typing import Union, Dict, Any, Type, Optional, cast

from grizzly_extras.arguments import split_value, parse_arguments

from grizzly.types import bool_type

from . import AtomicVariable, AtomicVariablePersist


def atomicintegerincrementer__base_type__(value: Union[str, int]) -> str:
    if isinstance(value, int):
        return str(value)

    if '|' in value:
        try:
            initial_value, incrementer_arguments = split_value(value)
            initial_value = str(int(float(initial_value)))
        except ValueError as e:
            raise ValueError(f'AtomicIntegerIncrementer: "{value}" is not a valid initial value') from e

        try:
            arguments = parse_arguments(incrementer_arguments)
        except ValueError as e:
            raise ValueError(f'AtomicIntegerIncrementer: {str(e)}') from e

        if 'step' not in arguments:
            raise ValueError(f'AtomicIntegerIncrementer: step is not specified: "{value}"')

        for argument in arguments:
            if argument not in AtomicIntegerIncrementer.arguments:
                raise ValueError(f'AtomicIntegerIncrementer: argument {argument} is not allowed')

            AtomicIntegerIncrementer.arguments[argument](arguments[argument])

        value = f'{initial_value} | {incrementer_arguments}'
    else:
        try:
            value = str(int(float(value.strip())))
        except ValueError as e:
            raise ValueError(f'AtomicIntegerIncrementer: "{value}" is not a valid initial value') from e

    return value


class AtomicIntegerIncrementer(AtomicVariable[int], AtomicVariablePersist):
    __base_type__ = atomicintegerincrementer__base_type__

    __initialized: bool = False
    _steps: Dict[str, Any]
    arguments: Dict[str, Any] = {'step': int, 'persist': bool_type}

    def __init__(self, variable: str, value: Union[str, int], outer_lock: bool = False) -> None:
        with self.semaphore(outer_lock):
            safe_value = self.__class__.__base_type__(value)

            if '|' in safe_value:
                incrementer_value, incrementer_arguments = split_value(safe_value)
                initial_value = incrementer_value
                arguments = parse_arguments(incrementer_arguments)
                step = int(arguments['step'])
                state = self.__class__.arguments['persist'](arguments['persist']) if 'persist' in arguments else False
            else:
                initial_value = safe_value
                step = 1
                state = False

            super().__init__(variable, int(initial_value), outer_lock=True)

            if self.__initialized:
                if variable not in self._steps:
                    self._steps[variable] = {'step': step, 'persist': state}

                return

            self._steps = {variable: {'step': step, 'persist': state}}
            self.__initialized = True

    def generate_initial_value(self, variable: str) -> str:
        persist = self._steps.get(variable, {}).get('persist', False)

        if not persist:
            raise ValueError(f'{self.__class__.__name__}.{variable} should not be persisted')

        value = self.__getitem__(variable)
        arguments = ', '.join([f'{key}={value}' for key, value in self._steps[variable].items()])

        return f'{value} | {arguments}'

    @classmethod
    def clear(cls: Type['AtomicIntegerIncrementer']) -> None:
        super().clear()

        instance = cast(AtomicIntegerIncrementer, cls.get())
        variables = list(instance._steps.keys())
        for variable in variables:
            del instance._steps[variable]

    def __getitem__(self, variable: str) -> Optional[int]:
        with self.semaphore():
            value = self._get_value(variable)

            if value is not None:
                self._values[variable] = value + self._steps[variable]['step']

            return value

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            try:
                del self._steps[variable]
            except KeyError:
                pass

            super().__delitem__(variable)
