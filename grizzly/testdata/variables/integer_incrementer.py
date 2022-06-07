'''
@anchor pydoc:grizzly.testdata.variables.integer_incrementer Integer Incrementer
This variable provides an unique integer each time it is accessed.

Useful to generate unique ID for each request.

## Format

The first value of an integer that is going to be used.

## Arguments

* `step` _int_, (optional) - how much the value should increment each time (default `1`)

## Example

``` gherkin
And value for variable "AtomicIntegerIncrementer.unique_id" is "100 | step=10"
```

This can then be used in a template:

``` json
{
    "id": {{ AtomicIntegerIncrementer.unique_id }}
}
```

First request `AtomicIntegerIncrementer.unique_id` will be `100`, second `110`, third `120` etc.
'''
from typing import Union, Dict, Any, Type, Optional, cast

from grizzly_extras.arguments import split_value, parse_arguments

from . import AtomicVariable


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

        try:
            AtomicIntegerIncrementer.arguments['step'](arguments['step'])
        except ValueError:
            raise ValueError(f'AtomicIntegerIncrementer: "{value}" was not an int')

        value = f'{initial_value} | {incrementer_arguments}'
    else:
        try:
            value = str(int(float(value.strip())))
        except ValueError as e:
            raise ValueError(f'AtomicIntegerIncrementer: "{value}" is not a valid initial value') from e

    return value


class AtomicIntegerIncrementer(AtomicVariable[int]):
    __base_type__ = atomicintegerincrementer__base_type__

    __initialized: bool = False
    _steps: Dict[str, int]
    arguments: Dict[str, Any] = {'step': int}

    def __init__(self, variable: str, value: Union[str, int]) -> None:
        safe_value = self.__class__.__base_type__(value)

        if '|' in safe_value:
            incrementer_value, incrementer_arguments = split_value(safe_value)
            initial_value = incrementer_value
            arguments = parse_arguments(incrementer_arguments)
            step = int(arguments['step'])
        else:
            initial_value = safe_value
            step = 1

        super().__init__(variable, int(initial_value))

        with self._semaphore:
            if self.__initialized:
                if variable not in self._steps:
                    self._steps[variable] = step

                return

            self._steps = {variable: step}
            self.__initialized = True

    @classmethod
    def clear(cls: Type['AtomicIntegerIncrementer']) -> None:
        super().clear()

        instance = cast(AtomicIntegerIncrementer, cls.get())
        variables = list(instance._steps.keys())
        for variable in variables:
            del instance._steps[variable]

    def __getitem__(self, variable: str) -> Optional[int]:
        with self._semaphore:
            value = self._get_value(variable)

            if value is not None:
                self._values[variable] = value + self._steps[variable]

            return value

    # not possible to override already set value
    def __setitem__(self, variable: str, value: Optional[int]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._steps[variable]
            except KeyError:
                pass

        super().__delitem__(variable)
