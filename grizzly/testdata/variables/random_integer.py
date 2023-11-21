"""@anchor pydoc:grizzly.testdata.variables.random_integer Random Integer
This variable provides an random integer between specified interval.

## Format

Interval from which the integer should be generated from, in the format `<min>..<max>`.

## Arguments

This variable does not have any arguments.

## Example

```gherkin
And value for variable "AtomicRandomInteger.weight" is "10..30"
```

This can then be used in a template:
```json
{
    "weight_tons": {{ AtomicRandomInteger.weight }}
}
```

`AtomicRandomInteger.weight` will then be anything between, and including, `10` and `30`.
"""
from __future__ import annotations

from contextlib import suppress
from secrets import choice
from typing import Dict, Type, cast

from . import AtomicVariable


def atomicrandominteger__base_type__(value: str) -> str:
    """Validate values that `AtomicRandomInteger` can be set with."""
    if '..' not in value:
        message = f'AtomicRandomInteger: {value} is not a valid value format, must be: "a..b"'
        raise ValueError(message)

    values = list(value.split('..', 1))

    try:
        for v in values:
            str(int(v))
    except ValueError as e:
        message = f'AtomicRandomInteger: {v} is not a valid integer'
        raise ValueError(message) from e

    minimum, maximum = (int(v) for v in values)

    if minimum > maximum:
        message = 'AtomicRandomInteger: first value needs to be less than second value'
        raise ValueError(message)

    return value


class AtomicRandomInteger(AtomicVariable[int]):
    __base_type__ = atomicrandominteger__base_type__
    __initialized: bool = False
    _max: Dict[str, int]

    def __init__(self, variable: str, value: str, *, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            safe_value = self.__class__.__base_type__(value)
            minimum, maximum = (int(v) for v in safe_value.split('..', 1))

            super().__init__(variable, minimum, outer_lock=True)

            if self.__initialized:
                if variable not in self._max:
                    self._max[variable] = maximum

                return

            self._max = {variable: maximum}
            self.__initialized = True

    @classmethod
    def clear(cls: Type[AtomicRandomInteger]) -> None:
        super().clear()

        instance = cast(AtomicRandomInteger, cls.get())
        variables = list(instance._max.keys())
        for variable in variables:
            del instance._max[variable]

    def __getitem__(self, variable: str) -> int:
        with self.semaphore():
            minimum = cast(int, self._get_value(variable))
            maximum = self._max[variable]

            return choice(range(minimum, maximum))

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            with suppress(KeyError):
                del self._max[variable]

            super().__delitem__(variable)
