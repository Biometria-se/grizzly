"""Generate a specified number of unique strings, based on a string format pattern.

The list is pre-populated to ensure that each string is unique.

## Format

Initial value is a string pattern specified with `%s` and `%d`, or `%g`.

* `%s` represents **one** ASCII letter
* `%d` represents **one** digit between `0` and `9`
* `%g` represents one complete UUID(4), cannot be combined with other string patterns

Parts of the string can be static, e.g. not random.

## Arguments

| Name    | Type   | Description                            | Default |
| ------- | ------ | -------------------------------------- | ------- |
| `count` | `int`  | number of unique strings to generate   | `1`     |
| `upper` | `bool` | if the strings should be in upper case | `False` |

## Example

```gherkin
And value for variable "AtomicRandomString.registration_plate_number" is "%s%sZ%d%d0 | upper=True, count=100"
And value for variable "AtomicRandomString.uuid" is "%g | count=100"
```

This can then be used in a template:
```json
{
    "registration_plate_number": "{{ AtomicRandomString.registration_plate_number }}"
}
```

`AtomicRandomString.registration_plate_number` will then be a string in the format `[A-Z][A-Z]Z[0-9][0-9]0` and there will be `100` unique values for disposal.
"""

from __future__ import annotations

from contextlib import suppress
from secrets import choice, randbelow
from string import ascii_letters
from typing import TYPE_CHECKING, ClassVar, cast
from uuid import uuid4

from grizzly_common.arguments import parse_arguments, split_value
from grizzly_common.text import has_separator

from grizzly.types import StrDict, bool_type, int_rounded_float_type

from . import AtomicVariable

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from grizzly.context import GrizzlyContextScenario


def atomicrandomstring__base_type__(value: str) -> str:
    if len(value) < 1:
        message = 'AtomicRandomString: no string pattern specified'
        raise ValueError(message)

    if has_separator('|', value):
        string_pattern, string_arguments = split_value(value)

        try:
            arguments = parse_arguments(string_arguments)
        except ValueError as e:
            message = f'AtomicRandomString: {e!s}'
            raise ValueError(message) from e

        for argument, v in arguments.items():
            if argument not in AtomicRandomString.arguments:
                message = f'AtomicRandomString: argument {argument} is not allowed'
                raise ValueError(message)

            AtomicRandomString.arguments[argument](v)

        value = f'{string_pattern} | {string_arguments}'
    else:
        string_pattern = value

    generators = AtomicRandomString.get_generators(string_pattern)

    if len(generators) < 1:
        message = 'AtomicRandomString: specified string pattern does not contain any generators'
        raise ValueError(message)

    if '%g' in string_pattern and string_pattern.count('%') != 1:
        message = 'AtomicRandomString: %g cannot be combined with other formats'
        raise ValueError(message)

    return value


class AtomicRandomString(AtomicVariable[str]):
    __base_type__ = atomicrandomstring__base_type__
    __initialized: bool = False

    _strings: dict[str, list[str]]
    arguments: ClassVar[StrDict] = {'upper': bool_type, 'count': int_rounded_float_type}

    @staticmethod
    def get_generators(format_string: str) -> list[Callable[[AtomicRandomString], str]]:
        """Map format modifiers to generator functions."""
        formats: list[Callable[[AtomicRandomString], str]] = []
        # first item is either empty, or it's a static character
        for format_modifier in format_string.split('%')[1:]:
            generator_name = format_modifier[0]  # could be static characters in the pattern, only supports one character formatters
            generator = getattr(AtomicRandomString, f'_generate_{generator_name}', None)
            if not callable(generator):
                message = f'AtomicRandomString: format "{generator_name}" is not implemented'
                raise NotImplementedError(message)

            formats.append(generator)

        return formats

    def __init__(self, *, scenario: GrizzlyContextScenario, variable: str, value: str, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            safe_value = self.__class__.__base_type__(value)

            settings = {'upper': False, 'count': 1}

            if has_separator('|', safe_value):
                string_pattern, string_arguments = split_value(safe_value)

                arguments = parse_arguments(string_arguments)

                for argument, caster in self.__class__.arguments.items():
                    if argument in arguments:
                        settings[argument] = caster(arguments[argument])

                settings['count'] = max(settings['count'], 1)
            else:
                string_pattern = value

            super().__init__(scenario=scenario, variable=variable, value=string_pattern, outer_lock=True)

            if self.__initialized:
                if variable not in self._strings:
                    self._strings[variable] = self._generate_strings(string_pattern, settings)

                return

            self._strings = {variable: self._generate_strings(string_pattern, settings)}
            self.__initialized = True

    def _generate_s(self) -> str:
        return choice(ascii_letters)

    def _generate_d(self) -> int:
        return randbelow(10)

    def _generate_g(self) -> str:
        return str(uuid4())

    def _generate_strings(self, string_pattern: str, settings: StrDict) -> list[str]:
        generated_strings: set[str] = set()
        generators = self.__class__.get_generators(string_pattern)

        string_pattern = string_pattern.replace('%g', '%s')

        for _ in range(settings['count']):
            generated_string: str | None = None

            while generated_string is None or generated_string in generated_strings:
                generated_part = tuple([generator(self) for generator in generators])
                generated_string = string_pattern % generated_part

                if settings['upper']:
                    generated_string = generated_string.upper()

            generated_strings.add(generated_string)

        return list(generated_strings)

    @classmethod
    def clear(cls: type[AtomicRandomString]) -> None:
        super().clear()

        instances = cls._instances.get(cls, {})
        for scenario in instances:
            instance = cast('AtomicRandomString', cls.get(scenario))
            variables = list(instance._strings.keys())

            for variable in variables:
                del instance._strings[variable]

    def __getitem__(self, variable: str) -> str | None:
        with self.semaphore():
            self._get_value(variable)

            try:
                return self._strings[variable].pop()
            except (IndexError, ValueError):
                return None

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            with suppress(KeyError):
                del self._strings[variable]

            super().__delitem__(variable)
