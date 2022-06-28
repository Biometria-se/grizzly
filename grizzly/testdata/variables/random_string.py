'''
@anchor pydoc:grizzly.testdata.variables.random_string Random String
This variable generates a specified number of unique strings, based on a string format pattern.

The list is pre-populated to ensure that each string is unique.

## Format

Initial value is a string pattern specified with `%s` and `%d`.

* `%s` represents **one** ASCII letter
* `%d` represents **one** digit between `0` and `9`

Parts of the string can be static, e.g. not random.

## Arguments

* `count` _int_ (optional) - number of unique strings to generate (default: `1`)
* `upper` _bool_ (optional) - if the strings should be in upper case (default: `False`)

## Example

``` gherkin
And value for variable "AtomicRandomString.registration_plate_number" is "%s%sZ%d%d0 | upper=True, count=100"
```

This can then be used in a template:
``` json
{
    "registration_plate_number": "{{ AtomicRandomString.registration_plate_number }}"
}
```

`AtomicRandomString.registration_plate_number` will then be a string in the format `[A-Z][A-Z]Z[0-9][0-9]0` and there will be `100` unique values for disposal.
'''
from typing import Dict, List, Any, Callable, Optional, Set, Type, cast
from random import randint, choice
from string import ascii_letters

from grizzly_extras.arguments import split_value, parse_arguments

from ...types import bool_type, int_rounded_float_type
from . import AtomicVariable


def atomicrandomstring__base_type__(value: str) -> str:
    if len(value) < 1:
        raise ValueError('AtomicRandomString: no string pattern specified')

    if '|' in value:
        string_pattern, string_arguments = split_value(value)

        try:
            arguments = parse_arguments(string_arguments)
        except ValueError as e:
            raise ValueError(f'AtomicRandomString: {str(e)}') from e

        for argument, v in arguments.items():
            if argument not in AtomicRandomString.arguments:
                raise ValueError(f'AtomicRandomString: argument {argument} is not allowed')
            else:
                AtomicRandomString.arguments[argument](v)

        value = f'{string_pattern} | {string_arguments}'
    else:
        string_pattern = value

    generators = AtomicRandomString.get_generators(string_pattern)

    if len(generators) < 1:
        raise ValueError('AtomicRandomString: specified string pattern does not contain any generators')

    return value


class AtomicRandomString(AtomicVariable[str]):
    __base_type__ = atomicrandomstring__base_type__
    __initialized: bool = False

    _strings: Dict[str, List[str]]
    arguments: Dict[str, Any] = {'upper': bool_type, 'count': int_rounded_float_type}

    @staticmethod
    def get_generators(format: str) -> List[Callable[['AtomicRandomString'], str]]:
        formats: List[Callable[[AtomicRandomString], str]] = []
        # first item is either empty, or it's a static character
        for f in format.split('%')[1:]:
            f = f[0]  # could be static characters in the pattern, only supports one character formatters
            generator = getattr(AtomicRandomString, f'_generate_{f}', None)
            if not callable(generator):
                raise NotImplementedError(f'AtomicRandomString: format "{f}" is not implemented')

            formats.append(generator)

        return formats

    def __init__(self, variable: str, value: str) -> None:
        safe_value = self.__class__.__base_type__(value)

        settings = {'upper': False, 'count': 1}

        if '|' in safe_value:
            string_pattern, string_arguments = split_value(safe_value)

            arguments = parse_arguments(string_arguments)

            for argument, caster in self.__class__.arguments.items():
                if argument in arguments:
                    settings[argument] = caster(arguments[argument])

            if settings['count'] < 1:
                settings['count'] = 1
        else:
            string_pattern = value

        super().__init__(variable, string_pattern)

        with self._semaphore:
            if self.__initialized:
                if variable not in self._strings:
                    self._strings[variable] = self._generate_strings(string_pattern, settings)

                return

            self._strings = {variable: self._generate_strings(string_pattern, settings)}
            self.__initialized = True

    def _generate_s(self) -> str:
        return choice(ascii_letters)

    def _generate_d(self) -> int:
        return randint(0, 9)

    def _generate_strings(self, string_pattern: str, settings: Dict[str, Any]) -> List[str]:
        generated_strings: Set[str] = set()
        generators = self.__class__.get_generators(string_pattern)

        for _ in range(0, settings['count']):
            generated_string: Optional[str] = None

            while generated_string is None or generated_string in generated_strings:
                generated_part: List[str] = []

                for generator in generators:
                    generated_part.append(generator(self))

                generated_string = string_pattern % tuple(generated_part)

                if settings['upper']:
                    generated_string = generated_string.upper()

            generated_strings.add(generated_string)

        return list(generated_strings)

    @classmethod
    def clear(cls: Type['AtomicRandomString']) -> None:
        super().clear()

        instance = cast(AtomicRandomString, cls.get())
        variables = list(instance._strings.keys())

        for variable in variables:
            del instance._strings[variable]

    def __getitem__(self, variable: str) -> Optional[str]:
        with self._semaphore:
            self._get_value(variable)

            try:
                return self._strings[variable].pop()
            except (IndexError, ValueError):
                return None

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._strings[variable]
            except KeyError:
                pass

        super().__delitem__(variable)
