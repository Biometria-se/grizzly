"""Read a JSON file that contains a list of objects and provides a new object from the list each time it is accessed.

The JSON file **must** contain a list of JSON objects, and each object **must** have the same properties.

## Format

Value is the path, relative to `requests/`, of an file ending with `.json`.

## Arguments

| Name     | Type   | Description                                                                                          | Default |
| -------- | ------ | ---------------------------------------------------------------------------------------------------- | ------- |
| `repeat` | `bool` | whether values should be reused, e.g. when reaching the end it should start from the beginning again | `False` |
| `random` | `bool` | if rows should be selected by random, instead of sequential from first to last                       | `False` |

## Example

```json title="requests/example.json"
[
  {
    "username": "bob1",
    "password": "some-password"
  },
  {
    "username": "alice1",
    "password": "some-other-password"
  },
  {
    "username": "bob2",
    "password": "password"
  }
]
```

```gherkin
And value for variable "AtomicJsonReader.example" is "example.json | random=False, repeat=True"

# Reference property by property
Then post request with name "authenticate" to endpoint "/api/v1/authenticate"
  \"\"\"
  {
      "username": "{{ AtomicJsonReader.example.username }}",
      "password": "{{ AtomicJsonReader.example.password }}"
  }
  \"\"\"

# Reference object
Then post request with name "authenticate" to endpoint "/api/v1/authenticate"
  \"\"\"
  {{ AtomicJsonReader.example }}
  \"\"\"
```

First request the payload will be:

```json
{
    "username": "bob1",
    "password": "some-password"
}
```

Second request:

```json
{
    "username": "alice1",
    "password": "some-other-password"
}
```
"""

from __future__ import annotations

import json
from contextlib import suppress
from os import environ
from pathlib import Path
from secrets import randbelow
from typing import TYPE_CHECKING, ClassVar, cast

from grizzly_common.arguments import parse_arguments, split_value
from grizzly_common.text import has_separator

from grizzly.types import StrDict, bool_type

from . import AtomicVariable

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario


def atomicjsonreader__base_type__(value: str) -> str:
    grizzly_context_requests = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'

    if has_separator('|', value):
        json_file, json_arguments = split_value(value)

        try:
            arguments = parse_arguments(json_arguments)
        except ValueError as e:
            message = f'AtomicCsvReader: {e!s}'
            raise ValueError(message) from e

        for k, v in arguments.items():
            if k not in AtomicJsonReader.arguments:
                message = f'AtomicJsonReader: argument {k} is not allowed'
                raise ValueError(message)

            AtomicJsonReader.arguments[k](v)

        value = f'{json_file} | {json_arguments}'
    else:
        json_file = value

    path = grizzly_context_requests / json_file

    if path.suffix != '.json':
        message = f'AtomicJsonReader: {json_file} must be a JSON file with file extension .json'
        raise ValueError(message)

    if not path.is_file():
        message = f'AtomicJsonReader: {json_file} is not a file in {grizzly_context_requests!s}'
        raise ValueError(message)

    data: list[StrDict] | None = None
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        message = f'AtomicJsonReader: failed to load contents of {json_file}'
        raise ValueError(message) from e

    if not isinstance(data, list):
        message = f'AtomicJsonReader: contents of {json_file} is not a list ({type(data).__name__})'
        raise ValueError(message) from None  # noqa: TRY004

    return value


class AtomicJsonReader(AtomicVariable[StrDict]):
    __base_type__ = atomicjsonreader__base_type__
    __initialized: bool = False

    _items: dict[str, list[StrDict]]
    _settings: dict[str, StrDict]
    context_root: Path
    arguments: ClassVar[StrDict] = {'repeat': bool_type, 'random': bool_type}

    def __init__(self, *, scenario: GrizzlyContextScenario, variable: str, value: str, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            if variable.count('.') != 0:
                message = f'{self.__class__.__name__}.{variable} is not a valid JSON source name, must be: {self.__class__.__name__}.<name>'
                raise ValueError(message)

            safe_value = self.__class__.__base_type__(value)

            settings = {'repeat': False, 'random': False}

            if has_separator('|', safe_value):
                json_file, json_arguments = split_value(safe_value)
                arguments = parse_arguments(json_arguments)

                for argument, caster in self.__class__.arguments.items():
                    if argument in arguments:
                        settings[argument] = caster(arguments[argument])
            else:
                json_file = safe_value.strip()

            super().__init__(scenario=scenario, variable=variable, value={}, outer_lock=True)

            if self.__initialized:
                if variable not in self._items:
                    self._items[variable] = self._create_row_queue(json_file)

                if variable not in self._settings:
                    self._settings[variable] = settings

                return

            self.context_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'
            self._items = {variable: self._create_row_queue(json_file)}
            self._settings = {variable: settings}
            self.__initialized = True

    def _create_row_queue(self, value: str) -> list[dict]:
        input_file = self.context_root / value

        return cast('list[StrDict]', json.loads(input_file.read_text()))

    @classmethod
    def clear(cls: type[AtomicJsonReader]) -> None:
        super().clear()

        instances = cls._instances.get(cls, {})
        for scenario in instances:
            instance = cast('AtomicJsonReader', cls.get(scenario))
            variables = list(instance._items.keys())

            for variable in variables:
                del instance._items[variable]
                del instance._settings[variable]

    def __getitem__(self, variable: str) -> StrDict | None:
        with self.semaphore():
            prop: str | None = None

            if '.' in variable:
                variable, prop = variable.rsplit('.', 1)

            try:
                settings = self._settings[variable]

                if settings['random'] is True:
                    roof = len(self._items[variable])
                    index = randbelow(roof)
                else:
                    index = 0

                item = self._items[variable].pop(index)

                if settings['repeat'] is True:
                    self._items[variable].append(item)
            except (IndexError, ValueError):
                return None

            if prop is not None:
                if prop not in item:
                    self._items[variable].insert(0, item)
                    message = f'{self.__class__.__name__}.{variable}: {prop} does not exists'
                    raise ValueError(message)

                item = {prop: item[prop]}

            return item

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            if '.' in variable:
                variable, _ = variable.rsplit('.', 1)

            with suppress(KeyError):
                del self._items[variable]

            with suppress(KeyError):
                del self._settings[variable]

            super().__delitem__(variable)
