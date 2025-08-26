"""Read a CSV file and provides a new row from the CSV file each time it is accessed.

!!! info

    CSV files **must** have headers for each column, since these are used to reference the value.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

| Name     | Type   | Description                                                                                         | Default |
| -------- | ------ | --------------------------------------------------------------------------------------------------- | ------- |
| `repeat` | `bool` | wheter values should be reused, e.g. when reaching the end it should start from the beginning again | `False` |
| `random` | `bool` | if rows should be selected by random, instead of sequential from first to last                      | `False` |

## Example

```plain title="requests/example.csv"
username,password
bob1,some-password
alice1,some-other-password
bob2,password
```

```gherkin
And value for variable "AtomicCsvReader.example" is "example.csv | random=False, repeat=True"
Then post request with name "authenticate" to endpoint "/api/v1/authenticate"
  \"\"\"
  {
      "username": "{{ AtomicCsvReader.example.username }}",
      "password": "{{ AtomicCsvReader.example.password }}"
  }
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

from contextlib import suppress
from csv import DictReader
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


def _atomiccsvreader(value: str) -> str:
    """Validate values that `AtomicCsvReader` can be initialized with."""
    grizzly_context_requests = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'

    if has_separator('|', value):
        csv_file, csv_arguments = split_value(value)

        try:
            arguments = parse_arguments(csv_arguments)
        except ValueError as e:
            message = f'AtomicCsvReader: {e!s}'
            raise ValueError(message) from e

        for k, v in arguments.items():
            if k not in AtomicCsvReader.arguments:
                message = f'AtomicCsvReader: argument {k} is not allowed'
                raise ValueError(message)

            AtomicCsvReader.arguments[k](v)

        value = f'{csv_file} | {csv_arguments}'
    else:
        csv_file = value

    path = grizzly_context_requests / csv_file

    if path.suffix != '.csv':
        message = f'AtomicCsvReader: {csv_file} must be a CSV file with file extension .csv'
        raise ValueError(message)

    if not path.is_file():
        message = f'AtomicCsvReader: {csv_file} is not a file in {grizzly_context_requests!s}'
        raise ValueError(message)

    return value


class AtomicCsvReader(AtomicVariable[StrDict]):
    __base_type__ = _atomiccsvreader
    __initialized: bool = False

    _rows: dict[str, list[StrDict]]
    _settings: dict[str, StrDict]
    context_root: Path
    arguments: ClassVar[StrDict] = {'repeat': bool_type, 'random': bool_type}

    def __init__(self, *, scenario: GrizzlyContextScenario, variable: str, value: str, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            if variable.count('.') != 0:
                message = f'{self.__class__.__name__}.{variable} is not a valid CSV source name, must be: {self.__class__.__name__}.<name>'
                raise ValueError(message)

            safe_value = self.__class__.__base_type__(value)

            settings = {'repeat': False, 'random': False}

            if has_separator('|', safe_value):
                csv_file, csv_arguments = split_value(safe_value)
                arguments = parse_arguments(csv_arguments)

                for argument, caster in self.__class__.arguments.items():
                    if argument in arguments:
                        settings[argument] = caster(arguments[argument])
            else:
                csv_file = safe_value.strip()

            super().__init__(scenario=scenario, variable=variable, value={}, outer_lock=True)

            if self.__initialized:
                if variable not in self._rows:
                    self._rows[variable] = self._create_row_queue(csv_file)

                if variable not in self._settings:
                    self._settings[variable] = settings

                return

            self.context_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'
            self._rows = {variable: self._create_row_queue(csv_file)}
            self._settings = {variable: settings}
            self.__initialized = True

    def _create_row_queue(self, value: str) -> list[StrDict]:
        input_file = self.context_root / value

        with input_file.open() as fd:
            reader = DictReader(fd)
            return [cast('StrDict', row) for row in reader]

    @classmethod
    def clear(cls: type[AtomicCsvReader]) -> None:
        super().clear()

        instances = cls._instances.get(cls, {})
        for scenario in instances:
            instance = cast('AtomicCsvReader', cls.get(scenario))
            variables = list(instance._rows.keys())

            for variable in variables:
                del instance._rows[variable]
                del instance._settings[variable]

    def __getitem__(self, variable: str) -> StrDict | None:
        with self.semaphore():
            column: str | None = None

            if '.' in variable:
                [variable, column] = variable.rsplit('.', 1)

            try:
                settings = self._settings[variable]

                if settings['random'] is True:
                    roof = len(self._rows[variable])
                    index = randbelow(roof)
                else:
                    index = 0

                row = self._rows[variable].pop(index)

                if settings['repeat'] is True:
                    self._rows[variable].append(row)
            except (IndexError, ValueError):
                return None

            if column is not None:
                if column not in row:
                    self._rows[variable].insert(0, row)
                    message = f'{self.__class__.__name__}.{variable}: {column} does not exists'
                    raise ValueError(message)
                value = row[column]
                row = {column: value}

            return row

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            if '.' in variable:
                [variable, _] = variable.rsplit('.', 1)

            with suppress(KeyError):
                del self._rows[variable]

            with suppress(KeyError):
                del self._settings[variable]

            super().__delitem__(variable)
