"""@anchor pydoc:grizzly.testdata.variables.csv_reader CSV Reader
This variable reads a CSV file and provides a new row from the CSV file each time it is accessed.

The CSV files **must** have headers for each column, since these are used to reference the value.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

* `repeat` _bool_ (optional) - whether values should be reused, e.g. when reaching the end it should start from the beginning again (default: `False`)
* `random` _bool_ (optional) - if rows should be selected by random, instead of sequential from first to last (default: `False`)

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

etc.
"""
from __future__ import annotations

from contextlib import suppress
from csv import DictReader
from os import environ
from pathlib import Path
from secrets import randbelow
from typing import Any, ClassVar, Dict, List, Optional, Type, cast

from grizzly.types import bool_type
from grizzly_extras.arguments import parse_arguments, split_value

from . import AtomicVariable


def atomiccsvreader__base_type__(value: str) -> str:
    """Validate values that `AtomicCsvReader` can be initialized with."""
    grizzly_context_requests = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'

    if '|' in value:
        csv_file, csv_arguments = split_value(value)

        try:
            arguments = parse_arguments(csv_arguments)
        except ValueError as e:
            message = f'AtomicCsvReader: {e!s}'
            raise ValueError(message) from e

        for argument, value in arguments.items():
            if argument not in AtomicCsvReader.arguments:
                message = f'AtomicCsvReader: argument {argument} is not allowed'
                raise ValueError(message)

            AtomicCsvReader.arguments[argument](value)

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


class AtomicCsvReader(AtomicVariable[Dict[str, Any]]):
    __base_type__ = atomiccsvreader__base_type__
    __initialized: bool = False

    _rows: Dict[str, List[Dict[str, Any]]]
    _settings: Dict[str, Dict[str, Any]]
    context_root: Path
    arguments: ClassVar[Dict[str, Any]] = {'repeat': bool_type, 'random': bool_type}

    def __init__(self, variable: str, value: str, *, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            if variable.count('.') != 0:
                message = f'{self.__class__.__name__}.{variable} is not a valid CSV source name, must be: {self.__class__.__name__}.<name>'
                raise ValueError(message)

            safe_value = self.__class__.__base_type__(value)

            settings = {'repeat': False, 'random': False}

            if '|' in safe_value:
                csv_file, csv_arguments = split_value(safe_value)
                arguments = parse_arguments(csv_arguments)

                for argument, caster in self.__class__.arguments.items():
                    if argument in arguments:
                        settings[argument] = caster(arguments[argument])
            else:
                csv_file = safe_value.strip()

            super().__init__(variable, {}, outer_lock=True)

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

    def _create_row_queue(self, value: str) -> List[Dict[str, Any]]:
        input_file = self.context_root / value

        with input_file.open() as fd:
            reader = DictReader(fd)
            return [cast(Dict[str, Any], row) for row in reader]

    @classmethod
    def clear(cls: Type[AtomicCsvReader]) -> None:
        super().clear()

        instance = cast(AtomicCsvReader, cls.get())
        variables = list(instance._rows.keys())

        for variable in variables:
            del instance._rows[variable]
            del instance._settings[variable]

    def __getitem__(self, variable: str) -> Optional[Dict[str, Any]]:
        with self.semaphore():
            column: Optional[str] = None

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
