'''This variable reads a CSV file and provides a new row from the CSV file each time it is accessed.

The CSV files **must** have headers for each column, since these are used to reference the value.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

* `repeat` _bool_ (optional) - whether values should be reused, e.g. when reaching the end it should start from the beginning again (default: `False`)
* `random` _bool_ (optional) - if rows should be selected by random, instead of sequential from first to last (default: `False`)

## Example

`requests/example.csv`:

```plain
username,password
bob1,some-password
alice1,some-other-password
bob2,password
```

```gherkin
And value of variable "AtomicCsvRow.example" is "example.csv | random=False, repeat=True"
Then post request with name "authenticate" to endpoint "/api/v1/authenticate"
  """
  {
      "username": "{{ AtomicCsvRow.example.username }}",
      "password": "{{ AtomicCsvRow.example.password }}"
  }
  """
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
'''
import os

from typing import Dict, List, Any, Type, Optional, cast
from csv import DictReader
from random import randint

from grizzly_extras.arguments import split_value, parse_arguments

from ...types import bool_typed, AtomicVariable


def atomiccsvrow__base_type__(value: str) -> str:
    grizzly_context_requests = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', ''), 'requests')

    if '|' in value:
        csv_file, csv_arguments = split_value(value)

        try:
            arguments = parse_arguments(csv_arguments)
        except ValueError as e:
            raise ValueError(f'AtomicCsvRow: {str(e)}') from e

        for argument, value in arguments.items():
            if argument not in AtomicCsvRow.arguments:
                raise ValueError(f'AtomicCsvRow: argument {argument} is not allowed')
            else:
                AtomicCsvRow.arguments[argument](value)

        value = f'{csv_file} | {csv_arguments}'
    else:
        csv_file = value

    path = os.path.join(grizzly_context_requests, csv_file)

    if not path.endswith('.csv'):
        raise ValueError(f'AtomicCsvRow: {csv_file} must be a CSV file with file extension .csv')

    if not os.path.isfile(path):
        raise ValueError(f'AtomicCsvRow: {csv_file} is not a file in {grizzly_context_requests}')

    return value


class AtomicCsvRow(AtomicVariable[Dict[str, Any]]):
    __base_type__ = atomiccsvrow__base_type__
    __initialized: bool = False

    _rows: Dict[str, List[Dict[str, Any]]]
    _settings: Dict[str, Dict[str, Any]]
    context_root: str
    arguments: Dict[str, Any] = {'repeat': bool_typed, 'random': bool_typed}

    def __init__(self, variable: str, value: str) -> None:
        if variable.count('.') != 0:
            raise ValueError(f'{self.__class__.__name__}.{variable} is not a valid CSV source name, must be: {self.__class__.__name__}.<name>')

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

        super().__init__(variable, {})

        with self._semaphore:
            if self.__initialized:
                if variable not in self._rows:
                    self._rows[variable] = self._create_row_queue(csv_file)

                if variable not in self._settings:
                    self._settings[variable] = settings

                return

            self.context_root = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', ''), 'requests')
            self._rows = {variable: self._create_row_queue(csv_file)}
            self._settings = {variable: settings}
            self.__initialized = True

    def _create_row_queue(self, value: str) -> List[Dict[str, Any]]:
        queue: List[Dict[str, Any]] = []

        with open(os.path.join(self.context_root, value)) as fd:
            reader = DictReader(fd)
            queue = [cast(Dict[str, Any], row) for row in reader]

        return queue

    @classmethod
    def clear(cls: Type['AtomicCsvRow']) -> None:
        super().clear()

        instance = cast(AtomicCsvRow, cls.get())
        variables = list(instance._rows.keys())

        for variable in variables:
            del instance._rows[variable]
            del instance._settings[variable]

    def __getitem__(self, variable: str) -> Optional[Dict[str, Any]]:
        column: Optional[str] = None

        if '.' in variable:
            [variable, column] = variable.rsplit('.', 1)

        try:
            settings = self._settings[variable]

            if settings['random'] is True:
                roof = len(self._rows[variable]) - 1
                index = randint(0, roof)
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
                raise ValueError(f'{self.__class__.__name__}.{variable}: {column} does not exists')
            value = row[column]
            row = {column: value}

        return row

    def __setitem__(self, variable: str, value: Optional[Dict[str, Any]]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            if '.' in variable:
                [variable, _] = variable.rsplit('.', 1)

            try:
                del self._rows[variable]
            except KeyError:
                pass

            try:
                del self._settings[variable]
            except KeyError:
                pass

        super().__delitem__(variable)
