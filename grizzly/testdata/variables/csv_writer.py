"""@anchor pydoc:grizzly.testdata.variables.csv_writer CSV Writer
This variable writes to a CSV file.

The CSV files **must** have headers for each column, since these are used to reference the value.

When setting the value of the variable there must be one value per specified header.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

* `headers` _List[str]_ - comma seperated list of headers to be used in destination file
* `overwrite` _bool_ (optional) - if destination file exists and should be overwritten (default: `False`)

## Example

```gherkin
And value for variable "AtomicCsvWriter.output" is "output.csv | headers='foo,bar'"
...
And value for variable "AtomicCsvWriter.output" is "{{ foo_value }}, {{ bar_value }}"
```
"""
from __future__ import annotations

from csv import DictWriter
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, cast

from grizzly.types import bool_type, list_type
from grizzly.types.locust import Environment, MasterRunner, Message
from grizzly_extras.arguments import parse_arguments, split_value

from . import AtomicVariable, AtomicVariableSettable

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.locust import MessageHandler


def atomiccsvwriter__base_type__(value: str) -> str:
    """Validate values that `AtomicCsvWriter` can be initialized with."""
    grizzly_context_requests = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'

    if '|' not in value:
        message = 'AtomicCsvWriter: arguments are required'
        raise ValueError(message)

    csv_file, csv_arguments = split_value(value)

    try:
        arguments = parse_arguments(csv_arguments)
    except ValueError as e:
        message = f'AtomicCsvWriter: {e!s}'
        raise ValueError(message) from e

    for argument, value in arguments.items():
        if argument not in AtomicCsvWriter.arguments:
            message = f'AtomicCsvWriter: argument {argument} is not allowed'
            raise ValueError(message)

        AtomicCsvWriter.arguments[argument](value)

    if 'headers' not in arguments:
        message = 'AtomicCsvWriter: argument headers is required'
        raise ValueError(message)

    value = f'{csv_file} | {csv_arguments}'

    path = grizzly_context_requests / csv_file

    if path.suffix != '.csv':
        message = f'AtomicCsvWriter: {csv_file} must be a CSV file with file extension .csv'
        raise ValueError(message)

    if path.exists() and not arguments.get('overwrite', False):
        message = f'AtomicCsvWriter: {csv_file} already exists, remove existing file or add argument overwrite=True'
        raise ValueError(message)

    return value


def atomiccsvwriter_message_handler(environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG001
    """Receive messages containing CSV data.
    Write the data to a CSV file.
    """
    with AtomicCsvWriter.semaphore():
        data = cast(dict, msg.data)
        destination_file = cast(str, data['destination'])
        headers = list(data['row'].keys())
        context_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'

        output_path = context_root / destination_file

        exists = output_path.exists()

        with output_path.open('a+', newline='') as csv_file:
            writer = DictWriter(csv_file, fieldnames=headers)
            if not exists:
                writer.writeheader()

            writer.writerow(data['row'])


class AtomicCsvWriter(AtomicVariable[str], AtomicVariableSettable):
    __base_type__ = atomiccsvwriter__base_type__
    __initialized: bool = False
    __message_handlers__: ClassVar[Dict[str, MessageHandler]] = {'atomiccsvwriter': atomiccsvwriter_message_handler}

    _settings: Dict[str, Dict[str, Any]]
    arguments: ClassVar[Dict[str, Any]] = {'headers': list_type, 'overwrite': bool_type}

    def __init__(self, variable: str, value: str, *, outer_lock: bool = False) -> None:
        with self.semaphore(outer=outer_lock):
            if variable.count('.') != 0:
                message = f'{self.__class__.__name__}.{variable} is not a valid CSV destination name, must be: {self.__class__.__name__}.<name>'
                raise ValueError(message)

            safe_value = self.__class__.__base_type__(value)

            csv_file, csv_arguments = split_value(safe_value)
            arguments = parse_arguments(csv_arguments)

            settings = {'headers': [], 'overwrite': False, 'destination': csv_file.strip()}

            for argument, caster in self.__class__.arguments.items():
                if argument in arguments:
                    settings[argument] = caster(arguments[argument])

            super().__init__(variable, value, outer_lock=True)

            if self.__initialized:
                if variable not in self._settings:
                    self._settings[variable] = settings

                return

            self._settings = {variable: settings}
            self.__initialized = True

    @classmethod
    def clear(cls: Type[AtomicCsvWriter]) -> None:
        super().clear()

        instance = cast(AtomicCsvWriter, cls.get())
        variables = list(instance._settings.keys())

        for variable in variables:
            del instance._settings[variable]

    def __getitem__(self, variable: str) -> Optional[str]:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented "__getitem__"'
        raise NotImplementedError(message)

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        """Set/write CSV row, by sending a message to master."""
        if value is None or isinstance(self.grizzly.state.locust, MasterRunner):
            return

        if variable not in self._settings:
            message = f'{self.__class__.__name__}.{variable} is not a valid reference'
            raise ValueError(message)

        values = [v.strip() for v in value.split(',')]

        headers = self._settings[variable].get('headers', [])
        values_count = len(values)
        header_count = len(headers)

        if values_count != header_count:
            delta = values_count - header_count
            diff_text = 'less' if delta < 0 else 'more'
            message = f'{self.__class__.__name__}.{variable}: {diff_text} values ({values_count}) than headers ({header_count})'
            raise ValueError(message)

        buffer = dict(zip(headers, values))

        # values for all headers set, flush to file
        data = {
            'destination': self._settings[variable]['destination'],
            'row': buffer,
        }

        self.grizzly.state.locust.send_message('atomiccsvwriter', data)
