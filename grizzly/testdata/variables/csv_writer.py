'''
@anchor pydoc:grizzly.testdata.variables.csv_writer CSV Writer
This variable writes to a CSV file.

The CSV files **must** have headers for each column, since these are used to reference the value.

When all specified headers has been set, the row will be "flushed" and written to the specified destination file.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

* `headers` _List[str]_ - comma seperated list of headers to be used in destination file
* `overwrite` _bool_ (optional) - if destination file exists and should be overwritten (default: `False`)

## Example

``` gherkin
And value for variable "AtomicCsvWriter.output" is "output.csv | headers='foo,bar'"
...
And value for variable "AtomicCsvWriter.output.foo" is "{{ value }}"
And value for variable "AtomicCsvWriter.output.bar" is "{{ value }}"
```

'''
import os

from typing import Dict, Any, Type, Optional, cast
from csv import DictWriter
from pathlib import Path

from grizzly_extras.arguments import split_value, parse_arguments

from grizzly.types import bool_type, list_type
from grizzly.types.locust import Environment, Message, MasterRunner

from . import AtomicVariable, AtomicVariableSettable


def atomiccsvwriter__base_type__(value: str) -> str:
    grizzly_context_requests = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', ''), 'requests')

    if '|' not in value:
        raise ValueError('AtomicCsvWriter: arguments are required')

    csv_file, csv_arguments = split_value(value)

    try:
        arguments = parse_arguments(csv_arguments)
    except ValueError as e:
        raise ValueError(f'AtomicCsvWriter: {str(e)}') from e

    for argument, value in arguments.items():
        if argument not in AtomicCsvWriter.arguments:
            raise ValueError(f'AtomicCsvWriter: argument {argument} is not allowed')
        else:
            AtomicCsvWriter.arguments[argument](value)

    if 'headers' not in arguments:
        raise ValueError('AtomicCsvWriter: argument headers is required')

    value = f'{csv_file} | {csv_arguments}'

    path = os.path.join(grizzly_context_requests, csv_file)

    if not path.endswith('.csv'):
        raise ValueError(f'AtomicCsvWriter: {csv_file} must be a CSV file with file extension .csv')

    if os.path.exists(path) and not arguments.get('overwrite', False):
        raise ValueError(f'AtomicCsvWriter: {csv_file} already exists, remove or add argument overwrite=True')

    return value


def atomiccsvwriter_message_handler(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
    with AtomicCsvWriter.semaphore():
        data = cast(dict, msg.data)
        destination_file = data['destination']
        headers = list(data['row'].keys())
        context_root = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', ''), 'requests')

        output_path = Path(context_root) / destination_file

        exists = output_path.exists()

        with open(output_path, 'a+', newline='') as csv_file:
            writer = DictWriter(csv_file, fieldnames=headers)
            if not exists:
                writer.writeheader()

            writer.writerow(data['row'])


class AtomicCsvWriter(AtomicVariable[str], AtomicVariableSettable):
    __base_type__ = atomiccsvwriter__base_type__
    __initialized: bool = False
    __message_handlers__ = {'atomiccsvwriter': atomiccsvwriter_message_handler}

    _settings: Dict[str, Dict[str, Any]]
    _buffer: Dict[str, Dict[str, Any]]
    arguments: Dict[str, Any] = {'headers': list_type, 'overwrite': bool_type}

    def __init__(self, variable: str, value: str, outer_lock: bool = False) -> None:
        with self.semaphore(outer_lock):
            if variable.count('.') != 0:
                raise ValueError(f'{self.__class__.__name__}.{variable} is not a valid CSV destination name, must be: {self.__class__.__name__}.<name>')

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
                    self._buffer[variable] = {}

                return

            self._settings = {variable: settings}
            self._buffer = {variable: {}}
            self.__initialized = True

    @classmethod
    def clear(cls: Type['AtomicCsvWriter']) -> None:
        super().clear()

        instance = cast(AtomicCsvWriter, cls.get())
        variables = list(instance._settings.keys())

        for variable in variables:
            del instance._settings[variable]
            del instance._buffer[variable]

    def __getitem__(self, variable: str) -> Optional[str]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented "__getitem__"')

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        if value is None or isinstance(self.grizzly.state.locust, MasterRunner):
            return

        if variable.count('.') < 1:
            raise ValueError(f'{self.__class__.__name__}.{variable} is not a valid reference')

        variable, header = variable.split('.', 1)

        if header not in self._settings[variable].get('headers', []):
            raise ValueError(f'{self.__class__.__name__}.{variable} has not specified header "{header}"')

        buffer = self._buffer.get(variable, {})

        if header in buffer:
            raise ValueError(f'{self.__class__.__name__}.{variable} has already set value for "{header}" in current row')

        buffer.update({header: value})

        # values for all headers set, flush to file
        if list(buffer.keys()) == self._settings[variable].get('headers', []):
            data = {
                'destination': self._settings[variable]['destination'],
                'row': buffer.copy(),
            }

            self.grizzly.state.locust.send_message('atomiccsvwriter', data)
            self._buffer[variable].clear()
