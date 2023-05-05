'''
@anchor pydoc:grizzly.testdata.variables.csv_writer CSV Writer
This variable writes to a CSV file.

The CSV files **must** have headers for each column, since these are used to reference the value.

When setting the value of the variable there must be one value per specified header.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

* `headers` _List[str]_ - comma seperated list of headers to be used in destination file
* `overwrite` _bool_ (optional) - if destination file exists and should be overwritten (default: `False`)

## Example

``` gherkin
And value for variable "AtomicCsvWriter.output" is "output.csv | headers='foo,bar'"
...
And value for variable "AtomicCsvWriter.output" is "{{ foo_value }}, {{ bar_value }}"
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

                return

            self._settings = {variable: settings}
            self.__initialized = True

    @classmethod
    def clear(cls: Type['AtomicCsvWriter']) -> None:
        super().clear()

        instance = cast(AtomicCsvWriter, cls.get())
        variables = list(instance._settings.keys())

        for variable in variables:
            del instance._settings[variable]

    def __getitem__(self, variable: str) -> Optional[str]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented "__getitem__"')  # pragma: no cover

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        if value is None or isinstance(self.grizzly.state.locust, MasterRunner):
            return

        if variable not in self._settings:
            raise ValueError(f'{self.__class__.__name__}.{variable} is not a valid reference')

        values = [v.strip() for v in value.split(',')]

        headers = self._settings[variable].get('headers', [])
        values_count = len(values)
        header_count = len(headers)

        if values_count != header_count:
            delta = values_count - header_count
            if delta < 0:
                diff_text = 'less'
            else:
                diff_text = 'more'

            raise ValueError(f'{self.__class__.__name__}.{variable}: {diff_text} values ({values_count}) than headers ({header_count})')

        buffer = {key: value for key, value in zip(headers, values)}

        # values for all headers set, flush to file
        data = {
            'destination': self._settings[variable]['destination'],
            'row': buffer,
        }

        self.grizzly.state.locust.send_message('atomiccsvwriter', data)
