'''
@anchor pydoc:grizzly.testdata.variables.csv_writer CSV Writer
This variable writes to a CSV file.

The CSV files **must** have headers for each column, since these are used to reference the value.

## Format

Value is the path, relative to `requests/`, of an file ending with `.csv`.

## Arguments

* `repeat` _bool_ (optional) - whether values should be reused, e.g. when reaching the end it should start from the beginning again (default: `False`)
* `random` _bool_ (optional) - if rows should be selected by random, instead of sequential from first to last (default: `False`)

## Example

``` gherkin
```

'''
import os

from typing import Dict, List, Any, Type, Optional, cast
from csv import DictReader
from random import randint

from grizzly_extras.arguments import split_value, parse_arguments

from grizzly.types import bool_type

from . import AtomicVariable


def atomiccsvwriter__base_type__(value: str) -> str:
    grizzly_context_requests = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', ''), 'requests')

    if '|' in value:
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

        value = f'{csv_file} | {csv_arguments}'
    else:
        csv_file = value

    path = os.path.join(grizzly_context_requests, csv_file)

    if not path.endswith('.csv'):
        raise ValueError(f'AtomicCsvWriter: {csv_file} must be a CSV file with file extension .csv')

    if not os.path.isfile(path):
        raise ValueError(f'AtomicCsvWriter: {csv_file} is not a file in {grizzly_context_requests}')

    return value


class AtomicCsvWriter(AtomicVariable[Dict[str, Any]]):
    pass
