'''This variable is used to format and use dates.

## Format

Initial value can, other than a parseable datetime string, be `now`. Each time the variable is accessed the value will represent that date and time
at the time of access.

## Arguments

* `format` _str_ - a python [`strftime` format string](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes), this argument is required
* `timezone` _str_ (optional) - a valid [timezone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
* `offset` _str_ (optional) - a time span string describing the offset, Y = years, M = months, D = days, h = hours, m = minutes, s = seconds, e.g. `1Y-2M10D`

## Example

```gherkin
And value of variable "AtomicDate.arrival" is "now | format='%Y-%m-%dT%H:%M:%S.000Z', timezone=UTC"
```

This can then be used in a template:

```json
{
    "arrival": "{{ AtomicDate.arrival }}",
    "location": "Port of Shanghai"
}
```
'''
from typing import Union, Dict, Any, List, Type, Optional, cast
from datetime import datetime

import pytz

from dateutil.parser import ParserError, parse as dateparse
from dateutil.relativedelta import relativedelta
from tzlocal import get_localzone as get_local_timezone

from grizzly_extras.arguments import split_value, parse_arguments

from ...types import AtomicVariable
from ...utils import parse_timespan


def atomicdate__base_type__(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f'AtomicDate: {value} ({type(value)}) is not a string')

    if '|' in value:
        date_value, date_arguments = split_value(value)

        try:
            arguments = parse_arguments(date_arguments)
        except ValueError as e:
            raise ValueError(f'AtomicDate: {str(e)}') from e

        for argument in arguments.keys():
            if argument not in AtomicDate.arguments:
                raise ValueError(f'AtomicDate: argument {argument} is not allowed')

        if 'format' not in arguments:
            raise ValueError(f'AtomicDate: date format is not specified: "{value}"')

        if 'timezone' in arguments:
            try:
                pytz.timezone(arguments['timezone'])
            except pytz.UnknownTimeZoneError:
                raise ValueError(f'AtomicDate: unknown timezone "{arguments["timezone"]}"')

        if 'offset' in arguments:
            parse_timespan(arguments['offset'])

        value = f'{date_value} | {date_arguments}'
    else:
        date_value = value

    if date_value in ['now']:
        return value

    try:
        dateparse(date_value)

        return value
    except (TypeError, ParserError) as e:
        raise ValueError(f'AtomicDate: {str(e)}') from e



class AtomicDate(AtomicVariable[Union[str, datetime]]):
    __base_type__ = atomicdate__base_type__
    __initialized: bool = False
    _settings: Dict[str, Dict[str, Any]]
    arguments: Dict[str, Any] = {'format': str, 'timezone': str, 'offset': int}

    _special_variables: List[str] = ['now']

    def __init__(
        self,
        variable: str,
        value: str,
    ) -> None:
        initial_value: str
        timezone: pytz.BaseTzInfo = get_local_timezone()
        date_format = '%Y-%m-%d %H:%M:%S'
        offset: Optional[Dict[str, int]] = None

        safe_value = self.__class__.__base_type__(value)

        if safe_value is not None and '|' in safe_value:
            initial_value, date_arguments = split_value(safe_value)
            arguments = parse_arguments(date_arguments)

            if 'format' in arguments:
                date_format = arguments['format']

            if 'timezone' in arguments:
                timezone = pytz.timezone(arguments['timezone'])

            if 'offset' in arguments:
                offset = parse_timespan(arguments['offset'])
        else:
            initial_value = safe_value

        settings = {
            'format': date_format,
            'timezone': timezone,
            'offset': offset,
        }

        date_value: Union[str, datetime]

        if initial_value is None or len(initial_value) < 1 or initial_value == 'now':
            date_value = 'now'
        else:
            date_value = dateparse(initial_value)

        super().__init__(variable, date_value)

        with self._semaphore:
            if self.__initialized:
                self._settings[variable] = settings

                return

            self._settings = {variable: settings}
            self.__initialized = True

    @classmethod
    def clear(cls: Type['AtomicDate']) -> None:
        super().clear()

        instance = cast(AtomicDate, cls.get())
        variables = list(instance._settings.keys())
        for variable in variables:
            del instance._settings[variable]

    def __getitem__(self, variable: str) -> Optional[str]:
        with self._semaphore:
            value = self._get_value(variable)

            date_value: datetime

            if isinstance(value, str) and value == 'now':
                date_value = datetime.now()
            elif isinstance(value, datetime):
                date_value = value
            else:
                raise ValueError(f'{self.__class__.__name__}.{variable} was incorrectly initialized with "{value}"')

            offset = self._settings[variable]['offset']

            if offset is not None:
                date_value += relativedelta(**offset)

            return date_value.astimezone(
                self._settings[variable]['timezone'],
            ).strftime(self._settings[variable]['format'])

    # not possible to override already set value
    def __setitem__(self, variable: str, value: Optional[Union[str, datetime]]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._settings[variable]
            except KeyError:
                pass

        super().__delitem__(variable)
