"""@anchor pydoc:grizzly.testdata.variables.date Date
This variable is used to format and use dates.

## Format

Initial value can, other than a parseable datetime string, be `now`. Each time the variable is accessed the value will represent that date and time
at the time of access.

## Arguments

* `format` _str_ - a python [`strftime` format string](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes), this argument is required
* `timezone` _str_ (optional) - a valid [timezone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
* `offset` _str_ (optional) - a time span string describing the offset, Y = years, M = months, D = days, h = hours, m = minutes, s = seconds, e.g. `1Y-2M10D`

## Example

```gherkin
And value for variable "AtomicDate.arrival" is "now | format='%Y-%m-%dT%H:%M:%S.000Z', timezone=UTC"
```

This can then be used in a template:

```json
{
    "arrival": "{{ AtomicDate.arrival }}",
    "location": "Port of Shanghai"
}
```
"""
from __future__ import annotations

from contextlib import suppress
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional, Set, Type, Union, cast

from dateutil.parser import ParserError
from dateutil.parser import parse as dateparse
from dateutil.relativedelta import relativedelta

from grizzly.types import ZoneInfo, ZoneInfoNotFoundError
from grizzly.utils import parse_timespan
from grizzly_extras.arguments import parse_arguments, split_value

from . import AtomicVariable


def atomicdate__base_type__(value: str) -> str:  # noqa: PLR0912
    """Validate values that `AtomicDate` can be initialized with."""
    if not isinstance(value, str):
        message = f'AtomicDate: {value} ({type(value)}) is not a string'  # type: ignore[unreachable]
        raise TypeError(message)

    if '|' in value:
        date_value, date_arguments = split_value(value)

        try:
            arguments = parse_arguments(date_arguments)
        except ValueError as e:
            message = f'AtomicDate: {e!s}'
            raise ValueError(message) from e

        for argument in arguments:
            if argument not in AtomicDate.arguments:
                message = f'AtomicDate: argument {argument} is not allowed'
                raise ValueError(message)

        if 'format' not in arguments:
            message = f'AtomicDate: date format is not specified: "{value}"'
            raise ValueError(message)

        if 'timezone' in arguments:
            try:
                ZoneInfo(arguments['timezone'])
            except ZoneInfoNotFoundError as e:
                message = f'AtomicDate: unknown timezone "{arguments["timezone"]}"'
                raise ValueError(message) from e

        if 'offset' in arguments:
            parse_timespan(arguments['offset'])

        value = f'{date_value} | {date_arguments}'
    else:
        date_value = value

    if date_value in ['now']:
        return value

    try:
        dateparse(date_value)
    except (TypeError, ParserError) as e:
        message = f'AtomicDate: {e!s}'
        raise ValueError(message) from e
    else:
        return value


class AtomicDate(AtomicVariable[Union[str, datetime]]):
    __base_type__ = atomicdate__base_type__
    __initialized: bool = False
    _settings: Dict[str, Dict[str, Any]]
    arguments: ClassVar[Dict[str, Any]] = {'format': str, 'timezone': str, 'offset': int}

    _special_variables: ClassVar[Set[str]] = {'now'}

    def __init__(
        self,
        variable: str,
        value: str,
        *,
        outer_lock: bool = False,
    ) -> None:
        with self.semaphore(outer=outer_lock):
            initial_value: str
            timezone: Optional[ZoneInfo] = None
            date_format = '%Y-%m-%d %H:%M:%S'
            offset: Optional[Dict[str, int]] = None

            safe_value = self.__class__.__base_type__(value)

            if safe_value is not None and '|' in safe_value:
                initial_value, date_arguments = split_value(safe_value)
                arguments = parse_arguments(date_arguments)

                if 'format' in arguments:
                    date_format = arguments['format']

                if 'timezone' in arguments:
                    timezone = ZoneInfo(arguments['timezone'])

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

            date_value = 'now' if initial_value is None or len(initial_value) < 1 or initial_value == 'now' else dateparse(initial_value)

            super().__init__(variable, date_value, outer_lock=True)

            if self.__initialized:
                self._settings[variable] = settings

                return

            self._settings = {variable: settings}
            self.__initialized = True

    @classmethod
    def clear(cls: Type[AtomicDate]) -> None:
        super().clear()

        instance = cast(AtomicDate, cls.get())
        variables = list(instance._settings.keys())
        for variable in variables:
            del instance._settings[variable]

    def __getitem__(self, variable: str) -> Optional[str]:
        with self.semaphore():
            value = self._get_value(variable)

            date_value: datetime

            if isinstance(value, str) and value == 'now':
                date_value = datetime.now()
            elif isinstance(value, datetime):
                date_value = value
            else:
                message = f'{self.__class__.__name__}.{variable} was incorrectly initialized with "{value}"'
                raise ValueError(message)

            offset = self._settings[variable]['offset']

            if offset is not None:
                date_value += relativedelta(**offset)

            return date_value.astimezone(
                self._settings[variable]['timezone'],
            ).strftime(self._settings[variable]['format'])

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            with suppress(KeyError):
                del self._settings[variable]

            super().__delitem__(variable)
