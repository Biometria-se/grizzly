# pylint: disable=line-too-long
'''
@anchor pydoc:grizzly.tasks.date Date
This task parses a string representation of a date/time and allows transformation of it, such as specifying an offset or changing the format,
and saves the result as a date/time string in an variable.


## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_date}

## Arguments

* `variable` _str_ - name of, initialized, variable the parsed date should be saved in

* `value` _str_ - value

## Format

### `value`

``` plain
<date> [| format=<format>][, timezone=<timezone>][, offset=<offset>]
```

* `date` _str/datetime_ - string representation of a date and/or time or a `datetime` object, e.g. `datetime.now()`

* `format` _str_ - a python [`strftime` format string](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) or `ISO-8601:[DateTime|Time][:ms]`, this argument is required

* `timezone` _str_ (optional) - a valid [timezone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)

* `offset` _str_ (optional) - a time span string describing the offset, Y = years, M = months, D = days, h = hours, m = minutes, s = seconds, e.g. `1Y-2M10D`
'''  # noqa: E501
# pylint: enable=line-too-long
from typing import TYPE_CHECKING, Callable, Dict, Any, Optional, cast
from datetime import datetime

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # pylint: disable=import-error
except ImportError:
    # pyright: reportMissingImports=false
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore[no-redef]  # pylint: disable=import-error

from dateutil.parser import ParserError, parse as dateparser
from dateutil.relativedelta import relativedelta

from grizzly_extras.arguments import get_unsupported_arguments, split_value, parse_arguments

from ..utils import parse_timespan
from . import GrizzlyTask, template

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


@template('value', 'arguments')
class DateTask(GrizzlyTask):
    variable: str
    value: str
    arguments: Dict[str, Optional[str]]

    def __init__(self, variable: str, value: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.variable = variable
        self.value = value

        if '|' in self.value:
            self.value, date_arguments = split_value(self.value)
            self.arguments = parse_arguments(date_arguments)

            unsupported_arguments = get_unsupported_arguments(['format', 'timezone', 'offset'], self.arguments)

            if len(unsupported_arguments) > 0:
                raise ValueError(f'unsupported arguments {", ".join(unsupported_arguments)}')
        else:
            raise ValueError('no arguments specified')

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            value_rendered = parent.render(self.value, dict(datetime=datetime))

            arguments_rendered: Dict[str, str] = {}

            for argument_name, argument_value in self.arguments.items():
                if argument_value is None:
                    continue
                arguments_rendered[argument_name] = parent.render(argument_value)

            try:
                date_value = dateparser(value_rendered)
            except ParserError as e:
                raise ValueError(f'"{value_rendered}" is not a valid datetime string') from e

            offset = self.arguments.get('offset', None)
            if offset is not None:
                offset_rendered = parent.render(offset)
                offset_params = cast(Any, parse_timespan(offset_rendered))
                date_value += relativedelta(**offset_params)

            timezone_argument = self.arguments.get('timezone', None)
            timezone: Optional[ZoneInfo] = None  # None in asttimezone == local time zone
            if timezone_argument is not None:
                try:
                    timezone = ZoneInfo(timezone_argument)
                except ZoneInfoNotFoundError as e:
                    raise ValueError(f'"{timezone_argument}" is not a valid time zone') from e

            date_format = cast(str, self.arguments.get('format', '%Y-%m-%d %H:%M:%S'))

            if date_format.startswith('ISO-8601'):
                _, date_format = date_format.split(':', 1)
                iso_date_value = date_value.astimezone(timezone)
                if not date_format.endswith(':ms'):
                    iso_date_value = iso_date_value.replace(microsecond=0)

                iso_value = iso_date_value.isoformat()

                if date_format.startswith('Time'):
                    _, iso_value = iso_value.split('T', 1)

                value = iso_value
            else:
                value = date_value.astimezone(timezone).strftime(date_format)

            parent.user._context['variables'][self.variable] = value

        return task
