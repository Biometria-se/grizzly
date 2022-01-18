'''This task parses a string representation of a date/time and allows transformation of it, such as specifying an offset or changing the format,
and saves the result as a date/time string in an variable.

At least one arguments needs to specified.

Instances of this task is created with the step expression:

* [`step_task_date`](/grizzly/usage/steps/scenario/tasks/#step_task_date)

## Arguments

* `format` _str_ - a python [`strftime` format string](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes), this argument is required
* `timezone` _str_ (optional) - a valid [timezone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
* `offset` _str_ (optional) - a time span string describing the offset, Y = years, M = months, D = days, h = hours, m = minutes, s = seconds, e.g. `1Y-2M10D`
'''
from typing import Callable, Dict, Any, Optional, cast
from dataclasses import dataclass, field
from datetime import datetime

import pytz

from jinja2 import Template
from dateutil.parser import ParserError, parse as dateparser
from dateutil.relativedelta import relativedelta
from tzlocal import get_localzone as get_local_timezone
from grizzly_extras.arguments import get_unsupported_arguments, split_value, parse_arguments

from ..context import GrizzlyTask, GrizzlyScenarioBase
from ..utils import parse_timespan


@dataclass
class DateTask(GrizzlyTask):
    variable: str
    value: str
    arguments: Dict[str, Optional[str]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        if '|' in self.value:
            self.value, arguments = split_value(self.value)
            self.arguments = parse_arguments(arguments)

            unsupported_arguments = get_unsupported_arguments(['format', 'timezone', 'offset'], self.arguments)

            if len(unsupported_arguments) > 0:
                raise ValueError(f'unsupported arguments {", ".join(unsupported_arguments)}')
        else:
            raise ValueError('no arguments specified')

    def implementation(self) -> Callable[[GrizzlyScenarioBase], Any]:
        def _implementation(parent: GrizzlyScenarioBase) -> Any:
            value_rendered = Template(self.value).render(**parent.user._context['variables'], datetime=datetime)

            arguments_rendered: Dict[str, str] = {}

            for argument_name, argument_value in self.arguments.items():
                if argument_value is None:
                    continue
                arguments_rendered[argument_name] = Template(argument_value).render(**parent.user._context['variables'])

            try:
                date_value = dateparser(value_rendered)
            except ParserError as e:
                raise ValueError(f'"{value_rendered}" is not a valid datetime string') from e

            offset = self.arguments.get('offset', None)
            if offset is not None:
                offset_params = cast(Any, parse_timespan(offset))
                date_value += relativedelta(**offset_params)

            timezone_argument = self.arguments.get('timezone', None)
            timezone: pytz.BaseTzInfo
            if timezone_argument is not None:
                try:
                    timezone = pytz.timezone(timezone_argument)
                except pytz.exceptions.UnknownTimeZoneError as e:
                    raise ValueError(f'"{timezone_argument}" is not a valid time zone') from e
            else:
                timezone = get_local_timezone()

            date_format = cast(str, self.arguments.get('format', '%Y-%m-%d %H:%M:%S'))

            parent.user._context['variables'][self.variable] = date_value.astimezone(timezone).strftime(date_format)

        return _implementation
