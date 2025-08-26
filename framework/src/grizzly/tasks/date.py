"""Task parses a string representation of a date/time and allows transformation of it, such as specifying an offset or changing the format,
and saves the result as a date/time string in an variable.


## Step implementations

* [Parse][grizzly.steps.scenario.tasks.date.step_task_date_parse]

## Statistics

No statistics available.

"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

from dateutil.parser import ParserError
from dateutil.parser import parse as dateparser
from dateutil.relativedelta import relativedelta
from grizzly_common.arguments import get_unsupported_arguments, parse_arguments, split_value
from grizzly_common.text import has_separator

from grizzly.types import ZoneInfo, ZoneInfoNotFoundError
from grizzly.utils import parse_timespan

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('value', 'arguments')
class DateTask(GrizzlyTask):
    variable: str
    value: str
    arguments: dict[str, str | None]

    def __init__(self, variable: str, value: str) -> None:
        super().__init__(timeout=None)

        self.variable = variable
        self.value = value

        if has_separator('|', self.value):
            self.value, date_arguments = split_value(self.value)
            self.arguments = parse_arguments(date_arguments)

            unsupported_arguments = get_unsupported_arguments(['format', 'timezone', 'offset'], self.arguments)

            if len(unsupported_arguments) > 0:
                message = f'unsupported arguments {", ".join(unsupported_arguments)}'
                raise ValueError(message)
        else:
            message = 'no arguments specified'
            raise ValueError(message)

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            value_rendered = parent.user.render(self.value)

            arguments_rendered: dict[str, str] = {}

            for argument_name, argument_value in self.arguments.items():
                if argument_value is None:
                    continue
                arguments_rendered[argument_name] = parent.user.render(argument_value)

            try:
                date_value = dateparser(value_rendered)
            except ParserError as e:
                message = f'"{value_rendered}" is not a valid datetime string'
                raise ValueError(message) from e

            offset = self.arguments.get('offset', None)
            if offset is not None:
                offset_rendered = parent.user.render(offset)
                offset_params = cast('Any', parse_timespan(offset_rendered))
                date_value += relativedelta(**offset_params)

            timezone_argument = self.arguments.get('timezone', None)
            timezone: ZoneInfo | None = None  # None in asttimezone == local time zone
            if timezone_argument is not None:
                timezone_argument = parent.user.render(timezone_argument)
                try:
                    timezone = ZoneInfo(timezone_argument)
                except ZoneInfoNotFoundError as e:
                    message = f'"{timezone_argument}" is not a valid time zone'
                    raise ValueError(message) from e

            date_format = cast('str', self.arguments.get('format', '%Y-%m-%d %H:%M:%S'))

            if date_format.startswith('ISO-8601'):
                _, date_format = date_format.split(':', 1)
                iso_date_value = date_value.astimezone(timezone)
                if ':ms' not in date_format:
                    iso_date_value = iso_date_value.replace(microsecond=0)

                iso_value = iso_date_value.isoformat()

                if date_format.startswith('Time'):
                    _, iso_value = iso_value.split('T', 1)

                if ':no-sep' in date_format:
                    offset_sep = iso_value[-6]
                    iso_value, timezone_offset = iso_value.rsplit(offset_sep, 1)
                    iso_value = iso_value.replace('-', '').replace(':', '').replace('.', '')
                    iso_value = f'{iso_value}{offset_sep}{timezone_offset}'

                value = iso_value
            else:
                value = date_value.astimezone(timezone).strftime(date_format)

            with suppress(ValueError):
                if str(int(value)) == value:
                    value = f'"{value}"'

            parent.user.set_variable(self.variable, value)

        return task
