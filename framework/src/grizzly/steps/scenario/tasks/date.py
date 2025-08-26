"""Module contains step implementations for the [Date][grizzly.tasks.date] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import DateTask
from grizzly.types.behave import Context, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('parse date "{value}" and save in variable "{variable}"')
def step_task_date_parse(context: Context, value: str, variable: str) -> None:
    """Create an instance of the [Date][grizzly.tasks.date] task.

    Parses a datetime string and transforms it according to specified arguments.

    See [Date][grizzly.tasks.date] task documentation for more information about arguments.

    This step is useful when changes has to be made to a datetime representation during an iteration of a scenario.

    Example:
    ```gherkin
    ...
    And value for variable "date1" is "none"
    And value for variable "date2" is "none"
    And value for variable "date3" is "none"
    And value for variable "AtomicDate.test" is "now"

    Then parse date "2022-01-17 12:21:37 | timezone=UTC, format="%Y-%m-%dT%H:%M:%S.%f", offset=1D" and save in variable "date1"
    Then parse date "{{ AtomicDate.test }} | offset=-1D" and save in variable "date2"
    Then parse date "{{ datetime.now() }} | offset=1Y" and save in variable "date3"
    ```

    Args:
        value (str): datetime string and arguments
        variable (str): name of, initialized, variable where response will be saved in

    # Format

    ## value

    ```plain
    <date> [| format=<format>][, timezone=<timezone>][, offset=<offset>]
    ```

    | Name       | Type             | Description                                                                                                                                                       | Default        |
    | ---------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
    | `date`     | `str | datetime` | string representation of a date and/or time or a `datetime` object, e.g. `datetime.now()`                                                                         | _required_     |
    | `format`   | `str`            | python [`strftime` format string](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) or `ISO-8601:[DateTime|Time][:ms][:no-sep]` | _required_     |
    | `timezone` | `str`            | valid [timezone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)                                                                               | `None` (local) |
    | `offset`   | `str`            | time span string describing the offset, `Y` = years, `M` = months, `D` = days, `h` = hours, `m` = minutes, `s` = seconds, e.g. `1Y-2M10D`                         | `None`         |

    See [wikipedia ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) for information about the `ISO-8601` format. Specifying `DateTime` would result in "Date and time with the offset"
    and `Time` results in everything after "T" in the same example.

    In addition to this it is also possible to append milliseconds with `:ms` and remove all the seperators in the date and time with `:no-sep`.

    """  # noqa: E501
    grizzly = cast('GrizzlyContext', context.grizzly)
    assert variable in grizzly.scenario.variables, f'variable {variable} has not been initialized'

    grizzly.scenario.tasks.add(
        DateTask(
            value=value,
            variable=variable,
        ),
    )
