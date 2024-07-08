"""@anchor pydoc:grizzly_extras.arguments
Parse text string and construct a dictionary with key-value pairs.

Logic to support grizzly arguments, which are passed in text strings.
"""
from __future__ import annotations

from typing import Any, Optional, cast

from .text import has_sequence


def split_value(value: str, separator: str = '|') -> tuple[str, str]:
    operators = ["=", "|"]

    try:
        if value.count(separator) > 1:
            left_index = value.index(separator)
            right_index = value.rindex(separator)

            if value[left_index + 1] not in operators:
                values = value.split(separator, 1)
            elif value[right_index + 1] not in operators:
                values = value.rsplit(separator, 1)
            else:
                raise ValueError  # default
        else:
            raise ValueError  # default
    except ValueError:
        values = value.split(separator, 1)

    return cast(tuple[str, str], tuple([v.strip() for v in values]))


def get_unsupported_arguments(valid_arguments: list[str], arguments: dict[str, Any]) -> list[str]:
    return [argument for argument in arguments if argument not in valid_arguments]


def unquote(argument: str) -> str:
    if argument[0] == argument[-1] and argument[0] in ['"', "'"]:
        argument = argument[1:-1]

    return argument


def parse_arguments(arguments: str, separator: str = '=', *, unquote: bool = True) -> dict[str, Any]:  # noqa: C901, PLR0912, PLR0915
    if separator not in arguments or (arguments.count(separator) > 1 and (arguments.count('"') < 2 and arguments.count("'") < 2) and ', ' not in arguments):
        message = f'incorrect format in arguments: "{arguments}"'
        raise ValueError(message)

    parsed: dict[str, Any] = {}
    previous_part: Optional[str] = None
    argument_parts = arguments.split(',')

    for part_index, _argument in enumerate(argument_parts):
        if previous_part is not None:
            argument = f'{previous_part},{_argument}'
            previous_part = None
        else:
            argument = _argument

        if len(argument.strip()) < 1:
            message = f'incorrect format for arguments: "{arguments}"'
            raise ValueError(message)

        if separator not in argument:
            message = f'incorrect format for argument: "{argument.strip()}"'
            raise ValueError(message)

        key, value = argument.strip().split(separator, 1)

        key = key.strip()
        if '"' in key or "'" in key or ' ' in key:
            message = 'no quotes or spaces allowed in argument names'
            raise ValueError(message)

        value = value.strip()

        if len(value) < 1:
            message = f'invalid value for argument "{key}"'
            raise ValueError(message)

        start_quote: Optional[str] = None

        has_equals = has_sequence('==', value)
        has_or = has_sequence('|=', value)
        inline_quotes = (has_equals or has_or) and '?' not in value and '@' not in value
        sequence = '==' if has_equals else '|='

        # == = 2 characters ------------------------------------------v
        start_index = 0 if not inline_quotes else value.index(sequence) + 2

        if value[start_index] in ['"', "'"]:
            if value[-1] != value[start_index]:
                if previous_part is None and part_index < len(argument_parts) - 1:
                    previous_part = argument
                    continue

                message = f'value is incorrectly quoted: "{value}"'
                raise ValueError(message)

            start_quote = value[start_index]
            if unquote and start_index == 0:
                value = value[1:]

        if value[-1] in ['"', "'"]:
            if start_quote is None:
                message = f'value is incorrectly quoted: "{value}"'
                raise ValueError(message)
            if unquote and start_index == 0:
                value = value[:-1]

        if start_quote is None and ' ' in value:
            message = f'value needs to be quoted: "{value}"'
            raise ValueError(message)

        parsed[key] = value

    return parsed
