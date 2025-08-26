"""Parse text string and construct a dictionary with key-value pairs.

Logic to support grizzly arguments, which are passed in text strings.
"""

from __future__ import annotations

from typing import Any, cast

from grizzly_common.text import has_sequence


def split_value(value: str, separator: str = '|') -> tuple[str, str]:
    """Split a value string into two parts using a separator.

    Handles special cases where the separator appears multiple times by checking
    if the character after the separator is an operator (= or |). If so, uses
    the opposite end's separator to split.

    Args:
        value: The string value to split
        separator: The separator character to split on (default: '|')

    Returns:
        A tuple of two stripped string values

    Examples:
        >>> split_value("foo|bar")
        ('foo', 'bar')
        >>> split_value("key|=value|default")
        ('key', '=value|default')

    """
    operators = ['=', '|']

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

    return cast('tuple[str, str]', tuple([v.strip() for v in values]))


def get_unsupported_arguments(valid_arguments: list[str], arguments: dict[str, Any]) -> list[str]:
    """Identify arguments that are not in the list of valid arguments.

    Args:
        valid_arguments: List of argument names that are considered valid
        arguments: Dictionary of arguments to validate (keys are argument names)

    Returns:
        List of argument names from the arguments dict that are not in valid_arguments

    Examples:
        >>> get_unsupported_arguments(['foo', 'bar'], {'foo': 1, 'baz': 2})
        ['baz']

    """
    return [argument for argument in arguments if argument not in valid_arguments]


def unquote(argument: str) -> str:
    """Remove surrounding quotes from a string if present.

    Removes matching quotes (either single or double) from the beginning and end
    of a string. If quotes don't match or aren't present, returns the string unchanged.

    Args:
        argument: The string to unquote

    Returns:
        The string without surrounding quotes, or the original string if no matching quotes

    Examples:
        >>> unquote('"hello"')
        'hello'
        >>> unquote("'world'")
        'world'
        >>> unquote('no quotes')
        'no quotes'
        >>> unquote('"mismatched\'')
        '"mismatched\''

    """
    if argument[0] == argument[-1] and argument[0] in ['"', "'"]:
        argument = argument[1:-1]

    return argument


def parse_arguments(arguments: str, separator: str = '=', *, unquote: bool = True) -> dict[str, Any]:  # noqa: C901, PLR0912, PLR0915
    """Parse a string of comma-separated key-value pairs into a dictionary.

    Supports various formats including quoted values, inline operators (==, |=),
    and handles edge cases like commas within quoted values. Validates that keys
    don't contain quotes or spaces, and that values are properly quoted if they
    contain spaces.

    Args:
        arguments: String containing comma-separated key-value pairs (e.g., "key1=value1, key2='value 2'")
        separator: Character that separates keys from values (default: '=')
        unquote: Whether to remove surrounding quotes from values (default: True)

    Returns:
        Dictionary with parsed key-value pairs

    Raises:
        ValueError: If arguments string has incorrect format, including:
            - Missing separator
            - Empty keys or values
            - Quotes or spaces in key names
            - Improperly quoted values
            - Values with spaces that aren't quoted

    Examples:
        >>> parse_arguments("key1=value1, key2=value2")
        {'key1': 'value1', 'key2': 'value2'}
        >>> parse_arguments("name='John Doe', age=30")
        {'name': 'John Doe', 'age': '30'}
        >>> parse_arguments("url==https://example.com")
        {'url': '==https://example.com'}
        >>> parse_arguments("default|=fallback")
        {'default': '|=fallback'}

    Notes:
        - Supports inline operators: '==' and '|=' as part of the value
        - Handles commas within quoted values correctly
        - Keys cannot contain quotes, spaces, or special characters
        - Values with spaces must be quoted

    """
    if separator not in arguments or (arguments.count(separator) > 1 and (arguments.count('"') < 2 and arguments.count("'") < 2) and ', ' not in arguments):
        message = f'incorrect format in arguments: "{arguments}"'
        raise ValueError(message)

    parsed: dict[str, Any] = {}
    previous_part: str | None = None
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

        start_quote: str | None = None

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
