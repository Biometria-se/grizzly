"""Grizzly native tempalting filters that can be used to manipulate variable values where they are used."""
from __future__ import annotations

import json
from base64 import b64decode as base64_b64decode
from base64 import b64encode as base64_b64encode
from typing import Any, Callable, NamedTuple, Optional, Union

from jinja2.filters import FILTERS


class templatingfilter:
    name: str
    func: Callable

    def __init__(self, func: Callable) -> None:
        self.func = func
        name = func.__name__
        existing_filter = FILTERS.get(name, None)

        if existing_filter is None:
            FILTERS[name] = func
        elif existing_filter is not func:
            message = f'{name} is already registered as a filter'
            raise AssertionError(message)
        else:
            # code executed twice, so adding the same filter again
            pass

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)


def _is_namedtuple(value: Any) -> bool:
    value_type = type(value)
    bases = value_type.__bases__
    if len(bases) != 1 or bases[0] != tuple:
        return False

    fields = getattr(value_type, '_fields', None)
    if not isinstance(fields, tuple):
        return False

    if not hasattr(value, '_asdict'):
        return False

    return all(isinstance(field, str) for field in fields)


@templatingfilter
def fromtestdata(value: NamedTuple) -> dict[str, Any]:
    """Convert testdata object to a dictionary.

    Nested testdata is a `namedtuple` object, e.g. `AtomicCsvReader.test`, where column values are accessed with
    `AtomicCsvReader.test.header1`. If anything should be done for the whole row/item it must be converted to a
    dictionary.

    Example:
    ```gherkin
    Given value of variable "AtomicCsvReader.test" is "test.csv"

    Then log message "test={{ AtomicCsvReader.test | fromtestdata | stringify }}"
    ```

    Args:
        value (NamedTuple): testdata objekt

    """
    testdata = dict(sorted(value._asdict().items()))
    for k, v in testdata.items():
        if _is_namedtuple(v):
            testdata.update({k: fromtestdata(v)})

    return testdata


@templatingfilter
def stringify(value: Optional[Union[list[Any], dict[str, Any], str, int, float]]) -> str:
    """Convert python object to JSON string.

    Convert any (valid) python object to a JSON string.

    Example:
    ```gherkin
    Given value of variable "AtomicCsvReader.test" is "test.csv"

    Then log message "test={{ AtomicCsvReader.test | fromtestdata | stringify }}"
    ```

    Args:
        value (JsonSerializable): value to convert to JSON string

    """
    return json.dumps(value)


@templatingfilter
def b64encode(value: str) -> str:
    """Base64 encode string value.

    Example:
    ```gherkin
    Given value of variable "input_value" is "foobar"
    Then log message "input_value (base64): {{ input_value | b64encode }}"
    ```

    Args:
        value (str): value to base64 encode

    """
    return base64_b64encode(value.encode()).decode()


@templatingfilter
def b64decode(value: str) -> str:
    """Base64 decode string value.

    Example:
    ```gherkin
    Given value of variable "input_value" is "Zm9vYmFy"
    Then log message "input_value: {{ input_value | b64decode }}"
    ```

    Args:
        value (str): value to base64 encode

    """
    return base64_b64decode(value).decode()
