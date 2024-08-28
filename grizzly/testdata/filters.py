"""Grizzly native tempalting filters that can be used to manipulate variable values where they are used."""
from __future__ import annotations

import json
from collections import namedtuple
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


@templatingfilter
def fromtestdata(value: NamedTuple) -> dict[str, Any]:
    """Convert testdata object to a dictionary.

    Nested testdata is a `namedtuple` object, e.g. `AtomicCsvReader.test`, where column values are accessed with
    `AtomicCsvReader.test.header1`. If anything should be done for the whole row/item it must be converted to a
    dictionary.

    Example:
    ```gherkin
    Given value of variable "AtomicCsvReader.test" is "test.csv"

    Then log message "test={{ AtomicCsvReader.test | fromtestdata | fromjson }}"
    ```

    Args:
        value (NamedTuple): testdata objekt

    """
    testdata = dict(sorted(value._asdict().items()))
    for k, v in testdata.items():
        if type(v) is namedtuple:  # noqa: PYI024
            testdata.update({k: fromtestdata(v)})

    return testdata


@templatingfilter
def fromjson(value: Optional[Union[list[Any], dict[str, Any], str, int, float]]) -> str:
    """Convert python object to JSON string.

    Convert any (valid) python object to a JSON string.

    Example:
    ```gherkin
    Given value of variable "AtomicCsvReader.test" is "test.csv"

    Then log message "test={{ AtomicCsvReader.test | fromtestdata | fromjson }}"
    ```

    Args:
    value (JsonSerializable): value to convert to JSON string

    """
    return json.dumps(value)
