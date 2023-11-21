"""Utilities used by grizzly_extras.async_message."""
from __future__ import annotations

from typing import Any, Union


def tohex(value: Union[int, str, bytes, bytearray, Any]) -> str:
    if isinstance(value, str):
        return ''.join(f'{ord(c):02x}' for c in value)

    if isinstance(value, (bytes, bytearray)):
        return value.hex()

    if isinstance(value, int):
        return hex(value)[2:]

    message = f'{value} has an unsupported type {type(value)}'
    raise ValueError(message)
