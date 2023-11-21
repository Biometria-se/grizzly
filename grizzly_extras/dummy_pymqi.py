"""Dummy pymqi interface implementation.
This is used if `pymqi` is not installed, to get around typing errors.
"""
from __future__ import annotations

from typing import Type


class MD:
    pass


class GMO:
    pass


class Queue:
    pass


class QueueManager:
    pass


def raise_for_error(cls: Type[object]) -> None:
    message = f'{cls.__name__} could not import pymqi, have you installed IBM MQ dependencies?'
    raise NotImplementedError(message)
