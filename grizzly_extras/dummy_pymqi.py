"""Dummy pymqi interface implementation.
This is used if `pymqi` is not installed, to get around typing errors.
"""

from __future__ import annotations


class MD:
    pass


class GMO:
    pass


class Queue:
    pass


class QueueManager:
    pass


def raise_for_error(cls: type[object]) -> None:
    message = f'{cls.__name__} could not import pymqi, have you installed IBM MQ dependencies and set environment variable LD_LIBRARY_PATH?'
    raise NotImplementedError(message)
