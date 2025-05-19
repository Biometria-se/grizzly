"""Locust types frequently used in grizzly."""
from collections.abc import Callable
from typing import Union

from locust.env import Environment
from locust.exception import CatchResponseError, LocustError, StopUser
from locust.rpc.protocol import Message
from locust.runners import LocalRunner, MasterRunner, WorkerRunner

MessageHandler = Callable[[Environment, Message], None]

LocustRunner = Union[MasterRunner, WorkerRunner, LocalRunner]

__all__ = [
    'MasterRunner',
    'WorkerRunner',
    'LocalRunner',
    'LocustRunner',
    'Environment',
    'Message',
    'StopUser',
    'LocustError',
    'CatchResponseError',
    'MessageHandler',
]
