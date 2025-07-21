"""Locust types frequently used in grizzly."""

from collections.abc import Callable

from locust.env import Environment
from locust.exception import CatchResponseError, LocustError, StopUser
from locust.rpc.protocol import Message
from locust.runners import LocalRunner, MasterRunner, WorkerRunner

MessageHandler = Callable[[Environment, Message], None]

LocustRunner = MasterRunner | WorkerRunner | LocalRunner

__all__ = [
    'CatchResponseError',
    'Environment',
    'LocalRunner',
    'LocustError',
    'LocustRunner',
    'MasterRunner',
    'Message',
    'MessageHandler',
    'StopUser',
    'WorkerRunner',
]
