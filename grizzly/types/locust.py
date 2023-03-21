from typing import Callable, Union

from locust.runners import MasterRunner, WorkerRunner, LocalRunner
from locust.env import Environment
from locust.rpc.protocol import Message
from locust.exception import StopUser, LocustError, CatchResponseError


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
