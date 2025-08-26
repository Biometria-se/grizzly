"""Locust types frequently used in grizzly."""

from collections.abc import Callable
from dataclasses import dataclass

from locust.env import Environment
from locust.exception import CatchResponseError, LocustError, ResponseError, StopUser
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
    'ResponseError',
    'StopUser',
    'WorkerRunner',
]


@dataclass
class LocustOption:
    headless: bool
    num_users: int
    spawn_rate: float
    tags: list[str]
    exclude_tags: list[str]
    enable_rebalancing: bool
    web_base_path: str | None
