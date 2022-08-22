from typing import Optional

from locust.exception import StopUser

__all__ = [
    'StopUser',
]


class ResponseHandlerError(StopUser):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message


class TransformerLocustError(StopUser):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message


class RestartScenario(Exception):
    pass


class StopScenario(Exception):
    pass
