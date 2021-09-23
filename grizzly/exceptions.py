from typing import Optional

from locust.exception import StopUser


class ResponseHandlerError(StopUser):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message


class TransformerError(StopUser):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message

