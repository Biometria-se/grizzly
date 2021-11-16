from typing import Any


class CD:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

class SCO:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

class MD:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return self


class GMO:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


class Message:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def decode(self, *args: Any, **kwargs: Any) -> Any:
        return self

class Queue(object):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def close(self) -> None:
        pass

    def put(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return Message()

class QueueManager:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def connect_with_options(self, *args: Any, **kwargs: Any) -> None:
        pass

def connect(*args: Any, **kwargs: Any) -> QueueManager:
    return QueueManager()

class MQMIError(Exception):
    pass

class CMQC:
    MQOO_INPUT_SHARED = 0x00000002
    MQOO_BROWSE = 0x00000008
    MQOO_FAIL_IF_QUIESCING = 0x00002000
    MQXPT_TCP = 2
    MQCHT_CLNTCONN = 6
    MQGMO_WAIT = 0x00000001
    MQGMO_FAIL_IF_QUIESCING = 0x00002000
    MQGMO_BROWSE_FIRST = 0x00000010
    MQCC_FAILED = 2
    MQRC_NO_MSG_AVAILABLE = 2033
