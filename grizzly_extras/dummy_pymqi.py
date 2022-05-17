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
    raise NotImplementedError(f'{cls.__name__} could not import pymqi, have you installed IBM MQ dependencies?')
