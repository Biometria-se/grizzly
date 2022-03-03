from dataclasses import dataclass, field
from abc import ABCMeta
from typing import Callable, Any

from ..context import GrizzlyContextScenario
from ..scenarios import GrizzlyScenario
from .request import RequestTask, RequestTaskHandlers, RequestTaskResponse
from .wait import WaitTask
from .print import PrintTask
from .transformer import TransformerTask
from .until import UntilRequestTask
from .date import DateTask

__all__ = [
    'RequestTaskHandlers',
    'RequestTaskResponse',
    'RequestTask',
    'PrintTask',
    'WaitTask',
    'TransformerTask',
    'UntilRequestTask',
    'DateTask',
]

@dataclass(unsafe_hash=True)
class GrizzlyTask(metaclass=ABCMeta):
    scenario: 'GrizzlyContextScenario' = field(init=False, repr=False)

    def implementation(self) -> Callable[[GrizzlyScenario], Any]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented "implementation"')
