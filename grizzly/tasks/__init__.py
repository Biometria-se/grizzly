from abc import ABCMeta
from typing import TYPE_CHECKING, Any, Callable, List

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario


class GrizzlyTask(metaclass=ABCMeta):
    scenario: 'GrizzlyContextScenario'

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented "implementation"')

    def get_templates(self) -> List[str]:
        return []


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
