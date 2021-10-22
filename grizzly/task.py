from dataclasses import dataclass, field
from typing import List, Optional, Callable, Any

from jinja2.environment import Template
from gevent import sleep as gsleep

from .types import ResponseContentType, HandlerType, RequestMethod
from .context import GrizzlyTask, GrizzlyTasksBase


@dataclass(unsafe_hash=True)
class RequestTaskHandlers:
    metadata: List[HandlerType] = field(init=False, hash=False, default_factory=list)
    payload: List[HandlerType] = field(init=False, hash=False, default_factory=list)

    def add_metadata(self, handler: HandlerType) -> None:
        self.metadata.append(handler)

    def add_payload(self, handler: HandlerType) -> None:
        self.payload.append(handler)


@dataclass(unsafe_hash=True)
class RequestTaskResponse:
    status_codes: List[int] = field(init=False, repr=False, hash=False, default_factory=list)
    content_type: ResponseContentType = field(init=False, repr=False, default=ResponseContentType.GUESS)
    handlers: RequestTaskHandlers = field(init=False, repr=False, default_factory=RequestTaskHandlers)

    def __post_init__(self) -> None:
        if 200 not in self.status_codes:
            self.status_codes.append(200)

    def add_status_code(self, status: int) -> None:
        absolute_status = abs(status)
        if absolute_status not in self.status_codes or status not in self.status_codes:
            if absolute_status == status:
                self.status_codes.append(status)
            else:
                index = self.status_codes.index(absolute_status)
                self.status_codes.pop(index)


@dataclass(unsafe_hash=True)
class RequestTask(GrizzlyTask):
    method: RequestMethod
    name: str
    endpoint: str
    template: Optional[Template] = field(init=False, repr=False, default=None)
    source: Optional[str] = field(init=False, repr=False, default=None)

    response: RequestTaskResponse = field(init=False, repr=False, default_factory=RequestTaskResponse)

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        def _implementation(parent: GrizzlyTasksBase) -> Any:
            return parent.user.request(self)

        return _implementation


@dataclass
class SleepTask(GrizzlyTask):
    sleep: float

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        def _implementation(parent: GrizzlyTasksBase) -> Any:
            parent.logger.debug(f'waiting for {self.sleep} seconds')
            gsleep(self.sleep)
            parent.logger.debug(f'done waiting for {self.sleep} seconds')

        return _implementation


@dataclass
class PrintTask(GrizzlyTask):
    message: str

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        def _implementation(parent: GrizzlyTasksBase) -> Any:
            message = Template(self.message).render(**parent.user._context['variables'])
            parent.logger.info(message)

        return _implementation
