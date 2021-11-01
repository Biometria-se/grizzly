from dataclasses import dataclass, field
from typing import List, Optional, Callable, Any, Type

from jinja2.environment import Template
from gevent import sleep as gsleep

from .types import ResponseContentType, HandlerType, RequestMethod
from .context import GrizzlyContext, GrizzlyTask, GrizzlyTasksBase
from .transformer import Transformer, transformer
from .exceptions import TransformerError


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

@dataclass
class TransformerTask(GrizzlyTask):
    expression: str
    variable: str
    content: str
    content_type: ResponseContentType

    _transformer: Type[Transformer] = field(init=False, repr=False)
    _get_values: Callable[[Any], List[str]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        grizzly = GrizzlyContext()
        if self.variable not in grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: {self.variable} has not been initialized')

        _transformer = transformer.available.get(self.content_type, None)

        if _transformer is None:
            raise ValueError(f'{self.__class__.__name__}: could not find a transformer for {self.content_type.name}')

        self._transformer = _transformer

        if not self._transformer.validate(self.expression):
            raise ValueError(f'{self.__class__.__name__}: {self.expression} is not a valid expression for {self.content_type.name}')

        self._get_values = self._transformer.parser(self.expression)

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        def _implementation(parent: GrizzlyTasksBase) -> Any:
            content_raw = Template(self.content).render(**parent.user._context['variables'])

            try:
                _, content = self._transformer.transform(self.content_type, content_raw)
            except TransformerError as e:
                raise RuntimeError(f'{self.__class__.__name__}: failed to transform {self.content_type.name}') from e

            values = self._get_values(content)

            number_of_values = len(values)

            if number_of_values != 1:
                raise RuntimeError(f'{self.__class__.__name__}: "{self.expression}" returned {number_of_values} matches')

            value = values[0]

            parent.user._context['variables'][self.variable] = value

        return _implementation
