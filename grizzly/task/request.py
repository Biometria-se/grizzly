'''This task calls the `request` method of a `grizzly.users` implementation.

This is the most essential task in `grizzly`, it defines requests that the specified load user is going to execute
against the target under test.

Instances of this task is created with the step expressions:

* [`step_task_request_text_with_name_to_endpoint`](/grizzly/usage/steps/scenario/tasks/#step_task_request_text_with_name_to_endpoint)

* [`step_task_request_file_with_name_endpoint`](/grizzly/usage/steps/scenario/tasks/#step_task_request_file_with_name_endpoint)

* [`step_task_request_file_with_name`](/grizzly/usage/steps/scenario/tasks/#step_task_request_file_with_name)

* [`step_task_request_text_with_name`](/grizzly/usage/steps/scenario/tasks/#step_task_request_text_with_name)
'''
from typing import List, Optional, Any, Callable
from dataclasses import dataclass, field

from jinja2.environment import Template
from grizzly_extras.transformer import TransformerContentType
from grizzly_extras.arguments import parse_arguments, split_value, unquote

from ..types import HandlerType, RequestMethod
from ..context import GrizzlyTask, GrizzlyTasksBase

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
    content_type: TransformerContentType = field(init=False, repr=False, default=TransformerContentType.GUESS)
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

    def __post_init__(self) -> None:
        content_type: TransformerContentType = TransformerContentType.GUESS

        if '|' in self.endpoint:
            value, value_arguments = split_value(self.endpoint)
            arguments = parse_arguments(value_arguments, unquote=False)

            if 'content_type' in arguments:
                content_type = TransformerContentType.from_string(unquote(arguments['content_type']))
                del arguments['content_type']

            value_arguments = ', '.join([f'{key}={value}' for key, value in arguments.items()])
            if len(value_arguments) > 0:
                self.endpoint = f'{value} | {value_arguments}'
            else:
                self.endpoint = value

        self.response.content_type = content_type

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        def _implementation(parent: GrizzlyTasksBase) -> Any:
            return parent.user.request(self)

        return _implementation
