'''This task calls the `request` method of a `grizzly.users` implementation.

This is the most essential task in `grizzly`, it defines requests that the specified load user is going to execute
against the target under test.

Instances of this task is created with the step expressions:

* [`step_task_request_text_with_name_to_endpoint`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_request_text_with_name_to_endpoint)

* [`step_task_request_file_with_name_endpoint`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_request_file_with_name_endpoint)

* [`step_task_request_file_with_name`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_request_file_with_name)

* [`step_task_request_text_with_name`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_request_text_with_name)
'''
from typing import TYPE_CHECKING, List, Optional, Any, Callable

from jinja2.environment import Template
from grizzly_extras.transformer import TransformerContentType
from grizzly_extras.arguments import parse_arguments, split_value, unquote

from ..types import RequestMethod
# need to rename to avoid unused-import collision due to RequestTask.template ?!
from . import GrizzlyTask, template as _template  # pylint: disable=unused-import

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario
    from ..users.base.response_handler import ResponseHandlerAction


class RequestTaskHandlers:
    metadata: List['ResponseHandlerAction']
    payload: List['ResponseHandlerAction']

    def __init__(self) -> None:
        self.metadata = []
        self.payload = []

    def add_metadata(self, handler: 'ResponseHandlerAction') -> None:
        self.metadata.append(handler)

    def add_payload(self, handler: 'ResponseHandlerAction') -> None:
        self.payload.append(handler)


class RequestTaskResponse:
    status_codes: List[int]
    content_type: TransformerContentType
    handlers: RequestTaskHandlers

    def __init__(self) -> None:
        self.status_codes = [200]
        self.content_type = TransformerContentType.UNDEFINED
        self.handlers = RequestTaskHandlers()

    def add_status_code(self, status: int) -> None:
        absolute_status = abs(status)
        if absolute_status not in self.status_codes or status not in self.status_codes:
            if absolute_status == status:
                self.status_codes.append(status)
            else:
                index = self.status_codes.index(absolute_status)
                self.status_codes.pop(index)


@_template('name', 'endpoint', 'source')
class RequestTask(GrizzlyTask):
    method: RequestMethod
    name: str
    endpoint: str
    _template: Optional[Template]
    _source: Optional[str]

    response: RequestTaskResponse

    def __init__(self, method: RequestMethod, name: str, endpoint: str, source: Optional[str] = None, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.method = method
        self.name = name
        self.endpoint = endpoint

        self._template = None
        self._source = source

        self.response = RequestTaskResponse()

        content_type: TransformerContentType = TransformerContentType.UNDEFINED

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

    @property
    def source(self) -> Optional[str]:
        return self._source

    @source.setter
    def source(self, value: Optional[str]) -> None:
        self._template = None
        self._source = value

    @property
    def template(self) -> Optional[Template]:
        if self._source is None:
            return None

        if self._template is None:
            self._template = Template(self._source)

        return self._template

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            return parent.user.request(self)

        return task
