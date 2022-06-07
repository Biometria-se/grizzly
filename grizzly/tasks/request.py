'''
@anchor pydoc:grizzly.tasks.request Request
This task calls the `request` method of a `grizzly.users` implementation.

This is the most essential task in `grizzly`, it defines requests that the specified load user is going to execute
against the target under test.

Optionally, the MIME type of the response can be set, this has to be done if any of the {@pylink grizzly.steps.scenario.response}
steps is going to be used.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_request_text_with_name_endpoint}

* {@pylink grizzly.steps.scenario.tasks.step_task_request_file_with_name_endpoint}

* {@pylink grizzly.steps.scenario.tasks.step_task_request_file_with_name}

* {@pylink grizzly.steps.scenario.tasks.step_task_request_text_with_name}

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `method`.

## Arguments

* `method` _RequestMethod_ - method used for the request, e.g. `GET` or `POST`, also includes the direction (to or from)

* `name` _str_ - name of the request, used in `locust` statistics

* `endpoint` _str_ - endpoint on the load testing target, have different meaning depending on the specified {@pylink grizzly.users}

* `source` _str_ (optional) - payload data sent to `endpoint`, can be a file path

## Format

### `endpoint`

All arguments will be removed from `endpoint` before creating the task instance.

``` plain
<endpoint> [| content_type=<content_type>]
```

* `endpoint` _str_ - endpoint in format that the specified {@pylink grizzly.users} understands

* `content_type` _TransformerContentType_ (optional) - MIME type of response from `endpoint`

Specifying MIME/content type as an argument to `endpoint` is the same as using {@pylink grizzly.steps.scenario.response.step_response_content_type}.

``` gherkin
Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test | content_type=json"

# same as
Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test"
And set response content type to "application/json"
```

'''
from typing import TYPE_CHECKING, List, Optional, Any, Callable

from jinja2.environment import Template
from grizzly_extras.transformer import TransformerContentType
from grizzly_extras.arguments import parse_arguments, split_value, unquote

from ..types import RequestMethod
# need to rename to avoid unused-import collision due to RequestTask.template ?!
from . import GrizzlyTask, template  # pylint: disable=unused-import

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


@template('name', 'endpoint', 'source')
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
