"""Task calls the `request` method of a `grizzly.users` implementation.

This is the most essential task in `grizzly`, it defines requests that the specified load user is going to execute
against the target under test.

Optionally, the MIME type of the response can be set, this has to be done if any of the [response][grizzly.steps.scenario.response]
steps is going to be used.

## Step implementations

* [Text with name endpoint][grizzly.steps.scenario.tasks.request.step_task_request_text_with_name_endpoint]

* [File with name endpoint][grizzly.steps.scenario.tasks.request.step_task_request_file_with_name_endpoint]

* [File with name][grizzly.steps.scenario.tasks.request.step_task_request_file_with_name]

* [Text with name][grizzly.steps.scenario.tasks.request.step_task_request_text_with_name]

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `method`.

## Arguments

| Name       | Type            | Description                                                                                                       | Default    |
| ---------- | --------------- | ----------------------------------------------------------------------------------------------------------------- | ---------- |
| `method`   | `RequestMethod` | method used for the request, e.g. `GET` or `POST`, also includes the direction (to or from)                       | _required_ |
| `name`     | `str`           | name of the request, used in `locust` statistics                                                                  | _required_ |
| `endpoint` | `str`           | endpoint on the load testing target, have different meaning depending on the specified [load user][grizzly.users] | _required_ |
| `source`   | `str`           | payload data sent to `endpoint`, can be a file path                                                               | `None`     |

## Format

### endpoint

All pipe arguments will be removed from `endpoint` before creating the task instance. Depending on the [load user][grizzly.users], other
pipe arguments might be supported.

```plain
<endpoint> [| content_type=<content_type>[, timeout=<seconds]]
```

| Name           | Type                     | Description                                                                                                        | Default               |
| -------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------ | --------------------- |
| `endpoint`     | `str`                    | endpoint in format that the specified [load user][grizzly.users] understands                                       | _required_            |
| `content_type` | `TransformerContentType` | MIME type of response from `endpoint`                                                                              | `None`                |
| `timeout`      | `float`                  | maximum number of seconds that the task is allowed to execute, if it is exceeded `TaskTimeOutError` will be raised | `None` (indefinitely) |

Specifying MIME/content type as an argument to `endpoint` is the same as using [content type][grizzly.steps.scenario.response.step_response_content_type] step.

```gherkin
Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test | content_type=json"

# same as
Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test"
And set response content type to "application/json"
```

"""  # noqa: E501

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from grizzly_common.arguments import parse_arguments, split_value, unquote
from grizzly_common.text import has_separator
from grizzly_common.transformer import TransformerContentType

from . import GrizzlyMetaRequestTask, grizzlytask
from . import template as task_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.events.response_handler import ResponseHandlerAction
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import GrizzlyResponse, RequestMethod


class RequestTaskHandlers:
    metadata: list[ResponseHandlerAction]
    payload: list[ResponseHandlerAction]

    def __init__(self) -> None:
        self.metadata = []
        self.payload = []

    def add_metadata(self, handler: ResponseHandlerAction) -> None:
        self.metadata.append(handler)

    def add_payload(self, handler: ResponseHandlerAction) -> None:
        self.payload.append(handler)


class RequestTaskResponse:
    status_codes: list[int]
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


@task_template('name', 'endpoint', 'source', 'arguments', 'metadata')
class RequestTask(GrizzlyMetaRequestTask):
    __rendered__: bool
    method: RequestMethod
    name: str
    endpoint: str
    source: str | None
    arguments: dict[str, str] | None
    metadata: dict[str, str]
    async_request: bool

    response: RequestTaskResponse

    def __init__(self, method: RequestMethod, name: str, endpoint: str, source: str | None = None) -> None:
        self.method = method
        self.name = name
        self.endpoint = endpoint
        self.arguments = None
        self.metadata = {}
        self.async_request = False

        self.source = source

        self.response = RequestTaskResponse()
        self.__rendered__ = False

        content_type: TransformerContentType = TransformerContentType.UNDEFINED
        timeout: float | None = None

        if has_separator('|', self.endpoint):
            value, value_arguments = split_value(self.endpoint)
            self.arguments = parse_arguments(value_arguments, unquote=True)

            if 'content_type' in self.arguments:
                content_type = TransformerContentType.from_string(unquote(self.arguments['content_type']))
                del self.arguments['content_type']

                if content_type == TransformerContentType.MULTIPART_FORM_DATA and (
                    'multipart_form_data_name' not in self.arguments or 'multipart_form_data_filename' not in self.arguments
                ):
                    message = f'Content type multipart/form-data requires endpoint arguments multipart_form_data_name and multipart_form_data_filename: {self.endpoint}'
                    raise AssertionError(message)

            if 'timeout' in self.arguments:
                try:
                    timeout = float(self.arguments['timeout'])
                    del self.arguments['timeout']
                except:
                    message = f'unable to interprent value of "timeout" argument in "{self.endpoint}"'
                    raise AssertionError(message) from None

            self.endpoint = value

        self.response.content_type = content_type
        self.content_type = content_type

        super().__init__(timeout=timeout)

    def add_metadata(self, key: str, value: str) -> None:
        """Add new metadata key value, where default value of metadata is None, it must be initialized as a dict."""
        self.metadata.update({key: value})

    def __call__(self) -> grizzlytask:
        @grizzlytask.metadata(timeout=self.timeout, method=self.method.name, name=self.name)
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            return parent.user.request(self)

        return task

    def execute(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        return parent.user.request(self)
