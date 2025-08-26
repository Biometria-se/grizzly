"""Client tasks is functionality that is executed by locust and is registred to an URL scheme.

These tasks is used to make a request to another host than the scenario is actually load testing.

## Statistics

Executions of all client tasks will be visible with request type `CLNT`.

## Arguments

| Name       | Type  | Description                                                                                                                      | Default    |
| ---------- | ----- | -------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `endpoint` | `str` | describes the request, used to chose the client task implementation. supports [templating][framework.usage.variables.templating] | _required_ |

If `endpoint` is a template variable which includes the scheme, the scheme for the request must be specified so the
correct [client][grizzly.tasks.clients] task implementation is used. The additional scheme will be removed when the request is
performed.
"""

from __future__ import annotations

import traceback
from abc import abstractmethod
from contextlib import contextmanager
from copy import copy
from datetime import datetime
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from time import perf_counter as time
from typing import TYPE_CHECKING, ClassVar, cast, final
from urllib.parse import unquote, urlparse

from grizzly_common.arguments import parse_arguments, split_value
from grizzly_common.text import has_separator
from grizzly_common.transformer import TransformerContentType

from grizzly.exceptions import StopScenario
from grizzly.tasks import GrizzlyMetaRequestTask, grizzlytask, template
from grizzly.testdata.utils import resolve_variable
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod, RequestType, StrDict
from grizzly.utils import merge_dicts, normalize

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

    from grizzly.context import GrizzlyContextScenario
    from grizzly.scenarios import GrizzlyScenario


# see https://github.com/python/mypy/issues/5374
@template('endpoint', 'destination', 'source', 'name', 'variable_template')
class ClientTask(GrizzlyMetaRequestTask):
    __scenario__: ClassVar[GrizzlyContextScenario]

    _scenario: GrizzlyContextScenario
    _schemes: list[str]
    _scheme: str
    _short_name: str
    _direction_arrow: ClassVar[dict[RequestDirection, str]] = {
        RequestDirection.FROM: '<-',
        RequestDirection.TO: '->',
    }
    _context: ClassVar[dict] = {}

    host: str
    direction: RequestDirection
    endpoint: str
    name: str | None
    payload_variable: str | None
    metadata_variable: str | None
    source: str | None
    destination: str | None
    _text: str | None
    method: RequestMethod
    arguments: dict[str, str]

    log_dir: Path

    def __init__(  # noqa: PLR0915, PLR0912
        self,
        direction: RequestDirection,
        endpoint: str,
        name: str | None = None,
        /,
        payload_variable: str | None = None,
        metadata_variable: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        text: str | None = None,
        method: RequestMethod | None = None,
    ) -> None:
        super().__init__(timeout=None)

        if text is not None:
            self.text = text
        else:
            self._text = None

        self._scenario = copy(self.__scenario__)
        self._scenario._tasks = self.__scenario__._tasks

        endpoint = cast('str', resolve_variable(self._scenario, endpoint, try_template=False))

        try:
            parsed = urlparse(endpoint)
            proto_sep = endpoint.index('://') + 3
            # if `proto_sep` is followed by the start of a jinja template, we should remove the specified protocol, since it will
            # be part of the rendered template
            if proto_sep != endpoint.rindex('://') + 3 or (endpoint[proto_sep : proto_sep + 2] == '{{' and '}}' in endpoint):
                endpoint = endpoint[proto_sep:]
        except ValueError:
            pass

        if parsed.scheme not in self._schemes:
            message = f'{self.__class__.__name__}: "{parsed.scheme}" is not supported, must be one of {", ".join(self._schemes)}'
            raise AttributeError(message)

        self._scheme = parsed.scheme

        content_type: TransformerContentType = TransformerContentType.UNDEFINED

        self.arguments = {}

        if has_separator('|', endpoint):
            value, value_arguments = split_value(endpoint)
            self.arguments = parse_arguments(value_arguments, unquote=True)

            if 'content_type' in self.arguments:
                content_type = TransformerContentType.from_string(unquote(self.arguments['content_type']))
                del self.arguments['content_type']

            endpoint = value

        self.content_type = content_type
        self.direction = direction
        self.endpoint = endpoint
        self.name = name
        self.payload_variable = payload_variable
        self.metadata_variable = metadata_variable
        self.source = source
        self.destination = destination
        self.host = endpoint

        if self.payload_variable is not None and self.direction != RequestDirection.FROM:
            message = f'{self.__class__.__name__}: variable argument is not applicable for direction {self.direction.name}'
            raise AssertionError(message)

        if self.source is not None and self.direction != RequestDirection.TO:
            message = f'{self.__class__.__name__}: source argument is not applicable for direction {self.direction.name}'
            raise AssertionError(message)

        if self.payload_variable is not None and self.payload_variable not in self._scenario.variables:
            message = f'{self.__class__.__name__}: variable {self.payload_variable} has not been initialized'
            raise AssertionError(message)

        if self.metadata_variable is not None and self.metadata_variable not in self._scenario.variables:
            message = f'{self.__class__.__name__}: variable {self.metadata_variable} has not been initialized'
            raise AssertionError(message)

        if self.payload_variable is None and self.metadata_variable is not None:
            message = f'{self.__class__.__name__}: payload variable is not set, but metadata variable is set'
            raise AssertionError(message)

        if self.source is None and self.direction == RequestDirection.TO:
            message = f'{self.__class__.__name__}: source must be set for direction {self.direction.name}'
            raise AssertionError(message)

        self._short_name = self.__class__.__name__.replace('ClientTask', '')

        context_root = environ.get('GRIZZLY_CONTEXT_ROOT', None)
        assert context_root is not None, 'environment variable GRIZZLY_CONTEXT_ROOT is not set!'

        self.log_dir = Path(context_root) / 'logs'
        log_dir = environ.get('GRIZZLY_LOG_DIR', None)
        if log_dir is not None:
            self.log_dir /= log_dir

        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.__class__._context = merge_dicts(self.__class__._context, self._context)

        if method is None:
            self.method = RequestMethod.GET if self.direction == RequestDirection.FROM else RequestMethod.PUT
        else:
            self.method = method

    def on_start(self, parent: GrizzlyScenario) -> None:
        pass

    def on_stop(self, parent: GrizzlyScenario) -> None:
        pass

    # SOW: see https://github.com/python/mypy/issues/5936#issuecomment-1429175144
    def text_fget(self) -> str | None:
        return self._text

    def text_fset(self, _: str) -> None:
        message = f'{self.__class__.__name__} has not implemented support for step text'
        raise NotImplementedError(message)  # pragma: no cover

    text = property(text_fget, text_fset)
    # EOW

    @property
    def variable_template(self) -> str | None:
        if self.payload_variable is None or ('{{' in self.payload_variable and '}}' in self.payload_variable):
            return self.payload_variable

        template = f'{{{{ {self.payload_variable} }}}}'

        if self.metadata_variable is not None:
            template = f'{template} {{{{ {self.metadata_variable} }}}}'

        return template

    @final
    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> GrizzlyResponse:
            return self.execute(parent)

        @task.on_start
        def on_start(parent: GrizzlyScenario) -> None:
            self.__class__._context = merge_dicts(parent.user._context, self._context)
            return self.on_start(parent)

        @task.on_stop
        def on_stop(parent: GrizzlyScenario) -> None:
            self.__class__._context = merge_dicts(parent.user._context, self._context)
            return self.on_stop(parent)

        @task.on_iteration
        def on_iteration(parent: GrizzlyScenario) -> None:
            self.__class__._context = merge_dicts(parent.user._context, self._context)
            return self.on_iteration(parent)

        return task

    def execute(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        """Execute correct method depending on the direction on the request. When wrapped in another task, the @grizzlytask decorated method above will not be called."""
        self.__class__._context = merge_dicts(parent.user._context, self.__class__._context)

        if self.direction == RequestDirection.FROM:
            return self.request_from(parent)

        return self.request_to(parent)

    @abstractmethod
    def request_from(self, parent: GrizzlyScenario) -> GrizzlyResponse:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented {self.method.name}'
        raise NotImplementedError(message)

    @abstractmethod
    def request_to(self, parent: GrizzlyScenario) -> GrizzlyResponse:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented {self.method.name}'
        raise NotImplementedError(message)

    @contextmanager
    def action(self, parent: GrizzlyScenario, action: str | None = None, *, suppress: bool = False) -> Generator[StrDict, None, None]:
        exception: Exception | None = None
        response_length = 0
        start_time = time()
        meta: StrDict = {}

        try:
            # get metadata back from actual implementation
            yield meta
        except Exception as e:
            exception = e
            parent.user.logger.exception('client action failed')
        finally:
            if isinstance(exception, StopScenario):
                raise exception

            if self.name is None:
                action = action or meta.get('action', self.payload_variable)
                name = f'{parent.user._scenario.identifier} {self._short_name}{meta.get("direction", self._direction_arrow[self.direction])}{action}'
            else:
                rendered_name = parent.user.render(self.name)
                name = f'{parent.user._scenario.identifier} {rendered_name}'

            response_time = int((time() - start_time) * 1000)
            response_length = meta.get('response_length') or 0

            if exception is None:
                exception = meta.get('exception')

            if not suppress or (exception is not None):
                parent.user.environment.events.request.fire(
                    request_type=RequestType.CLIENT_TASK(),
                    name=name,
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

            if exception is not None or parent.user._scenario.context.get('log_all_requests', False):
                log_name = normalize(name)
                log_date = datetime.now()
                log_file = self.log_dir / f'{log_name}.{log_date.strftime("%Y%m%dT%H%M%S%f")}.log'

                meta.get('request', {}).update({'time': response_time})

                request_log: StrDict = {
                    'stacktrace': None,
                    'response': meta.get('response'),
                    'request': meta.get('request'),
                }

                if exception is not None:
                    request_log.update(
                        {
                            'stacktrace': traceback.format_exception(
                                type(exception),
                                value=exception,
                                tb=exception.__traceback__,
                            ),
                        },
                    )

                log_file.write_text(jsondumps(request_log, indent=2))

            parent.user.failure_handler(exception, task=self)


class client:
    available: ClassVar[dict[str, type[ClientTask]]] = {}
    schemes: list[str]

    def __init__(self, scheme: str, *additional_schemes: str) -> None:
        schemes = [scheme]
        if len(additional_schemes) > 0:
            schemes += list(additional_schemes)
        self.schemes = schemes

    def __call__(self, impl: type[ClientTask]) -> type[ClientTask]:
        available = dict.fromkeys(self.schemes, impl)
        impl._schemes = self.schemes
        client.available.update(available)

        return impl


from .blobstorage import BlobStorageClientTask
from .http import HttpClientTask
from .messagequeue import MessageQueueClientTask
from .servicebus import ServiceBusClientTask

__all__ = [
    'BlobStorageClientTask',
    'HttpClientTask',
    'MessageQueueClientTask',
    'ServiceBusClientTask',
]
