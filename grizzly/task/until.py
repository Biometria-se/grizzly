'''This task calls the `request` method of a `grizzly.users` implementation, until condition matches the
payload returned for the request.

`condition` is a JSON- or Xpath expression, that also has support for "grizzly style" arguments:

Arguments:

* `retries` (int): maximum number of times to repeat the request if `condition` is not met (default `3`)

* `wait` (float): number of seconds to wait between retries (default `1.0`)

Instances of this task is created with step expression:

* [`step_task_request_text_with_name_to_endpoint_until`](/grizzly/usage/steps/scenario/tasks/#step_task_request_text_with_name_to_endpoint_until)
'''
from typing import Callable, Any, Type, List, Optional
from dataclasses import dataclass, field
from time import perf_counter as time

from jinja2 import Template
from gevent import sleep as gsleep
from locust.exception import StopUser
from grizzly_extras.transformer import Transformer, TransformerContentType, transformer
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments, split_value

from ..context import GrizzlyTask, GrizzlyTasksBase
from .request import RequestTask

@dataclass
class UntilRequestTask(GrizzlyTask):
    request: RequestTask
    condition: str

    transform: Type[Transformer] = field(init=False)
    matcher: Callable[[Any], List[str]] = field(init=False)

    retries: int = field(init=False, default=3)
    wait: float = field(init=False, default=1.0)

    def __post_init__(self) -> None:
        if self.request.response.content_type == TransformerContentType.GUESS:
            raise ValueError(f'content type must be specified for request')

        self.transform = transformer.available[self.request.response.content_type]

        if '|' in self.condition:
            self.condition, until_arguments = split_value(self.condition)

            arguments = parse_arguments(until_arguments)

            unsupported_arguments = get_unsupported_arguments(['retries', 'wait'], arguments)

            if len(unsupported_arguments) > 0:
                raise ValueError(f'unsupported arguments {", ".join(unsupported_arguments)}')

            self.retries = int(arguments.get('retries', self.retries))
            self.wait = float(arguments.get('wait', self.wait))

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        if self.transform is None:
            raise TypeError(f'could not find a transformer for {self.request.response.content_type.name}')

        def _implementation(parent: GrizzlyTasksBase) -> Any:
            interpolated_expression = Template(self.condition).render(parent.user.context_variables)

            if self.transform.validate(interpolated_expression):
                raise RuntimeError(f'{interpolated_expression} is not a valid expression for {self.request.response.content_type.name}')

            parser = self.transform.parser(self.condition)
            number_of_matches = 0
            retry = 0
            exception: Optional[Exception] = None

            start = time()

            try:
                while number_of_matches != 1 and retry < self.retries:
                    _, payload = parent.user.request(self.request)

                    number_of_matches = len(parser(payload))

                    if number_of_matches != 1:
                        parent.logger.debug(f'')
                        gsleep(self.wait)
                        retry += 1
            except Exception as e:
                exception = e
            finally:
                response_time = int((time() - start) * 1000)

                if exception is not None and number_of_matches != 1:
                    exception = RuntimeError(f'found {number_of_matches} matching values for {interpolated_expression} in payload')

                parent.user.environment.events.request.fire(
                    request_type='UNTIL',
                    name=f'{self.request.scenario.identifier} {self.request.name}, wait={self.wait}s, retries={self.retries}',
                    response_time=response_time,
                    response_length=0,
                    context=parent.user._context,
                    exception=exception,
                )

                if exception is not None:
                    raise StopUser()

        return _implementation
