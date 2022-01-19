'''This task calls the `request` method of a `grizzly.users` implementation, until condition matches the
payload returned for the request.

`condition` is a JSON- or Xpath expression, that also has support for "grizzly style" arguments:

Arguments:

* `retries` (int): maximum number of times to repeat the request if `condition` is not met (default `3`)

* `wait` (float): number of seconds to wait between retries (default `1.0`)

Instances of this task is created with step expression:

* [`step_task_request_text_with_name_to_endpoint_until`](/grizzly/usage/steps/scenario/tasks/#step_task_request_text_with_name_to_endpoint_until)
'''
from typing import Callable, Any, Type, List, Optional, cast
from dataclasses import dataclass, field
from time import perf_counter as time

from jinja2 import Template
from gevent import sleep as gsleep
from grizzly_extras.transformer import Transformer, TransformerContentType, transformer
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments, split_value

from ..context import GrizzlyContext, GrizzlyTask, GrizzlyScenarioBase
from .request import RequestTask

@dataclass
class UntilRequestTask(GrizzlyTask):
    request: RequestTask
    condition: str

    transform: Optional[Type[Transformer]] = field(init=False)
    matcher: Callable[[Any], List[str]] = field(init=False)

    retries: int = field(init=False, default=3)
    wait: float = field(init=False, default=1.0)

    def __post_init__(self) -> None:
        if self.request.response.content_type == TransformerContentType.GUESS:
            raise ValueError('content type must be specified for request')

        self.transform = transformer.available.get(self.request.response.content_type, None)

        if '|' in self.condition:
            self.condition, until_arguments = split_value(self.condition)

            if '{{' in until_arguments and '}}' in until_arguments:
                grizzly = GrizzlyContext()
                until_arguments = Template(until_arguments).render(**grizzly.state.variables)

            arguments = parse_arguments(until_arguments)

            unsupported_arguments = get_unsupported_arguments(['retries', 'wait'], arguments)

            if len(unsupported_arguments) > 0:
                raise ValueError(f'unsupported arguments {", ".join(unsupported_arguments)}')

            self.retries = int(arguments.get('retries', self.retries))
            self.wait = float(arguments.get('wait', self.wait))

    def implementation(self) -> Callable[[GrizzlyScenarioBase], Any]:
        if self.transform is None:
            raise TypeError(f'could not find a transformer for {self.request.response.content_type.name}')

        transform = cast(Transformer, self.transform)

        def _implementation(parent: GrizzlyScenarioBase) -> Any:
            if '{{' in self.condition and '}}' in self.condition:
                condition_rendered = Template(self.condition).render(**parent.user._context['variables'])
            else:
                condition_rendered = self.condition

            if not transform.validate(condition_rendered):
                raise RuntimeError(f'{condition_rendered} is not a valid expression for {self.request.response.content_type.name}')

            parser = transform.parser(condition_rendered)
            number_of_matches = 0
            retry = 0
            exception: Optional[Exception] = None

            start = time()

            try:
                while number_of_matches != 1 and retry < self.retries:
                    gsleep(self.wait)
                    number_of_matches = 0

                    try:
                        _, payload = parent.user.request(self.request)
                        _, transformed = transform.transform(self.request.response.content_type, payload)

                        matches = parser(transformed)
                        parent.logger.debug(f'{payload=}, condition={condition_rendered}, {matches=}')
                        number_of_matches = len(matches)
                    except Exception as e:
                        exception = e
                        number_of_matches = 0
                    finally:
                        if number_of_matches != 1:
                            retry += 1
            except Exception as e:
                exception = e
            finally:
                response_time = int((time() - start) * 1000)

                if number_of_matches == 1:
                    exception = None
                elif exception is None and number_of_matches != 1:
                    exception = RuntimeError(f'found {number_of_matches} matching values for {condition_rendered} in payload')

                parent.user.environment.events.request.fire(
                    request_type='UNTL',
                    name=f'{self.request.scenario.identifier} {self.request.name}, w={self.wait}s, r={self.retries}',
                    response_time=response_time,
                    response_length=0,
                    context=parent.user._context,
                    exception=exception,
                )

                if exception is not None and self.request.scenario.failure_exception is not None:
                    raise self.request.scenario.failure_exception()

        return _implementation
