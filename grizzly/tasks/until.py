"""@anchor pydoc:grizzly.tasks.until Until
This task calls the `request` method of a `grizzly.users` implementation, until condition matches the
payload returned for the request.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.until.step_task_request_with_name_endpoint_until}

* {@pylink grizzly.steps.scenario.tasks.until.step_task_client_get_endpoint_until}

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `UNTL` indicating how
long time it took to finish the task. `name` will be suffixed with ` r=<retries>, w=<wait>, em=<expected_matches>`.

The request task that is being repeated until `condition` is true will have it's own entry in the statistics as an
ordinary {@pylink grizzly.tasks.request} or {@pylink grizzly.tasks.clients} task.

## Arguments

* `request` _{@pylink grizzly.tasks.request} / {@pylink grizzly.tasks.clients}_ - request that is going to be repeated

* `condition` _str_ - condition expression that specifies how `request` should be repeated

## Format

### `condition`

```plain
<expression> [| [retries=<retries>][, wait=<wait>][, expected_matches=<expected_matches>]]
```

* `expression` _str_ - JSON- or Xpath expression

* `retries` _int_ (optional) - maximum number of times to repeat the request if `condition` is not met (default `3`)

* `wait` _float_ (optional) - number of seconds to wait between retries (default `1.0`)

* `expected_matches` _int_ (optional): number of matches that the expression should match (default `1`)
"""

from __future__ import annotations

import json
import logging
from contextlib import suppress
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast

from gevent import sleep as gsleep

from grizzly.exceptions import StopScenario
from grizzly.testdata.utils import resolve_variable
from grizzly.types import RequestType
from grizzly.types.locust import StopUser
from grizzly.utils import safe_del
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments, split_value
from grizzly_extras.text import has_separator
from grizzly_extras.transformer import Transformer, TransformerContentType, TransformerError, transformer

from . import GrizzlyMetaRequestTask, GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from grizzly.scenarios import GrizzlyScenario


logger = logging.getLogger(__name__)


@template('condition', 'request')
class UntilRequestTask(GrizzlyTask):
    request: GrizzlyMetaRequestTask
    condition: str

    transform: type[Transformer] | None
    matcher: Callable[[Any], list[str]]

    retries: int
    wait: float
    expected_matches: int

    def __init__(self, request: GrizzlyMetaRequestTask, condition: str) -> None:
        super().__init__(timeout=None)

        self.request = request
        self.condition = condition
        self.retries = 3
        self.wait = 1.0
        self.expected_matches = 1

        assert self.request.content_type != TransformerContentType.UNDEFINED, 'content type must be specified for request'

        self.transform = transformer.available.get(self.request.content_type, None)

        if has_separator('|', self.condition):
            self.condition, until_arguments = split_value(self.condition)

            if '{{' in until_arguments and '}}' in until_arguments:
                until_arguments = cast('str', resolve_variable(self.grizzly.scenario, until_arguments, guess_datatype=False))

            arguments = parse_arguments(until_arguments)

            unsupported_arguments = get_unsupported_arguments(['retries', 'wait', 'expected_matches'], arguments)

            assert len(unsupported_arguments) == 0, f'unsupported arguments {", ".join(unsupported_arguments)}'

            self.retries = int(arguments.get('retries', self.retries))
            self.wait = float(arguments.get('wait', self.wait))
            self.expected_matches = int(arguments.get('expected_matches', '1'))

            assert self.retries > 0, 'retries argument cannot be less than 1'
            assert self.wait >= 0.1, 'wait argument cannot be less than 0.1 seconds'

    def __call__(self) -> grizzlytask:  # noqa: C901, PLR0915
        if self.transform is None:
            message = f'could not find a transformer for {self.request.content_type.name}'
            raise TypeError(message)

        transform = cast('Transformer', self.transform)

        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:  # noqa: C901, PLR0912, PLR0915
            task_name = f'{parent.user._scenario.identifier} {self.request.name}, w={self.wait}s, r={self.retries}, em={self.expected_matches}'
            condition_rendered = parent.user.render(self.condition)
            endpoint_rendered = parent.user.render(self.request.endpoint)

            if not transform.validate(condition_rendered):
                message = f'{condition_rendered} is not a valid expression for {self.request.content_type.name}'
                raise RuntimeError(message)

            parser = transform.parser(condition_rendered)
            number_of_matches = 0
            retry = 0
            exception: Exception | None = None
            response_length = 0

            # when doing the until task, disable that the wrapped task will throw an exception
            original_failure_exception = parent.user._scenario.failure_handling.get(None)

            with suppress(KeyError):
                del parent.user._scenario.failure_handling[None]

            start = perf_counter()

            try:
                error_count_before = len(parent.user.environment.stats.errors.keys())

                while retry < self.retries:
                    number_of_matches = 0

                    try:
                        gsleep(self.wait)

                        _, payload = self.request.execute(parent)

                        if payload is not None:
                            transformed = transform.transform(payload)
                            response_length += len(payload)
                        else:
                            message = 'response payload was not set'
                            raise TransformerError(message)

                        matches = parser(transformed)
                        parent.logger.debug('payload=%r, condition=%r, matches=%r', payload, condition_rendered, matches)
                        number_of_matches = len(matches)
                    except Exception as e:
                        parent.logger.exception('%s: retry=%d, endpoint=%s', task_name, retry, endpoint_rendered)
                        if exception is None:
                            exception = e
                        number_of_matches = 0

                        # only way to get these exceptions here is if we've beem abprted
                        # by injecting an exception in the task greenlet
                        if isinstance(e, StopUser | StopScenario):
                            break
                    finally:
                        retry += 1

                    if number_of_matches == self.expected_matches:
                        break

                # remove any errors produced during until loop
                error_count_after = len(parent.user.environment.stats.errors.keys())
                if error_count_after > error_count_before:
                    error_keys: list[str] = []
                    for error_key, error in parent.user.environment.stats.errors.items():
                        if error.name == f'{parent.user._scenario.identifier} {self.request.name}':
                            error_keys.append(error_key)

                    for error_key in error_keys:
                        safe_del(parent.user.environment.stats.errors, error_key)
            except Exception as e:
                parent.logger.exception('%s: error retry=%d', task_name, retry)
                if exception is None:
                    exception = e
            finally:
                # restore original
                if original_failure_exception is not None:
                    parent.user._scenario.failure_handling.update({None: original_failure_exception})

                response_time = int((perf_counter() - start) * 1000)

                if number_of_matches == self.expected_matches:
                    exception = None
                elif exception is None and number_of_matches != self.expected_matches:
                    message = f'found {number_of_matches} matching values for {condition_rendered} in payload'
                    exception = RuntimeError(message)

                    try:
                        payload_formatted = json.dumps(json.loads(payload or ''), indent=2)
                    except Exception:  # pragma: no cover
                        payload_formatted = payload or ''

                    parent.logger.error(
                        '%s: endpoint=%s, number_of_matches=%d, condition=%r, retry=%d, response_time=%d payload=\n%s',
                        task_name,
                        endpoint_rendered,
                        number_of_matches,
                        condition_rendered,
                        retry,
                        response_time,
                        payload_formatted,
                    )

                parent.user.environment.events.request.fire(
                    request_type=RequestType.UNTIL(),
                    name=task_name,
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

                parent.user.failure_handler(exception, task=self)

        @task.on_start
        def on_start(parent: GrizzlyScenario) -> None:
            self.request.on_start(parent)

        @task.on_stop
        def on_stop(parent: GrizzlyScenario) -> None:
            self.request.on_stop(parent)

        return task
