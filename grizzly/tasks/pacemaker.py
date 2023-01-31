"""
@anchor pydoc:grizzly.tasks.pacemaker Pacemaker
This task ensures that the time between two executions of it is at least the specified amount of time. This
is useful when using {@pylink grizzly.scenarios.iterator.IterationScenario} to ensure that each iteration of
the scenario takes about the same time. This help setting the intensity that the scenario will execute against
the target.

If the time between two executions are greater than the specified value an error will be logged and the error for
this task will increment, but the test will continue.

## Step implementations

*

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `PACE`. Response time will be the amount of time
needed to sleep to keep the pace at the specified value. If the time between two executions of the task is greater than the specified value
an error will be logged.

## Arguments

* `name` _str_: name of the pacemaker, used in `locust` statistics

* `value` _str_: {@link framework.usage.variables.templating} string that must render to a valid `float`, if not the user will stop
"""
from typing import TYPE_CHECKING, Any, Callable, Optional
from hashlib import sha1
from time import perf_counter

from gevent import sleep as gsleep

from . import GrizzlyTask, template
from ..exceptions import StopUser

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario


@template('name', 'value')
class PacemakerTask(GrizzlyTask):
    name: str
    variable: str
    value: str

    def __init__(self, name: str, value: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        name_hash = sha1(f'pacemaker-{name}'.encode('utf-8')).hexdigest()[:8]

        self.name = name
        self.variable = f'{name_hash}::{name}'
        self.value = value

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        name = f'{self.scenario.identifier} {self.name}'

        def task(parent: 'GrizzlyScenario') -> Any:
            exception: Optional[Exception] = None
            response_length: int = 0

            try:
                start = perf_counter()
                try:
                    value = float(parent.render(self.value))
                except ValueError as ve:
                    raise ValueError(f'{self.value} does not render to a number') from ve

                variable = parent.user._context['variables'].get(self.variable, None)

                if variable is not None:
                    pacemaker_sleep = (start - variable) * 1000

                    if pacemaker_sleep < value:
                        parent.logger.debug(f'keeping pace by sleeping {pacemaker_sleep} milliseconds')
                        gsleep(value - pacemaker_sleep)
                        response_length = 1
                    else:
                        parent.logger.error(f'pace falling behind, currently at {abs(pacemaker_sleep)} milliseconds')
                        raise RuntimeError('pace falling behind')
            except Exception as e:
                exception = e
            finally:
                done = perf_counter()
                parent.user._context['variables'][self.variable] = done
                response_time = int((done - start) * 1000)

                parent.user.environment.events.request.fire(
                    request_type='PACE',
                    name=name,
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

                if exception is not None and isinstance(exception, ValueError):
                    raise StopUser()

        return task
