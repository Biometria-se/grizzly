"""Sure thing."""  # noqa: INP001
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from gevent import sleep as gsleep

from grizzly.behave import after_feature, after_scenario, after_step, before_feature, before_scenario, before_step  # noqa: F401
from grizzly.context import GrizzlyContext
from grizzly.steps import *
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.types.behave import then
from grizzly.utils import ModuleLoader

if TYPE_CHECKING:
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types.behave import Context


class SleepTask(GrizzlyTask):
    def __init__(self, time: float) -> None:
        super().__init__()

        self.time = time

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def implementation(_parent: GrizzlyScenario) -> Any:
            gsleep(self.time)

            return None

        return implementation


@then('sleep in "{time:f}"')
def step_sleep(context: Context, time: float) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(SleepTask(time=time))


class RaiseExceptionTask(GrizzlyTask):
    def __init__(self, exception_name: str) -> None:
        super().__init__()

        if '.' in exception_name:
            package, module = exception_name.rsplit('.')
        else:
            package = 'builtins'
            module = exception_name

        self.exception = ModuleLoader[Exception].load(package, module)

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def implementation(_parent: GrizzlyScenario) -> Any:
            raise self.exception

        return implementation


@then('raise exception "{exception}"')
def step_raise_exception(context: Context, exception: str) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(RaiseExceptionTask(exception))

