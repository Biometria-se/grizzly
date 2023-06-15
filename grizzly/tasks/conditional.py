"""
@anchor pydoc:grizzly.tasks.conditional Conditional
This task executes one or more other tasks based on `condition`.

This is useful when a set of tasks should be executed if `condition` is `True`, and another set of tasks if `condition` is `False`.

All tasks created between {@pylink grizzly.steps.scenario.tasks.step_task_conditional_if} and {@pylink grizzly.steps.scenario.tasks.step_task_conditional_end}
will be wrapped in this instance and executed conditionally. If the task has its own `name` attribute, it will be prefixed with this tasks `name`.

The {@pylink grizzly.steps.scenario.tasks.step_task_conditional_else} step expression is optional, if not used no additional tasks will be executed if
`condition` is false.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_conditional_if}

* {@pylink grizzly.steps.scenario.tasks.step_task_conditional_else} (optional)

* {@pylink grizzly.steps.scenario.tasks.step_task_conditional_end}

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `COND`. `name` is suffixed with `<condition> (<n>)`,
where `<condition>` is the runtime resolved condition and `<n>` is the number of tasks that is executed for the resolved condition. Each task in
the set for `condition` will have its own entry in the statistics, see respective {@pylink grizzly.tasks} documentation.

## Arguments

* `name` _str_: name of the conditional, used in `locust` statistics

* `condition` _str_: {@link framework.usage.variables.templating} string that must render `True` or `False`
"""
from typing import TYPE_CHECKING, Any, List, Optional, Dict
from time import perf_counter

from gevent import sleep as gsleep

from grizzly.exceptions import StopUser, RestartScenario

from . import GrizzlyTask, GrizzlyTaskWrapper, template, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('condition', 'tasks', 'name')
class ConditionalTask(GrizzlyTaskWrapper):
    tasks: Dict[bool, List[GrizzlyTask]]

    name: str
    condition: str

    _pointer: Optional[bool]

    def __init__(self, name: str, condition: str) -> None:
        super().__init__()

        self.name = name
        self.condition = condition

        self.tasks = {}

        self._pointer = None

    def switch(self, pointer: Optional[bool]) -> None:
        self._pointer = pointer

        if pointer is not None and pointer not in self.tasks:
            self.tasks[pointer] = []

    def add(self, task: GrizzlyTask) -> None:
        task_name = getattr(task, 'name', None)
        if task_name is not None:
            setattr(task, 'name', f'{self.name}:{task_name}')

        if self._pointer is not None:
            if self._pointer not in self.tasks:
                self.tasks.update({self._pointer: []})

            self.tasks[self._pointer].append(task)

    def peek(self) -> List[GrizzlyTask]:
        if self._pointer is not None:
            return self.tasks[self._pointer]

        return []

    def __call__(self) -> grizzlytask:
        tasks: Dict[bool, List[grizzlytask]] = {}

        for pointer, pointer_tasks in self.tasks.items():
            tasks.update({pointer: list(map(lambda t: t(), pointer_tasks))})

        @grizzlytask
        def task(parent: 'GrizzlyScenario') -> Any:
            condition_rendered = parent.render(self.condition)
            exception: Optional[Exception] = None
            task_count = 0

            start = perf_counter()

            try:
                pointer: bool
                if condition_rendered == 'True':
                    pointer = True
                elif condition_rendered == 'False':
                    pointer = False
                else:
                    condition_rendered_failed = condition_rendered
                    condition_rendered = 'Invalid'
                    raise RuntimeError(f'"{self.condition}" resolved to "{condition_rendered_failed}" which is invalid')

                _tasks = tasks.get(pointer, [])
                task_count = len(_tasks)

                # execute all "wrapped" tasks
                for task in _tasks:
                    task(parent)
                    gsleep(parent.user.wait_time())  # use defined pace
            except Exception as e:
                exception = e
            finally:
                response_time = int((perf_counter() - start) * 1000)
                name = f'{parent.user._scenario.identifier} {self.name}: {condition_rendered} ({task_count})'

                # do not log these exceptions if thrown from wrapped task, just log the error for this task
                if not isinstance(exception, (StopUser, RestartScenario,)):
                    parent.user.environment.events.request.fire(
                        request_type='COND',
                        name=name,
                        response_time=response_time,
                        response_length=task_count,
                        context=parent.user._context,
                        exception=exception,
                    )
                else:
                    stats = parent.user.environment.stats.get(name, 'COND')
                    stats.log_error(None)

                if exception is not None and parent.user._scenario.failure_exception is not None:
                    raise parent.user._scenario.failure_exception()

        @task.on_start
        def on_start(parent: 'GrizzlyScenario') -> None:
            for task in tasks.get(True, []) + tasks.get(False, []):
                task.on_start(parent)

        @task.on_stop
        def on_stop(parent: 'GrizzlyScenario') -> None:
            for task in tasks.get(True, []) + tasks.get(False, []):
                task.on_stop(parent)

        return task
