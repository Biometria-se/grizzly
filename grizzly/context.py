"""Grizzly context, the glue between behave and locust."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

import yaml
from jinja2 import DebugUndefined, Environment, FileSystemLoader
from jinja2.filters import FILTERS
from typing_extensions import Self

from grizzly.testdata import GrizzlyVariables
from grizzly.types import MessageCallback, MessageDirection
from grizzly.utils import MergeYamlTag, flatten, merge_dicts

if TYPE_CHECKING:  # pragma: no cover
    from locust.dispatch import UsersDispatcher

    from grizzly.events import GrizzlyEvents
    from grizzly.types.behave import Scenario
    from grizzly.types.locust import LocalRunner, MasterRunner, WorkerRunner

    from .tasks import AsyncRequestGroupTask, ConditionalTask, GrizzlyTask, GrizzlyTaskWrapper, LoopTask, TimerTask


logger = logging.getLogger(__name__)

def load_configuration_file() -> dict[str, Any]:
    """Load a grizzly environment file and flatten the structure."""
    configuration_file = environ.get('GRIZZLY_CONFIGURATION_FILE', None)
    configuration: dict[str, Any] = {}

    if configuration_file is None:
        return configuration

    try:
        file = Path(configuration_file)
        if file.suffix not in ['.yml', '.yaml']:
            logger.error('configuration file must have file extension yml or yaml')
            raise SystemExit(1)

        environment = Environment(autoescape=False, extensions=[MergeYamlTag])
        environment.extend(source_file=file)
        loader = yaml.SafeLoader

        yaml_template = environment.from_string(file.read_text())
        yaml_content = yaml_template.render()

        yaml_configurations = list(yaml.load_all(yaml_content, Loader=loader))
        yaml_configurations.reverse()
        for yaml_configuration in yaml_configurations:
            layer = flatten(yaml_configuration['configuration'])
            configuration = merge_dicts(configuration, layer)
    except FileNotFoundError as e:
        logger.exception('%s does not exist', configuration_file)
        raise SystemExit(1) from e
    else:
        return configuration


class GrizzlyContext:
    __instance: Optional[GrizzlyContext] = None

    _initialized: bool
    _state: GrizzlyContextState
    _setup: GrizzlyContextSetup
    _scenarios: GrizzlyContextScenarios
    _events: GrizzlyEvents


    def __new__(cls, *_args: Any, **_kwargs: Any) -> GrizzlyContext:  # noqa: PYI034
        """Class is a singleton, there should only be once instance of it."""
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialized = False

        return cls.__instance

    @classmethod
    def destroy(cls) -> None:
        if cls.__instance is None:
            message = f"'{cls.__name__}' is not instantiated"
            raise ValueError(message)

        cls.__instance = None

    def __init__(self) -> None:
        if not self._initialized:
            from grizzly.events import events
            self._setup = GrizzlyContextSetup()
            self._scenarios = GrizzlyContextScenarios(self)
            self._state = GrizzlyContextState()
            self._events = events
            self._initialized = True

    @property
    def setup(self) -> GrizzlyContextSetup:
        return self._setup

    @property
    def state(self) -> GrizzlyContextState:
        return self._state

    @property
    def scenario(self) -> GrizzlyContextScenario:
        """Read-only scenario child instance. Shortcut to the current (latest) scenario in the context."""
        if len(self._scenarios) < 1:
            message = 'no scenarios created!'
            raise ValueError(message)

        return self._scenarios[self._scenarios._active]

    @property
    def scenarios(self) -> GrizzlyContextScenarios:
        return self._scenarios

    @property
    def events(self) -> GrizzlyEvents:
        return self._events


class DebugChainableUndefined(DebugUndefined):
    _undefined_name: str | None

    def __getattr__(self, attr: str) -> Self:
        self._undefined_name = f'{self._undefined_name}.{attr}'
        return self

    def __getitem__(self, key: str) -> Self:
        self._undefined_name = f"{self._undefined_name}['{key}']"
        return self



def jinja2_environment_factory() -> Environment:
    """Create a Jinja2 environment, so same instance is used throughout each grizzly scenario, with custom filters."""
    environment = Environment(autoescape=False, loader=FileSystemLoader(Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'requests'), undefined=DebugChainableUndefined)

    environment.globals.update({
        'datetime': datetime,
        'timezone': timezone,
    })

    return environment


@dataclass
class GrizzlyContextState:
    spawning_complete: bool = field(default=False)
    background_done: bool = field(default=False)
    configuration: dict[str, Any] = field(init=False, default_factory=load_configuration_file)
    verbose: bool = field(default=False)
    locust: Union[MasterRunner, WorkerRunner, LocalRunner] = field(init=False, repr=False)


@dataclass
class GrizzlyContextScenarioResponseTimePercentile:
    response_time: int
    percentile: float


@dataclass(unsafe_hash=True)
class GrizzlyContextScenarioValidation:
    fail_ratio: Optional[float] = field(init=False, default=None)
    avg_response_time: Optional[int] = field(init=False, default=None)
    response_time_percentile: Optional[GrizzlyContextScenarioResponseTimePercentile] = field(init=False, default=None)


@dataclass(unsafe_hash=True)
class GrizzlyContextScenarioUser:
    class_name: str = field(init=False, repr=False, hash=True, compare=False)
    weight: int = field(init=False, hash=True, default=1)
    fixed_count: Optional[int] = field(init=False, repr=False, hash=False, compare=False, default=None)
    sticky_tag: Optional[str] = field(init=False, repr=False, hash=False, compare=False, default=None)


StackedFuncType = Callable[['GrizzlyContextTasksTmp'], Optional['GrizzlyTaskWrapper']]


def stackproperty(func: StackedFuncType) -> property:
    """Wrap any get property for temporary task pointer depending in the context tasks are added."""
    def setter(self: GrizzlyContextTasksTmp, value: Optional[GrizzlyTaskWrapper]) -> None:
        attr_name = f'_{func.__name__}'
        instance = getattr(self, attr_name, None)

        if value is None:
            assert instance is not None, f'{func.__name__} is not in stack'
            pointer = self.__stack__[-1]
            assert isinstance(pointer, instance.__class__), f'{func.__name__} is not last in stack'
            self.__stack__.pop()
        elif value is not None:
            assert instance is None, f'{func.__name__} is already in stack'
            self.__stack__.append(value)

        setattr(self, attr_name, value)

    return property(func, setter)


class GrizzlyContextTasksTmp:
    _async_group: Optional[AsyncRequestGroupTask]
    _conditional: Optional[ConditionalTask]
    _loop: Optional[LoopTask]

    _timers: dict[str, Optional[TimerTask]]
    _custom: dict[str, Optional[GrizzlyTaskWrapper]]

    __stack__: list[GrizzlyTaskWrapper]

    def __init__(self) -> None:
        self.__stack__ = []

        self._async_group = None
        self._conditional = None
        self._loop = None

        self._timers = {}
        self._custom = {}

    @stackproperty
    def async_group(self) -> Optional[AsyncRequestGroupTask]:
        return self._async_group

    @stackproperty
    def conditional(self) -> Optional[ConditionalTask]:
        return self._conditional

    @stackproperty
    def loop(self) -> Optional[LoopTask]:
        return self._loop

    @property
    def custom(self) -> dict[str, Optional[GrizzlyTaskWrapper]]:
        return self._custom

    @custom.setter
    def custom(self, value: dict[str, Optional[GrizzlyTaskWrapper]]) -> None:
        self._custom = value

    @property
    def timers(self) -> dict[str, Optional[TimerTask]]:
        return self._timers

    @timers.setter
    def timers(self, value: dict[str, Optional[TimerTask]]) -> None:
        self._timers = value


class GrizzlyContextTasks(list['GrizzlyTask']):
    _tmp: GrizzlyContextTasksTmp
    behave_steps: dict[int, str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._tmp = GrizzlyContextTasksTmp()
        self.behave_steps = {}

    @property
    def tmp(self) -> GrizzlyContextTasksTmp:
        return self._tmp

    def __call__(self, *filtered_type: type[GrizzlyTask]) -> list[GrizzlyTask]:
        tasks = self.tmp.__stack__[-1].peek() if len(self.tmp.__stack__) > 0 else cast(list['GrizzlyTask'], self)

        if len(filtered_type) > 0:
            tasks = [task for task in tasks if isinstance(task, filtered_type)]

        return tasks

    def add(self, task: GrizzlyTask) -> None:
        if len(self.tmp.__stack__) > 0:
            self.tmp.__stack__[-1].add(task)
        else:
            self.append(task)


@dataclass(unsafe_hash=True)
class GrizzlyContextScenario:
    _name: str = field(init=False, hash=True)
    class_type: str = field(init=False, hash=True, default='IteratorScenario')
    description: str = field(init=False, hash=False)
    user: GrizzlyContextScenarioUser = field(init=False, hash=False, compare=False, default_factory=GrizzlyContextScenarioUser)
    index: int = field(init=True)
    iterations: int = field(init=False, repr=False, hash=False, compare=False, default=1)
    pace: Optional[str] = field(init=False, repr=False, hash=False, compare=False, default=None)

    grizzly: GrizzlyContext = field(init=True, repr=False, hash=False, compare=False)
    behave: Scenario = field(init=True, repr=False, hash=False, compare=False)

    context: dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)
    variables: GrizzlyVariables = field(init=False, repr=False, hash=False, default_factory=GrizzlyVariables)
    _tasks: GrizzlyContextTasks = field(init=False, repr=False, hash=False, compare=False)
    validation: GrizzlyContextScenarioValidation = field(init=False, hash=False, compare=False, default_factory=GrizzlyContextScenarioValidation)
    failure_exception: Optional[type[Exception]] = field(init=False, default=None)
    orphan_templates: list[str] = field(init=False, repr=False, hash=False, compare=False, default_factory=list)
    _jinja2: Environment = field(init=False, repr=False, default_factory=jinja2_environment_factory)

    @property
    def jinja2(self) -> Environment:
        # something might have changed in the filters department
        self._jinja2.filters = FILTERS

        return self._jinja2

    def __post_init__(self) -> None:
        self.name = self.behave.name
        self.description = self.behave.name

        self._tasks = GrizzlyContextTasks()

    @property
    def tasks(self) -> GrizzlyContextTasks:
        return self._tasks

    @property
    def identifier(self) -> str:
        return f'{self.index:03}'

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def locust_name(self) -> str:
        return f'{self.identifier} {self.description}'

    @property
    def class_name(self) -> str:
        return f'{self.class_type}_{self.identifier}'

    def should_validate(self) -> bool:
        return (
            self.validation.fail_ratio is not None
            or self.validation.avg_response_time is not None
            or self.validation.response_time_percentile is not None
        )


class GrizzlyContextSetupLocustMessages(dict[MessageDirection, dict[str, MessageCallback]]):
    def register(self, direction: MessageDirection, message_type: str, callback: MessageCallback) -> None:
        if direction not in self:
            self[direction] = {}

        if message_type not in self[direction]:
            self[direction][message_type] = callback
        else:
            message = f'message type {message_type} was already registered in direction {direction.name}'
            raise AttributeError(message)


@dataclass
class GrizzlyContextSetupLocust:
    messages: GrizzlyContextSetupLocustMessages = field(default_factory=GrizzlyContextSetupLocustMessages)


@dataclass
class GrizzlyContextSetup:
    log_level: str = field(init=False, default='INFO')
    global_context: dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)
    user_count: Optional[int] = field(init=False, default=None)
    spawn_rate: Optional[float] = field(init=False, default=None)
    timespan: Optional[str] = field(init=False, default=None)
    dispatcher_class: Optional[type[UsersDispatcher]] = field(init=False, default=None)
    statistics_url: Optional[str] = field(init=False, default=None)
    locust: GrizzlyContextSetupLocust = field(init=False, default_factory=GrizzlyContextSetupLocust)


class GrizzlyContextScenarios(list[GrizzlyContextScenario]):
    grizzly: GrizzlyContext
    _active: int

    __map__: dict[Scenario, GrizzlyContextScenario]

    def __init__(self, grizzly: GrizzlyContext, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.grizzly = grizzly
        self._active = -1
        self.__map__ = {}

    def __call__(self) -> list[GrizzlyContextScenario]:
        return cast(list[GrizzlyContextScenario], self)

    def find_by_class_name(self, class_name: str) -> Optional[GrizzlyContextScenario]:
        return self._find(class_name, 'class_name')

    def find_by_name(self, name: str) -> Optional[GrizzlyContextScenario]:
        return self._find(name, 'name')

    def find_by_description(self, description: str) -> Optional[GrizzlyContextScenario]:
        return self._find(description, 'description')

    def select(self, behave: Scenario) -> None:
        scenario = self.__map__.get(behave, None)
        if scenario is None:
            message = f'behave scenario "{scenario}" is not mapped to a grizzly scenario!'
            raise ValueError(message)

        self._active = self.index(scenario)

    def deselect(self) -> None:
        self._active = -1

    def _find(self, value: str, attribute: str) -> Optional[GrizzlyContextScenario]:
        for item in self:
            if getattr(item, attribute, None) == value:
                return item

        return None

    def create(self, behave: Scenario) -> GrizzlyContextScenario:
        """Create a new scenario based on the behave Scenario, and add it to the current list of scenarios in this context."""
        grizzly_scenario = GrizzlyContextScenario(len(self) + 1, behave=behave, grizzly=self.grizzly)

        self.__map__.update({behave: grizzly_scenario})

        self.append(grizzly_scenario)
        self.deselect()

        return grizzly_scenario
