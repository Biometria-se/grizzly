"""Grizzly context, the glue between behave and locust."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

import yaml
from jinja2 import Environment
from jinja2.filters import FILTERS

from grizzly.types import MessageCallback, MessageDirection
from grizzly.utils import flatten

from .testdata import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
    from locust.dispatch import UsersDispatcher

    from grizzly.types.behave import Scenario
    from grizzly.types.locust import LocalRunner, MasterRunner, WorkerRunner

    from .tasks import AsyncRequestGroupTask, ConditionalTask, GrizzlyTask, GrizzlyTaskWrapper, LoopTask, TimerTask


logger = logging.getLogger(__name__)


def load_configuration_file() -> Dict[str, Any]:
    """Load a grizzly environment file and flatten the structure."""
    configuration_file = environ.get('GRIZZLY_CONFIGURATION_FILE', None)

    if configuration_file is None:
        return {}

    try:
        file = Path(configuration_file)
        if file.suffix not in ['.yml', '.yaml']:
            logger.error('configuration file must have file extension yml or yaml')
            raise SystemExit(1)

        with file.open() as fd:
            yaml_configuration = yaml.safe_load(fd)
            return flatten(yaml_configuration['configuration'])
    except FileNotFoundError as e:
        logger.exception('%s does not exist', configuration_file)
        raise SystemExit(1) from e


class GrizzlyContext:
    __instance: Optional[GrizzlyContext] = None

    _initialized: bool
    _state: GrizzlyContextState
    _setup: GrizzlyContextSetup
    _scenarios: GrizzlyContextScenarios

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
            self._state = GrizzlyContextState()
            self._setup = GrizzlyContextSetup()
            self._scenarios = GrizzlyContextScenarios()
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

        return self._scenarios[-1]

    @property
    def scenarios(self) -> GrizzlyContextScenarios:
        return self._scenarios


def jinja2_environment_factory() -> Environment:
    """Create a Jinja2 environment, so same instance is used throughout grizzly, with custom filters."""
    return Environment(autoescape=False)


@dataclass
class GrizzlyContextState:
    spawning_complete: bool = field(default=False)
    background_section_done: bool = field(default=False)
    variables: GrizzlyVariables = field(init=False, default_factory=GrizzlyVariables)
    configuration: Dict[str, Any] = field(init=False, default_factory=load_configuration_file)
    alias: Dict[str, str] = field(init=False, default_factory=dict)
    verbose: bool = field(default=False)
    locust: Union[MasterRunner, WorkerRunner, LocalRunner] = field(init=False, repr=False)
    persistent: Dict[str, str] = field(init=False, repr=False, default_factory=dict)
    _jinja2: Environment = field(init=False, repr=False, default_factory=jinja2_environment_factory)

    @property
    def jinja2(self) -> Environment:
        # something might have changed in the filters department
        self._jinja2.filters = FILTERS

        return self._jinja2


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
    class_name: str = field(init=False, hash=True)
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

    _timers: Dict[str, Optional[TimerTask]]
    _custom: Dict[str, Optional[GrizzlyTaskWrapper]]

    __stack__: List[GrizzlyTaskWrapper]

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
    def custom(self) -> Dict[str, Optional[GrizzlyTaskWrapper]]:
        return self._custom

    @custom.setter
    def custom(self, value: Dict[str, Optional[GrizzlyTaskWrapper]]) -> None:
        self._custom = value

    @property
    def timers(self) -> Dict[str, Optional[TimerTask]]:
        return self._timers

    @timers.setter
    def timers(self, value: Dict[str, Optional[TimerTask]]) -> None:
        self._timers = value


class GrizzlyContextTasks(List['GrizzlyTask']):
    _tmp: GrizzlyContextTasksTmp
    behave_steps: Dict[int, str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._tmp = GrizzlyContextTasksTmp()
        self.behave_steps = {}

    @property
    def tmp(self) -> GrizzlyContextTasksTmp:
        return self._tmp

    def __call__(self, *filtered_type: type[GrizzlyTask]) -> List[GrizzlyTask]:
        tasks = self.tmp.__stack__[-1].peek() if len(self.tmp.__stack__) > 0 else cast(List['GrizzlyTask'], self)

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

    behave: Scenario = field(init=True, repr=False, hash=False, compare=False)
    context: Dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)
    _tasks: GrizzlyContextTasks = field(init=False, repr=False, hash=False, compare=False)
    validation: GrizzlyContextScenarioValidation = field(init=False, hash=False, compare=False, default_factory=GrizzlyContextScenarioValidation)
    failure_exception: Optional[Type[Exception]] = field(init=False, default=None)
    orphan_templates: List[str] = field(init=False, repr=False, hash=False, compare=False, default_factory=list)

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


class GrizzlyContextSetupLocustMessages(Dict[MessageDirection, Dict[str, MessageCallback]]):
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

    global_context: Dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)

    user_count: Optional[int] = field(init=False, default=None)
    spawn_rate: Optional[float] = field(init=False, default=None)
    timespan: Optional[str] = field(init=False, default=None)
    dispatcher_class: Optional[Type[UsersDispatcher]] = field(init=False, default=None)

    statistics_url: Optional[str] = field(init=False, default=None)

    locust: GrizzlyContextSetupLocust = field(init=False, default_factory=GrizzlyContextSetupLocust)


class GrizzlyContextScenarios(List[GrizzlyContextScenario]):
    def __call__(self) -> List[GrizzlyContextScenario]:
        return cast(List[GrizzlyContextScenario], self)

    def find_by_class_name(self, class_name: str) -> Optional[GrizzlyContextScenario]:
        return self._find(class_name, 'class_name')

    def find_by_name(self, name: str) -> Optional[GrizzlyContextScenario]:
        return self._find(name, 'name')

    def find_by_description(self, description: str) -> Optional[GrizzlyContextScenario]:
        return self._find(description, 'description')

    def _find(self, value: str, attribute: str) -> Optional[GrizzlyContextScenario]:
        for item in self:
            if getattr(item, attribute, None) == value:
                return item

        return None

    def create(self, behave_scenario: Scenario) -> None:
        """Create a new scenario based on the behave Scenario, and add it to the current list of scenarios in this context."""
        grizzly_scenario = GrizzlyContextScenario(len(self) + 1, behave=behave_scenario)

        self.append(grizzly_scenario)
