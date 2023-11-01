"""Grizzly context, the glue between behave and locust."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast

import yaml
from jinja2 import Environment
from jinja2.filters import FILTERS

from grizzly.types import MessageCallback, MessageDirection
from grizzly.utils import flatten

from .testdata import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
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

    @classmethod
    def __new__(cls, *_args: Any, **_kwargs: Any) -> GrizzlyContext:  # noqa: PYI034
        """Class is a singleton, there should only be once instance of it."""
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialized = False

        return cls.__instance

    @classmethod
    def destroy(cls) -> None:
        """Destroy singleton instance."""
        if cls.__instance is None:
            message = f"'{cls.__name__}' is not instantiated"
            raise ValueError(message)

        cls.__instance = None

    def __init__(self) -> None:
        """Initialize child objects."""
        if not self._initialized:
            self._state = GrizzlyContextState()
            self._setup = GrizzlyContextSetup()
            self._scenarios = GrizzlyContextScenarios()
            self._initialized = True

    @property
    def setup(self) -> GrizzlyContextSetup:
        """Read-only setup child instance."""
        return self._setup

    @property
    def state(self) -> GrizzlyContextState:
        """Read-only state child instance."""
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
        """Read-only scenarios child instance."""
        return self._scenarios


def jinja2_environment_factory() -> Environment:
    """Create a Jinja2 environment."""
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
        """Use the same environment always, with the specified filters."""
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


StackedFuncType = Callable[['GrizzlyContextTasksTmp'], Optional['GrizzlyTaskWrapper']]


def stackproperty(func: StackedFuncType) -> property:
    """Wrap any get property for temporary task pointer depending in the context tasks are added."""
    def setter(self: GrizzlyContextTasksTmp, value: Optional[GrizzlyTaskWrapper]) -> None:
        attr_name = f'_{func.__name__}'
        instance = getattr(self, attr_name, None)

        if value is None:
            if instance is None:
                message = f'{func.__name__} is not in stack'
                raise AssertionError(message)
            pointer = self.__stack__[-1]
            if not isinstance(pointer, instance.__class__):
                message = f'{func.__name__} is not last in stack'
                raise AssertionError(message)
            self.__stack__.pop()
        elif value is not None:
            if instance is not None:
                message = f'{func.__name__} is already in stack'
                raise AssertionError(message)
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
        """Read-only property of AsyncRequestGroupTask."""
        return self._async_group

    @stackproperty
    def conditional(self) -> Optional[ConditionalTask]:
        """Read-only property of ConditionalTask."""
        return self._conditional

    @stackproperty
    def loop(self) -> Optional[LoopTask]:
        """Read-only property of LoopTask."""
        return self._loop

    @property
    def custom(self) -> Dict[str, Optional[GrizzlyTaskWrapper]]:
        """Get dictionary of custom stacked tasks."""
        return self._custom

    @custom.setter
    def custom(self, value: Dict[str, Optional[GrizzlyTaskWrapper]]) -> None:
        self._custom = value

    @property
    def timers(self) -> Dict[str, Optional[TimerTask]]:
        """Get dictionary of timer tasks, mapped with name to instance."""
        return self._timers

    @timers.setter
    def timers(self, value: Dict[str, Optional[TimerTask]]) -> None:
        self._timers = value


class GrizzlyContextTasks(List['GrizzlyTask']):
    _tmp: GrizzlyContextTasksTmp
    behave_steps: Dict[int, str]

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

        self._tmp = GrizzlyContextTasksTmp()
        self.behave_steps = {}

    @property
    def tmp(self) -> GrizzlyContextTasksTmp:
        """Read-only property of temporary tasks instance."""
        return self._tmp

    def __call__(self) -> List[GrizzlyTask]:
        """Get a list of tasks in the current context."""
        if len(self.tmp.__stack__) > 0:
            return self.tmp.__stack__[-1].peek()

        return cast(List['GrizzlyTask'], self)

    def add(self, task: GrizzlyTask) -> None:
        """Add a task in current context."""
        if len(self.tmp.__stack__) > 0:
            self.tmp.__stack__[-1].add(task)
        else:
            self.append(task)


@dataclass(unsafe_hash=True)
class GrizzlyContextScenario:
    _name: str = field(init=False, hash=True)
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
        """Initialize implementation specific properties of the dataclass."""
        self.name = self.behave.name
        self.description = self.behave.name

        self._tasks = GrizzlyContextTasks()

    @property
    def tasks(self) -> GrizzlyContextTasks:
        """Read-only property of tasks instance."""
        return self._tasks

    @property
    def identifier(self) -> str:
        """Format scenario index to a readable identifier."""
        return f'{self.index:03}'

    @property
    def name(self) -> str:
        """Scenario name without identifier suffix."""
        if self._name.endswith(f'_{self.identifier}'):
            return self._name.replace(f'_{self.identifier}', '')

        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def locust_name(self) -> str:
        """Name of the scenario as used in locust."""
        return f'{self.identifier} {self.description}'

    @property
    def class_name(self) -> str:
        """Class name should always be suffixed with identifier."""
        if not self.name.endswith(f'_{self.identifier}'):
            return f'{self.name}_{self.identifier}'

        return self.name

    def should_validate(self) -> bool:
        """Check if this scenario has any validation rules."""
        return (
            self.validation.fail_ratio is not None
            or self.validation.avg_response_time is not None
            or self.validation.response_time_percentile is not None
        )


class GrizzlyContextSetupLocustMessages(Dict[MessageDirection, Dict[str, MessageCallback]]):
    def register(self, direction: MessageDirection, message_type: str, callback: MessageCallback) -> None:
        """Register callback for a custom message type, and specify where the callback should be executed."""
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

    user_count: int = field(init=False, default=0)
    spawn_rate: Optional[float] = field(init=False, default=None)
    timespan: Optional[str] = field(init=False, default=None)

    statistics_url: Optional[str] = field(init=False, default=None)

    locust: GrizzlyContextSetupLocust = field(init=False, default_factory=GrizzlyContextSetupLocust)


class GrizzlyContextScenarios(List[GrizzlyContextScenario]):
    def __call__(self) -> List[GrizzlyContextScenario]:
        """Get all scenarios in this context."""
        return cast(List[GrizzlyContextScenario], self)

    def find_by_class_name(self, class_name: str) -> Optional[GrizzlyContextScenario]:
        """Find a scenario based on the class name."""
        return self._find(class_name, 'class_name')

    def find_by_name(self, name: str) -> Optional[GrizzlyContextScenario]:
        """Find a scenario based on the name."""
        return self._find(name, 'name')

    def find_by_description(self, description: str) -> Optional[GrizzlyContextScenario]:
        """Find a scenario based on the description."""
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
