import logging

from typing import TYPE_CHECKING, Callable, Optional, Dict, Any, Tuple, List, Type, Union, cast
from os import environ, path
from dataclasses import dataclass, field

import yaml

from jinja2 import Environment
from jinja2.filters import FILTERS

from grizzly.types import MessageCallback, MessageDirection
from grizzly.types.locust import MasterRunner, WorkerRunner, LocalRunner
from grizzly.types.behave import Scenario

from .testdata import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
    from .tasks import GrizzlyTask, GrizzlyTaskWrapper, AsyncRequestGroupTask, TimerTask, ConditionalTask, LoopTask


logger = logging.getLogger(__name__)


def load_configuration_file() -> Dict[str, Any]:
    def _flatten(node: Dict[str, Any], parents: Optional[List[str]] = None) -> Dict[str, Any]:
        flat: Dict[str, Any] = {}
        if parents is None:
            parents = []

        for key, value in node.items():
            parents.append(key)
            if isinstance(value, dict):
                flat = {**flat, **_flatten(value, parents)}
            else:
                flat['.'.join(parents)] = value

            parents.pop()

        return flat

    configuration_file = environ.get('GRIZZLY_CONFIGURATION_FILE', None)

    if configuration_file is None:
        return {}

    try:
        if path.splitext(configuration_file)[1] not in ['.yml', '.yaml']:
            logger.error('configuration file must have file extension yml or yaml')
            raise SystemExit(1)

        with open(configuration_file, 'r') as fd:
            yaml_configuration = yaml.load(fd, Loader=yaml.Loader)
            return _flatten(yaml_configuration['configuration'])
    except FileNotFoundError:
        logger.error(f'{configuration_file} does not exist')
        raise SystemExit(1)


class GrizzlyContext:
    __instance: Optional['GrizzlyContext'] = None

    _initialized: bool
    _state: 'GrizzlyContextState'
    _setup: 'GrizzlyContextSetup'
    _scenarios: 'GrizzlyContextScenarios'

    @classmethod
    def __new__(cls, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> 'GrizzlyContext':
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialized = False

        return cls.__instance

    @classmethod
    def destroy(cls) -> None:
        if cls.__instance is None:
            raise ValueError(f"'{cls.__name__}' is not instantiated")

        cls.__instance = None

    def __init__(self) -> None:
        if not self._initialized:
            self._state = GrizzlyContextState()
            self._setup = GrizzlyContextSetup()
            self._scenarios = GrizzlyContextScenarios()
            self._initialized = True

    @property
    def setup(self) -> 'GrizzlyContextSetup':
        return self._setup

    @property
    def state(self) -> 'GrizzlyContextState':
        return self._state

    @property
    def scenario(self) -> 'GrizzlyContextScenario':
        if len(self._scenarios) < 1:
            raise ValueError('no scenarios created!')

        return self._scenarios[-1]

    @property
    def scenarios(self) -> 'GrizzlyContextScenarios':
        return self._scenarios


def jinja2_environment_factory() -> Environment:
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


StackedFuncType = Callable[['GrizzlyContextTasksTmp'], Optional['GrizzlyTaskWrapper']]


def stackproperty(func: StackedFuncType) -> property:
    def setter(self: 'GrizzlyContextTasksTmp', value: Optional['GrizzlyTaskWrapper']) -> None:
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
    _async_group: Optional['AsyncRequestGroupTask']
    _conditional: Optional['ConditionalTask']
    _loop: Optional['LoopTask']

    _timers: Dict[str, Optional['TimerTask']]
    _custom: Dict[str, Optional['GrizzlyTaskWrapper']]

    __stack__: List['GrizzlyTaskWrapper']

    def __init__(self) -> None:
        self.__stack__ = []

        self._async_group = None
        self._conditional = None
        self._loop = None

        self._timers = {}
        self._custom = {}

    @stackproperty
    def async_group(self) -> Optional['AsyncRequestGroupTask']:
        return self._async_group

    @stackproperty
    def conditional(self) -> Optional['ConditionalTask']:
        return self._conditional

    @stackproperty
    def loop(self) -> Optional['LoopTask']:
        return self._loop

    @property
    def custom(self) -> Dict[str, Optional['GrizzlyTaskWrapper']]:
        return self._custom

    @custom.setter
    def custom(self, value: Dict[str, Optional['GrizzlyTaskWrapper']]) -> None:
        self._custom = value

    @property
    def timers(self) -> Dict[str, Optional['TimerTask']]:
        return self._timers

    @timers.setter
    def timers(self, value: Dict[str, Optional['TimerTask']]) -> None:
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
        return self._tmp

    def __call__(self) -> List['GrizzlyTask']:
        if len(self.tmp.__stack__) > 0:
            return self.tmp.__stack__[-1].peek()
        else:
            return cast(List['GrizzlyTask'], self)

    def add(self, task: 'GrizzlyTask') -> None:
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
        if self._name.endswith(f'_{self.identifier}'):
            return self._name.replace(f'_{self.identifier}', '')
        else:
            return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def locust_name(self) -> str:
        return f'{self.identifier} {self.description}'

    @property
    def class_name(self) -> str:
        if not self.name.endswith(f'_{self.identifier}'):
            return f'{self.name}_{self.identifier}'
        else:
            return self.name

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
            raise AttributeError(f'message type {message_type} was already registered in direction {direction.name}')


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
        grizzly_scenario = GrizzlyContextScenario(len(self) + 1, behave=behave_scenario)

        self.append(grizzly_scenario)
