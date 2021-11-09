import logging

from typing import Optional, Dict, Any, Tuple, List, Union, Callable
from os import environ, path
from hashlib import sha1 as sha1_hash
from dataclasses import dataclass, field
from abc import ABCMeta

import yaml

from behave.model import Scenario
from locust.user.sequential_taskset import SequentialTaskSet
from locust.env import Environment

from .types import GrizzlyDict


logger = logging.getLogger(__name__)


# @TODO: these two classes is needed here to avoid circular imports
class GrizzlyTasksBase(SequentialTaskSet):
    pass


@dataclass(unsafe_hash=True)
class GrizzlyTask(metaclass=ABCMeta):
    scenario: 'GrizzlyContextScenario' = field(init=False, repr=False)

    def implementation(self) -> Callable[[GrizzlyTasksBase], Any]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented "implementation"')


def generate_identifier(name: str) -> str:
    return sha1_hash(name.encode('utf-8')).hexdigest()[:8]


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
            logger.error(f'configuration file must have file extension yml or yaml')
            raise SystemExit(1)

        with open(configuration_file, 'r') as fd:
            yaml_configuration = yaml.load(fd, Loader=yaml.Loader)
            return _flatten(yaml_configuration['configuration'])
    except FileNotFoundError:
        logger.error(f'{configuration_file} does not exist')
        raise SystemExit(1)


@dataclass
class GrizzlyContextState:
    spawning_complete: bool = field(default=False)
    background_section_done: bool = field(default=False)
    variables: GrizzlyDict = field(init=False, default_factory=GrizzlyDict)
    configuration: Dict[str, Any] = field(init=False, default_factory=load_configuration_file)
    alias: Dict[str, str] = field(init=False, default_factory=dict)
    verbose: bool = field(default=False)
    environment: Environment = field(init=False, repr=False)


@dataclass
class GrizzlyContextScenarioWait:
    minimum: float = field(default=1.0)
    maximum: float = field(default=1.0)


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

@dataclass(unsafe_hash=True)
class GrizzlyContextScenario:
    name: str = field(init=False, hash=True)
    user: GrizzlyContextScenarioUser = field(init=False, hash=False, compare=False, default_factory=GrizzlyContextScenarioUser)
    _identifier: Optional[str] = field(init=False, hash=True, default=None)
    iterations: int = field(init=False, repr=False, hash=False, compare=False, default=1)

    behave: Scenario = field(init=False, repr=False, hash=False, compare=False)
    context: Dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)
    wait: GrizzlyContextScenarioWait = field(init=False, repr=False, hash=False, compare=False, default_factory=GrizzlyContextScenarioWait)
    tasks: List[GrizzlyTask] = field(init=False, repr=False, hash=False, compare=False, default_factory=list)
    validation: GrizzlyContextScenarioValidation = field(init=False, hash=False, compare=False, default_factory=GrizzlyContextScenarioValidation)
    stop_on_failure: bool = field(init=False, default=False)
    orphan_templates: List[str] = field(init=False, repr=False, hash=False, compare=False, default_factory=list)

    @property
    def identifier(self) -> str:
        if not hasattr(self, 'name') or self.name is None:
            raise ValueError('scenario has no name')

        if self._identifier is None:
            self._identifier = generate_identifier(self.name)

        return self._identifier

    def get_name(self) -> str:
        if self._identifier is None:
            self._identifier = generate_identifier(self.name)

        if not self.name.endswith(f'_{self._identifier}'):
            return f'{self.name}_{self.identifier}'
        else:
            return self.name

    def should_validate(self) -> bool:
        return (
            self.validation.fail_ratio is not None or
            self.validation.avg_response_time is not None or
            self.validation.response_time_percentile is not None
        )

    def add_task(self, task: GrizzlyTask) -> None:
        if isinstance(task, GrizzlyTask) and (
            not hasattr(task, 'scenario') or
            task.scenario is None or
            task.scenario is not self
        ):
            task.scenario = self

        self.tasks.append(task)


@dataclass
class GrizzlyContextSetup:
    log_level: str = field(init=False, default='INFO')

    global_context: Dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)

    user_count: int = field(init=False, default=0)
    spawn_rate: Optional[float] = field(init=False, default=None)
    timespan: Optional[str] = field(init=False, default=None)

    statistics_url: Optional[str] = field(init=False, default=None)


class GrizzlyContext:
    __instance: Optional['GrizzlyContext'] = None

    _initialized: bool
    _state: GrizzlyContextState
    _setup: GrizzlyContextSetup
    _scenarios: List[GrizzlyContextScenario]

    @classmethod
    def __new__(cls, *_args: Tuple[Any, ...], **_kwargs: Dict[str, Any]) -> 'GrizzlyContext':
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
            self._scenarios = []
            self._initialized = True

    @property
    def setup(self) -> GrizzlyContextSetup:
        return self._setup

    @property
    def state(self) -> GrizzlyContextState:
        return self._state

    @property
    def scenario(self) -> GrizzlyContextScenario:
        if len(self._scenarios) < 1:
            self._scenarios.append(GrizzlyContextScenario())

        return self._scenarios[-1]

    def add_scenario(self, source: Union[Scenario, str]) -> None:
        scenario = GrizzlyContextScenario()
        if isinstance(source, Scenario):
            name = source.name
            scenario.behave = source
        else:
            name = source

        scenario.name = name
        self._scenarios.append(scenario)

    def scenarios(self) -> List[GrizzlyContextScenario]:
        return self._scenarios

    def get_scenario(self, name: str) -> Optional[GrizzlyContextScenario]:
        for scenario in self._scenarios:
            if scenario.get_name() == name:
                return scenario

        return None
