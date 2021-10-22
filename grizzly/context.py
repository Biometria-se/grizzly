import logging

from typing import Optional, Dict, Any, Tuple, List, Union
from os import environ, path
from hashlib import sha1 as sha1_hash
from dataclasses import dataclass, field
from abc import ABCMeta

import yaml

from behave.model import Scenario

from .testdata.models import TemplateData

logger = logging.getLogger(__name__)


@dataclass(unsafe_hash=True)
class GrizzlyTask(metaclass=ABCMeta):
    scenario: 'LocustContextScenario' = field(init=False, repr=False)


from .task import RequestTask


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
class LocustContextState:
    spawning_complete: bool = field(default=False)
    background_section_done: bool = field(default=False)
    variables: TemplateData = field(init=False, default_factory=TemplateData)
    configuration: Dict[str, Any] = field(init=False, default_factory=load_configuration_file)
    alias: Dict[str, str] = field(init=False, default_factory=dict)


@dataclass
class LocustContextScenarioWait:
    minimum: float = field(default=1.0)
    maximum: float = field(default=1.0)


@dataclass
class LocustContextScenarioResponseTimePercentile:
    response_time: int
    percentile: float


@dataclass(unsafe_hash=True)
class LocustContextScenarioValidation:
    fail_ratio: Optional[float] = field(init=False, default=None)
    avg_response_time: Optional[int] = field(init=False, default=None)
    response_time_percentile: Optional[LocustContextScenarioResponseTimePercentile] = field(init=False, default=None)


@dataclass(unsafe_hash=True)
class LocustContextScenario:
    name: str = field(init=False, hash=True)
    user_class_name: str = field(init=False, hash=True)
    _identifier: Optional[str] = field(init=False, hash=True, default=None)
    iterations: int = field(init=False, repr=False, hash=False, compare=False, default=1)

    behave: Scenario = field(init=False, repr=False, hash=False, compare=False)
    context: Dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)
    wait: LocustContextScenarioWait = field(init=False, repr=False, hash=False, compare=False, default_factory=LocustContextScenarioWait)
    tasks: List[Union[RequestTask, float]] = field(init=False, repr=False, hash=False, compare=False, default_factory=list)
    validation: LocustContextScenarioValidation = field(init=False, hash=False, compare=False, default_factory=LocustContextScenarioValidation)
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

    def add_task(self, task: Union[RequestTask, float]) -> None:
        if isinstance(task, GrizzlyTask) and (
            not hasattr(task, 'scenario') or
            task.scenario is None or
            task.scenario is not self
        ):
            task.scenario = self

        self.tasks.append(task)


@dataclass
class LocustContextSetup:
    log_level: str = field(init=False, default='INFO')

    global_context: Dict[str, Any] = field(init=False, repr=False, hash=False, compare=False, default_factory=dict)

    user_count: int = field(init=False, default=0)
    spawn_rate: Optional[int] = field(init=False, default=None)
    timespan: Optional[str] = field(init=False, default=None)

    statistics_url: Optional[str] = field(init=False, default=None)


class LocustContext:
    __instance: Optional['LocustContext'] = None

    _initialized: bool
    _state: LocustContextState
    _setup: LocustContextSetup
    _scenarios: List[LocustContextScenario]

    @classmethod
    def __new__(cls, *_args: Tuple[Any, ...], **_kwargs: Dict[str, Any]) -> 'LocustContext':
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
            self._state = LocustContextState()
            self._setup = LocustContextSetup()
            self._scenarios = []
            self._initialized = True

    @property
    def setup(self) -> LocustContextSetup:
        return self._setup

    @property
    def state(self) -> LocustContextState:
        return self._state

    @property
    def scenario(self) -> LocustContextScenario:
        if len(self._scenarios) < 1:
            self._scenarios.append(LocustContextScenario())

        return self._scenarios[-1]

    def add_scenario(self, source: Union[Scenario, str]) -> None:
        scenario = LocustContextScenario()
        if isinstance(source, Scenario):
            name = source.name
            scenario.behave = source
        else:
            name = source

        scenario.name = name
        self._scenarios.append(scenario)

    def scenarios(self) -> List[LocustContextScenario]:
        return self._scenarios

    def get_scenario(self, name: str) -> Optional[LocustContextScenario]:
        for scenario in self._scenarios:
            if scenario.get_name() == name:
                return scenario

        return None
