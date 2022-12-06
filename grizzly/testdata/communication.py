import logging

from typing import TYPE_CHECKING, Dict, Optional, Any, cast
from os import environ
from pathlib import Path
from json import dumps as jsondumps
from itertools import chain

from zmq.sugar.constants import NOBLOCK as ZMQ_NOBLOCK, REQ as ZMQ_REQ, REP as ZMQ_REP
from zmq.error import ZMQError, Again as ZMQAgain
import zmq.green as zmq

from gevent import sleep as gsleep
from gevent.lock import Semaphore
from locust.exception import StopUser
from locust.env import Environment

from grizzly.types import TestdataType

from .utils import transform
from .variables import AtomicVariablePersist
from . import GrizzlyVariables


if TYPE_CHECKING:
    from ..context import GrizzlyContext
    from ..scenarios import GrizzlyScenario


class TestdataConsumer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    scenario: 'GrizzlyScenario'
    logger: logging.Logger
    identifier: str
    stopped: bool

    def __init__(self, scenario: 'GrizzlyScenario', identifier: str, address: str = 'tcp://127.0.0.1:5555') -> None:
        self.scenario = scenario
        self.identifier = identifier
        self.logger = logging.getLogger(f'{__name__}/{self.identifier}')

        self.context = zmq.Context()
        self.socket = self.context.socket(ZMQ_REQ)
        self.socket.connect(address)
        self.stopped = False

        self.logger.debug(f'conntected to producer at {address}')

    def stop(self) -> None:
        if self.stopped:
            return

        self.logger.debug('stopping consumer')
        try:
            self.context.destroy(linger=0)
        except:
            self.logger.error('failed to stop', exc_info=True)
        finally:
            self.context.term()
            self.stopped = True
            gsleep(0.1)

    def request(self, scenario: str) -> Optional[Dict[str, Any]]:
        self.logger.debug('available')
        self.socket.send_json({
            'message': 'available',
            'identifier': self.identifier,
            'scenario': scenario,
        })

        self.logger.debug('waiting for response from producer')

        # loop and NOBLOCK needed when running in local mode, to let other gevent threads get time
        while True:
            try:
                message = self.socket.recv_json(flags=ZMQ_NOBLOCK)
                break
            except ZMQAgain:
                gsleep(0.1)  # let other greenlets execute

        if message['action'] == 'stop':
            self.logger.debug('received stop command')
            return None

        if not message['action'] == 'consume':
            self.logger.error(f'unknown action "{message["action"]}" received, stopping user')
            raise StopUser()

        data = message['data']

        self.logger.debug(f'received: {data}')

        variables: Optional[Dict[str, Any]] = None
        if 'variables' in data:
            variables = transform(self.scenario.grizzly, data['variables'], objectify=True, scenario=self.scenario.user._scenario)
            del data['variables']

        data = transform(self.scenario.grizzly, data, objectify=False, scenario=self.scenario.user._scenario)

        if variables is not None:
            data['variables'] = variables

        return cast(Dict[str, Any], data)


class TestdataProducer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    _stopping: bool

    logger: logging.Logger
    semaphore = Semaphore()
    grizzly: 'GrizzlyContext'
    scenarios_iteration: Dict[str, int]
    testdata: TestdataType
    environment: Environment

    def __init__(self, grizzly: 'GrizzlyContext', testdata: TestdataType, address: str = 'tcp://127.0.0.1:5555') -> None:
        self.grizzly = grizzly
        self.testdata = testdata
        self.environment = self.grizzly.state.locust.environment

        self.logger = logging.getLogger(f'{__name__}/producer')

        self.logger.debug(f'starting on {address}')

        self.context = zmq.Context()
        self.socket = self.context.socket(ZMQ_REP)
        self.socket.bind(address)
        self.scenarios_iteration = {}

        self._stopping = False

        self.logger.debug(f'serving:\n{self.testdata}')

    def reset(self) -> None:
        self.logger.debug('reseting')
        with self.semaphore:
            for scenario_name in self.scenarios_iteration.keys():
                self.scenarios_iteration[scenario_name] = 0

    def persist_testdata(self) -> None:
        try:
            feature_file = environ.get('GRIZZLY_FEATURE_FILE', None)
            context_root = environ.get('GRIZZLY_CONTEXT_ROOT', None)

            assert feature_file is not None
            assert context_root is not None

            persist_root = Path(context_root) / 'persistent'
            variable_state: Dict[str, str] = {}

            for testdata in self.testdata.values():
                for key, variable in testdata.items():
                    try:
                        if '.' in key and not variable == '__on_consumer__':
                            _, _, variable_name = GrizzlyVariables.get_variable_spec(key)

                            if not isinstance(variable, AtomicVariablePersist):
                                continue

                            variable_state.update({key: variable.generate_initial_value(variable_name)})
                    except:
                        continue

            # only write file if we actually have something to write
            if len(variable_state.keys()) > 0 and len(list(chain(*variable_state.values()))) > 0:
                persist_root.mkdir(exist_ok=True, parents=True)
                persist_file = persist_root / f'{Path(feature_file).stem}.json'
                persist_file.write_text(jsondumps(variable_state, indent=2))
                self.logger.info(f'wrote variables next initial values for {feature_file} to {persist_file}')
        except:
            self.logger.error('failed do persist variables next initial values', exc_info=True)

    def stop(self) -> None:
        self._stopping = True
        self.logger.debug('stopping producer')
        try:
            self.context.destroy(linger=0)
        except:
            self.logger.error('failed to stop', exc_info=True)
        finally:
            # make sure that socket is properly released
            gsleep(0.1)
            self.context.term()

            self.persist_testdata()

    def run(self) -> None:
        self.logger.debug('start producing...')
        try:
            while True:
                try:
                    recv = self.socket.recv_json(flags=ZMQ_NOBLOCK)
                    consumer_identifier = recv.get('identifier', '')
                    self.logger.debug(f'got request from consumer {consumer_identifier}')

                    if recv['message'] == 'available':
                        message: Dict[str, Any] = {
                            'action': 'stop',
                        }

                        try:
                            with self.semaphore:
                                scenario_name = recv['scenario']
                                scenario = self.grizzly.scenarios.find_by_class_name(scenario_name)

                                if scenario is not None:
                                    if scenario_name not in self.scenarios_iteration and scenario.iterations > 0:
                                        self.scenarios_iteration[scenario_name] = 0

                                    if (
                                        scenario_name in self.scenarios_iteration
                                        and self.scenarios_iteration[scenario_name] < scenario.iterations
                                    ) or scenario_name not in self.scenarios_iteration:
                                        testdata = self.testdata.get(scenario_name, {})
                                        message['action'] = 'consume'
                                        data: Dict[str, Any] = {'variables': {}}
                                        loaded_variable_datatypes: Dict[str, Any] = {}

                                        for key, variable in testdata.items():
                                            if '.' in key and not variable == '__on_consumer__':
                                                module_name, variable_type, variable_name = GrizzlyVariables.get_variable_spec(key)
                                                _, data_attribute = key.rsplit('.', 1)

                                                if variable_name != data_attribute:
                                                    testdata_type = f'{variable_type}.{variable_name}'
                                                    if module_name != 'grizzly.testdata.variables':
                                                        testdata_type = f'{module_name}.{testdata_type}'

                                                    if testdata_type not in loaded_variable_datatypes:
                                                        loaded_variable_datatypes[testdata_type] = variable[variable_name]

                                                    value = loaded_variable_datatypes[testdata_type][data_attribute]
                                                else:
                                                    value = variable[variable_name]
                                            else:
                                                value = variable

                                            if value is None and scenario_name not in self.scenarios_iteration:
                                                message['action'] = 'stop'
                                                self.logger.warning(f'{key} does not have a value and iterations is not set for {scenario_name}, stop test')
                                                data = {}
                                                break
                                            else:
                                                if key in self.grizzly.state.alias:
                                                    key = self.grizzly.state.alias[key]
                                                    data[key] = value
                                                else:
                                                    data['variables'][key] = value

                                        message['data'] = data

                                    if scenario_name in self.scenarios_iteration:
                                        self.scenarios_iteration[scenario_name] += 1
                                        self.logger.debug(f'{consumer_identifier}/{scenario_name}: iteration={self.scenarios_iteration[scenario_name]}')
                        except TypeError:
                            message = {
                                'action': 'stop',
                            }
                            self.logger.error(f'test data error, stop consumer {consumer_identifier}', exc_info=True)

                        self.logger.debug(f'producing {message} for consumer {consumer_identifier}')
                        self.socket.send_json(message)
                    else:
                        self.logger.error(f'received unknown message "{recv["messsage"]}"')

                    gsleep(0)
                except ZMQAgain:
                    gsleep(0.1)
        except ZMQError:
            if not self._stopping:
                self.logger.error('failed when waiting for consumers', exc_info=True)
            self.environment.events.test_stop.fire(environment=self.environment)
