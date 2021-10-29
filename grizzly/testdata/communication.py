import logging

from typing import Dict, Optional, Any, cast

import zmq
import gevent

from gevent.lock import Semaphore
from locust.exception import StopUser
from locust.env import Environment

from ..context import GrizzlyContext
from ..types import TestdataType
from .utils import transform


logger = logging.getLogger(__name__)


class TestdataConsumer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    def __init__(self, address: str ='tcp://127.0.0.1:5555') -> None:
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(address)
        logger.debug(f'conntected to producer at {address}')

    def stop(self) -> None:
        try:
            self.context.destroy(linger=0)
        except:
            logger.error('failed to stop consumer', exc_info=True)
        finally:
            gevent.sleep(0.1)

    def request(self, scenario: str) -> Optional[Dict[str, Any]]:
        logger.debug('consumer available')
        self.socket.send_json({
            'message': 'available',
            'scenario': scenario,
        })

        logger.debug('waiting for response from producer')

        # loop and NOBLOCK needed when running in local mode, to let other gevent threads get time
        while True:
            try:
                message = self.socket.recv_json(flags=zmq.NOBLOCK)
                break
            except zmq.error.Again:
                gevent.sleep(0.1)  # let TestdataProducer greenlet execute

        if message['action'] == 'stop':
            raise StopUser(f'stop command received')

        if not message['action'] == 'consume':
            raise StopUser(f'unknown action "{message["action"]}" received')

        data = message['data']

        logger.debug(f'got {data} from producer')

        variables: Optional[Dict[str, Any]] = None
        if 'variables' in data:
            variables = transform(data['variables'], objectify=True)
            del data['variables']

        data = transform(data, objectify=False)

        if variables is not None:
            data['variables'] = variables

        return cast(Dict[str, Any], data)


class TestdataProducer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    semaphore = Semaphore()
    grizzly: GrizzlyContext
    scenarios_iteration: Dict[str, int]
    testdata: TestdataType
    environment: Environment

    def __init__(
        self,
        testdata: TestdataType,
        environment: Environment,
        address: str = 'tcp://127.0.0.1:5555',
    ) -> None:
        self.testdata = testdata
        self.environment = environment

        logger.debug(f'starting producer on {address}')

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(address)
        self.grizzly = GrizzlyContext()
        self.scenarios_iteration = {}

        logger.debug(self.testdata)

    def reset(self) -> None:
        logger.debug('reseting TestdataProducer')
        with self.semaphore:
            for scenario_name in self.scenarios_iteration.keys():
                self.scenarios_iteration[scenario_name] = 0

    def stop(self) -> None:
        try:
            self.context.destroy(linger=0)
        except:
            logger.error('failed to stop producer', exc_info=True)
        finally:
            # make sure that socket is properly released
            gevent.sleep(0.1)

    def run(self) -> None:
        logger.debug('start producing...')
        try:
            while True:
                try:
                    recv = self.socket.recv_json(flags=zmq.NOBLOCK)
                    logger.debug('got data from consumer')

                    if recv['message'] == 'available':
                        message: Dict[str, Any] = {
                            'action': 'stop',
                        }
                        try:
                            with self.semaphore:
                                scenario_name = recv['scenario']
                                scenario = self.grizzly.get_scenario(scenario_name)

                                if scenario is not None:
                                    if scenario_name not in self.scenarios_iteration and scenario.iterations > 0:
                                        self.scenarios_iteration[scenario_name] = 0

                                    if (
                                        scenario_name in self.scenarios_iteration and
                                        self.scenarios_iteration[scenario_name] < scenario.iterations
                                    ) or scenario_name not in self.scenarios_iteration:
                                        testdata = self.testdata.get(scenario_name, {})
                                        message['action'] = 'consume'
                                        data: Dict[str, Any] = {'variables': {}}
                                        loaded_variable_datatypes: Dict[str, Any] = {}

                                        for key, variable in testdata.items():
                                            if '.' in key and not variable == '__on_consumer__':
                                                [data_type, testdata_value] = key.split('.', 1)
                                                if '.' in testdata_value:
                                                    # @TODO: what if name contains deeper levels (.)?
                                                    [testdata_value, name] = testdata_value.split('.', 1)
                                                    testdata_type = '.'.join([data_type, testdata_value])
                                                    if testdata_type not in loaded_variable_datatypes:
                                                        loaded_variable_datatypes[testdata_type] = variable[testdata_value]

                                                    value = loaded_variable_datatypes[testdata_type][name]
                                                else:
                                                    value = variable[testdata_value]
                                            else:
                                                value = variable

                                            if value is None and scenario_name not in self.scenarios_iteration:
                                                message['action'] = 'stop'
                                                logger.warning(f'{key} does not have a value and iterations is not set for {scenario_name}, stop test')
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
                                        logger.debug(f'{scenario_name}: iterations={self.scenarios_iteration[scenario_name]}')
                        except TypeError:
                            logger.error('test data error, stop consumer', exc_info=True)

                        logger.debug(f'producing {message} for consumer')
                        self.socket.send_json(message)

                    gevent.sleep(0)
                except zmq.error.Again:
                    gevent.sleep(0.01)
        except zmq.error.ZMQError:
            self.environment.events.test_stop.fire(environment=self.environment)
