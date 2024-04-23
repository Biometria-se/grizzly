"""RPC client and server for synchronized testdata."""
from __future__ import annotations

import logging
from contextlib import suppress
from itertools import chain
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union, cast

import zmq.green as zmq
from gevent import sleep as gsleep
from gevent.lock import Semaphore
from zmq.error import Again as ZMQAgain
from zmq.error import ZMQError

from grizzly.types.locust import Environment, StopUser

from . import GrizzlyVariables
from .utils import transform
from .variables import AtomicVariablePersist

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import TestdataType


class TestdataConsumer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    scenario: GrizzlyScenario
    logger: logging.Logger
    identifier: str
    stopped: bool

    def __init__(self, scenario: GrizzlyScenario, identifier: str, address: str = 'tcp://127.0.0.1:5555') -> None:
        self.scenario = scenario
        self.identifier = identifier
        self.logger = logging.getLogger(f'{__name__}/{self.identifier}')

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(address)
        self.stopped = False

        self.logger.debug('connected to producer at %s', address)

    def stop(self) -> None:
        if self.stopped:
            return

        self.logger.debug('stopping consumer')
        try:
            self.context.destroy(linger=0)
        except:
            self.logger.exception('failed to stop')
        finally:
            self.context.term()
            self.stopped = True
            gsleep(0.1)

    def testdata(self, scenario: str) -> Optional[Dict[str, Any]]:
        request = {
            'message': 'testdata',
            'identifier': self.identifier,
            'scenario': scenario,
        }

        response = self._request(request)

        if response is None:
            self.logger.error('no testdata received')
            return None

        if response['action'] == 'stop':
            self.logger.debug('received stop command')
            return None

        if response['action'] != 'consume':
            self.logger.error('unknown action "%s" received, stopping user', response['action'])
            raise StopUser

        data = response['data']

        self.logger.debug('received: %r', data)

        variables: Optional[Dict[str, Any]] = None
        if 'variables' in data:
            variables = transform(self.scenario.grizzly, data['variables'], objectify=True, scenario=self.scenario.user._scenario)
            del data['variables']

        data = transform(self.scenario.grizzly, data, objectify=False, scenario=self.scenario.user._scenario)

        if variables is not None:
            data['variables'] = variables

        return cast(Dict[str, Any], data)

    def keystore_get(self, key: str) -> Optional[Any]:
        request = {
            'action': 'get',
            'key': key,
        }

        response = self._keystore_request(request)

        return (response or {}).get('data', None)

    def keystore_set(self, key: str, value: Any) -> None:
        request = {
            'action': 'set',
            'key': key,
            'data': value,
        }

        self._keystore_request(request)

    def _keystore_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        request.update({
            'message': 'keystore',
            'identifier': self.identifier,
        })

        return self._request(request)

    def _request(self, request: Dict[str, str]) -> Optional[Dict[str, Any]]:
        self.socket.send_json(request)

        self.logger.debug('waiting for response from producer')
        message: Dict[str, Any]

        # loop and NOBLOCK needed when running in local mode, to let other gevent threads get time
        while True:
            try:
                message = cast(Dict[str, Any], self.socket.recv_json(flags=zmq.NOBLOCK))
                break
            except ZMQAgain:
                gsleep(0.1)  # let other greenlets execute

        return message


class TestdataProducer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    _stopping: bool
    _persist_file: Path

    logger: logging.Logger
    semaphore = Semaphore()
    grizzly: GrizzlyContext
    scenarios_iteration: Dict[str, int]
    testdata: TestdataType
    environment: Environment
    has_persisted: bool
    keystore: Dict[str, Any]

    def __init__(self, grizzly: GrizzlyContext, testdata: TestdataType, address: str = 'tcp://127.0.0.1:5555') -> None:
        self.grizzly = grizzly
        self.testdata = testdata
        self.environment = self.grizzly.state.locust.environment

        self.logger = logging.getLogger(f'{__name__}/producer')

        self.logger.debug('starting on %s', address)

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(address)
        self.scenarios_iteration = {}

        self._stopping = False
        self.has_persisted = False

        self.logger.debug('serving:\n%r', self.testdata)

        feature_file = environ.get('GRIZZLY_FEATURE_FILE', None)
        context_root = environ.get('GRIZZLY_CONTEXT_ROOT', None)
        assert feature_file is not None
        assert context_root is not None

        persist_root = Path(context_root) / 'persistent'
        self._persist_file = persist_root / f'{Path(feature_file).stem}.json'

        self.keystore = {}

    def on_test_stop(self) -> None:
        self.logger.debug('test stopping')
        with self.semaphore:
            self.persist_data()
            for scenario_name in self.scenarios_iteration:
                self.scenarios_iteration[scenario_name] = 0

    def persist_data(self) -> None:
        if self.has_persisted:
            return

        try:
            variable_state: Dict[str, Union[str, Dict[str, Any]]] = {}

            for testdata in self.testdata.values():
                for key, variable in testdata.items():
                    if '.' not in key or variable == '__on_consumer__':
                        continue

                    with suppress(Exception):
                        _, _, variable_name, _ = GrizzlyVariables.get_variable_spec(key)

                        if not isinstance(variable, AtomicVariablePersist):
                            continue

                        variable_state.update({key: variable.generate_initial_value(variable_name)})

            # only write file if we actually have something to write
            if len(variable_state.keys()) > 0 and len(list(chain(*variable_state.values()))) > 0:
                self._persist_file.parent.mkdir(exist_ok=True, parents=True)
                self._persist_file.write_text(jsondumps(variable_state, indent=2))
                self.logger.info('feature file data persisted in %s', self._persist_file)
                self.has_persisted = True
        except:
            self.logger.exception('failed to persist feature file data')

    def stop(self) -> None:
        self._stopping = True
        self.logger.debug('stopping producer')
        try:
            self.context.destroy(linger=0)
        except:
            self.logger.exception('failed to stop')
        finally:
            # make sure that socket is properly released
            gsleep(0.1)
            self.context.term()

            self.persist_data()

    def _handle_request_keystore(self, request: Dict[str, Any]) -> Dict[str, Any]:
        response = request
        if request['action'] == 'get':
            response['data'] = self.keystore.get(request['key'], None)
        elif request['action'] == 'set':
            key = response.get('key', None)
            value = response.get('data', None)

            if key is not None:
                self.keystore.update({key: value})
        else:
            self.logger.error('received unknown keystore action "%s"', request['action'])
            response['data'] = None

        return response

    def _handle_request_testdata(self, request: Dict[str, Any]) -> Dict[str, Any]:  # noqa: PLR0912
        consumer_identifier = request.get('identifier', '')
        response: Dict[str, Any] = {
            'action': 'stop',
        }

        try:
            scenario_name = request['scenario']
            scenario = self.grizzly.scenarios.find_by_class_name(scenario_name)

            if scenario is not None:
                if scenario_name not in self.scenarios_iteration and scenario.iterations > 0:
                    self.scenarios_iteration[scenario_name] = 0

                if not (
                    scenario_name in self.scenarios_iteration
                    and self.scenarios_iteration[scenario_name] < scenario.iterations
                ) or scenario_name not in self.scenarios_iteration:
                    return response

                testdata = self.testdata.get(scenario_name, {})
                response['action'] = 'consume'
                data: Dict[str, Any] = {'variables': {}}
                loaded_variable_datatypes: Dict[str, Any] = {}

                for key, variable in testdata.items():
                    if '.' in key and variable != '__on_consumer__':
                        module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(key)
                        _, data_attribute = key.rsplit('.', 1)

                        if variable_name != data_attribute:
                            testdata_type = f'{variable_type}.{variable_name}'
                            if module_name != 'grizzly.testdata.variables':
                                testdata_type = f'{module_name}.{testdata_type}'

                            if testdata_type not in loaded_variable_datatypes:
                                try:
                                    loaded_variable_datatypes[testdata_type] = variable[variable_name]
                                except NotImplementedError:
                                    continue

                            value = loaded_variable_datatypes[testdata_type][data_attribute]
                        else:
                            try:
                                value = variable[variable_name]
                            except NotImplementedError:
                                continue
                    else:
                        value = variable

                    if value is None and scenario_name not in self.scenarios_iteration:
                        response['action'] = 'stop'
                        self.logger.warning('%s does not have a value and iterations is not set for %s, stop test', key, scenario_name)
                        data = {}
                        break

                    data['variables'][key] = value

                    if key in self.grizzly.state.alias:
                        data_key = self.grizzly.state.alias[key]
                        data[data_key] = value

                response['data'] = data

                if scenario_name in self.scenarios_iteration:
                    self.scenarios_iteration[scenario_name] += 1
                    self.logger.debug('%s/%s: iteration=%d', consumer_identifier, scenario_name, self.scenarios_iteration[scenario_name])
        except TypeError:
            response = {
                'action': 'stop',
            }
            self.logger.exception('test data error, stop consumer %s', consumer_identifier)

        return response

    def run(self) -> None:
        self.logger.debug('start producing...')
        try:
            while True:
                try:
                    recv = cast(Dict[str, Any], self.socket.recv_json(flags=zmq.NOBLOCK))
                    consumer_identifier = recv.get('identifier', '')
                    self.logger.debug('got request from consumer %s', consumer_identifier)
                    response: Dict[str, Any]

                    with self.semaphore:
                        if recv['message'] == 'keystore':
                            response = self._handle_request_keystore(recv)
                        elif recv['message'] == 'testdata':
                            response = self._handle_request_testdata(recv)
                        else:
                            self.logger.error('received unknown message "%s"', recv['message'])
                            response = {}

                        self.logger.debug('producing %r for consumer %s', response, consumer_identifier)
                        self.socket.send_json(response)

                    gsleep(0)
                except ZMQAgain:  # noqa: PERF203
                    gsleep(0.1)
        except ZMQError:
            if not self._stopping:
                self.logger.exception('failed when waiting for consumers')
            self.environment.events.test_stop.fire(environment=self.environment)
