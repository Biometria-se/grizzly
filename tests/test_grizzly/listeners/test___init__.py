import logging
import pickle

from typing import Any, Dict, Tuple, Generator, Optional
from os import environ, path
from behave.model import Scenario

import pytest

from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture
from locust.env import Environment
from locust.runners import LocalRunner, MasterRunner, WorkerRunner
from locust.rpc.protocol import Message

from grizzly.listeners import (
    _init_testdata_producer,
    grizzly_worker_quit,
    init,
    init_statistics_listener,
    locust_test_start,
    locust_test_stop,
    quitting,
    spawning_complete,
    validate_result,
)
from grizzly.context import GrizzlyContext, GrizzlyContextScenarioResponseTimePercentile

from ...fixtures import LocustFixture


class Running(Exception):
    pass


def mocked_TestdataProducer_run(self: Any) -> None:
    raise Running()


def mocked_TestdataProducer___init__(self: Any, testdata: Any, address: str, environment: Environment) -> None:
    setattr(self, 'address', address)
    setattr(self, 'testdata', testdata)
    setattr(self, 'environment', environment)


def mocked_noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    pass


@pytest.fixture
def listener_test(mocker: MockerFixture, locust_fixture: LocustFixture) -> Generator[Environment, None, None]:
    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.run',
        mocked_TestdataProducer_run,
    )

    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.__init__',
        mocked_TestdataProducer___init__,
    )

    mocker.patch(
        'zmq.sugar.socket.Socket.bind',
        mocked_noop,
    )

    mocker.patch(
        'zmq.sugar.socket.Socket.connect',
        mocked_noop,
    )

    mocker.patch(
        'zmq.sugar.socket.Socket.send',
        mocked_noop,
    )

    yield locust_fixture.env


def test__init_testdata_producer(listener_test: Environment) -> None:
    init_function = _init_testdata_producer('1337', {}, listener_test)

    assert callable(init_function)

    with pytest.raises(Running):
        init_function()

    from grizzly.listeners import producer

    assert producer is not None
    assert producer.__class__.__name__ == 'TestdataProducer'
    assert producer.__class__.__module__ == 'grizzly.testdata.communication'
    assert getattr(producer, 'address', None) == 'tcp://0.0.0.0:1337'
    assert producer.testdata == {}


def test_init_master(listener_test: Environment, caplog: LogCaptureFixture) -> None:
    runner: Optional[MasterRunner] = None
    try:
        runner = MasterRunner(listener_test, '0.0.0.0', 5555)

        init_function = init({})
        assert callable(init_function)

        init_function(runner)

        from grizzly.listeners import producer_greenlet

        assert producer_greenlet is not None
        producer_greenlet.kill(block=False)

        with caplog.at_level(logging.ERROR):
            init_function = init(None)
            assert callable(init_function)
            init_function(runner)
        assert 'there is no test data' in caplog.text
    finally:
        if runner is not None:
            runner.greenlet.kill(block=False)


def test_init_worker(listener_test: Environment) -> None:
    runner: Optional[WorkerRunner] = None

    try:
        init_function = init()
        assert callable(init_function)

        runner = WorkerRunner(listener_test, 'localhost', 5555)

        init_function(runner)

        assert environ.get('TESTDATA_PRODUCER_ADDRESS', None) == 'tcp://localhost:5555'
    finally:
        if runner is not None:
            runner.greenlet.kill(block=False)

        try:
            del environ['TESTDATA_PRODUCER_ADDRESS']
        except KeyError:
            pass


def test_init_local(listener_test: Environment) -> None:
    runner: Optional[LocalRunner] = None

    try:
        runner = LocalRunner(listener_test)

        init_function = init({})
        assert callable(init_function)

        init_function(runner)

        from grizzly.listeners import producer_greenlet
        assert producer_greenlet is not None
        producer_greenlet.kill(block=False)

        assert environ.get('TESTDATA_PRODUCER_ADDRESS', None) == 'tcp://127.0.0.1:5555'
    finally:
        if runner is not None:
            runner.greenlet.kill(block=False)

        try:
            del environ['TESTDATA_PRODUCER_ADDRESS']
        except KeyError:
            pass


def test_init_statistics_listener(mocker: MockerFixture, locust_fixture: LocustFixture) -> None:
    # Influx -- short circuit
    mocker.patch(
        'grizzly.listeners.influxdb.InfluxDb.connect',
        mocked_noop,
    )

    mocker.patch(
        'grizzly.listeners.influxdb.InfluxDbListener.run',
        mocked_noop,
    )

    # ApplicationInsight -- short circuit
    mocker.patch(
        'grizzly.listeners.appinsights.AzureLogHandler',
        autospec=True,
    )

    try:
        grizzly = GrizzlyContext()

        environment = locust_fixture.env

        environment.events.request_success._handlers = []
        environment.events.request_failure._handlers = []
        environment.events.quitting._handlers = []
        environment.events.spawning_complete._handlers = []

        # not a valid scheme
        grizzly.setup.statistics_url = 'http://localhost'
        init_statistics_listener(grizzly.setup.statistics_url)(environment)
        assert len(environment.events.request_success._handlers) == 0
        assert len(environment.events.request_failure._handlers) == 0
        assert len(environment.events.request._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0
        assert len(environment.events.spawning_complete._handlers) == 0

        grizzly.setup.statistics_url = 'influxdb://test/database?Testplan=test'
        init_statistics_listener(grizzly.setup.statistics_url)(environment)
        assert len(environment.events.request_success._handlers) == 0
        assert len(environment.events.request_failure._handlers) == 0
        assert len(environment.events.request._handlers) == 2
        assert len(environment.events.quitting._handlers) == 0
        assert len(environment.events.spawning_complete._handlers) == 0

        grizzly.setup.statistics_url = 'insights://?InstrumentationKey=b9601868-cbf8-43ea-afaf-0a2b820ae1c5'
        with pytest.raises(AssertionError):
            init_statistics_listener(grizzly.setup.statistics_url)(environment)

        grizzly.setup.statistics_url = 'insights://insights.example.se/?InstrumentationKey=b9601868-cbf8-43ea-afaf-0a2b820ae1c5'
        init_statistics_listener(grizzly.setup.statistics_url)(environment)
        assert len(environment.events.request_success._handlers) == 0
        assert len(environment.events.request_failure._handlers) == 0
        assert len(environment.events.request._handlers) == 3
        assert len(environment.events.quitting._handlers) == 0
        assert len(environment.events.spawning_complete._handlers) == 0
    finally:
        GrizzlyContext.destroy()


def test_locust_test_start(listener_test: Environment) -> None:
    try:
        grizzly = GrizzlyContext()
        grizzly.add_scenario('Test Scenario')
        grizzly.scenario.iterations = -1
        runner = MasterRunner(listener_test, '0.0.0.0', 5555)
        listener_test.runner = runner

        locust_test_start(grizzly)(listener_test)
    finally:
        GrizzlyContext.destroy()


def test_locust_test_stop(mocker: MockerFixture, listener_test: Environment) -> None:
    def mocked_reset(self: Any) -> None:
        raise Running()

    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.reset',
        mocked_reset,
    )
    init_function = _init_testdata_producer('1337', {}, listener_test)

    assert callable(init_function)

    with pytest.raises(Running):
        init_function()

    with pytest.raises(Running):
        locust_test_stop()


def test_spawning_complete() -> None:
    grizzly = GrizzlyContext()

    try:
        assert not grizzly.state.spawning_complete
        func = spawning_complete(grizzly)

        func()

        assert grizzly.state.spawning_complete
    finally:
        GrizzlyContext.destroy()


def test_quitting(mocker: MockerFixture, listener_test: Environment) -> None:
    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.stop',
        mocked_noop,
    )

    runner: Optional[MasterRunner] = None

    try:
        runner = MasterRunner(listener_test, '0.0.0.0', 5555)

        init_testdata = _init_testdata_producer('5557', {}, listener_test)

        with pytest.raises(Running):
            init_testdata()

        init_function = init({})
        assert callable(init_function)

        init_function(runner)

        from grizzly.listeners import producer_greenlet, producer

        assert producer_greenlet is not None
        assert producer is not None

        quitting()

        from grizzly.listeners import producer_greenlet, producer

        assert producer_greenlet is None
        assert producer is None
    finally:
        if runner is not None:
            runner.greenlet.kill(block=False)


def test_validate_result(mocker: MockerFixture, listener_test: Environment, caplog: LogCaptureFixture) -> None:
    def print_stats(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        pass

    mocker.patch(
        'locust.stats.print_error_report',
        print_stats,
    )

    mocker.patch(
        'locust.stats.print_percentile_stats',
        print_stats,
    )

    mocker.patch(
        'locust.stats.print_stats',
        print_stats,
    )

    grizzly = GrizzlyContext()

    static_dir = path.realpath(path.join(path.dirname(__file__), '..', '_static'))

    # load pickled stats object, and modify it for triggering each result validation
    listener_test.stats = pickle.load(open(f'{static_dir}/stats.p', 'rb'))
    for stats_entry in listener_test.stats.entries.values():
        stats_entry.num_failures = stats_entry.num_requests
    listener_test.stats.total.total_response_time = 2000

    validate_result_wrapper = validate_result(grizzly)
    assert callable(validate_result_wrapper)

    # environment has statistics, but can't find a matching scenario
    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)
    assert 'does not match any scenario' in caplog.text
    caplog.clear()

    # scenario name must match with the name that the pickled stats object was dumped from
    scenario = Scenario(None, None, '', 'do some posts')
    grizzly.add_scenario(scenario)

    # fail ratio
    listener_test.process_exit_code = 0
    grizzly.scenario.validation.fail_ratio = 0.1

    assert listener_test.process_exit_code == 0
    assert grizzly.scenario.behave.status == 'passed'

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)

    assert 'failed due to' in caplog.text
    assert listener_test.process_exit_code == 1
    assert grizzly.scenario.behave.status == 'failed'

    grizzly.scenario.validation.fail_ratio = None
    grizzly.scenario.behave.set_status('passed')
    caplog.clear()

    # avg response time
    listener_test.process_exit_code = 0
    grizzly.scenario.validation.avg_response_time = 2

    assert listener_test.process_exit_code == 0
    assert grizzly.scenario.behave.status == 'passed'

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)

    assert 'failed due to' in caplog.text
    assert listener_test.process_exit_code == 1
    assert grizzly.scenario.behave.status == 'failed'

    grizzly.scenario.validation.avg_response_time = None
    grizzly.scenario.behave.set_status('passed')
    caplog.clear()

    # response time percentile
    listener_test.process_exit_code = 0
    grizzly.scenario.validation.response_time_percentile = GrizzlyContextScenarioResponseTimePercentile(2, 0.99)

    assert listener_test.process_exit_code == 0
    assert grizzly.scenario.behave.status == 'passed'

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)

    assert 'failed due to' in caplog.text
    assert listener_test.process_exit_code == 1
    assert grizzly.scenario.behave.status == 'failed'

    grizzly.scenario.validation.response_time_percentile = None
    grizzly.scenario.behave.set_status('passed')


def test_grizzly_worker_quit_non_worker(locust_fixture: LocustFixture, caplog: LogCaptureFixture) -> None:
    environment = locust_fixture.env
    environment.runner = LocalRunner(environment=environment)

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(SystemExit) as se:
            grizzly_worker_quit(environment, Message(message_type='test', data=None, node_id=None))
        assert se.value.code == 1

    assert len(caplog.messages) == 2
    assert caplog.messages[0] == 'received message grizzly_worker_quit'
    assert caplog.messages[1] == 'received grizzly_worker_quit message on a non WorkerRunner?!'


def test_grizzly_worker_quit_worker(locust_fixture: LocustFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
    mocker.patch('locust.runners.rpc.Client.__init__', return_value=None)
    mocker.patch('locust.runners.rpc.BaseSocket.send', autospec=True)
    mocker.patch('locust.runners.WorkerRunner.heartbeat', autospec=True)
    mocker.patch('locust.runners.WorkerRunner.worker', autospec=True)

    environment = locust_fixture.env
    environment.runner = WorkerRunner(environment=environment, master_host='localhost', master_port=1337)

    runner_stop_mock = mocker.patch.object(environment.runner, 'stop', autospec=True)
    runner_send_stat_mock = mocker.patch.object(environment.runner, '_send_stats', autospec=True)
    runner_client_send = mocker.patch('locust.runners.rpc.Client.send', return_value=None)

    message = Message(message_type='test', data=None, node_id=None)

    environment.process_exit_code = None
    environment.runner.stats.errors = {}
    environment.runner.exceptions = {}

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(SystemExit) as se:
            grizzly_worker_quit(environment, message)
        assert se.value.code == 0

    assert len(caplog.messages) == 1
    assert caplog.messages[0] == 'received message grizzly_worker_quit'
    caplog.clear()

    runner_stop_mock.assert_called_once()
    runner_send_stat_mock.assert_called_once()
    runner_client_send.assert_called_once()
    args, _ = runner_client_send.call_args_list[-1]
    assert len(args) == 1
    assert isinstance(args[0], Message)
    assert args[0].type == 'client_stopped'
    assert args[0].data is None
    assert args[0].node_id == environment.runner.client_id

    environment.process_exit_code = 1337

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(SystemExit) as se:
            grizzly_worker_quit(environment, message)
        assert se.value.code == 1337

    assert len(caplog.messages) == 1
    assert caplog.messages[0] == 'received message grizzly_worker_quit'
    caplog.clear()

    environment.process_exit_code = None
    environment.runner.errors.update({'test': 1})

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(SystemExit) as se:
            grizzly_worker_quit(environment, message)
        assert se.value.code == 3

    assert len(caplog.messages) == 1
    assert caplog.messages[0] == 'received message grizzly_worker_quit'
    caplog.clear()
