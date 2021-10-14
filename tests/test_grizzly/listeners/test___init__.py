import logging
import pickle

from typing import Any, Dict, Tuple, Generator, Optional
from os import environ, path
from behave.model import Scenario

import pytest

from _pytest.logging import LogCaptureFixture
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from locust.runners import LocalRunner, MasterRunner, WorkerRunner

from grizzly.listeners import _init_testdata_producer, init, init_statistics_listener, locust_test_start, locust_test_stop, quitting, spawning_complete, validate_result
from grizzly.context import LocustContext, LocustContextScenarioResponseTimePercentile

from ..fixtures import locust_environment  # pylint: disable=unused-import


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
def listener_test(mocker: MockerFixture, locust_environment: Environment) -> Generator[Environment, None, None]:
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

    yield locust_environment


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
        assert 'There is no test data' in caplog.text
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


@pytest.mark.usefixtures('locust_environment')
def test_init_statistics_listener(mocker: MockerFixture, locust_environment: Environment) -> None:
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
        'logging.Logger.addHandler',
        mocked_noop,
    )

    try:
        context_locust = LocustContext()

        locust_environment.events.request_success._handlers = []
        locust_environment.events.request_failure._handlers = []
        locust_environment.events.quitting._handlers = []
        locust_environment.events.spawning_complete._handlers = []

        # not a valid scheme
        context_locust.setup.statistics_url = 'http://localhost'
        init_statistics_listener(context_locust.setup.statistics_url)(locust_environment)
        assert len(locust_environment.events.request_success._handlers) == 0
        assert len(locust_environment.events.request_failure._handlers) == 0
        assert len(locust_environment.events.request._handlers) == 1
        assert len(locust_environment.events.quitting._handlers) == 0
        assert len(locust_environment.events.spawning_complete._handlers) == 0

        context_locust.setup.statistics_url = 'influxdb://test/database?Testplan=test'
        init_statistics_listener(context_locust.setup.statistics_url)(locust_environment)
        assert len(locust_environment.events.request_success._handlers) == 0
        assert len(locust_environment.events.request_failure._handlers) == 0
        assert len(locust_environment.events.request._handlers) == 2
        assert len(locust_environment.events.quitting._handlers) == 0
        assert len(locust_environment.events.spawning_complete._handlers) == 0

        context_locust.setup.statistics_url = 'insights://?InstrumentationKey=b9601868-cbf8-43ea-afaf-0a2b820ae1c5'
        with pytest.raises(AssertionError):
            init_statistics_listener(context_locust.setup.statistics_url)(locust_environment)

        context_locust.setup.statistics_url = 'insights://insights.example.se/?InstrumentationKey=b9601868-cbf8-43ea-afaf-0a2b820ae1c5'
        init_statistics_listener(context_locust.setup.statistics_url)(locust_environment)
        assert len(locust_environment.events.request_success._handlers) == 0
        assert len(locust_environment.events.request_failure._handlers) == 0
        assert len(locust_environment.events.request._handlers) == 3
        assert len(locust_environment.events.quitting._handlers) == 0
        assert len(locust_environment.events.spawning_complete._handlers) == 0
    finally:
        LocustContext.destroy()


def test_locust_test_start(listener_test: Environment) -> None:
    try:
        context_locust = LocustContext()
        context_locust.add_scenario('Test Scenario')
        context_locust.scenario.iterations = -1
        runner = MasterRunner(listener_test, '0.0.0.0', 5555)
        listener_test.runner = runner

        locust_test_start(context_locust)(listener_test)
    finally:
        LocustContext.destroy()


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
    locust_context = LocustContext()

    try:
        assert not locust_context.state.spawning_complete
        func = spawning_complete(locust_context)

        func()

        assert locust_context.state.spawning_complete
    finally:
        LocustContext.destroy()


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

    locust_context = LocustContext()

    static_dir = path.realpath(path.join(path.dirname(__file__), '..', '_static'))

    # load pickled stats object, and modify it for triggering each result validation
    listener_test.stats = pickle.load(open(f'{static_dir}/stats.p', 'rb'))
    for stats_entry in listener_test.stats.entries.values():
        stats_entry.num_failures = stats_entry.num_requests
    listener_test.stats.total.total_response_time = 2000

    validate_result_wrapper = validate_result(locust_context)
    assert callable(validate_result_wrapper)

    # environment has statistics, but can't find a matching scenario
    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)
    assert 'does not match any scenario' in caplog.text
    caplog.clear()

    # scenario name must match with the name that the pickled stats object was dumped from
    scenario = Scenario(None, None, '', 'do some posts')
    locust_context.add_scenario(scenario)

    # fail ratio
    listener_test.process_exit_code = 0
    locust_context.scenario.validation.fail_ratio = 0.1

    assert listener_test.process_exit_code == 0
    assert locust_context.scenario.behave.status == 'passed'

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)

    assert 'failed due to' in caplog.text
    assert listener_test.process_exit_code == 1
    assert locust_context.scenario.behave.status == 'failed'

    locust_context.scenario.validation.fail_ratio = None
    locust_context.scenario.behave.set_status('passed')
    caplog.clear()

    # avg response time
    listener_test.process_exit_code = 0
    locust_context.scenario.validation.avg_response_time = 2

    assert listener_test.process_exit_code == 0
    assert locust_context.scenario.behave.status == 'passed'

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)

    assert 'failed due to' in caplog.text
    assert listener_test.process_exit_code == 1
    assert locust_context.scenario.behave.status == 'failed'

    locust_context.scenario.validation.avg_response_time = None
    locust_context.scenario.behave.set_status('passed')
    caplog.clear()

    # response time percentile
    listener_test.process_exit_code = 0
    locust_context.scenario.validation.response_time_percentile = LocustContextScenarioResponseTimePercentile(2, 0.99)

    assert listener_test.process_exit_code == 0
    assert locust_context.scenario.behave.status == 'passed'

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(listener_test)

    assert 'failed due to' in caplog.text
    assert listener_test.process_exit_code == 1
    assert locust_context.scenario.behave.status == 'failed'

    locust_context.scenario.validation.response_time_percentile = None
    locust_context.scenario.behave.set_status('passed')
