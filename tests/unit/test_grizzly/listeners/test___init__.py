import logging

from typing import Any, Dict, Tuple, Optional, Callable, cast
from os import environ
from random import randint

import pytest

from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture
from locust.stats import RequestStats, StatsError

from grizzly.types.behave import Scenario, Status
from grizzly.types.locust import Environment, LocalRunner, MasterRunner, WorkerRunner, Message
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
from grizzly.types import MessageDirection

from tests.fixtures import LocustFixture, GrizzlyFixture, NoopZmqFixture


class Running(Exception):
    pass


def mocked_TestdataProducer_run(self: Any) -> None:
    raise Running()


def mocked_TestdataProducer___init__(self: Any, grizzly: Any, testdata: Any, address: str) -> None:
    setattr(self, 'grizzly', GrizzlyContext())
    setattr(self, 'address', address)
    setattr(self, 'testdata', testdata)


@pytest.fixture
def listener_test_mocker(mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.run',
        mocked_TestdataProducer_run,
    )

    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.__init__',
        mocked_TestdataProducer___init__,
    )

    noop_zmq('locust.rpc.zmqrpc')

    mocker.patch('locust.runners.rpc.Client.__init__', return_value=None)
    mocker.patch('locust.runners.rpc.BaseSocket.send', autospec=True)
    mocker.patch('locust.runners.WorkerRunner.heartbeat', autospec=True)
    mocker.patch('locust.runners.WorkerRunner.worker', autospec=True)
    mocker.patch('locust.runners.WorkerRunner.connect_to_master', autospec=True)
    mocker.patch('locust.runners.MasterRunner.client_listener', autospec=True)


def test__init_testdata_producer(listener_test_mocker: None, grizzly_fixture: GrizzlyFixture) -> None:
    init_function = _init_testdata_producer(grizzly_fixture.grizzly, '1337', {})

    assert callable(init_function)

    with pytest.raises(Running):
        init_function()

    from grizzly.listeners import producer

    assert producer is not None
    assert producer.__class__.__name__ == 'TestdataProducer'
    assert producer.__class__.__module__ == 'grizzly.testdata.communication'
    assert getattr(producer, 'address', None) == 'tcp://0.0.0.0:1337'
    assert producer.testdata == {}


def test_init_master(listener_test_mocker: None, caplog: LogCaptureFixture, grizzly_fixture: GrizzlyFixture) -> None:
    runner: Optional[MasterRunner] = None
    try:
        grizzly_fixture()
        grizzly = grizzly_fixture.grizzly
        runner = MasterRunner(grizzly_fixture.behave.locust.environment, '0.0.0.0', 5555)
        grizzly.state.locust = runner

        init_function = init(grizzly, {})
        assert callable(init_function)

        init_function(runner)

        assert grizzly.state.locust.custom_messages == {}

        from grizzly.listeners import producer_greenlet

        assert producer_greenlet is not None
        producer_greenlet.kill(block=False)

        with caplog.at_level(logging.ERROR):
            init_function = init(grizzly, None)
            assert callable(init_function)
            init_function(runner)
        assert 'there is no test data' in caplog.text

        def callback(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        def callback_ack(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        grizzly.setup.locust.messages.register(MessageDirection.CLIENT_SERVER, 'test_message', callback)
        grizzly.setup.locust.messages.register(MessageDirection.SERVER_CLIENT, 'test_message_ack', callback_ack)

        init_function = init(grizzly, {})

        init_function(runner)

        assert grizzly.state.locust.custom_messages == cast(Dict[str, Callable], {
            'test_message': callback,
        })
    finally:
        if runner is not None:
            runner.quit()


def test_init_worker(listener_test_mocker: None, grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture()
    runner: Optional[WorkerRunner] = None

    try:
        grizzly = grizzly_fixture.grizzly

        init_function = init(grizzly)
        assert callable(init_function)

        runner = WorkerRunner(grizzly_fixture.behave.locust.environment, 'localhost', 5555)

        grizzly.state.locust = runner

        init_function(runner)

        assert environ.get('TESTDATA_PRODUCER_ADDRESS', None) == 'tcp://localhost:5555'
        assert runner.custom_messages == cast(Dict[str, Callable], {
            'grizzly_worker_quit': grizzly_worker_quit,
        })

        def callback(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        def callback_ack(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        grizzly.setup.locust.messages.register(MessageDirection.CLIENT_SERVER, 'test_message', callback)
        grizzly.setup.locust.messages.register(MessageDirection.SERVER_CLIENT, 'test_message_ack', callback_ack)

        init_function(runner)

        assert grizzly.state.locust.custom_messages == cast(Dict[str, Callable], {
            'grizzly_worker_quit': grizzly_worker_quit,
            'test_message_ack': callback_ack,
        })
    finally:
        if runner is not None:
            runner.quit()

        try:
            del environ['TESTDATA_PRODUCER_ADDRESS']
        except KeyError:
            pass


def test_init_local(listener_test_mocker: None, grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture()
    runner: Optional[LocalRunner] = None

    try:
        grizzly = grizzly_fixture.grizzly
        runner = LocalRunner(grizzly_fixture.behave.locust.environment)
        grizzly.state.locust = runner

        init_function = init(grizzly, {})
        assert callable(init_function)

        init_function(runner)

        assert grizzly.state.locust.custom_messages == {}

        from grizzly.listeners import producer_greenlet
        assert producer_greenlet is not None
        producer_greenlet.kill(block=False)

        assert environ.get('TESTDATA_PRODUCER_ADDRESS', None) == 'tcp://127.0.0.1:5555'

        def callback(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        def callback_ack(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        grizzly.setup.locust.messages.register(MessageDirection.CLIENT_SERVER, 'test_message', callback)
        grizzly.setup.locust.messages.register(MessageDirection.SERVER_CLIENT, 'test_message_ack', callback_ack)

        init_function(runner)

        assert grizzly.state.locust.custom_messages == cast(Dict[str, Callable], {
            'test_message': callback,
            'test_message_ack': callback_ack,
        })
    finally:
        if runner is not None:
            runner.quit()

        try:
            del environ['TESTDATA_PRODUCER_ADDRESS']
        except KeyError:
            pass


def test_init_statistics_listener(mocker: MockerFixture, locust_fixture: LocustFixture) -> None:
    # Influx -- short circuit
    mocker.patch(
        'grizzly.listeners.influxdb.InfluxDb.connect',
        return_value=None,
    )

    mocker.patch(
        'grizzly.listeners.influxdb.InfluxDbListener.run_events',
        return_value=None,
    )

    mocker.patch(
        'grizzly.listeners.influxdb.InfluxDbListener.run_user_count',
        return_value=None,
    )

    # ApplicationInsight -- short circuit
    mocker.patch(
        'grizzly.listeners.appinsights.AzureLogHandler',
        autospec=True,
    )

    try:
        grizzly = GrizzlyContext()

        environment = locust_fixture.environment

        environment.events.quitting._handlers = []
        environment.events.spawning_complete._handlers = []

        # not a valid scheme
        grizzly.setup.statistics_url = 'http://localhost'
        init_statistics_listener(grizzly.setup.statistics_url)(environment)
        assert len(environment.events.request._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0
        assert len(environment.events.quit._handlers) == 0
        assert len(environment.events.spawning_complete._handlers) == 0

        grizzly.setup.statistics_url = 'influxdb://test/database?Testplan=test'
        init_statistics_listener(grizzly.setup.statistics_url)(environment)
        assert len(environment.events.request._handlers) == 2
        assert len(environment.events.quitting._handlers) == 0
        assert len(environment.events.quit._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 0

        grizzly.setup.statistics_url = 'insights://?InstrumentationKey=b9601868-cbf8-43ea-afaf-0a2b820ae1c5'
        with pytest.raises(AssertionError):
            init_statistics_listener(grizzly.setup.statistics_url)(environment)

        grizzly.setup.statistics_url = 'insights://insights.example.se/?InstrumentationKey=b9601868-cbf8-43ea-afaf-0a2b820ae1c5'
        init_statistics_listener(grizzly.setup.statistics_url)(environment)
        assert len(environment.events.request._handlers) == 3
        assert len(environment.events.quitting._handlers) == 0
        assert len(environment.events.quit._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 0
    finally:
        GrizzlyContext.destroy()


def test_locust_test_start(listener_test_mocker: None, grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture()
    try:
        grizzly = grizzly_fixture.grizzly
        scenario = Scenario(filename=None, line=None, keyword='', name='Test Scenario')
        grizzly.scenarios.create(scenario)
        grizzly.scenario.iterations = -1
        runner = MasterRunner(grizzly_fixture.behave.locust.environment, '0.0.0.0', 5555)
        grizzly.state.locust = runner

        locust_test_start(grizzly)(grizzly.state.locust.environment)
    finally:
        GrizzlyContext.destroy()


def test_locust_test_stop(mocker: MockerFixture, listener_test_mocker: None, grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture()

    def mocked_reset(self: Any) -> None:
        raise Running()

    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.on_test_stop',
        mocked_reset,
    )
    init_function = _init_testdata_producer(grizzly_fixture.grizzly, '1337', {})

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


def test_quitting(mocker: MockerFixture, listener_test_mocker: None, grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture()
    mocker.patch(
        'grizzly.testdata.communication.TestdataProducer.stop',
        return_value=None,
    )

    runner: Optional[MasterRunner] = None

    try:
        grizzly = grizzly_fixture.grizzly
        runner = MasterRunner(grizzly_fixture.behave.locust.environment, '0.0.0.0', 5555)
        grizzly.state.locust = runner

        init_testdata = _init_testdata_producer(grizzly, '5557', {})

        with pytest.raises(Running):
            init_testdata()

        init_function = init(grizzly, {})
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


def test_validate_result(mocker: MockerFixture, listener_test_mocker: None, caplog: LogCaptureFixture, grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture()

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

    grizzly = grizzly_fixture.grizzly
    grizzly.scenarios.clear()

    environment = grizzly.state.locust.environment

    environment.stats = RequestStats()
    for method, name in [
        ('POST', '001 OAuth2 client token',),
        ('POST', '001 Register',),
        ('GET', '001 Read',),
    ]:
        for i in range(100):
            environment.stats.log_request(method, name, randint(10, 57), len(name))
            if i % 5 == 0:
                environment.stats.log_error(method, name, RuntimeError('Error'))

    environment.stats.total.total_response_time = 2000

    validate_result_wrapper = validate_result(grizzly)
    assert callable(validate_result_wrapper)

    # environment has statistics, but can't find a matching scenario
    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(environment)
    assert len(caplog.messages) == 3
    assert 'does not match any scenario' in caplog.text
    caplog.clear()

    # scenario name must match with the name that the pickled stats object was dumped from
    scenario = Scenario(None, None, '', 'do some posts')
    grizzly.scenarios.create(scenario)

    # fail ratio
    environment.process_exit_code = 0
    grizzly.scenario.validation.fail_ratio = 0.1

    assert environment.process_exit_code == 0
    assert grizzly.scenario.behave.status == Status.passed

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(environment)

    assert 'failed due to' in caplog.text
    assert environment.process_exit_code == 1
    assert grizzly.scenario.behave.status == Status.failed

    grizzly.scenario.validation.fail_ratio = None
    grizzly.scenario.behave.set_status(Status.passed)
    caplog.clear()

    # avg response time
    environment.process_exit_code = 0
    grizzly.scenario.validation.avg_response_time = 2

    assert environment.process_exit_code == 0
    assert grizzly.scenario.behave.status == Status.passed

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(environment)

    assert 'failed due to' in caplog.text
    assert environment.process_exit_code == 1
    assert grizzly.scenario.behave.status == Status.failed

    grizzly.scenario.validation.avg_response_time = None
    grizzly.scenario.behave.set_status(Status.passed)
    caplog.clear()

    # response time percentile
    environment.process_exit_code = 0
    grizzly.scenario.validation.response_time_percentile = GrizzlyContextScenarioResponseTimePercentile(2, 0.99)

    assert environment.process_exit_code == 0
    assert grizzly.scenario.behave.status == Status.passed

    with caplog.at_level(logging.ERROR):
        validate_result_wrapper(environment)

    assert 'failed due to' in caplog.text
    assert environment.process_exit_code == 1
    assert grizzly.scenario.behave.status == Status.failed

    grizzly.scenario.validation.response_time_percentile = None
    grizzly.scenario.behave.set_status(Status.passed)


def test_grizzly_worker_quit_non_worker(locust_fixture: LocustFixture, caplog: LogCaptureFixture) -> None:
    environment = locust_fixture.environment
    environment.runner = LocalRunner(environment=environment)

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(SystemExit) as se:
            grizzly_worker_quit(environment, Message(message_type='test', data=None, node_id=None))
        assert se.value.code == 1

    assert len(caplog.messages) == 2
    assert caplog.messages[0] == 'received message grizzly_worker_quit'
    assert caplog.messages[1] == 'received grizzly_worker_quit message on a non WorkerRunner?!'


def test_grizzly_worker_quit_worker(listener_test_mocker: None, locust_fixture: LocustFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
    environment = locust_fixture.environment
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

    log_messages = list(filter(lambda m: 'CPU usage' not in m, caplog.messages))

    assert len(log_messages) == 1
    assert log_messages[0] == 'received message grizzly_worker_quit'
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

    log_messages = list(filter(lambda m: 'CPU usage' not in m, caplog.messages))

    assert len(log_messages) == 1
    assert log_messages[0] == 'received message grizzly_worker_quit'
    caplog.clear()

    environment.process_exit_code = None
    environment.runner.errors.update({'test': StatsError('GET', 'test', 'something', 1)})

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(SystemExit) as se:
            grizzly_worker_quit(environment, message)
        assert se.value.code == 3

    log_messages = list(filter(lambda m: 'CPU usage' not in m, caplog.messages))

    assert len(log_messages) == 1
    assert log_messages[0] == 'received message grizzly_worker_quit'
    caplog.clear()
