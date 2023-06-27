import logging
import sys
import re

from os import environ
from typing import cast, Tuple, Any, Dict, Type, List
from types import FunctionType
from random import randint
from socket import error as socket_error
from unittest.mock import MagicMock

import pytest
import gevent

from _pytest.logging import LogCaptureFixture
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture
from locust.stats import RequestStats
from locust.user.users import User
from jinja2 import TemplateError

from grizzly.locust import (
    greenlet_exception_logger,
    grizzly_print_percentile_stats,
    grizzly_print_stats,
    on_master,
    on_worker,
    on_local,
    print_scenario_summary,
    run,
    setup_environment_listeners,
    setup_locust_scenarios,
    setup_resource_limits,
)
from grizzly.types import RequestDirection, RequestMethod, RequestType
from grizzly.types.locust import Environment
from grizzly.types.behave import Context, Scenario
from grizzly.context import GrizzlyContext
from grizzly.tasks import LogMessageTask, RequestTask, WaitTask
from grizzly.tasks.clients import MessageQueueClientTask
from grizzly.users import RestApiUser, MessageQueueUser
from grizzly.scenarios import IteratorScenario
from grizzly.testdata.variables import AtomicIntegerIncrementer

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

from tests.fixtures import BehaveFixture, NoopZmqFixture


def test_greenlet_exception_logger(caplog: LogCaptureFixture) -> None:
    logger = logging.getLogger()
    exception_handler = greenlet_exception_logger(logger)

    assert callable(exception_handler)

    greenlet = gevent.Greenlet()

    from grizzly.locust import unhandled_greenlet_exception
    assert not unhandled_greenlet_exception

    with caplog.at_level(logging.CRITICAL):
        exception_handler(greenlet)
    assert 'unhandled exception in greenlet: ' in caplog.text

    # re-import to get updated value
    import grizzly.locust
    assert getattr(grizzly.locust, 'unhandled_greenlet_exception', False)

    caplog.clear()


def test_on_master(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    try:
        assert environ.get('LOCUST_IS_MASTER', None) is None
        assert not on_master(behave)
        assert environ.get('LOCUST_IS_MASTER', None) is None

        behave.config.userdata['master'] = 'TRUE'
        assert on_master(behave)
        assert environ.get('LOCUST_IS_MASTER', None) == 'true'
    finally:
        try:
            del environ['LOCUST_IS_MASTER']
        except KeyError:
            pass

        try:
            del behave.config.userdata['master']
        except KeyError:
            pass


def test_on_worker(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    try:
        assert environ.get('LOCUST_IS_WORKER', None) is None
        assert not on_worker(behave)
        assert environ.get('LOCUST_IS_WORKER', None) is None

        behave.config.userdata['worker'] = 'TRUE'
        assert on_worker(behave)
        assert environ.get('LOCUST_IS_WORKER', None) == 'true'
    finally:
        try:
            del environ['LOCUST_IS_WORKER']
        except KeyError:
            pass

        try:
            del behave.config.userdata['worker']
        except KeyError:
            pass


def test_on_local(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    try:
        assert environ.get('LOCUST_IS_LOCAL', None) is None
        behave.config.userdata['master'] = 'TrUe'
        assert not on_local(behave)
        assert environ.get('LOCUST_IS_LOCAL', None) is None
        del behave.config.userdata['master']

        assert environ.get('LOCUST_IS_LOCAL', None) is None
        behave.config.userdata['worker'] = 'TrUe'
        assert not on_local(behave)
        assert environ.get('LOCUST_IS_LOCAL', None) is None
        del behave.config.userdata['worker']

        assert on_local(behave)
        assert environ.get('LOCUST_IS_LOCAL', None) == 'true'
    finally:
        try:
            del environ['LOCUST_IS_LOCAL']
        except KeyError:
            pass

        try:
            del behave.config.userdata['master']
        except KeyError:
            pass

        try:
            del behave.config.userdata['worker']
        except KeyError:
            pass


def test_setup_locust_scenarios(behave_fixture: BehaveFixture, noop_zmq: NoopZmqFixture) -> None:
    noop_zmq('grizzly.tasks.clients.messagequeue')
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(grizzly)
    assert 'no scenarios in feature' in str(ae)

    # scenario is missing host
    grizzly.scenarios.create(behave_fixture.create_scenario('test'))

    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(grizzly)
    assert 'increase the number in step' in str(ae)

    grizzly.setup.user_count = 1

    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(grizzly)
    assert 'variable "host" is not found in the context for' in str(ae)

    grizzly.scenario.context['host'] = 'https://test.example.org'

    # no tasks in scenario
    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(grizzly)
    assert 'no tasks has been added to' in str(ae)

    task = RequestTask(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    grizzly.scenario.tasks.add(task)
    grizzly.scenario.tasks.add(WaitTask(time_expression='1.5'))
    grizzly.scenario.tasks.add(LogMessageTask(message='test message'))

    # incorrect user type
    grizzly.scenario.user.class_name = 'NonExistingUser'
    with pytest.raises(AttributeError):
        setup_locust_scenarios(grizzly)

    grizzly.scenario.user.class_name = 'RestApiUser'
    user_classes, start_messagequeue_daemon = setup_locust_scenarios(grizzly)

    assert not start_messagequeue_daemon
    assert len(user_classes) == 1

    user_class = user_classes[-1]
    assert issubclass(user_class, (RestApiUser, ))
    assert len(user_class.tasks) == 1
    assert user_class.host == 'https://test.example.org'
    assert grizzly.scenario.name.startswith('IteratorScenario')

    from locust.user.sequential_taskset import SequentialTaskSetMeta

    user_tasks = user_class.tasks[-1]
    assert issubclass(type(user_tasks), SequentialTaskSetMeta)
    user_tasks = cast(IteratorScenario, user_tasks)
    assert len(user_tasks.tasks) == 3 + 2  # IteratorScenario has two internal task other than what we've added
    assert isinstance(user_tasks.tasks[0], FunctionType)
    assert user_tasks.tasks[0].__name__ == 'iterator'
    assert isinstance(user_tasks.tasks[-1], FunctionType)
    assert user_tasks.tasks[-1].__name__ == 'pace'

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        grizzly.scenario.user.class_name = 'MessageQueueUser'
        grizzly.scenario.context['host'] = 'mq://test.example.org?QueueManager=QM01&Channel=TEST.CHANNEL'
        user_classes, start_messagequeue_daemon = setup_locust_scenarios(grizzly)

        assert start_messagequeue_daemon == set(['async-messaged'])
        assert len(user_classes) == 1

        user_class = user_classes[-1]
        assert issubclass(user_class, (MessageQueueUser, ))
        assert len(user_class.tasks) == 1
        assert user_class.host == 'mq://test.example.org?QueueManager=QM01&Channel=TEST.CHANNEL'
        assert grizzly.scenario.name.startswith('IteratorScenario')

        user_tasks = user_class.tasks[-1]
        assert issubclass(type(user_tasks), SequentialTaskSetMeta)
        user_tasks = cast(IteratorScenario, user_tasks)
        assert len(user_tasks.tasks) == 3 + 2  # IteratorScenario has two internal task other than what we've added

        grizzly.scenario.user.class_name = 'RestApiUser'
        grizzly.scenario.context['host'] = 'https://api.example.io'
        MessageQueueClientTask.__scenario__ = grizzly.scenario
        grizzly.scenario.tasks.add(MessageQueueClientTask(RequestDirection.FROM, 'mqs://username:password@mq.example.io/queue:INCOMING?QueueManager=QM01&Channel=TCP.IN'))
        user_classes, start_messagequeue_daemon = setup_locust_scenarios(grizzly)

        assert start_messagequeue_daemon == set(['async-messaged'])
        assert len(user_classes) == 1


@pytest.mark.parametrize('user_count, user_distribution', [
    (6, [1, 1, 1, 1, 1, 1],),
    (10, [2, 4, 1, 1, 1, 1],),
    (60, [14, 25, 5, 8, 4, 4],),
    (70, [17, 29, 5, 9, 5, 5],),
])
def test_setup_locust_scenarios_user_distribution(behave_fixture: BehaveFixture, user_count: int, user_distribution: List[int]) -> None:
    distribution: List[int] = [25, 42, 8, 13, 6, 6]
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.setup.user_count = user_count
    grizzly.scenarios.clear()

    for index in range(0, len(distribution)):
        scenario = Scenario(filename=None, line=None, keyword='', name=f'Test-{(index + 1)}')
        grizzly.scenarios.create(scenario)
        grizzly.scenario.context['host'] = 'http://localhost:8003'
        grizzly.scenario.tasks.add(LogMessageTask(message='foo bar'))
        grizzly.scenario.user.class_name = 'RestApiUser'
        grizzly.scenario.user.weight = distribution[index]

    user_classes, _ = setup_locust_scenarios(grizzly)

    for index, user_class in enumerate(user_classes):
        assert user_class.fixed_count == user_distribution[index]
        assert user_class.weight == distribution[index]


@pytest.mark.skipif(sys.platform == 'win32', reason='resource module is posix only, this is not done in locust on windows')
def test_setup_resource_limits(behave_fixture: BehaveFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    import resource
    behave = behave_fixture.context

    def mock_on_master(is_master: bool) -> None:
        def mocked_on_master(context: Context) -> bool:
            return is_master

        mocker.patch(
            'grizzly.locust.on_master',
            mocked_on_master,
        )

    def mock_sys_platform(name: str) -> None:
        mocker.patch(
            'grizzly.locust.sys.platform',
            name
        )

    def mock_getrlimit(limit: int) -> MagicMock:
        return mocker.patch(
            'resource.getrlimit',
            return_value=(limit, limit,),
        )

    getrlimit_mock = mock_getrlimit(1024)

    setrlimit_mock = mocker.patch(
        'resource.setrlimit',
        return_value=None,
    )

    # win32
    mock_on_master(False)
    mock_sys_platform('win32')
    setup_resource_limits(behave)
    assert setrlimit_mock.call_count == 0
    assert getrlimit_mock.call_count == 0

    mock_on_master(True)
    mock_sys_platform('win32')
    setup_resource_limits(behave)
    assert setrlimit_mock.call_count == 0
    assert getrlimit_mock.call_count == 0

    # linux
    mock_on_master(True)
    mock_sys_platform('linux')
    setup_resource_limits(behave)
    assert setrlimit_mock.call_count == 0
    assert getrlimit_mock.call_count == 0

    # make sure setrlimit is called
    mock_on_master(False)
    getrlimit_mock = mock_getrlimit(1024)
    setup_resource_limits(behave)
    assert setrlimit_mock.call_count == 1
    assert getrlimit_mock.call_count == 1
    args, kwargs = setrlimit_mock.call_args_list[-1]
    assert kwargs == {}
    assert len(args) == 2
    assert args[0] == resource.RLIMIT_NOFILE
    limit_min, limit_max = args[1]
    assert limit_min == 10000
    assert limit_max == resource.RLIM_INFINITY

    # failed to set resource limits
    setrlimit_mock = mocker.patch(
        'resource.setrlimit',
        side_effect=[OSError],
    )

    with caplog.at_level(logging.WARNING):
        setup_resource_limits(behave)
    assert "and the OS didn't allow locust to increase it by itself" in caplog.text
    assert setrlimit_mock.call_count == 1
    assert getrlimit_mock.call_count == 2
    caplog.clear()

    setrlimit_mock = mocker.patch(
        'resource.setrlimit',
        side_effect=[ValueError],
    )
    with caplog.at_level(logging.WARNING):
        setup_resource_limits(behave)
    assert "and the OS didn't allow locust to increase it by itself" in caplog.text
    assert setrlimit_mock.call_count == 1
    assert getrlimit_mock.call_count == 3
    caplog.clear()

    getrlimit_mock = mock_getrlimit(10001)
    setup_resource_limits(behave)
    assert setrlimit_mock.call_count == 1
    assert getrlimit_mock.call_count == 1


def test_setup_environment_listeners(behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
    from locust import events

    behave = behave_fixture.context

    def mock_on_worker(on_worker: bool) -> None:
        def mocked_on_worker(context: Context) -> bool:
            return on_worker

        mocker.patch(
            'grizzly.locust.on_worker',
            mocked_on_worker,
        )

    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    user_classes: List[Type[User]] = []
    environment = Environment(
        user_classes=user_classes,
        shape_class=None,
        events=events,
    )
    grizzly.state.locust.environment = environment

    try:
        # event listeners for worker node
        mock_on_worker(True)
        external_dependencies, message_handlers = setup_environment_listeners(behave)

        assert len(environment.events.init._handlers) == 1
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0
        assert external_dependencies == set()
        assert message_handlers == {}

        environment.events.spawning_complete._handlers = []  # grizzly handler should only append
        grizzly.setup.statistics_url = 'influxdb://influx.example.com/testdb'

        external_dependencies, message_handlers = setup_environment_listeners(behave)

        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0
        assert external_dependencies == set()
        assert message_handlers == {}
        assert grizzly.state.locust.environment is environment

        environment.events.spawning_complete._handlers = []
        grizzly.setup.statistics_url = None
        grizzly.state.variables['AtomicIntegerIncrementer.value'] = '1 | step=10'

        # event listeteners for master node, not validating results
        mock_on_worker(False)

        task = RequestTask(RequestMethod.POST, 'test-post-1', '/api/v3/test/post/1')
        task.source = '{{ AtomicIntegerIncrementer.value }}, {{ test_id }}'
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.scenario.tasks.clear()
        grizzly.scenario.tasks.add(task)

        with pytest.raises(AssertionError) as ae:
            setup_environment_listeners(behave)
        assert 'variables has been found in templates, but have not been declared: test_id' in str(ae)

        grizzly.state.variables['test_id'] = 'test-1'
        environment.events.spawning_complete._handlers = []

        external_dependencies, message_handlers = setup_environment_listeners(behave)
        assert len(environment.events.init._handlers) == 1
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 1
        assert external_dependencies == set()
        assert message_handlers == {}

        AtomicIntegerIncrementer.destroy()
        grizzly.setup.statistics_url = 'influxdb://influx.example.com/testdb'
        environment.events.spawning_complete._handlers = []

        external_dependencies, message_handlers = setup_environment_listeners(behave)
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 1
        assert external_dependencies == set()
        assert message_handlers == {}

        AtomicIntegerIncrementer.destroy()

        grizzly.scenario.validation.fail_ratio = 0.1
        environment.events.spawning_complete._handlers = []

        external_dependencies, message_handlers = setup_environment_listeners(behave)
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 2
        assert external_dependencies == set()
        assert message_handlers == {}

        AtomicIntegerIncrementer.destroy()

        grizzly.setup.statistics_url = None
        environment.events.spawning_complete._handlers = []

        # problems initializing testdata
        def mocked_initialize_testdata(grizzly: GrizzlyContext) -> Any:
            raise TemplateError('failed to initialize testdata')

        mocker.patch(
            'grizzly.locust.initialize_testdata',
            mocked_initialize_testdata,
        )

        with pytest.raises(AssertionError) as ae:
            setup_environment_listeners(behave)
        assert 'error parsing request payload: ' in str(ae)
    finally:
        try:
            AtomicIntegerIncrementer.destroy()
        except:
            pass


def test_print_scenario_summary(behave_fixture: BehaveFixture, capsys: CaptureFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    grizzly.scenarios.create(behave_fixture.create_scenario('test-1'))

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    assert '''Scenario
ident   iter  status      description
------|-----|-----------|-------------|
001      0/1  undefined   test-1
------|-----|-----------|-------------|
''' == summary
    capsys.readouterr()

    grizzly.scenarios.create(behave_fixture.create_scenario('test-2-test-2-test-2-test-2'))
    grizzly.scenario.iterations = 4
    stat = grizzly.state.locust.environment.stats.get(grizzly.scenario.locust_name, RequestType.SCENARIO())
    stat.num_failures = 1
    stat.num_requests = 3

    stat = grizzly.state.locust.environment.stats.get(grizzly.scenarios[-2].locust_name, RequestType.SCENARIO())
    stat.num_failures = 0
    stat.num_requests = 1

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    assert '''Scenario
ident   iter  status   description
------|-----|--------|-----------------------------|
001      1/1  passed   test-1
002      3/4  failed   test-2-test-2-test-2-test-2
------|-----|--------|-----------------------------|
''' == summary
    capsys.readouterr()

    grizzly.scenarios.create(behave_fixture.create_scenario('#3'))
    stat = grizzly.state.locust.environment.stats.get(grizzly.scenario.locust_name, RequestType.SCENARIO())
    stat.num_failures = 0
    stat.num_requests = 998

    grizzly.scenario.iterations = 999

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    assert '''Scenario
ident      iter  status   description
------|--------|--------|-----------------------------|
001         1/1  passed   test-1
002         3/4  failed   test-2-test-2-test-2-test-2
003     998/999  failed   #3
------|--------|--------|-----------------------------|
''' == summary
    capsys.readouterr()

    grizzly.scenarios.create(behave_fixture.create_scenario('foo bar hello world'))

    grizzly.scenario.iterations = 99999

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    assert '''Scenario
ident      iter  status      description
------|--------|-----------|-----------------------------|
001         1/1  passed      test-1
002         3/4  failed      test-2-test-2-test-2-test-2
003     998/999  failed      #3
004     0/99999  undefined   foo bar hello world
------|--------|-----------|-----------------------------|
''' == summary
    capsys.readouterr()


def test_run_worker(behave_fixture: BehaveFixture, capsys: CaptureFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
    behave = behave_fixture.context

    def mock_on_node(master: bool, worker: bool) -> None:
        mocker.patch(
            'grizzly.locust.on_worker',
            return_value=worker,
        )

        mocker.patch(
            'grizzly.locust.on_master',
            return_value=master,
        )

    mocker.patch(
        'grizzly.locust.Environment.create_worker_runner',
        side_effect=[socket_error()]
    )

    for method in [
        'locust.runners.WorkerRunner.start_worker',
        'gevent.sleep',
        'grizzly.listeners._init_testdata_producer',
    ]:
        mocker.patch(
            method,
            return_value=None,
        )

    def mocked_popen___init__(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        setattr(args[0], 'returncode', -15)

    mocker.patch(
        'grizzly.locust.gevent.subprocess.Popen.__init__',
        mocked_popen___init__,
    )

    mocker.patch('grizzly.locust.gevent.subprocess.Popen.wait', autospec=True)

    import subprocess as subprocess_spy

    messagequeue_process_spy = mocker.spy(subprocess_spy.Popen, '__init__')

    mock_on_node(master=False, worker=True)
    grizzly = cast(GrizzlyContext, behave.grizzly)

    grizzly.setup.user_count = 1
    grizzly.setup.spawn_rate = 1
    grizzly.scenarios.create(behave_fixture.create_scenario('test-non-mq'))
    grizzly.scenario.user.class_name = 'RestApiUser'
    grizzly.scenario.context['host'] = 'https://test.example.org'
    grizzly.scenario.tasks.add(WaitTask(time_expression='1.5'))
    task = RequestTask(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    grizzly.scenario.tasks.add(task)

    assert run(behave) == 1
    capture = capsys.readouterr()

    assert 'failed to connect to the locust master' in capture.err

    assert messagequeue_process_spy.call_count == 0

    # @TODO: test coverage further down in run is needed!


def test_run_master(behave_fixture: BehaveFixture, capsys: CaptureFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
    behave = behave_fixture.context

    def mock_on_node(master: bool, worker: bool) -> None:
        mocker.patch(
            'grizzly.locust.on_worker',
            return_value=worker,
        )

        mocker.patch(
            'grizzly.locust.on_master',
            return_value=master,
        )

    for method in [
        'locust.runners.MasterRunner.start',
        'locust.runners.MasterRunner.client_listener',
        'gevent.sleep',
        'locust.rpc.zmqrpc.Server.__init__',
        'grizzly.listeners._init_testdata_producer',
    ]:
        mocker.patch(
            method,
            return_value=None,
        )

    for printer in ['lstats.print_error_report', 'lstats.print_percentile_stats', 'grizzly_print_stats', 'grizzly_stats_printer', 'lstats.stats_history']:
        mocker.patch(
            f'grizzly.locust.{printer}',
            return_value=None,
        )

    def mocked_popen___init__(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        setattr(args[0], 'returncode', -15)

    mocker.patch(
        'grizzly.locust.gevent.subprocess.Popen.__init__',
        mocked_popen___init__,
    )

    import subprocess as subprocess_spy

    messagequeue_process_spy = mocker.spy(subprocess_spy.Popen, '__init__')

    behave.config.userdata = {
        'master': 'true',
        'worker': 'true',
    }

    assert run(behave) == 254

    capture = capsys.readouterr()
    assert 'cannot be both master and worker' in capture.err

    behave.config.userdata = {'master': 'true'}

    grizzly = cast(GrizzlyContext, behave.grizzly)

    grizzly.setup.spawn_rate = None
    assert run(behave) == 254

    capture = capsys.readouterr()
    assert 'spawn rate is not set' in capture.err

    grizzly.setup.spawn_rate = 1

    assert run(behave) == 254

    capture = capsys.readouterr()
    assert 'step \'Given "user_count" users\' is not in the feature file' in capture.err

    grizzly.setup.user_count = 2
    grizzly.scenarios.create(behave_fixture.create_scenario('test'))
    grizzly.scenario.user.class_name = 'RestApiUser'
    grizzly.scenario.context['host'] = 'https://test.example.org'
    grizzly.scenario.tasks.add(WaitTask(time_expression='1.5'))
    task = RequestTask(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    grizzly.scenario.tasks.add(task)
    grizzly.setup.spawn_rate = 1

    grizzly.setup.timespan = 'adsf'
    assert run(behave) == 1
    assert 'invalid timespan' in capsys.readouterr().err

    grizzly.setup.timespan = None

    behave.config.userdata = {
        'expected-workers': 3,
    }

    mock_on_node(master=True, worker=False)

    with pytest.raises(AssertionError) as ae:
        run(behave)
    assert str(ae.value) == 'there are more workers (3) than users (2), which is not supported'

    assert messagequeue_process_spy.call_count == 0

    del behave.config.userdata['expected-workers']

    # @TODO: this is where it gets hard(er)...

    '''
    def mock_clients_ready(count: int) -> None:
        def mocked_get_by_state(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> List[Any]:
            return [x for x in range(0, count)]

        mocker.patch(
            'locust.runners.WorkerNodes.get_by_state',
            mocked_get_by_state,
        )

    grizzly.state.spawning_complete = True  # fake that all worker nodes has fired spawning complete

    mocked_user_count = mocker.patch(
        'locust.runners.MasterRunner.user_count',
        new_callable=PropertyMock,
    )
    mocked_user_count.return_value = 0

    mock_clients_ready(2)  # same as grizzly.setup.user_count

    run(behave)
    '''


def test_grizzly_print_stats(caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
    # grizzly.locust.run calls locust.log.setup_logging, which messes with the loggers
    test_stats_logger = logging.getLogger('test_grizzly_print_stats')
    mocker.patch('grizzly.locust.stats_logger', test_stats_logger)
    mocker.patch('locust.stats.console_logger', test_stats_logger)

    # create stats
    stats = RequestStats()

    request_types_sequence = ['GET', 'GET', 'POST', 'PUT', 'UNTL', 'ASYNC']
    request_types_sequence_count = 5
    scenario_count = 10

    for ident in range(1, scenario_count + 1):
        for index, method in enumerate(request_types_sequence * request_types_sequence_count, 1):
            name = f'{ident:03} {index:02}-{method.lower()}-test'

            stats.log_request(
                method,
                name,
                response_time=randint(200, 300),
                content_length=(index * randint(10, 20)),
            )

        stats.log_request('DOC', 'TPM report OUT', response_time=randint(200, 300), content_length=999)
        stats.log_request('DOC', 'TPM report IN', response_time=randint(200, 300), content_length=999)

        for method in ['SCEN', 'TSTD', 'VAR', 'CLTSK']:
            stats.log_request(
                method,
                f'{ident:03} {method.lower()}-test',
                response_time=randint(200, 300),
                content_length=(2 * randint(10, 20)),
            )

    caplog.clear()

    # print stats, grizzly style
    with caplog.at_level(logging.INFO):
        # start = perf_counter()
        grizzly_print_stats(stats, grizzly_style=True)
        # grizzly_time = perf_counter() - start

    grizzly_stats = caplog.messages
    caplog.clear()

    # print stats, locust style
    with caplog.at_level(logging.INFO):
        # start = perf_counter()
        grizzly_print_stats(stats, grizzly_style=False)
        # locust_time = perf_counter() - start

    locust_stats = caplog.messages
    caplog.clear()

    for ident in range(1, scenario_count):
        index = (ident - 1) * ((len(request_types_sequence) * request_types_sequence_count) + 4) + 2
        assert re.match(fr'^SCEN\s+{ident:03}', grizzly_stats[index].strip())
        assert re.match(fr'^TSTD\s+{ident:03}', grizzly_stats[index + 1].strip())
        assert re.match(fr'^GET\s+{ident:03} 01-get-test', grizzly_stats[index + 2].strip())
        assert re.match(fr'^VAR\s+{ident:03} var-test', grizzly_stats[index + (len(request_types_sequence) * request_types_sequence_count) + 3].strip())

    last_stat_row = len(grizzly_stats) - 4

    assert re.match(r'^DOC\s+TPM report OUT\s+10', grizzly_stats[last_stat_row].strip())
    assert re.match(r'^DOC\s+TPM report IN\s+10', grizzly_stats[last_stat_row - 1].strip())

    for stat in grizzly_stats:
        assert stat in locust_stats

    # total = grizzly_time + locust_time
    # grizzly_ratio = (grizzly_time / total) * 100
    # locust_ratio = (locust_time / total) * 100

    # grizzly style must be less than 15% slower...
    # assert grizzly_ratio - locust_ratio < 15.0


def test_grizzly_print_percentile_stats(caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
    test_stats_logger = logging.getLogger('test_grizzly_print_percentile_stats')
    mocker.patch('grizzly.locust.stats_logger', test_stats_logger)
    mocker.patch('locust.stats.console_logger', test_stats_logger)

    # create stats
    stats = RequestStats()

    request_types_sequence = ['GET', 'GET', 'POST', 'PUT', 'UNTL', 'ASYNC']
    request_types_sequence_count = 5
    scenario_count = 10

    for ident in range(1, scenario_count):
        for index, method in enumerate(request_types_sequence * request_types_sequence_count, 1):
            name = f'{ident:03} {index:02}-{method.lower()}-test'

            stats.log_request(
                method,
                name,
                response_time=randint(200, 300),
                content_length=(index * randint(10, 20)),
            )

        for method in ['SCEN', 'TSTD', 'VAR', 'CLTSK']:
            stats.log_request(
                method,
                f'{ident:03} {method.lower()}-test',
                response_time=randint(200, 300),
                content_length=(2 * randint(10, 20)),
            )

    caplog.clear()

    # print percentile stats, grizzly style
    with caplog.at_level(logging.INFO):
        grizzly_print_percentile_stats(stats, grizzly_style=True)

    grizzly_stats = caplog.messages
    caplog.clear()

    # print percentile stats, locust style
    with caplog.at_level(logging.INFO):
        grizzly_print_percentile_stats(stats, grizzly_style=False)

    locust_stats = caplog.messages
    caplog.clear()

    for ident in range(1, scenario_count):
        index = (ident - 1) * ((len(request_types_sequence) * request_types_sequence_count) + 4) + 3
        assert re.match(fr'^SCEN\s+{ident:03}', grizzly_stats[index].strip()), grizzly_stats[index].strip()
        assert re.match(fr'^TSTD\s+{ident:03}', grizzly_stats[index + 1].strip())
        assert re.match(fr'^GET\s+{ident:03} 01-get-test', grizzly_stats[index + 2].strip())
        assert re.match(fr'^VAR\s+{ident:03} var-test', grizzly_stats[index + (len(request_types_sequence) * request_types_sequence_count) + 3].strip())

    for stat in grizzly_stats:
        assert stat in locust_stats
