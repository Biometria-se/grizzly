import logging
import sys

from os import environ
from typing import cast, Tuple, Any, Dict, Type, List

import pytest
import gevent

from _pytest.logging import LogCaptureFixture
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture
from behave.runner import Context
from behave.model import Scenario
from locust.env import Environment
from jinja2 import Template, TemplateError

from grizzly.locust import (
    greenlet_exception_logger,
    on_master,
    on_worker,
    on_local,
    print_scenario_summary,
    run,
    setup_environment_listeners,
    setup_locust_scenarios,
    setup_resource_limits,
)
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.tasks import GrizzlyTask, PrintTask, RequestTask, WaitTask
from grizzly.users import RestApiUser, MessageQueueUser
from grizzly.users.base import GrizzlyUser
from grizzly.scenarios import IteratorScenario
from grizzly.testdata.variables import AtomicMessageQueue, AtomicIntegerIncrementer
from grizzly_extras.async_message import AsyncMessageResponse

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

from .fixtures import BehaveFixture, NoopZmqFixture


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


def test_setup_locust_scenarios(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(grizzly)
    assert 'no scenarios in feature' in str(ae)

    # scenario is missing host
    grizzly.add_scenario('test')

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
    grizzly.scenario.add_task(task)
    grizzly.scenario.add_task(WaitTask(time=1.5))
    grizzly.scenario.add_task(PrintTask(message='test message'))

    # incorrect user type
    grizzly.scenario.user.class_name = 'NonExistingUser'
    with pytest.raises(AttributeError):
        setup_locust_scenarios(grizzly)

    grizzly.scenario.user.class_name = 'RestApiUser'
    user_classes, tasks, start_messagequeue_daemon = setup_locust_scenarios(grizzly)

    assert not start_messagequeue_daemon
    assert len(user_classes) == 1
    assert len(tasks) == 3

    user_class = user_classes[-1]
    assert issubclass(user_class, (RestApiUser, ))
    assert len(user_class.tasks) == 1
    assert user_class.host == 'https://test.example.org'
    assert grizzly.scenario.name.startswith('IteratorScenario')

    from locust.user.sequential_taskset import SequentialTaskSetMeta

    user_tasks = user_class.tasks[-1]
    assert issubclass(type(user_tasks), SequentialTaskSetMeta)
    user_tasks = cast(IteratorScenario, user_tasks)
    assert len(user_tasks.tasks) == 3 + 1  # IteratorScenario has an internal task other than what we've added

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        grizzly.scenario.user.class_name = 'MessageQueueUser'
        grizzly.scenario.context['host'] = 'mq://test.example.org?QueueManager=QM01&Channel=TEST.CHANNEL'
        user_classes, tasks, start_messagequeue_daemon = setup_locust_scenarios(grizzly)

        assert start_messagequeue_daemon
        assert len(user_classes) == 1
        assert len(tasks) == 3

        user_class = user_classes[-1]
        assert issubclass(user_class, (MessageQueueUser, ))
        assert len(user_class.tasks) == 1
        assert user_class.host == 'mq://test.example.org?QueueManager=QM01&Channel=TEST.CHANNEL'
        assert grizzly.scenario.name.startswith('IteratorScenario')

        user_tasks = user_class.tasks[-1]
        assert issubclass(type(user_tasks), SequentialTaskSetMeta)
        user_tasks = cast(IteratorScenario, user_tasks)
        assert len(user_tasks.tasks) == 3 + 1  # IteratorScenario has an internal task other than what we've added


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
    grizzly._scenarios = []

    for index in range(0, len(distribution)):
        scenario = Scenario(filename=None, line=None, keyword='', name=f'Test-{(index + 1)}')
        grizzly.add_scenario(scenario)
        grizzly.scenario.context['host'] = 'http://localhost:8003'
        grizzly.scenario.add_task(PrintTask(message='foo bar'))
        grizzly.scenario.user.class_name = 'RestApiUser'
        grizzly.scenario.user.weight = distribution[index]

    user_classes, _, _ = setup_locust_scenarios(grizzly)

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

    def mock_getrlimit(limit: int) -> None:
        def mocked_getrlimit(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Tuple[int, int]:
            return limit, 0

        mocker.patch(
            'resource.getrlimit',
            mocked_getrlimit,
        )

    def mock_setrlimit(exception_type: Type[Exception]) -> None:
        def mocked_setrlimit(_resource: int, limits: Tuple[int, int]) -> Any:
            if sys.platform != 'win32':
                assert _resource == resource.RLIMIT_NOFILE
                assert limits == (10000, resource.RLIM_INFINITY, )

            raise exception_type()

        mocker.patch(
            'resource.setrlimit',
            mocked_setrlimit,
        )

    mock_on_master(False)
    mock_sys_platform('win32')
    setup_resource_limits(behave)

    mock_sys_platform('linux')
    setup_resource_limits(behave)

    # make sure setrlimit is called
    mock_on_master(True)
    mock_getrlimit(1024)
    mock_setrlimit(RuntimeError)
    with pytest.raises(RuntimeError):
        setup_resource_limits(behave)

    # failed to set resource limits
    mock_setrlimit(OSError)
    with caplog.at_level(logging.WARNING):
        setup_resource_limits(behave)
    assert "and the OS didn't allow locust to increase it by itself" in caplog.text
    caplog.clear()

    mock_setrlimit(ValueError)
    with caplog.at_level(logging.WARNING):
        setup_resource_limits(behave)
    assert "and the OS didn't allow locust to increase it by itself" in caplog.text
    caplog.clear()

    mock_getrlimit(10001)
    try:
        setup_resource_limits(behave)
    except RuntimeError:
        pytest.fail('setrlimit was unexpectedly called')


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
    user_classes: List[Type[GrizzlyUser]] = []
    environment = Environment(
        user_classes=user_classes,
        shape_class=None,
        events=events,
    )

    noop_zmq('grizzly.testdata.variables.messagequeue')

    def mock_response(response: AsyncMessageResponse) -> None:
        def mocked_response(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> AsyncMessageResponse:
            return response

        mocker.patch(
            'grizzly.testdata.variables.messagequeue.zmq.Socket.recv_json',
            mocked_response,
        )

    try:
        # event listeners for worker node
        mock_on_worker(True)
        external_dependencies = setup_environment_listeners(behave, environment, [])

        assert len(environment.events.init._handlers) == 1
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0
        assert external_dependencies == set()

        grizzly.setup.statistics_url = 'influxdb://influx.example.com/testdb'

        external_dependencies = setup_environment_listeners(behave, environment, [])
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0
        assert external_dependencies == set()
        assert grizzly.state.environment is environment

        grizzly.setup.statistics_url = None
        grizzly.state.variables['AtomicIntegerIncrementer.value'] = '1 | step=10'

        # event listeteners for master node, not validating results
        mock_on_worker(False)

        task = RequestTask(RequestMethod.POST, 'test-post-1', '/api/v3/test/post/1')
        task.source = '{{ AtomicIntegerIncrementer.value }}, {{ test_id }}'
        task.template = Template(task.source)
        task.scenario = GrizzlyContextScenario()
        task.scenario.name = 'test-scenario-1'
        task.scenario.user.class_name = 'RestApiUser'
        tasks: List[GrizzlyTask] = [task]

        with pytest.raises(AssertionError) as ae:
            setup_environment_listeners(behave, environment, tasks)
        assert 'variable test_id has not been initialized' in str(ae)

        AtomicIntegerIncrementer.destroy()
        grizzly.state.variables['test_id'] = 'test-1'

        external_dependencies = setup_environment_listeners(behave, environment, tasks)
        assert len(environment.events.init._handlers) == 1
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 1
        assert external_dependencies == set()

        AtomicIntegerIncrementer.destroy()
        grizzly.setup.statistics_url = 'influxdb://influx.example.com/testdb'

        external_dependencies = setup_environment_listeners(behave, environment, tasks)
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 1
        assert external_dependencies == set()

        AtomicIntegerIncrementer.destroy()

        mock_response({
            'success': True,
            'worker': 'aaaa-bbbb-cccc',
        })

        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            grizzly.state.variables.update({
                'AtomicMessageQueue.test': (
                    'TEST.QUEUE | url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", expression="$.result.value", content_type="application/json"'
                ),
            })
            task.endpoint = '/api/v1/{{ AtomicMessageQueue.test }}'

        grizzly.scenario.validation.fail_ratio = 0.1

        external_dependencies = setup_environment_listeners(behave, environment, tasks)
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 2
        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            assert external_dependencies == set(['async-messaged'])
        else:
            assert external_dependencies == set()

        AtomicIntegerIncrementer.destroy()
        try:
            AtomicMessageQueue.destroy()
        except:
            pass

        grizzly.setup.statistics_url = None

        # problems initializing testdata
        def mocked_initialize_testdata(request_tasks: List[RequestTask]) -> Any:
            raise TemplateError('failed to initialize testdata')

        mocker.patch(
            'grizzly.locust.initialize_testdata',
            mocked_initialize_testdata,
        )

        with pytest.raises(AssertionError) as ae:
            setup_environment_listeners(behave, environment, tasks)
        assert 'error parsing request payload: ' in str(ae)
    finally:
        try:
            AtomicIntegerIncrementer.destroy()
            AtomicMessageQueue.destroy()
        except:
            pass


def test_print_scenario_summary(behave_fixture: BehaveFixture, capsys: CaptureFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    grizzly.add_scenario('test-1')

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    print(summary)
    assert '''Scenario
identifier   #  description
-----------|--|-------------|
cbda8191     1  test-1
-----------|--|-------------|
''' == summary
    capsys.readouterr()

    grizzly.add_scenario('test-2-test-2-test-2-test-2')
    grizzly.scenario.iterations = 4

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    print(summary)
    assert '''Scenario
identifier   #  description
-----------|--|-----------------------------|
cbda8191     1  test-1
b4959834     4  test-2-test-2-test-2-test-2
-----------|--|-----------------------------|
''' == summary
    capsys.readouterr()

    grizzly.add_scenario('#3')

    grizzly.scenario.iterations = 999

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    print(summary)

    assert '''Scenario
identifier     #  description
-----------|----|-----------------------------|
cbda8191       1  test-1
b4959834       4  test-2-test-2-test-2-test-2
83189503     999  #3
-----------|----|-----------------------------|
''' == summary
    capsys.readouterr()

    grizzly.add_scenario('foo bar hello world')

    grizzly.scenario.iterations = 99999

    print_scenario_summary(grizzly)

    summary = capsys.readouterr().out
    print(summary)

    assert '''Scenario
identifier       #  description
-----------|------|-----------------------------|
cbda8191         1  test-1
b4959834         4  test-2-test-2-test-2-test-2
83189503       999  #3
0b345183     99999  foo bar hello world
-----------|------|-----------------------------|
''' == summary
    capsys.readouterr()


def test_run_worker(behave_fixture: BehaveFixture, capsys: CaptureFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
    behave = behave_fixture.context

    def mock_on_node(master: bool, worker: bool) -> None:
        def mocked_on_worker(context: Context) -> bool:
            return worker

        mocker.patch(
            'grizzly.locust.on_worker',
            mocked_on_worker,
        )

        def mocked_on_master(context: Context) -> bool:
            return master

        mocker.patch(
            'grizzly.locust.on_master',
            mocked_on_master,
        )

    def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        pass

    def mocked_create_worker_runner(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        from socket import error
        raise error()

    mocker.patch(
        'grizzly.locust.Environment.create_worker_runner',
        mocked_create_worker_runner,
    )

    noop_zmq('grizzly.testdata.variables.messagequeue')

    for method in [
        'locust.runners.WorkerRunner.start_worker',
        'gevent.sleep',
        'grizzly.listeners._init_testdata_producer',
    ]:
        mocker.patch(
            method,
            noop,
        )

    def mocked_response(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> AsyncMessageResponse:
        return {
            'success': True,
            'worker': 'aaaa-bbbb-ccc',
        }

    mocker.patch(
        'grizzly.testdata.variables.messagequeue.zmq.Socket.recv_json',
        mocked_response,
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
    grizzly.add_scenario('test-non-mq')
    grizzly.scenario.user.class_name = 'RestApiUser'
    grizzly.scenario.context['host'] = 'https://test.example.org'
    grizzly.scenario.add_task(WaitTask(time=1.5))
    task = RequestTask(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    grizzly.scenario.add_task(task)

    assert run(behave) == 1
    assert 'failed to connect to the locust master' in capsys.readouterr().err

    assert messagequeue_process_spy.call_count == 0

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        grizzly.add_scenario('test-mq')
        grizzly.scenario.user.class_name = 'MessageQueueUser'
        grizzly.scenario.context['host'] = 'mq://mq.example.org?QueueManager=QM01&Channel=TEST.CONN'
        grizzly.scenario.add_task(RequestTask(RequestMethod.PUT, 'test-2', 'TEST.QUEUE'))

        with pytest.raises(AssertionError) as ae:
            run(behave)
        assert 'increase the number in step' in str(ae)

        grizzly.setup.user_count = 2

        assert run(behave) == 1
        assert 'failed to connect to the locust master' in capsys.readouterr().err

        assert messagequeue_process_spy.call_count == 1
        assert messagequeue_process_spy.call_args_list[0][0][1][0] == 'async-messaged'
        messagequeue_process_spy.reset_mock()

        # messagequeue-daemon should start on worker if a scenario has AtomicMessageQueue variable
        grizzly._scenarios.pop()  # remove test-mq scenario

        grizzly.state.variables.update({
            'AtomicMessageQueue.test': 'TEST.QUEUE | url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", expression="$.result.value", content_type="application/json"',
        })

        task = cast(RequestTask, grizzly.scenario.tasks.pop())

        task.endpoint = '/api/v1/{{ AtomicMessageQueue.test }}'

        grizzly.scenario.add_task(task)

        assert run(behave) == 1
        assert 'failed to connect to the locust master' in capsys.readouterr().err
        assert messagequeue_process_spy.call_count == 1
        assert messagequeue_process_spy.call_args_list[0][0][1][0] == 'async-messaged'

        try:
            AtomicMessageQueue.destroy()
        except:
            pass


def test_run_master(behave_fixture: BehaveFixture, capsys: CaptureFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
    behave = behave_fixture.context

    def mock_on_node(master: bool, worker: bool) -> None:
        def mocked_on_worker(context: Context) -> bool:
            return worker

        mocker.patch(
            'grizzly.locust.on_worker',
            mocked_on_worker,
        )

        def mocked_on_master(context: Context) -> bool:
            return master

        mocker.patch(
            'grizzly.locust.on_master',
            mocked_on_master,
        )

    def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        pass

    noop_zmq('grizzly.testdata.variables.messagequeue')

    for method in [
        'locust.runners.MasterRunner.start',
        'locust.runners.MasterRunner.client_listener',
        'gevent.sleep',
        'locust.rpc.zmqrpc.Server.__init__',
        'grizzly.listeners._init_testdata_producer',
    ]:
        mocker.patch(
            method,
            noop,
        )

    def mocked_response(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> AsyncMessageResponse:
        return {
            'success': True,
            'worker': 'aaaa-bbbb-ccc',
        }

    mocker.patch(
        'grizzly.testdata.variables.messagequeue.zmq.Socket.recv_json',
        mocked_response,
    )

    for printer in ['print_error_report', 'print_percentile_stats', 'print_stats', 'stats_printer', 'stats_history']:
        mocker.patch(
            f'grizzly.locust.{printer}',
            noop,
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
    assert 'cannot be both master and worker' in capsys.readouterr().err

    behave.config.userdata = {}

    grizzly = cast(GrizzlyContext, behave.grizzly)

    grizzly.setup.spawn_rate = None
    assert run(behave) == 254
    assert 'spawn rate is not set' in capsys.readouterr().err

    grizzly.setup.spawn_rate = 1

    assert run(behave) == 254
    assert 'step \'Given "user_count" users\' is not in the feature file' in capsys.readouterr().err

    grizzly.setup.user_count = 2
    grizzly.add_scenario('test')
    grizzly.scenario.user.class_name = 'RestApiUser'
    grizzly.scenario.context['host'] = 'https://test.example.org'
    grizzly.scenario.add_task(WaitTask(time=1.5))
    task = RequestTask(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    grizzly.scenario.add_task(task)
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
    assert 'there are more workers (3) than users (2), which is not supported' in str(ae)

    assert messagequeue_process_spy.call_count == 0

    # make sure messagequeue-daemon is started on master if variable AtomicMessageQueue is used
    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        task = cast(RequestTask, grizzly.scenario.tasks.pop())

        grizzly.state.variables.update({
            'AtomicMessageQueue.test': 'TEST.QUEUE | url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", expression="$.result.value", content_type="application/json"',
        })

        task.endpoint = '/api/v1/{{ AtomicMessageQueue.test }}'

        grizzly.scenario.add_task(task)

        with pytest.raises(AssertionError) as ae:
            run(behave)
        assert 'there are more workers (3) than users (2), which is not supported' in str(ae)

        try:
            AtomicMessageQueue.destroy()
        except:
            pass

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
