import logging
import subprocess

from typing import Optional, Callable, List, Tuple, cast
from os import environ, name as osname
from signal import SIGTERM
from socket import error as SocketError

import gevent

from behave.runner import Context
from locust.runners import WorkerRunner
from locust.user.users import User
from locust.env import Environment
from locust.stats import (
    print_error_report,
    print_percentile_stats,
    print_stats,
    stats_printer,
    stats_history,
)
from locust.log import setup_logging
from locust.util.timespan import parse_timespan
from locust import events
from jinja2.exceptions import TemplateError

from .listeners import init, init_statistics_listener, quitting, validate_result, spawning_complete, locust_test_start, locust_test_stop
from .testdata.utils import initialize_testdata
from .types import TestdataType
from .context import LocustContext, RequestContext

from .utils import create_task_class_type, create_user_class_type


unhandled_greenlet_exception = False


logger = logging.getLogger('behave.locust')


def greenlet_exception_logger(logger: logging.Logger, level: int=logging.CRITICAL) -> Callable[[gevent.Greenlet], None]:
    def exception_handler(greenlet: gevent.Greenlet) -> None:
        global unhandled_greenlet_exception
        logger.log(level, f'unhandled exception in greenlet: {greenlet}', exc_info=True)
        unhandled_greenlet_exception = True

    return exception_handler


def on_master(context: Context) -> bool:
    value: bool = 'master' in context.config.userdata and context.config.userdata['master'].lower() == 'true'
    if value:
        environ['LOCUST_IS_MASTER'] = str(value).lower()

    return value


def on_worker(context: Context) -> bool:
    value: bool = 'worker' in context.config.userdata and context.config.userdata['worker'].lower() == 'true'
    if value:
        environ['LOCUST_IS_WORKER'] = str(value).lower()

    return value


def on_local(context: Context) -> bool:
    value: bool = not on_master(context) and not on_worker(context)
    if value:
        environ['LOCUST_IS_LOCAL'] = str(value).lower()

    return value


def setup_locust_scenarios(context: LocustContext) -> Tuple[List[User], List[RequestContext], bool]:
    user_classes: List[User] = []
    request_tasks: List[RequestContext] = []

    scenarios = context.scenarios()

    assert len(scenarios) > 0, f'no scenarios in feature'

    start_messagequeue_daemon = False

    for scenario in scenarios:
        # Given a user of type "" load testing ""
        assert 'host' in scenario.context, f'variable "host" is not found in the context for {scenario.name}'
        assert len(scenario.tasks) > 0, f'no tasks has been added to {scenario.name}'

        user_class_type = create_user_class_type(scenario, context.setup.global_context)
        user_class_type.host = scenario.context['host']

        # fail early if there is a problem with creating an instance of the user class
        try:
            user_class_type()
        except TypeError:  # missing required environment argument, is OK
            pass

        if user_class_type.__name__.startswith('MessageQueueUser_') and not start_messagequeue_daemon:
            start_messagequeue_daemon = True

        scenario_type = create_task_class_type('TrafficIteratorTasks', scenario)
        scenario.name = scenario_type.__name__
        for task in scenario.tasks:
            scenario_type.add_scenario_task(task)
            if isinstance(task, RequestContext):
                request_tasks.append(task)

        setattr(user_class_type, 'tasks', [scenario_type])

        user_classes.append(user_class_type)

    return user_classes, request_tasks, start_messagequeue_daemon


def setup_resource_limits(context: Context) -> None:
    if osname != 'nt' and on_master(context):
        try:
            import resource
            minimum_open_file_limit = 10000
            current_open_file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)

            if current_open_file_limit < minimum_open_file_limit:
                resource.setrlimit(resource.RLIMIT_NOFILE, (minimum_open_file_limit, resource.RLIM_INFINITY))
        except (ValueError, OSError):
            logger.warning(
                (
                    f"system open file limit '{current_open_file_limit}' is below minimum setting '{minimum_open_file_limit}'. "
                    "it's not high enough for load testing, and the OS didn't allow locust to increase it by itself. "
                    "see https://github.com/locustio/locust/wiki/Installation#increasing-maximum-number-of-open-files-limit for more info."
                )
            )


def setup_environment_listeners(context: Context, environment: Environment, request_tasks: List[RequestContext]) -> None:
    # make sure we don't have any listeners
    environment.events.init._handlers = []
    environment.events.test_start._handlers = []
    environment.events.test_stop._handlers = []
    environment.events.spawning_complete._handlers = []
    environment.events.quitting._handlers = []

    context_locust = cast(LocustContext, context.locust)

    # add standard listeners
    testdata: Optional[TestdataType] = None
    if not on_worker(context):
        # initialize testdata
        try:
            testdata = initialize_testdata(request_tasks)
            logger.debug(f'{testdata=}')
            for scenario_testdata in testdata.values():
                for variable, value in scenario_testdata.items():
                    assert value is not None, f'variable {variable} has not been initialized'
        except TemplateError as e:
            logger.error(e, exc_info=True)
            assert False, f'error parsing request payload: {e}'

        validate_results = False

        # only add the listener if there are any rules for validating results
        for scenario in context_locust.scenarios():
            validate_results = scenario.should_validate()
            if validate_results:
                break

        logger.debug(f'{validate_results=}')

        if validate_results:
            environment.events.quitting.add_listener(validate_result(context_locust))

        environment.events.quitting.add_listener(quitting)

    environment.events.init.add_listener(init(testdata))
    environment.events.test_start.add_listener(locust_test_start(context_locust))
    environment.events.test_stop.add_listener(locust_test_stop)
    environment.events.spawning_complete.add_listener(spawning_complete(context_locust))

    # And save statistics to "..."
    if context_locust.setup.statistics_url is not None:
        environment.events.init.add_listener(init_statistics_listener(context_locust.setup.statistics_url))


def run(context: Context) -> int:
    context_locust = cast(LocustContext, context.locust)

    log_level = 'DEBUG' if context.config.verbose else context_locust.setup.log_level

    # And locust log level is
    setup_logging(log_level, None)

    # make sure the user hasn't screwed up
    if on_master(context) and on_worker(context):
        logger.error(f'seems to be a problem with "behave" arguments, cannot be both master and worker')
        return 254

    if context_locust.setup.spawn_rate is None:
        logger.error(f'spawn rate is not set')
        return 254

    if context_locust.setup.user_count < 1:
        logger.error(f"step 'Given \"user_count\" users' is not in the feature file")
        return 254

    greenlet_exception_handler = greenlet_exception_logger(logger)

    user_classes: List[User] = []
    request_tasks: List[RequestContext] = []
    messagequeue_daemon_process: Optional[subprocess.Popen] = None

    user_classes, request_tasks, start_messagequeue_daemon = setup_locust_scenarios(context_locust)

    assert len(user_classes) > 0, 'no users specified in feature'
    assert len(request_tasks) > 0, 'no requests specified for users'
    assert len(user_classes) <= context_locust.setup.user_count, f"increase the number in step 'Given \"{context_locust.setup.user_count}\" users' to at least {len(user_classes)}"

    try:
        if not on_master(context) and start_messagequeue_daemon:
            logger.info('starting messagequeue-daemon for MessageQueueUser')
            messagequeue_daemon_process = subprocess.Popen(
                ['messagequeue-daemon'],
                env=environ.copy(),
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            gevent.sleep(2)

        setup_resource_limits(context)

        environment = Environment(
            user_classes=user_classes,
            shape_class=None,
            events=events,
        )

        setup_environment_listeners(context, environment, request_tasks)

        if on_master(context):
            host = '0.0.0.0'
            port = int(context.config.userdata.get('master-port', 5557))
            runner = environment.create_master_runner(
                master_bind_host=host,
                master_bind_port=port,
            )
            logger.debug(f'started master runner: {host}:{port}')
        elif on_worker(context):
            try:
                host = context.config.userdata.get('master-host', 'master')
                port = context.config.userdata.get('master-port', 5557)
                logger.debug(f'trying to connect to locust master: {host}:{port}')
                runner = environment.create_worker_runner(
                    host,
                    port,
                )
                logger.debug(f'connected to locust master: {host}:{port}')
            except SocketError as e:
                logger.error('failed to connect to the locust master: %s', e)
                return 1
        else:
            runner = environment.create_local_runner()

        main_greenlet = runner.greenlet

        # And run for maximum
        run_time: Optional[int] = None
        if context_locust.setup.timespan is not None and not on_worker(context):
            try:
                run_time = parse_timespan(context_locust.setup.timespan)
            except ValueError:
                logger.error(f'invalid timespan "{context_locust.setup.timespan}" expected: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.')
                return 1

        stats_printer_greenlet: Optional[gevent.Greenlet] = None

        environment.events.init.fire(environment=environment, runner=runner, web_ui=None)

        class LocustOption:
            headless: bool
            num_users: int
            spawn_rate: int

        setattr(environment, 'parsed_options', LocustOption())
        setattr(environment.parsed_options, 'headless', True)
        setattr(environment.parsed_options, 'num_users', context_locust.setup.user_count)
        setattr(environment.parsed_options, 'spawn_rate', context_locust.setup.spawn_rate)

        if on_master(context):
            expected_workers = int(context.config.userdata.get('expected-workers', 1))
            if context_locust.setup.user_count is not None:
                assert expected_workers <= context_locust.setup.user_count, (
                    f'there are more workers ({expected_workers}) than users ({context_locust.setup.user_count}), which is not supported'
                )

            while len(environment.runner.clients.ready) < expected_workers:
                logger.debug(
                    f'waiting for workers to be ready, {len(environment.runner.clients.ready)} of {expected_workers}'
                )
                gevent.sleep(1)

            logger.info(
                f'all {expected_workers} workers have connected and are ready'
            )

        if not on_worker(context):
            environment.runner.start(context_locust.setup.user_count, context_locust.setup.spawn_rate)

            stats_printer_greenlet = gevent.spawn(stats_printer(environment.stats))
            stats_printer_greenlet.link_exception(greenlet_exception_handler)

        def spawn_run_time_limit_greenlet() -> None:
            def timelimit_stop() -> None:
                logger.info('time limit reached. stopping locust.')
                runner.quit()

            gevent.spawn_later(run_time, timelimit_stop).link_exception(greenlet_exception_handler)

        if run_time is not None:
            logger.info(f'run time limit set to {run_time} seconds')
            spawn_run_time_limit_greenlet()

        gevent.spawn(stats_history, environment.runner)

        watch_active_users_greenlet: Optional[gevent.Greenlet] = None

        def watch_active_users() -> None:
            while runner.user_count > 0:
                logger.debug(f'{runner.user_count=}')
                gevent.sleep(1.0)

            logger.info(f'{runner.user_count=}, stopping runner')
            gevent.sleep(3.0)

        # start watching for user_count after 5 second
        while not context_locust.state.spawning_complete:
            gevent.sleep(1.0)

        logger.info('all users spawn, start watching user count')

        watch_active_users_greenlet = gevent.spawn(watch_active_users)
        watch_active_users_greenlet.link_exception(greenlet_exception_handler)

        # shutdown when user_count reaches 0
        main_greenlet = watch_active_users_greenlet

        def shutdown() -> int:
            logger.info('running teardowns...')

            environment.events.quitting.fire(environment=environment, reverse=True)

            if unhandled_greenlet_exception:
                code = 2
            elif environment.process_exit_code is not None:
                code = environment.process_exit_code
            elif len(runner.errors) > 0 or len(runner.exceptions) > 0:
                code = 3
            else:
                code = 0

            if stats_printer_greenlet is not None:
                stats_printer_greenlet.kill(block=False)

            if watch_active_users_greenlet is not None:
                watch_active_users_greenlet.kill(block=False)

            logger.info('cleaning up runner...')
            if runner is not None:
                runner.quit()

            if not isinstance(runner, WorkerRunner):
                print_stats(runner.stats, current=False)
                print_percentile_stats(runner.stats)
                print_error_report(runner.stats)


            return code

        def sig_term_handler() -> None:
            logger.info('got SIGTERM signal')
            shutdown()

        gevent.signal_handler(SIGTERM, sig_term_handler)

        logger.info('starting locust via behave')
        try:
            main_greenlet.join()
        except KeyboardInterrupt as e:
            raise e
        finally:
            return shutdown()
    finally:
        if messagequeue_daemon_process is not None:
            logger.info('stopping messagequeue-daemon')
            messagequeue_daemon_process.terminate()
            if context.config.verbose:
                stdout, _ = messagequeue_daemon_process.communicate()
                logger.debug(stdout.decode())
            logger.debug(f'{messagequeue_daemon_process.returncode=}')
