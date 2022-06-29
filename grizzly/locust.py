import sys
import logging
import subprocess

from typing import NoReturn, Optional, Callable, List, Tuple, Set, Dict, Type, cast
from os import environ
from signal import SIGTERM
from socket import error as SocketError
from datetime import datetime
from math import ceil
from operator import itemgetter

import gevent

from behave.runner import Context
from behave.model import Status
from locust.runners import MasterRunner, WorkerRunner, Runner
from locust.env import Environment
from locust.stats import (
    RequestStats,
    CONSOLE_STATS_INTERVAL_SEC,
    STATS_NAME_WIDTH,
    STATS_TYPE_WIDTH,
    PERCENTILES_TO_REPORT,
    print_error_report,
    print_percentile_stats,
    print_stats,
    stats_history,
    get_readable_percentiles,
)
from locust.log import setup_logging
from locust.util.timespan import parse_timespan
from locust import events
from jinja2.exceptions import TemplateError

from .listeners import init, init_statistics_listener, quitting, validate_result, spawning_complete, locust_test_start, locust_test_stop
from .testdata.utils import initialize_testdata
from .types import RequestType, TestdataType
from .context import GrizzlyContext
from .tasks import GrizzlyTask
from .users.base import GrizzlyUser

from .utils import create_scenario_class_type, create_user_class_type

__all__: List[str] = []


unhandled_greenlet_exception = False


logger = logging.getLogger('grizzly.locust')

stats_logger = logging.getLogger('locust.stats_logger')


def greenlet_exception_logger(logger: logging.Logger, level: int = logging.CRITICAL) -> Callable[[gevent.Greenlet], None]:
    def exception_handler(greenlet: gevent.Greenlet) -> None:
        global unhandled_greenlet_exception
        logger.log(level, f'unhandled exception in greenlet: {greenlet}: {greenlet.value}', exc_info=True)
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


def setup_locust_scenarios(grizzly: GrizzlyContext) -> Tuple[List[Type[GrizzlyUser]], List[GrizzlyTask], Set[str]]:
    user_classes: List[Type[GrizzlyUser]] = []
    tasks: List[GrizzlyTask] = []

    scenarios = grizzly.scenarios()

    assert len(scenarios) > 0, 'no scenarios in feature'

    external_dependencies: Set[str] = set()
    dummy_environment = Environment()
    distribution: Dict[str, int] = {}

    total_weight = sum([scenario.user.weight for scenario in scenarios])
    for scenario in scenarios:
        user_count = ceil(grizzly.setup.user_count * (scenario.user.weight / total_weight))
        distribution[scenario.name] = user_count

    total_user_count = sum([user_count for user_count in distribution.values()])
    user_overflow = total_user_count - grizzly.setup.user_count

    assert len(distribution.keys()) <= grizzly.setup.user_count, f"increase the number in step 'Given \"{grizzly.setup.user_count}\" users' to at least {len(distribution.keys())}"

    if user_overflow < 0:
        logger.warning(f'there should be {grizzly.setup.user_count} users, but there will only be {total_user_count} users spawned')

    while user_overflow > 0:
        for scenario_name in dict(sorted(distribution.items(), key=lambda d: d[1], reverse=True)).keys():
            if distribution[scenario_name] <= 1:
                continue

            distribution[scenario_name] -= 1
            user_overflow -= 1

            if user_overflow < 1:
                break

    for scenario in scenarios:
        # Given a user of type "" load testing ""
        assert 'host' in scenario.context, f'variable "host" is not found in the context for {scenario.name}'
        assert len(scenario.tasks) > 0, f'no tasks has been added to {scenario.name}'

        fixed_count = distribution.get(scenario.name, None)
        user_class_type = create_user_class_type(scenario, grizzly.setup.global_context, fixed_count=fixed_count)
        user_class_type.host = scenario.context['host']

        # fail early if there is a problem with creating an instance of the user class
        user_class_type(dummy_environment)

        external_dependencies.update(user_class_type.__dependencies__)

        # @TODO: how do we specify other type of grizzly.scenarios?
        scenario_type = create_scenario_class_type('IteratorScenario', scenario)
        scenario.name = scenario_type.__name__
        for task in scenario.tasks:
            scenario_type.populate(task)
            tasks.append(task)

            dependencies = getattr(task, '__dependencies__', None)
            if dependencies is not None:
                external_dependencies.update(dependencies)

        logger.debug(
            f'{user_class_type.__name__}/{scenario_type.__name__}: tasks={len(scenario.tasks)}, weight={user_class_type.weight}, fixed_count={user_class_type.fixed_count}'
        )

        setattr(user_class_type, 'tasks', [scenario_type])

        user_classes.append(user_class_type)

    return user_classes, tasks, external_dependencies


def setup_resource_limits(context: Context) -> None:
    if sys.platform != 'win32' and on_master(context):
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


def setup_environment_listeners(context: Context, tasks: List[GrizzlyTask]) -> Set[str]:
    grizzly = cast(GrizzlyContext, context.grizzly)

    environment = grizzly.state.locust.environment

    # make sure we don't have any listeners
    environment.events.init._handlers = []
    environment.events.test_start._handlers = []
    environment.events.test_stop._handlers = []
    environment.events.spawning_complete._handlers = []
    environment.events.quitting._handlers = []

    # add standard listeners
    testdata: Optional[TestdataType] = None
    external_dependencies: Set[str] = set()

    # initialize testdata
    try:
        testdata, external_dependencies = initialize_testdata(grizzly, tasks)

        for scenario_testdata in testdata.values():
            for variable, value in scenario_testdata.items():
                assert value is not None, f'variable {variable} has not been initialized'
    except TemplateError as e:
        logger.error(e, exc_info=True)
        assert False, f'error parsing request payload: {e}'

    if not on_worker(context):
        validate_results = False

        # only add the listener if there are any rules for validating results
        for scenario in grizzly.scenarios():
            validate_results = scenario.should_validate()
            if validate_results:
                break

        logger.debug(f'{validate_results=}')

        if validate_results:
            environment.events.quitting.add_listener(validate_result(grizzly))

        environment.events.quitting.add_listener(quitting)

    environment.events.init.add_listener(init(grizzly, testdata))
    environment.events.test_start.add_listener(locust_test_start(grizzly))
    environment.events.test_stop.add_listener(locust_test_stop)

    if not on_master(context):
        environment.events.spawning_complete.add_listener(spawning_complete(grizzly))

    # And save statistics to "..."
    if grizzly.setup.statistics_url is not None:
        environment.events.init.add_listener(init_statistics_listener(grizzly.setup.statistics_url))

    return external_dependencies


def print_scenario_summary(grizzly: GrizzlyContext) -> None:
    def create_separator(max_length_iterations: int, max_length_status: int, max_length_description: int) -> str:
        separator: List[str] = []
        separator.append('-' * 5)
        separator.append('-|-')
        separator.append('-' * max_length_iterations)
        separator.append('|-')
        separator.append('-' * max_length_status)
        separator.append('-|-')
        separator.append('-' * max_length_description)
        separator.append('-|')

        return ''.join(separator)

    rows: List[str] = []
    max_length_description = len('description')
    max_length_iterations = len('iter')
    max_length_status = len('status')

    stats = grizzly.state.locust.environment.stats

    for scenario in grizzly.scenarios():
        stat = stats.get(scenario.locust_name, RequestType.SCENARIO())
        max_length_description = max(len(scenario.description or 'unknown'), max_length_description)
        max_length_iterations = max(len(f'{stat.num_requests}/{scenario.iterations or 0}'), max_length_iterations)
        max_length_status = max(len(Status.undefined.name) if stat.num_requests < 1 else len(Status.passed.name), max_length_status)

    for scenario in grizzly.scenarios():
        stat = stats.get(scenario.locust_name, RequestType.SCENARIO())
        if stat.num_requests > 0:
            if stat.num_failures == 0 and stat.num_requests == scenario.iterations:
                status = Status.passed
            else:
                status = Status.failed
        else:
            status = Status.undefined

        description = scenario.description or 'unknown'
        row = '{:5}   {:>{}}  {:{}}   {}'.format(
            scenario.identifier,
            f'{stat.num_requests}/{scenario.iterations}',
            max_length_iterations,
            status.name,
            max_length_status,
            description,
        )
        rows.append(row)

    print('Scenario')
    print('{:5}   {:>{}}  {:{}}   {}'.format('ident', 'iter', max_length_iterations, 'status', max_length_status, 'description'))
    separator = create_separator(max_length_iterations, max_length_status, max_length_description)
    print(separator)
    for row in rows:
        print(row)
    print(separator)


def run(context: Context) -> int:
    def shutdown_external_processes(processes: Dict[str, subprocess.Popen]) -> None:
        if len(processes) > 0:
            for dependency, process in processes.items():
                logger.info(f'stopping {dependency}')
                process.terminate()
                process.wait()
                logger.debug(f'{process.returncode=}')

            processes.clear()

    grizzly = cast(GrizzlyContext, context.grizzly)

    log_level = 'DEBUG' if context.config.verbose else grizzly.setup.log_level

    # And locust log level is
    setup_logging(log_level, None)

    # make sure the user hasn't screwed up
    if on_master(context) and on_worker(context):
        logger.error('seems to be a problem with "behave" arguments, cannot be both master and worker')
        return 254

    if grizzly.setup.spawn_rate is None:
        logger.error('spawn rate is not set')
        return 254

    if grizzly.setup.user_count < 1:
        logger.error("step 'Given \"user_count\" users' is not in the feature file")
        return 254

    greenlet_exception_handler = greenlet_exception_logger(logger)

    external_processes: Dict[str, subprocess.Popen] = {}

    user_classes, tasks, external_dependencies = setup_locust_scenarios(grizzly)

    assert len(user_classes) > 0, 'no users specified in feature'
    assert len(tasks) > 0, 'no tasks specified in feature'

    try:
        setup_resource_limits(context)

        environment = Environment(
            user_classes=user_classes,
            shape_class=None,
            events=events,
            stop_timeout=300,  # only wait at most?
        )

        runner: Runner

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

        grizzly.state.locust = runner

        variable_dependencies = setup_environment_listeners(context, tasks)
        external_dependencies.update(variable_dependencies)

        if not on_master(context) and len(external_dependencies) > 0:
            env = environ.copy()
            if grizzly.state.verbose:
                env['GRIZZLY_EXTRAS_LOGLEVEL'] = 'DEBUG'

            for external_dependency in external_dependencies:
                logger.info(f'starting {external_dependency}')
                external_processes.update({external_dependency: subprocess.Popen(
                    [external_dependency],
                    env=env,
                    shell=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )})
                gevent.sleep(2)

        main_greenlet = runner.greenlet

        # And run for maximum
        run_time: Optional[int] = None
        if grizzly.setup.timespan is not None and not on_worker(context):
            try:
                run_time = parse_timespan(grizzly.setup.timespan)
            except ValueError:
                logger.error(f'invalid timespan "{grizzly.setup.timespan}" expected: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.')
                return 1

        stats_printer_greenlet: Optional[gevent.Greenlet] = None

        environment.events.init.fire(environment=environment, runner=runner, web_ui=None)

        class LocustOption:
            headless: bool
            num_users: int
            spawn_rate: float
            tags: List[str]
            exclude_tags: List[str]
            enable_rebalancing: bool

        setattr(environment, 'parsed_options', LocustOption())
        setattr(environment.parsed_options, 'headless', True)
        setattr(environment.parsed_options, 'num_users', grizzly.setup.user_count)
        setattr(environment.parsed_options, 'spawn_rate', grizzly.setup.spawn_rate)
        setattr(environment.parsed_options, 'tags', [])
        setattr(environment.parsed_options, 'exclude_tags', [])
        setattr(environment.parsed_options, 'enable_rebalancing', False)

        if isinstance(runner, MasterRunner):
            expected_workers = int(context.config.userdata.get('expected-workers', 1))
            if grizzly.setup.user_count is not None:
                assert expected_workers <= grizzly.setup.user_count, (
                    f'there are more workers ({expected_workers}) than users ({grizzly.setup.user_count}), which is not supported'
                )

            while len(runner.clients.ready) < expected_workers:
                logger.debug(
                    f'waiting for workers to be ready, {len(runner.clients.ready)} of {expected_workers}'
                )
                gevent.sleep(1)

            logger.info(
                f'all {expected_workers} workers have connected and are ready'
            )

        if not isinstance(runner, WorkerRunner):
            logger.info('starting locust via grizzly')
            runner.start(grizzly.setup.user_count, grizzly.setup.spawn_rate)

            stats_printer_greenlet = gevent.spawn(grizzly_stats_printer(environment.stats))
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

        if not isinstance(runner, WorkerRunner):
            watch_running_users_greenlet: Optional[gevent.Greenlet] = None

            def watch_running_users() -> None:
                count = 0
                while runner.user_count > 0:
                    gevent.sleep(1.0)
                    count += 1
                    if count % 10 == 0:
                        logger.debug(f'{runner.user_count=}')
                        count = 0

                logger.info(f'{runner.user_count=}, quit {runner.__class__.__name__}')
                if isinstance(runner, MasterRunner):
                    runner.send_message('grizzly_worker_quit', None)

                    runner.stop(send_stop_to_client=False)

                    # wait for all clients to quit
                    while len(runner.clients.all) > 0:
                        gevent.sleep(0.5)

                    runner.greenlet.kill(block=True)
                else:
                    runner.quit()

                if stats_printer_greenlet is not None:
                    stats_printer_greenlet.kill(block=False)

                if watch_running_users_greenlet is not None:
                    watch_running_users_greenlet.kill(block=False)

                grizzly_print_stats(runner.stats, current=False)
                grizzly_print_percentile_stats(runner.stats)
                print_error_report(runner.stats)
                print_scenario_summary(grizzly)

                # show start and stop date time
                stopped = datetime.now()

                print('')
                print(f'Started: {context.started}')
                print(f'Stopped: {stopped}')

            def spawning_complete() -> bool:
                if isinstance(runner, MasterRunner):
                    return runner.spawning_completed
                else:
                    return grizzly.state.spawning_complete

            while not spawning_complete():
                logger.debug('spawning not completed...')
                gevent.sleep(1.0)

            logger.info('all users spawn, start watching user count')

            watch_running_users_greenlet = gevent.spawn(watch_running_users)
            watch_running_users_greenlet.link_exception(greenlet_exception_handler)

            # stop when user_count reaches 0
            main_greenlet = watch_running_users_greenlet

        def sig_term_handler() -> None:
            logger.info('got SIGTERM signal')
            runner.quit()

        gevent.signal_handler(SIGTERM, sig_term_handler)

        try:
            main_greenlet.join()
            logger.debug('main greenlet finished')
        except KeyboardInterrupt as e:
            raise e
        finally:
            if unhandled_greenlet_exception:
                code = 2
            elif environment.process_exit_code is not None:
                code = environment.process_exit_code
            elif len(runner.errors) > 0 or len(runner.exceptions) > 0:
                code = 3
            else:
                code = 0

            return code
    finally:
        shutdown_external_processes(external_processes)


def _grizzly_sort_stats(stats: RequestStats) -> List[Tuple[str, str, int]]:
    locust_keys: List[Tuple[str, str]] = sorted(stats.entries.keys())

    previous_ident: Optional[str] = None
    scenario_keys: List[Tuple[str, str]] = []
    scenario_sorted_keys: List[Tuple[str, str, int]] = []
    for index, key in enumerate(locust_keys):
        ident, _ = key[0].split(' ', 1)
        is_last = index == len(locust_keys) - 1
        if (previous_ident is not None and previous_ident != ident) or is_last:
            if is_last:
                scenario_keys.append(key[:2])

            scenario_sorted_keys += sorted([
                (name, method, RequestType.get_method_weight(method), ) for name, method in scenario_keys
            ], key=itemgetter(2, 0))
            scenario_keys.clear()

        previous_ident = ident
        scenario_keys.append(key[:2])

    return scenario_sorted_keys


def grizzly_stats_printer(stats: RequestStats) -> Callable[[], NoReturn]:
    def _grizzly_stats_printer() -> NoReturn:
        while True:
            grizzly_print_stats(stats)
            gevent.sleep(CONSOLE_STATS_INTERVAL_SEC)

    return _grizzly_stats_printer


def grizzly_print_stats(stats: RequestStats, current: bool = True, grizzly_style: bool = True) -> None:
    if not grizzly_style:
        print_stats(stats, current=current)
        return

    name_column_width = (STATS_NAME_WIDTH - STATS_TYPE_WIDTH) + 4  # saved characters by compacting other columns
    stats_logger.info(
        ("%-" + str(STATS_TYPE_WIDTH) + "s %-" + str(name_column_width) + "s %7s %12s |%7s %7s %7s%7s | %7s %11s")
        % ("Type", "Name", "# reqs", "# fails", "Avg", "Min", "Max", "Med", "req/s", "failures/s")
    )
    separator = f'{"-" * STATS_TYPE_WIDTH}|{"-" * (name_column_width)}|{"-" * 7}|{"-" * 13}|{"-" * 7}|{"-" * 7}|{"-" * 7}|{"-" * 7}|{"-" * 8}|{"-" * 11}'
    stats_logger.info(separator)

    keys = _grizzly_sort_stats(stats)

    for key in keys:
        r = stats.entries[key[:2]]
        stats_logger.info(r.to_string(current=current))

    stats_logger.info(separator)
    stats_logger.info(stats.total.to_string(current=current))
    stats_logger.info('')


def grizzly_print_percentile_stats(stats: RequestStats, grizzly_style: bool = True) -> None:
    if not grizzly_style:
        print_percentile_stats(stats)
        return

    stats_logger.info('Response time percentiles (approximated)')
    headers = ('Type', 'Name') + tuple(get_readable_percentiles(PERCENTILES_TO_REPORT)) + ('# reqs',)
    stats_logger.info(
        (
            f'%-{str(STATS_TYPE_WIDTH)}s %-{str(STATS_NAME_WIDTH)}s %8s '
            f'{" ".join(["%6s"] * len(PERCENTILES_TO_REPORT))}'
        )
        % headers
    )
    separator = (
        f'{"-" * STATS_TYPE_WIDTH}|{"-" * STATS_NAME_WIDTH}|{"-" * 8}|{("-" * 6 + "|") * len(PERCENTILES_TO_REPORT)}'
    )[:-1]
    stats_logger.info(separator)

    keys = _grizzly_sort_stats(stats)

    for key in keys:
        r = stats.entries[key[:2]]
        if r.response_times:
            stats_logger.info(r.percentile())
    stats_logger.info(separator)

    if stats.total.response_times:
        stats_logger.info(stats.total.percentile())
    stats_logger.info('')
