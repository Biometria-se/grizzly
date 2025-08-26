"""Core grizzly listeners, that hooks on grizzly specific logic to locust."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, cast
from urllib.parse import urlparse

from locust.stats import (
    RequestStats,
    StatsEntry,
    print_error_report,
    print_percentile_stats,
    print_stats,
)

from grizzly.testdata.communication import GrizzlyDependencies, TestdataConsumer, TestdataProducer
from grizzly.types import MessageDirection, RequestType, StrDict, TestdataType
from grizzly.types.behave import Status
from grizzly.types.locust import Environment, LocustRunner, MasterRunner, Message, WorkerRunner

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from grizzly.context import GrizzlyContext

P = ParamSpec('P')

logger = logging.getLogger(__name__)


def init(grizzly: GrizzlyContext, dependencies: GrizzlyDependencies, testdata: TestdataType | None = None) -> Callable[Concatenate[LocustRunner, P], None]:
    def init_wrapper(runner: LocustRunner, *_args: P.args, **_kwargs: P.kwargs) -> None:
        # acquire lock, that will be released when all users has spawned (on_spawning_complete)
        grizzly.state.spawning_complete.acquire()

        if not isinstance(runner, WorkerRunner):
            if testdata is not None:
                grizzly.state.producer = TestdataProducer(
                    runner=runner,
                    testdata=testdata,
                )
            else:
                logger.error('there is no test data!')

        if isinstance(runner, WorkerRunner):
            runner.register_message('locust_quit', locust_quit, concurrent=False)

        if not isinstance(runner, MasterRunner):
            for message_type, callback in grizzly.setup.locust.messages.get(MessageDirection.SERVER_CLIENT, {}).items():
                runner.register_message(message_type, callback, concurrent=True)

            runner.register_message('consume_testdata', TestdataConsumer.handle_response, concurrent=True)
            for dependency in dependencies:
                if not isinstance(dependency, type):
                    continue

                runner.register_message(dependency.__message_types__['response'], dependency.handle_response, concurrent=True)

        if not isinstance(runner, WorkerRunner):
            for message_type, callback in grizzly.setup.locust.messages.get(MessageDirection.CLIENT_SERVER, {}).items():
                runner.register_message(message_type, callback, concurrent=True)

            for dependency in dependencies:
                if not isinstance(dependency, type):
                    continue

                runner.register_message(dependency.__message_types__['request'], dependency.handle_request, concurrent=True)

    return cast('Callable[Concatenate[LocustRunner, P], None]', init_wrapper)


def init_statistics_listener(url: str) -> Callable[Concatenate[Environment, P], None]:
    def statistics_listener(environment: Environment, *_args: P.args, **_kwargs: P.kwargs) -> None:
        parsed = urlparse(url)

        if parsed.scheme in ('influxdb', 'influxdb2'):
            from .influxdb import InfluxDbListener  # noqa: PLC0415

            InfluxDbListener(
                environment=environment,
                url=url,
            )

    return cast('Callable[Concatenate[Environment, P], None]', statistics_listener)


def locust_test_start() -> Callable[Concatenate[Environment, P], None]:
    def locust_test_start_listener(environment: Environment, *_args: P.args, **_kwargs: P.kwargs) -> None:
        if isinstance(environment.runner, MasterRunner):
            num_connected_workers = len(environment.runner.clients.ready) + len(environment.runner.clients.running) + len(environment.runner.clients.spawning)

            logger.debug('connected workers: %d', num_connected_workers)

    return cast('Callable[Concatenate[Environment, P], None]', locust_test_start_listener)


def locust_quit(environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG001
    logger.debug('received locust_quit message from master, quitting')

    runner = environment.runner
    code: int = 1

    if isinstance(runner, WorkerRunner):
        runner.stop()
        runner._send_stats()
        runner.send_message('quit')
        runner.greenlet.kill(block=True)

        if environment.process_exit_code is not None:
            code = environment.process_exit_code
        elif len(runner.errors) > 0 or len(runner.exceptions) > 0:
            code = 3
        else:
            code = 0
    else:
        logger.error('received locust_quit message on non-worker node')

    raise SystemExit(code)


def spawning_complete(grizzly: GrizzlyContext) -> Callable[Concatenate[int, P], None]:
    def gspawning_complete(user_count: int, *_args: P.args, **_kwargs: P.kwargs) -> None:
        logger.debug('spawning of %d users completed', user_count)
        grizzly.state.spawning_complete.release()

    return gspawning_complete


def worker_report(client_id: str, data: StrDict) -> None:  # noqa: ARG001
    logger.debug('received worker_report from %s', client_id)


def validate_result(grizzly: GrizzlyContext) -> Callable[Concatenate[Environment, P], None]:
    def gvalidate_result(environment: Environment, *_args: P.args, **_kwargs: P.kwargs) -> None:
        # first, aggregate statistics per scenario
        scenario_stats: dict[str, RequestStats] = {}

        for scenario in grizzly.scenarios():
            request_stats = RequestStats()
            request_stats.total = StatsEntry(environment.stats, scenario.identifier, '', use_response_times_cache=False)
            scenario_stats[scenario.identifier] = request_stats

        for stats_entry in environment.stats.entries.values():
            prefix = stats_entry.name.split(' ', 1)[0]

            if prefix in scenario_stats:
                scenario_stats[prefix].total.extend(stats_entry)
                scenario_stats[prefix].entries[(stats_entry.name, stats_entry.method)] = stats_entry
            else:
                logger.error('"%s" does not match any scenario', prefix)

        # then validate against scenario rules
        for scenario in grizzly.scenarios():
            stats = scenario_stats[scenario.identifier]
            print_stats(stats, current=False)
            print_percentile_stats(stats)
            print_error_report(stats)

            if scenario.validation.fail_ratio is not None:
                expected = scenario.validation.fail_ratio
                actual = stats.total.fail_ratio
                if actual > expected:
                    error_message = f'failure ration {int(actual * 100)}% > {int(expected * 100)}%'
                    logger.error('scenario "%s" (%s) failed due to %s', scenario.name, prefix, error_message)
                    environment.stats.log_error(
                        RequestType.SCENARIO(),
                        scenario.locust_name,
                        RuntimeError(error_message),
                    )
                    environment.process_exit_code = 1

            if scenario.validation.avg_response_time is not None:
                expected = scenario.validation.avg_response_time
                actual = stats.total.avg_response_time
                if actual > expected:
                    error_message = f'average response time {int(actual)} ms > {int(expected)} ms'
                    logger.error('scenario %s failed due to %s', prefix, error_message)
                    environment.stats.log_error(
                        RequestType.SCENARIO(),
                        scenario.locust_name,
                        RuntimeError(error_message),
                    )
                    environment.process_exit_code = 1

            if scenario.validation.response_time_percentile is not None:
                percentile = scenario.validation.response_time_percentile.percentile
                expected = scenario.validation.response_time_percentile.response_time

                actual = stats.total.get_response_time_percentile(percentile)
                if actual > expected:
                    error_message = f'{int(percentile * 100)}%-tile response time {int(actual)} ms > {expected} ms'
                    logger.error('scenario %s failed due to %s', prefix, error_message)
                    environment.stats.log_error(
                        RequestType.SCENARIO(),
                        scenario.locust_name,
                        RuntimeError(error_message),
                    )
                    environment.process_exit_code = 1

            if environment.process_exit_code == 1 and hasattr(scenario, 'behave') and scenario.behave is not None:
                scenario.behave.set_status(Status.failed)

    return cast('Callable[Concatenate[Environment, P], None]', gvalidate_result)
