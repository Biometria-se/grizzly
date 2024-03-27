"""Core grizzly listeners, that hooks on grizzly specific logic to locust."""
from __future__ import annotations

import logging
from os import environ
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, cast
from urllib.parse import urlparse

import gevent
from locust.stats import (
    RequestStats,
    StatsEntry,
    print_error_report,
    print_percentile_stats,
    print_stats,
)
from typing_extensions import Concatenate, ParamSpec

from grizzly.testdata.communication import TestdataProducer
from grizzly.types import MessageDirection, RequestType, TestdataType
from grizzly.types.behave import Status
from grizzly.types.locust import Environment, LocustRunner, MasterRunner, Message, WorkerRunner

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

P = ParamSpec('P')

producer: Optional[TestdataProducer] = None

producer_greenlet: Optional[gevent.Greenlet] = None

logger = logging.getLogger(__name__)


def _init_testdata_producer(grizzly: GrizzlyContext, port: str, testdata: TestdataType) -> Callable[[], None]:
    def gtestdata_producer() -> None:
        global producer  # noqa: PLW0603
        producer_address = f'tcp://0.0.0.0:{port}'
        producer = TestdataProducer(
            grizzly=grizzly,
            address=producer_address,
            testdata=testdata,
        )
        producer.run()

    return gtestdata_producer


def init(grizzly: GrizzlyContext, testdata: Optional[TestdataType] = None) -> Callable[Concatenate[LocustRunner, P], None]:
    def ginit(runner: LocustRunner, **_kwargs: P.kwargs) -> None:
        producer_port = environ.get('TESTDATA_PRODUCER_PORT', '5555')
        if not isinstance(runner, MasterRunner):
            producer_address = runner.master_host if isinstance(runner, WorkerRunner) else '127.0.0.1'

            producer_address = f'tcp://{producer_address}:{producer_port}'
            logger.debug('producer_address=%s', producer_address)
            environ['TESTDATA_PRODUCER_ADDRESS'] = producer_address

        if not isinstance(runner, WorkerRunner):
            if testdata is not None:
                global producer_greenlet  # noqa: PLW0603
                producer_greenlet = gevent.spawn(
                    _init_testdata_producer(
                        grizzly,
                        producer_port,
                        testdata,
                    ),
                )
            else:
                logger.error('there is no test data!')
        else:
            logger.debug('registered message "grizzly_worker_quit"')
            runner.register_message('grizzly_worker_quit', grizzly_worker_quit)

        if not isinstance(runner, MasterRunner):
            for message_type, callback in grizzly.setup.locust.messages.get(MessageDirection.SERVER_CLIENT, {}).items():
                runner.register_message(message_type, callback)

        if not isinstance(runner, WorkerRunner):
            for message_type, callback in grizzly.setup.locust.messages.get(MessageDirection.CLIENT_SERVER, {}).items():
                runner.register_message(message_type, callback)

    return cast(Callable[Concatenate[LocustRunner, P], None], ginit)


def init_statistics_listener(url: str) -> Callable[Concatenate[Environment, P], None]:
    def gstatistics_listener(environment: Environment, *_args: P.args, **_kwargs: P.kwargs) -> None:
        parsed = urlparse(url)

        if parsed.scheme == 'influxdb':
            from .influxdb import InfluxDbListener
            InfluxDbListener(
                environment=environment,
                url=url,
            )
        elif parsed.scheme == 'insights':
            from .appinsights import ApplicationInsightsListener
            ApplicationInsightsListener(
                environment=environment,
                url=url,
            )

    return cast(Callable[Concatenate[Environment, P], None], gstatistics_listener)


def locust_test_start(grizzly: GrizzlyContext) -> Callable[Concatenate[Environment, P], None]:
    def gtest_start(environment: Environment, **_kwargs: P.kwargs) -> None:
        if isinstance(environment.runner, MasterRunner):
            num_connected_workers = (
                len(environment.runner.clients.ready)
                + len(environment.runner.clients.running)
                + len(environment.runner.clients.spawning)
            )

            logger.debug('connected workers: %d', num_connected_workers)

            total_iterations = sum([scenario.iterations for scenario in grizzly.scenarios()])
            if total_iterations < num_connected_workers:
                logger.error('number of iterations is lower than number of workers, %d < %d', total_iterations, num_connected_workers)

    return cast(Callable[Concatenate[Environment, P], None], gtest_start)


def locust_test_stop(**_kwargs: Any) -> None:
    if producer is not None:
        producer.on_test_stop()


def spawning_complete(grizzly: GrizzlyContext) -> Callable[..., None]:
    def gspawning_complete(**_kwargs: Any) -> None:
        logger.debug('spawning complete!')
        grizzly.state.spawning_complete = True

    return gspawning_complete


def quitting(**_kwargs: Any) -> None:
    logger.debug('locust quitting')
    global producer_greenlet, producer  # noqa: PLW0603
    if producer is not None:
        logger.debug('stopping producer')
        producer.stop()
        producer = None

    if producer_greenlet is not None:
        producer_greenlet.kill(block=True)
        producer_greenlet = None


def grizzly_worker_quit(environment: Environment, msg: Message, **_kwargs: Any) -> None:
    logger.debug('received message grizzly_worker_quit: msg=%r', msg)
    runner = environment.runner
    code: Optional[int] = None

    if isinstance(runner, WorkerRunner):
        runner.stop()
        runner._send_stats()
        runner.client.send(Message('client_stopped', None, runner.client_id))

        runner.greenlet.kill(block=True)

        if environment.process_exit_code is not None:
            code = environment.process_exit_code
        elif len(runner.errors) > 0 or len(runner.exceptions) > 0:
            code = 3
        else:
            code = 0
    else:
        logger.error('received grizzly_worker_quit message on a non WorkerRunner?!')

    if code is None:
        code = 1

    raise SystemExit(code)


def validate_result(grizzly: GrizzlyContext) -> Callable[Concatenate[Environment, P], None]:
    def gvalidate_result(environment: Environment, **_kwargs: P.kwargs) -> None:
        # first, aggregate statistics per scenario
        scenario_stats: Dict[str, RequestStats] = {}

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
                    error_message = f'failure ration {int(actual*100)}% > {int(expected*100)}%'
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

    return cast(Callable[Concatenate[Environment, P], None], gvalidate_result)
