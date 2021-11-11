import logging

from typing import Callable, Dict, Any, Tuple, Optional
from os import environ
from urllib.parse import urlparse
from mypy_extensions import KwArg, Arg, VarArg

import gevent

from locust.env import Environment
from locust.exception import CatchResponseError
from locust.runners import MasterRunner, LocalRunner, WorkerRunner
from locust.runners import Runner
from locust.stats import RequestStats, StatsEntry
from locust.stats import (
    print_error_report,
    print_percentile_stats,
    print_stats,
)

from ..context import GrizzlyContext
from ..types import TestdataType
from ..testdata.communication import TestdataProducer

producer: Optional[TestdataProducer] = None

producer_greenlet: Optional[gevent.Greenlet] = None

logger = logging.getLogger(__name__)

def _init_testdata_producer(port: str, testdata: TestdataType, environment: Environment) -> Callable[[], None]:
    # pylint: disable=global-statement
    def wrapper() -> None:
        global producer
        producer_address = f'tcp://0.0.0.0:{port}'
        producer = TestdataProducer(address=producer_address, testdata=testdata, environment=environment)
        producer.run()

    return wrapper

def init(testdata: Optional[TestdataType] = None) -> Callable[[Arg(Runner, 'runner'), KwArg(Dict[str, Any])], None]:
    def wrapper(runner: Runner, **_kwargs: Dict[str, Any]) -> None:
        producer_port = environ.get('TESTDATA_PRODUCER_PORT', '5555')
        if not isinstance(runner, MasterRunner):
            if isinstance(runner, LocalRunner):
                producer_address = '127.0.0.1'
            else:
                producer_address = runner.master_host

            producer_address = f'tcp://{producer_address}:{producer_port}'
            logger.debug(f'{producer_address=}')
            environ['TESTDATA_PRODUCER_ADDRESS'] = producer_address

        if not isinstance(runner, WorkerRunner):
            if testdata is not None:
                global producer_greenlet
                producer_greenlet = gevent.spawn(_init_testdata_producer(producer_port, testdata, runner.environment))
            else:
                logger.error(f'There is no test data!')

    return wrapper


def init_statistics_listener(url: str) -> Callable[[Arg(Environment, 'environment'), VarArg(Tuple[Any]), KwArg(Dict[str, Any])], None]:
    def wrapper(environment: Environment, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
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

    return wrapper


def locust_test_start(context: GrizzlyContext) -> Callable[[Arg(Environment, 'environment'), KwArg(Dict[str, Any])], None]:
    def wrapper(environment: Environment, **_kwargs: Dict[str, Any]) -> None:
        if isinstance(environment.runner, MasterRunner):
            workers = (
                len(environment.runner.clients.ready) +
                len(environment.runner.clients.running) +
                len(environment.runner.clients.spawning)
            )

            logger.debug(f'connected workers: {workers}')

            for scenario in context.scenarios():
                if scenario.iterations < workers:
                    logger.error(f'{scenario.name}: iterations is lower than number of workers')

    return wrapper


def locust_test_stop(**_kwargs: Dict[str, Any]) -> None:
    if producer is not None:
        producer.reset()


def spawning_complete(grizzly: GrizzlyContext) -> Callable[[KwArg(Dict[str, Any])], None]:
    def wrapper(**_kwargs: Dict[str, Any]) -> None:
        logger.debug('spawning complete!')
        grizzly.state.spawning_complete = True

    return wrapper


def quitting(**_kwargs: Dict[str, Any]) -> None:
    global producer_greenlet, producer
    if producer is not None:
        logger.debug('stopping producer')
        producer.stop()
        producer = None

    if producer_greenlet is not None:
        producer_greenlet.kill(block=True)
        producer_greenlet = None


def validate_result(grizzly: GrizzlyContext) -> Callable[[Arg(Environment, 'environment'), KwArg(Dict[str, Any])], None]:
    def wrapper(environment: Environment, **_kwargs: Dict[str, Any]) -> None:
        # first, aggregate statistics per scenario
        scenario_stats: Dict[str, StatsEntry] = {}

        for scenario in grizzly.scenarios():
            request_stats = RequestStats()
            request_stats.total = StatsEntry(environment.stats, scenario.identifier, None, use_response_times_cache=False)
            scenario_stats[scenario.identifier] = request_stats

        for stats_entry in environment.stats.entries.values():
            prefix = stats_entry.name.split(' ', 1)[0]

            if prefix in scenario_stats:
                scenario_stats[prefix].total.extend(stats_entry)
                scenario_stats[prefix].entries[(stats_entry.name, stats_entry.method)] = stats_entry
            else:
                logger.error(f'"{prefix}" does not match any scenario')

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
                    logger.error(f'scenario {prefix} failed due to {error_message}')
                    environment.events.request.fire(
                        request_type='ALL ',
                        name=f'{prefix} scenario failed',
                        response_time=stats.total.total_response_time,
                        response_length=stats.total.total_content_length,
                        context=None,
                        exception=CatchResponseError(error_message),
                    )
                    environment.process_exit_code = 1

            if scenario.validation.avg_response_time is not None:
                expected = scenario.validation.avg_response_time
                actual = stats.total.avg_response_time
                if actual > expected:
                    error_message = f'average response time {int(actual)} ms > {int(expected)} ms'
                    logger.error(f'scenario {prefix} failed due to {error_message}')
                    environment.events.request.fire(
                        request_type='ALL ',
                        name=f'{prefix} scenario failed',
                        response_time=stats.total.total_response_time,
                        response_length=stats.total.total_content_length,
                        context=None,
                        exception=CatchResponseError(error_message),
                    )
                    environment.process_exit_code = 1

            if scenario.validation.response_time_percentile is not None:
                percentile = scenario.validation.response_time_percentile.percentile
                expected = scenario.validation.response_time_percentile.response_time

                actual = stats.total.get_response_time_percentile(percentile)
                if actual > expected:
                    error_message = f'{int(percentile * 100)}%-tile response time {int(actual)} ms > {expected} ms'
                    logger.error(f'scenario {prefix} failed due to {error_message}')
                    environment.events.request.fire(
                        request_type='ALL ',
                        name=f'{prefix} scenario failed',
                        response_time=stats.total.total_response_time,
                        response_length=stats.total.total_content_length,
                        context=None,
                        exception=CatchResponseError(error_message),
                    )
                    environment.process_exit_code = 1

            if environment.process_exit_code == 1 and hasattr(scenario, 'behave') and scenario.behave is not None:
                scenario.behave.set_status('failed')

    return wrapper
