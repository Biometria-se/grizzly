from __future__ import annotations
import logging
import re

from typing import TYPE_CHECKING, Generic, Type, List, Any, Dict, Tuple, Optional, cast, Generator, Union
from types import FunctionType
from importlib import import_module
from contextlib import contextmanager
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from datetime import datetime, timezone
from json import dumps as jsondumps, loads as jsonloads

from dateutil.parser import parse as dateparser, ParserError
from locust.stats import STATS_NAME_WIDTH

from grizzly.types import T
from grizzly.types.behave import Context
from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse, async_message_request

if TYPE_CHECKING:  # pragma: no cover
    import zmq.green as zmq
    from .context import GrizzlyContextScenario
    from .scenarios import GrizzlyScenario
    from .users.base import GrizzlyUser


logger = logging.getLogger(__name__)


class ModuleLoader(Generic[T]):
    @staticmethod
    def load(default_module: str, value: str) -> Type[T]:
        try:
            [module_name, class_name] = value.rsplit('.', 1)
        except ValueError:
            module_name = default_module
            class_name = value

        if class_name not in globals():
            module = import_module(module_name)
            globals()[class_name] = getattr(module, class_name)

        class_type_instance = globals()[class_name]

        return cast(Type[T], class_type_instance)


@contextmanager
def fail_direct(context: Context) -> Generator[None, None, None]:
    # save original values
    orig_stop_value = context.config.stop
    orig_verbose_value = context.config.verbose

    # do not continue with other features, stop
    context.config.stop = True
    # we do not want stacktrace for this hook-error, if the encapsuled assert fails
    context.config.verbose = False

    try:
        yield None
    finally:
        pass

    # only restore if the ecapsuled assert passes
    context.config.stop = orig_stop_value
    context.config.verbose = orig_verbose_value


def create_user_class_type(scenario: 'GrizzlyContextScenario', global_context: Optional[Dict[str, Any]] = None, fixed_count: Optional[int] = None) -> Type['GrizzlyUser']:
    if global_context is None:
        global_context = {}

    if not hasattr(scenario, 'user') or scenario.user is None:
        raise ValueError(f'scenario {scenario.description} has not set a user')

    if not hasattr(scenario.user, 'class_name') or scenario.user.class_name is None:
        raise ValueError(f'scenario {scenario.description} does not have a user type set')

    if scenario.user.class_name.count('.') > 0:
        module, user_class_name = scenario.user.class_name.rsplit('.', 1)
    else:
        module = 'grizzly.users'
        user_class_name = scenario.user.class_name

    base_user_class_type = cast(Type['GrizzlyUser'], ModuleLoader['GrizzlyUser'].load(module, user_class_name))
    user_class_name = f'{scenario.user.class_name}_{scenario.identifier}'

    context: Dict[str, Any] = {}
    contexts: List[Dict[str, Any]] = [
        base_user_class_type._context,
        global_context,
        scenario.context,
    ]

    for merge_context in contexts:
        context = merge_dicts(context, merge_context)

    distribution: Dict[str, Union[int, float]] = {
        'weight': scenario.user.weight,
    }

    if fixed_count is not None:
        distribution.update({'fixed_count': fixed_count})

    return type(user_class_name, (base_user_class_type, ), {
        '__module__': base_user_class_type.__module__,
        '__dependencies__': base_user_class_type.__dependencies__,
        '__scenario__': scenario,
        '_context': context,
        **distribution,
    })


def create_scenario_class_type(base_type: str, scenario: 'GrizzlyContextScenario') -> Type['GrizzlyScenario']:
    if base_type.count('.') > 0:
        module, base_type = base_type.rsplit('.', 1)
    else:
        module = 'grizzly.scenarios'

    base_task_class_type = ModuleLoader['GrizzlyScenario'].load(module, base_type)
    task_class_name = f'{base_type}_{scenario.identifier}'

    return type(task_class_name, (base_task_class_type, ), {
        '__module__': base_task_class_type.__module__,
        'pace_time': scenario.pace,
        'tasks': [],
    })


def merge_dicts(merged: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(merged)
    source = deepcopy(source)

    for k in source.keys():
        if (k in merged and isinstance(merged[k], dict)
                and isinstance(source[k], Mapping)):
            merged[k] = merge_dicts(merged[k], source[k])
        else:
            value = source[k]
            if value == 'None':
                value = None
            merged[k] = value

    return merged


def in_correct_section(func: FunctionType, expected: List[str]) -> bool:
    try:
        actual = '.'.join(func.__module__.rsplit('.', 1)[:-1])
    except AttributeError:  # function does not belong to a module
        actual = 'custom'

    return (
        actual.startswith('grizzly.') and actual in expected
    ) or not actual.startswith('grizzly.')


def parse_timespan(timespan: str) -> Dict[str, int]:
    if re.match(r'^-?\d+$', timespan):
        # if an int is specified we assume they want days
        return {'days': int(timespan)}

    pattern = re.compile(r'((?P<years>-?\d+?)Y)?((?P<months>-?\d+?)M)?((?P<days>-?\d+?)D)?((?P<hours>-?\d+?)h)?((?P<minutes>-?\d+?)m)?((?P<seconds>-?\d+?)s)?')
    parts = pattern.match(timespan)
    if not parts:
        raise ValueError(f'invalid time span format: {timespan}')
    group = parts.groupdict()
    parameters = {name: int(value) for name, value in group.items() if value}
    if not parameters:
        raise ValueError(f'invalid time span format: {timespan}')

    return parameters


def fastdeepcopy(input: Dict[str, Any]) -> Dict[str, Any]:
    return dict(zip(input.keys(), map(dict.copy, input.values())))


def check_mq_client_logs(context: Context) -> None:
    """
    ```bash
    $ pwd && ls -1
    /home/vscode/IBM/MQ/data/errors
    AMQ6150.0.FDC
    AMQERR01.LOG
    ```
    """
    def print_table(subject: str, header: str, data: List[Tuple[datetime, str]]) -> None:
        if len(data) < 1:
            return

        from .locust import stats_logger

        data = sorted(data, key=lambda k: k[0])

        stats_logger.info(f'{subject}:')
        stats_logger.info('%-20s %-100s' % ('Timestamp (UTC)', header))
        separator = f'{"-" * 20}|{"-" * ((80 + STATS_NAME_WIDTH) - 19)}'
        stats_logger.info(separator)

        for timestamp, info in data:
            stats_logger.info('%-20s %-100s' % (timestamp.strftime('%Y-%m-%d %H:%M:%S'), info,))
        stats_logger.info(separator)
        stats_logger.info('')

        for handler in stats_logger.handlers:
            handler.flush()

    if not hasattr(context, 'started'):
        return

    started = cast(datetime, context.started).astimezone(tz=timezone.utc)

    amqerr_log_entries: List[Tuple[datetime, str]] = []
    amqerr_fdc_files: List[Tuple[datetime, str]] = []

    log_directory = Path('~/IBM/MQ/data/errors').expanduser()

    # check errors files
    if not log_directory.exists():
        return

    for amqerr_log_file in log_directory.glob('AMQERR*.LOG'):
        with open(amqerr_log_file, 'r') as fd:
            line: Optional[str] = None

            for line in fd:
                while line and not re.match(r'^\s+Time\(', line):
                    try:
                        line = next(fd)
                    except StopIteration:
                        line = None
                        break

                if not line:
                    break

                try:
                    time_start = line.index('Time(') + 5
                    time_end = line.index(')')
                    time_str = line[time_start:time_end]
                    time_date = dateparser(time_str)

                    if time_date < started:
                        continue
                except ParserError:
                    logger.error(f'{time_str} is not a valid date', exc_info=context.config.verbose)
                    continue
                except ValueError as ve:
                    logger.error(f'{time_str}: {str(ve)}', exc_info=context.config.verbose)
                    continue

                while not line.startswith('AMQ'):
                    line = next(fd)

                amqerr_log_entries.append((time_date, line.strip(),))

    for amqerr_fdc_file in log_directory.glob('AMQ*.FDC'):
        modification_date = datetime.fromtimestamp(amqerr_fdc_file.stat().st_mtime).astimezone(tz=timezone.utc)

        if modification_date < started:
            continue

        amqerr_fdc_files.append((modification_date, str(amqerr_fdc_file),))

    # present entries created during run
    print_table('AMQ error log entries', 'Message', amqerr_log_entries)
    print_table('AMQ FDC files', 'File', amqerr_fdc_files)


def async_message_request_wrapper(parent: GrizzlyScenario, client: zmq.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    request_json = jsondumps(request)
    request = jsonloads(parent.render(request_json))

    if request.get('client', None) is None:
        request.update({'client': id(parent.user)})

    parent.logger.debug(f'{request=}')

    return async_message_request(client, request)


def safe_del(struct: Dict[str, Any], key: str) -> None:
    try:
        del struct[key]
    except KeyError:
        pass
