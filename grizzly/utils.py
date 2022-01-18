import logging
import re

from typing import Generic, Type, List, Any, Dict, Tuple, Optional, cast, Generator
from types import FunctionType
from importlib import import_module
from functools import wraps
from contextlib import contextmanager
from collections.abc import Mapping
from copy import deepcopy

from behave.runner import Context
from behave.model import Scenario
from behave.model_core import Status
from locust.user.users import User
from locust import TaskSet, between

from .context import GrizzlyContextScenario
from .types import WrappedFunc, T


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


class catch:
    def __init__(self, exception_type: Type[BaseException]) -> None:
        self.exception_type = exception_type

    def __call__(self, func: WrappedFunc) -> WrappedFunc:
        exception_type = self.exception_type

        @wraps(func)
        def wrapper(context: Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            try:
                return func(context, *args, **kwargs)
            except exception_type as e:
                context._set_root_attribute('failed', True)

                if len(args) > 0:
                    if isinstance(args[0], Scenario):
                        scenario = args[0]
                        scenario.set_status(Status.failed)
                    else:
                        raise e from e
                else:
                    raise e from e

        return cast(WrappedFunc, wrapper)


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


def create_user_class_type(scenario: GrizzlyContextScenario, global_context: Optional[Dict[str, Any]] = None) -> Type[User]:
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

    base_user_class_type = cast(Type[User], ModuleLoader[User].load(module, user_class_name))
    user_class_name = f'{scenario.user.class_name}_{scenario.identifier}'

    context: Dict[str, Any] = {}
    contexts: List[Dict[str, Any]] = []

    from .users.meta import ContextVariables

    if issubclass(base_user_class_type, ContextVariables):
        contexts.append(base_user_class_type._context)

    contexts += [global_context, scenario.context]

    for merge_context in [base_user_class_type._context, global_context, scenario.context]:
        context = merge_dicts(context, merge_context)

    return type(user_class_name, (base_user_class_type, ), {
        '__dependencies__': base_user_class_type.__dependencies__,
        '_context': context,
        '_scenario': scenario,
        'weight': scenario.user.weight,
    })


def create_scenario_class_type(base_type: str, scenario: GrizzlyContextScenario) -> Type[TaskSet]:
    if base_type.count('.') > 0:
        module, base_type = base_type.rsplit('.', 1)
    else:
        module = 'grizzly.scenarios'

    base_task_class_type = cast(Type[TaskSet], ModuleLoader[TaskSet].load(module, base_type))
    task_class_name = f'{base_type}_{scenario.identifier}'

    return type(task_class_name, (base_task_class_type, ), {
        'tasks': [],
        'wait_time': between(scenario.wait.minimum, scenario.wait.maximum)
    })


def merge_dicts(merged: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(merged)
    source = deepcopy(source)

    for k in source.keys():
        if (k in merged and isinstance(merged[k], dict)
                and isinstance(source[k], Mapping)):
            merged[k] = merge_dicts(merged[k], source[k])
        else:
            merged[k] = source[k]

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
        raise ValueError('invalid time span format')
    group = parts.groupdict()
    parameters = {name: int(value) for name, value in group.items() if value}
    if not parameters:
        raise ValueError('invalid time span format')

    return parameters
