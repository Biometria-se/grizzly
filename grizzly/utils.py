import logging
import os


from typing import Callable, Generic, TypeVar, Type, List, Any, Dict, Tuple, Optional, cast, Generator
from types import FunctionType
from importlib import import_module
from functools import wraps
from contextlib import contextmanager
from collections import namedtuple
from collections.abc import Mapping
from copy import deepcopy

from behave.runner import Context
from behave.model import Scenario
from behave.model_core import Status
from locust.user.users import User
from locust import TaskSet, between
from jinja2 import Template, Environment
from jinja2.meta import find_undeclared_variables

from .types import TemplateData, TemplateDataType
from .context import GrizzlyContext, GrizzlyContextScenario


logger = logging.getLogger(__name__)

T = TypeVar('T')

WrappedFunc = TypeVar('WrappedFunc', bound=Callable[..., Any])


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

    # @TODO: allow user implementations outside of grizzly?
    if not hasattr(scenario, 'user_class_name') or scenario.user_class_name is None:
        raise ValueError(f'{scenario.identifier} does not have user_class_name set')

    base_user_class_type = cast(Type[User], ModuleLoader[User].load('grizzly.users', scenario.user_class_name))
    user_class_name = f'{scenario.user_class_name}_{scenario.identifier}'

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
    })


def create_task_class_type(base_type: str, scenario: GrizzlyContextScenario) -> Type[TaskSet]:
    # @TODO: allow scenario implementation outside of grizzly?
    base_task_class_type = cast(Type[TaskSet], ModuleLoader[TaskSet].load('grizzly.tasks', base_type))
    task_class_name = f'{base_type}_{scenario.identifier}'

    return type(task_class_name, (base_task_class_type, ), {
        'tasks': [],
        'wait_time': between(scenario.wait.minimum, scenario.wait.maximum)
    })


def create_context_variable(grizzly: GrizzlyContext, variable: str, value: str) -> Dict[str, Any]:
    casted_value = resolve_variable(grizzly, value)

    variable = variable.lower().replace(' ', '_').replace('/', '.')

    return transform({variable: casted_value}, True)


def resolve_variable(grizzly: GrizzlyContext, value: str, guess_datatype: Optional[bool] = True) -> TemplateDataType:
    if len(value) < 1:
        return value

    resolved_variable: TemplateDataType
    if '{{' in value and '}}' in value:
        template = Template(value)
        j2env = Environment(autoescape=False)
        template_parsed = j2env.parse(value)
        template_variables = find_undeclared_variables(template_parsed)

        for template_variable in template_variables:
            assert template_variable in grizzly.state.variables, f'value contained variable "{template_variable}" which has not been set'

        resolved_variable = template.render(**grizzly.state.variables)
    elif len(value) > 4 and value[0] == '$':
        if value[0:5] == '$conf':
            variable = value[7:]
            assert variable in grizzly.state.configuration, f'configuration variable "{variable}" is not set'
            resolved_variable = grizzly.state.configuration[variable]
        elif value[0:4] == '$env':
            variable = value[6:]
            env_value = os.environ.get(variable, None)
            assert env_value is not None, f'environment variable "{variable}" is not set'
            resolved_variable = env_value
        else:
            raise ValueError(f'{value.split("::", 1)[0]} is not implemented')
    else:
        resolved_variable = value

    if guess_datatype:
        resolved_variable = TemplateData.guess_datatype(resolved_variable)

    return resolved_variable


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


def transform(data: Dict[str, Any], raw: Optional[bool] = False) -> Dict[str, Any]:
    testdata: Dict[str, Any] = {}

    for key, value in data.items():
        if '.' in key:
            paths: List[str] = key.split('.')
            variable = paths.pop(0)
            path = paths.pop()
            struct = {path: value}
            paths.reverse()

            for path in paths:
                struct = {path: {**struct}}

            if variable in testdata:
                testdata[variable] = merge_dicts(testdata[variable], struct)
            else:
                testdata[variable] = {**struct}
        else:
            testdata[key] = value

    if not raw:
        return _objectify(testdata)
    else:
        return testdata


def _objectify(testdata: Dict[str, Any]) -> Dict[str, Any]:
    for variable, attributes in testdata.items():
        if not isinstance(attributes, dict):
            continue

        attributes = _objectify(attributes)
        testdata[variable] = namedtuple('Testdata', attributes.keys())(**attributes)

    return testdata


def in_correct_section(func: FunctionType, expected: List[str]) -> bool:
    try:
        actual = '.'.join(func.__module__.rsplit('.', 1)[:-1])
    except AttributeError:  # function does not belong to a module
        actual = 'custom'

    return (
        actual.startswith('grizzly.') and actual in expected
    ) or not actual.startswith('grizzly.')
