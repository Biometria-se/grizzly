import logging
import os
import re
import json


from typing import Callable, Generic, TypeVar, Type, List, Any, Dict, Tuple, Optional, cast, Generator
from types import FunctionType
from importlib import import_module
from functools import wraps
from urllib.parse import urlparse
from contextlib import contextmanager

import jinja2 as j2

from behave.runner import Context
from behave.model import Scenario, Row
from behave.model_core import Status
from locust.clients import ResponseContextManager
from locust.user.users import User
from locust import TaskSet, between
from jinja2 import Template, Environment
from jinja2.meta import find_undeclared_variables

from .testdata.utils import transform, merge_dicts
from .testdata.models import TemplateData, TemplateDataType
from .users.meta import ContextVariables
from .exceptions import ResponseHandlerError
from .types import HandlerType, ResponseContentType, RequestMethod
from .transformer import PlainTransformer, transformer
from .context import (
    LocustContext,
    LocustContextScenario,
    RequestContext,
    ResponseTarget,
    ResponseAction,
)


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


def create_request_context(context: Context, method: RequestMethod, source: str, endpoint: str, name: Optional[str] = None) -> RequestContext:
    locust_context = cast(LocustContext, context.locust)
    request = _create_request_context(context.config.base_dir, method, source, endpoint, name)
    request.scenario = locust_context.scenario

    return request


def _create_request_context(base_dir: str, method: RequestMethod, source: str, endpoint: str, name: Optional[str] = None) -> RequestContext:
    path = os.path.join(base_dir, 'requests')
    j2env = j2.Environment(
        autoescape=False,
        loader=j2.FileSystemLoader(path),
    )

    template: Optional[j2.Template] = None

    try:
        if source is not None:
            possible_file = os.path.join(path, source)
            # minify json files, to increase performance when jinja creates template
            if os.path.isfile(possible_file):
                with open(possible_file, 'r') as fd:
                    try:
                        source = json.dumps(json.load(fd))
                        raise RuntimeError()
                    except json.decoder.JSONDecodeError:
                        # not json contents, so do not minify
                        pass

            template = j2env.get_template(source)
            if name is None:
                name = source.replace('.j2.json', '')

            with open(os.path.join(path, source), 'r') as fd:
                source = fd.read()
    except (j2.exceptions.TemplateNotFound, RuntimeError):
        if source is not None:
            template = j2env.from_string(source)

        if name is None:
            name = '<unknown>'

    request = RequestContext(method, name=name, endpoint=endpoint)
    request.template = template
    request.source = source

    return request


def add_request_context_response_status_codes(request: RequestContext, status_list: str) -> None:
    for status in status_list.split(','):
        request.response.add_status_code(int(status.strip()))


def add_request_context(context: Context, method: RequestMethod, source: str, name: Optional[str] = None, endpoint: Optional[str] = None) -> None:
    context_locust = cast(LocustContext, context.locust)
    scenario_tasks_count = len(context_locust.scenario.tasks)

    table: List[Optional[Row]]

    if context.table is not None:
        table = context.table
    else:
        table = [None]

    for row in table:
        if endpoint is None:
            if scenario_tasks_count == 0:
                raise ValueError(f'no endpoint specified')

            last_request = context_locust.scenario.tasks[-1]

            if not isinstance(last_request, RequestContext):
                raise ValueError('previous task was not a request')

            if last_request.method != method:
                raise ValueError(f'can not use endpoint from previous request, it has different method')

            endpoint = last_request.endpoint
        else:
            parsed = urlparse(endpoint)
            if len(parsed.netloc) > 0:
                raise ValueError(f'endpoints should only contain path relative to {context_locust.scenario.context["host"]}')

        orig_endpoint = endpoint
        orig_name = name
        orig_source = source

        if row is not None:
            for key, value in row.as_dict().items():
                endpoint = endpoint.replace(f'{{{{ {key} }}}}', value)
                if name is not None:
                    name = name.replace(f'{{{{ {key} }}}}', value)
                if source is not None:
                    source = source.replace(f'{{{{ {key} }}}}', value)

        request_context = create_request_context(context, method, source, endpoint, name)

        endpoint = orig_endpoint
        name = orig_name
        source = orig_source

        context_locust.scenario.tasks.append(request_context)


def get_matches(
    input_get_values: Callable[[Any], List[str]],
    match_get_values: Callable[[Any], List[str]],
    input_payload: Any,
) -> Tuple[List[Any], List[Any]]:
    '''Find all values in `input_context`.

    Args:
        input_get_values (Callable[[Any], List[str]]): function that returns all values matching `expression`
        input_match_values (Callable[[Any], List[str]]): function that checks that a value has correct value
        input_context (Tuple[ResponseContentType, Any]): content type and transformed payload

    Returns:
        Tuple[List[Any], List[Any]]: list of all values and list of all matched values of those
    '''
    values = input_get_values(input_payload)

    # get a list of all matches in values
    matches: List[str] = []
    for value in values:
        matched_values = match_get_values(value)

        if len(matched_values) < 1:
            continue

        matched_value = matched_values[0]

        if matched_value is None or len(matched_value) < 1:
            continue

        matches.append(matched_value)

    return values, matches


def handler_logic(
    input_context: Tuple[ResponseContentType, Any],
    expression: str,
    match_with: str,
    user: ContextVariables,
    callback: Callable[[str, Optional[Any]], None],
    condition: bool,
) -> None:
    '''Contains common logic for both save and validation handlers.

    Args:
        input_context (Tuple[ResponseContentType, Any]): content type and transformed payload
        expression (str): expression to extract value from `input_context`
        match_with (str): regular expression that the extracted value must match
        user (ContextVariablesUser): user that executed task (request)
        callback (Callable[[str, Optional[Any]], None]): specific logic for either save or validation handler
        condition (bool): used by validation handler for negative matching
    '''
    input_content_type, input_payload = input_context
    interpolated_expression = j2.Template(expression).render(user.context_variables)
    interpolated_match_with = j2.Template(match_with).render(user.context_variables)

    transform = transformer.available.get(input_content_type, None)
    if transform is None:
        raise TypeError(f'could not find a transformer for {input_content_type.name}')

    if not transform.validate(interpolated_expression):
        raise TypeError(f'"{interpolated_expression}" is not a valid expression for {input_content_type.name}')

    input_get_values = transform.parser(interpolated_expression)
    match_get_values = PlainTransformer.parser(interpolated_match_with)

    values, matches = get_matches(
        input_get_values,
        match_get_values,
        input_payload,
    )

    number_of_matches = len(matches)

    if number_of_matches != 1:
        if number_of_matches < 1 and not condition:
            logger.error(f'"{interpolated_expression}": "{interpolated_match_with}" not in "{values}"')
        elif number_of_matches > 1:
            logger.error(f'"{interpolated_expression}": "{interpolated_match_with}" has multiple matches in "{values}"')

        match = None
    else:
        match = matches[0]

    callback(interpolated_expression, match)


def generate_validation_handler(expression: str, match_with: str, condition: bool) -> HandlerType:
    '''Generates a handler that will validate a value from an input.

    Args:
        expression (str): how to find the specified value, can contain templating variables
        match_with (str): regular expression that any result from `expression` must match
        condition (bool): if the match should or should not match

    Returns:
        HandlerType: function that will validate values in a response during runtime
    '''
    def validate(
        input_context: Tuple[ResponseContentType, Any],
        user: ContextVariables,
        response: Optional[ResponseContextManager] = None,
    ) -> None:
        '''Actual handler that will run after a response has been received by an task.

        Args:
            input_context (Tuple[ResponseContentType, Any]): content type and transformed payload
            user (ContextVariablesUser): user that executed task (request)
            response (Optional[ResponseContextManager]): optional response context, only if `user` does HTTP requests
        '''
        def callback(
            interpolated_expression: str,
            match: Optional[Any],
        ) -> None:
            '''Validation specific logic that will handle the `match` based on `expression` and `match_with`.

            Args:
                interpolated_expression (str): `expression` with templating variables resolved
                match (Optional[Any]): value based on `expression` that matches `match_with`
            '''
            result = match is not None if condition == True else match is None

            if result:
                message = f'"{interpolated_expression}": "{match_with} was {match}"'
                if response is not None:
                    response.failure(message)
                else:
                    raise ResponseHandlerError(message)

        handler_logic(input_context, expression, match_with, user, callback, condition)

    return validate


def generate_save_handler(expression: str, match_with: str, variable: str) -> HandlerType:
    '''Generates a handler that will extract a value from the response and save it in a templating variable.

    Args:
        expression (str): how to find the specified value, can contain templating variables
        match_with (str): regular expression that any result from `expression` must match
        variable (str): name of templating variable in the user context

    Returns:
        HandlerType: function that will save values from responses during run time
    '''
    def save(
        input_context: Tuple[ResponseContentType, Any],
        user: ContextVariables,
        response: Optional[ResponseContextManager] = None,
    ) -> None:
        '''Actual handler that will run after a response has been received by an task.

        Args:
            input_context (Tuple[ResponseContentType, Any]): content type and transformed payload
            user (ContextVariablesUser): user that executed task (request)
            response (Optional[ResponseContextManager]): optional response context, only if `user` does HTTP requests
        '''
        def callback(
            interpolated_expression: str,
            match: Optional[Any],
        ) -> None:
            '''Validation specific logic that will handle the `match` based on `expression` and `match_with`.

            Args:
                interpolated_expression (str): `expression` with templating variables resolved
                match (Optional[Any]): value based on `expression` that matches `match_with`
            '''
            user.set_context_variable(variable, match)

            if match is None:
                message = f'"{interpolated_expression}" did not match value'
                if response is not None:
                    response.failure(message)
                else:
                    raise ResponseHandlerError(message)

        handler_logic(input_context, expression, match_with, user, callback, False)

    return save


def _add_response_handler(
    context: LocustContext,
    target: ResponseTarget,
    action: ResponseAction,
    expression: str,
    match_with: str,
    variable: Optional[str] = None,
    condition: Optional[bool] = None,
) -> None:
    scenario_tasks_count = len(context.scenario.tasks)

    if variable is not None and variable not in context.state.variables:
        raise ValueError(f'variable {variable} has not been declared')

    if not scenario_tasks_count > 0:
        raise ValueError('no request source has been added!')

    if len(expression) < 1:
        raise ValueError('expression is empty')

    # latest request
    request = context.scenario.tasks[-1]

    if not isinstance(request, RequestContext):
        raise ValueError('latest task was not a request')

    if '{{' in match_with and '}}' in match_with:
        context.scenario.orphan_templates.append(match_with)

    if '{{' in expression and '}}' in match_with:
        context.scenario.orphan_templates.append(expression)

    if action == ResponseAction.SAVE:
        if variable is None:
            raise ValueError('variable is not set')

        handler = generate_save_handler(expression, match_with, variable)
    elif action == ResponseAction.VALIDATE:
        if condition is None:
            raise ValueError('condition is not set')

        handler = generate_validation_handler(expression, match_with, condition)

    if target == ResponseTarget.METADATA:
        add_listener = request.response.handlers.add_metadata
    elif target == ResponseTarget.PAYLOAD:
        add_listener = request.response.handlers.add_payload

    add_listener(handler)


def add_save_handler(context: LocustContext, target: ResponseTarget, expression: str, match_with: str, variable: str) -> None:
    _add_response_handler(context, target, ResponseAction.SAVE, expression=expression, match_with=match_with, variable=variable)


def add_validation_handler(context: LocustContext, target: ResponseTarget, expression: str, match_with: str, condition: bool) -> None:
    _add_response_handler(context, target, ResponseAction.VALIDATE, expression=expression, match_with=match_with, condition=condition)


def normalize_step_name(step_name: str) -> str:
    return re.sub(r'"[^"]*"', '""', step_name)


def in_correct_section(func: FunctionType, expected: List[str]) -> bool:
    try:
        actual = '.'.join(func.__module__.rsplit('.', 1)[:-1])
    except AttributeError:  # function does not belong to a module
        actual = 'custom'

    return (
        actual.startswith('grizzly.') and actual in expected
    ) or not actual.startswith('grizzly.')


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


def create_user_class_type(scenario: LocustContextScenario, global_context: Optional[Dict[str, Any]] = None) -> Type[User]:
    if global_context is None:
        global_context = {}

    # @TODO: allow user implementations outside of grizzly?
    if not hasattr(scenario, 'user_class_name') or scenario.user_class_name is None:
        raise ValueError(f'{scenario.identifier} does not have user_class_name set')

    base_user_class_type = cast(Type[User], ModuleLoader[User].load('grizzly.users', scenario.user_class_name))
    user_class_name = f'{scenario.user_class_name}_{scenario.identifier}'

    context: Dict[str, Any] = {}
    contexts: List[Dict[str, Any]] = []

    if issubclass(base_user_class_type, ContextVariables):
        contexts.append(base_user_class_type._context)

    contexts += [global_context, scenario.context]

    for merge_context in [base_user_class_type._context, global_context, scenario.context]:
        context = merge_dicts(context, merge_context)

    return type(user_class_name, (base_user_class_type, ), {
        '_context': context,
    })


def create_task_class_type(base_type: str, scenario: LocustContextScenario) -> Type[TaskSet]:
    # @TODO: allow scenario implementation outside of grizzly?
    base_task_class_type = cast(Type[TaskSet], ModuleLoader[TaskSet].load('grizzly.tasks', base_type))
    task_class_name = f'{base_type}_{scenario.identifier}'

    return type(task_class_name, (base_task_class_type, ), {
        'tasks': [],
        'wait_time': between(scenario.wait.minimum, scenario.wait.maximum)
    })


def create_context_variable(context_locust: LocustContext, variable: str, value: str) -> Dict[str, Any]:
    casted_value = resolve_variable(context_locust, value)

    variable = variable.lower().replace(' ', '_').replace('/', '.')

    return transform({variable: casted_value}, True)


def resolve_variable(context_locust: LocustContext, value: str, guess_datatype: Optional[bool] = True) -> TemplateDataType:
    if len(value) < 1:
        return value

    resolved_variable: TemplateDataType
    if '{{' in value and '}}' in value:
        template = Template(value)
        j2env = Environment(autoescape=False)
        template_parsed = j2env.parse(value)
        template_variables = find_undeclared_variables(template_parsed)

        for template_variable in template_variables:
            assert template_variable in context_locust.state.variables, f'value contained variable "{template_variable}" which has not been set'

        resolved_variable = template.render(**context_locust.state.variables)
    elif len(value) > 4 and value[0] == '$':
        if value[0:5] == '$conf':
            variable = value[7:]
            assert variable in context_locust.state.configuration, f'configuration variable "{variable}" is not set'
            resolved_variable = context_locust.state.configuration[variable]
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
