import re
import json
import os
import logging

from typing import Optional, List, Callable, Any, Tuple, Dict, cast
from urllib.parse import urlparse

import jinja2 as j2

from behave.runner import Context
from behave.model import Row
from locust.clients import ResponseContextManager

from grizzly_extras.transformer import PlainTransformer, transformer, TransformerError, TransformerContentType

from ..users.meta import ContextVariables
from ..context import GrizzlyContext
from ..exceptions import ResponseHandlerError, TransformerLocustError
from ..types import HandlerType, RequestMethod, ResponseTarget, ResponseAction
from ..task import RequestTask

logger = logging.getLogger(__name__)


def create_request_task(
    context: Context, method: RequestMethod, source: Optional[str], endpoint: str, name: Optional[str] = None, substitutes: Optional[Dict[str, str]] = None,
) -> RequestTask:
    grizzly = cast(GrizzlyContext, context.grizzly)
    request = _create_request_task(context.config.base_dir, method, source, endpoint, name, substitutes=substitutes)
    request.scenario = grizzly.scenario

    return request


def _create_request_task(
    base_dir: str,method: RequestMethod, source: Optional[str], endpoint: str, name: Optional[str] = None, substitutes: Optional[Dict[str, str]] = None,
) -> RequestTask:
    if substitutes is None:
        substitutes = {}
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

    if source is not None:
        for key, value in substitutes.items():
            source = source.replace(f'{{{{ {key} }}}}', value)

    request = RequestTask(method, name=cast(str, name), endpoint=endpoint)
    request.template = template
    request.source = source

    return request


def add_request_task_response_status_codes(request: RequestTask, status_list: str) -> None:
    for status in status_list.split(','):
        request.response.add_status_code(int(status.strip()))


def add_request_task(context: Context, method: RequestMethod, source: Optional[str] = None, name: Optional[str] = None, endpoint: Optional[str] = None) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)
    scenario_tasks_count = len(grizzly.scenario.tasks)

    table: List[Optional[Row]]
    content_type: Optional[TransformerContentType] = None

    if context.table is not None:
        table = context.table
    else:
        table = [None]

    for row in table:
        if endpoint is None:
            if scenario_tasks_count == 0:
                raise ValueError(f'no endpoint specified')

            last_request = grizzly.scenario.tasks[-1]

            if not isinstance(last_request, RequestTask):
                raise ValueError('previous task was not a request')

            if last_request.method != method:
                raise ValueError(f'can not use endpoint from previous request, it has different method')

            endpoint = last_request.endpoint
            content_type = last_request.response.content_type
        else:
            parsed = urlparse(endpoint)
            if len(parsed.netloc) > 0:
                raise ValueError(f'endpoints should only contain path relative to {grizzly.scenario.context["host"]}')

        orig_endpoint = endpoint
        orig_name = name
        orig_source = source

        substitutes: Dict[str, str] = {}

        if row is not None:
            for key, value in row.as_dict().items():
                endpoint = endpoint.replace(f'{{{{ {key} }}}}', value)
                if name is not None:
                    name = name.replace(f'{{{{ {key} }}}}', value)
                if source is not None:
                    substitutes.update({key: value})
                    source = source.replace(f'{{{{ {key} }}}}', value)

        request_task = create_request_task(context, method, source, endpoint, name, substitutes=substitutes)
        if content_type is not None:
            request_task.response.content_type = content_type

        endpoint = orig_endpoint
        name = orig_name
        source = orig_source

        grizzly.scenario.tasks.append(request_task)


def get_matches(
    input_get_values: Callable[[Any], List[str]],
    match_get_values: Callable[[Any], List[str]],
    input_payload: Any,
) -> Tuple[List[Any], List[Any]]:
    '''Find all values in `input_context`.

    Args:
        input_get_values (Callable[[Any], List[str]]): function that returns all values matching `expression`
        input_match_values (Callable[[Any], List[str]]): function that checks that a value has correct value
        input_context (Tuple[TransformerContentType, Any]): content type and transformed payload

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
    input_context: Tuple[TransformerContentType, Any],
    expression: str,
    match_with: str,
    user: ContextVariables,
    callback: Callable[[str, Optional[Any]], None],
    condition: bool,
) -> None:
    '''Contains common logic for both save and validation handlers.

    Args:
        input_context (Tuple[TransformerContentType, Any]): content type and transformed payload
        expression (str): expression to extract value from `input_context`
        match_with (str): regular expression that the extracted value must match
        user (ContextVariablesUser): user that executed task (request)
        callback (Callable[[str, Optional[Any]], None]): specific logic for either save or validation handler
        condition (bool): used by validation handler for negative matching
    '''
    input_content_type, input_payload = input_context
    interpolated_expression = j2.Template(expression).render(user.context_variables)
    interpolated_match_with = j2.Template(match_with).render(user.context_variables)

    try:
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
    except TransformerError as e:
        raise TransformerLocustError(e.message) from e

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
        input_context: Tuple[TransformerContentType, Any],
        user: ContextVariables,
        response: Optional[ResponseContextManager] = None,
    ) -> None:
        '''Actual handler that will run after a response has been received by an task.

        Args:
            input_context (Tuple[TransformerContentType, Any]): content type and transformed payload
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
        input_context: Tuple[TransformerContentType, Any],
        user: ContextVariables,
        response: Optional[ResponseContextManager] = None,
    ) -> None:
        '''Actual handler that will run after a response has been received by an task.

        Args:
            input_context (Tuple[TransformerContentType, Any]): content type and transformed payload
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
    context: GrizzlyContext,
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

    if not isinstance(request, RequestTask):
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


def add_save_handler(context: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, variable: str) -> None:
    _add_response_handler(context, target, ResponseAction.SAVE, expression=expression, match_with=match_with, variable=variable)


def add_validation_handler(context: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, condition: bool) -> None:
    _add_response_handler(context, target, ResponseAction.VALIDATE, expression=expression, match_with=match_with, condition=condition)


def normalize_step_name(step_name: str) -> str:
    return re.sub(r'"[^"]*"', '""', step_name)


