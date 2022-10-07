import re
import json
import os
import logging

from typing import Optional, List, Tuple, Dict, Type, cast
from urllib.parse import urlparse

import jinja2 as j2

from behave.runner import Context
from behave.model import Row

from grizzly_extras.transformer import TransformerContentType
from grizzly_extras.arguments import split_value, parse_arguments, get_unsupported_arguments

from ..context import GrizzlyContext
from ..types import RequestMethod, ResponseTarget, ResponseAction
from ..tasks import RequestTask
from ..tasks.clients import client, ClientTask
from ..testdata.utils import resolve_variable
from ..users.base.response_handler import ResponseHandlerAction, ValidationHandlerAction, SaveHandlerAction

logger = logging.getLogger(__name__)


def create_request_task(
    context: Context, method: RequestMethod, source: Optional[str], endpoint: str, name: Optional[str] = None, substitutes: Optional[Dict[str, str]] = None,
) -> RequestTask:
    grizzly = cast(GrizzlyContext, context.grizzly)
    request = _create_request_task(context.config.base_dir, method, source, endpoint, name, substitutes=substitutes)
    request.scenario = grizzly.scenario

    return request


def _create_request_task(
    base_dir: str, method: RequestMethod, source: Optional[str], endpoint: str, name: Optional[str] = None, substitutes: Optional[Dict[str, str]] = None,
) -> RequestTask:
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
        for key, value in (substitutes or {}).items():
            source = source.replace(f'{{{{ {key} }}}}', value)

    request = RequestTask(method, name=cast(str, name), endpoint=endpoint)
    request._source = source
    request._template = template

    return request


def add_request_task_response_status_codes(request: RequestTask, status_list: str) -> None:
    for status in status_list.split(','):
        request.response.add_status_code(int(status.strip()))


def add_request_task(
    context: Context,
    method: RequestMethod,
    source: Optional[str] = None,
    name: Optional[str] = None,
    endpoint: Optional[str] = None,
    in_scenario: Optional[bool] = True,
) -> List[Tuple[RequestTask, Dict[str, str]]]:
    grizzly = cast(GrizzlyContext, context.grizzly)

    scenario_tasks_count = len(grizzly.scenario.tasks())

    request_tasks: List[Tuple[RequestTask, Dict[str, str]]] = []

    table: List[Optional[Row]]
    content_type: Optional[TransformerContentType] = None

    if endpoint is not None and endpoint[:4] in ['$env', '$con']:
        endpoint = cast(str, resolve_variable(grizzly, endpoint, guess_datatype=False))

    if context.table is not None:
        table = context.table
    else:
        table = [None]

    for row in table:
        if endpoint is None:
            if scenario_tasks_count == 0:
                raise ValueError('no endpoint specified')

            last_request = grizzly.scenario.tasks()[-1]

            if not isinstance(last_request, RequestTask):
                raise ValueError('previous task was not a request')

            if last_request.method != method:
                raise ValueError('can not use endpoint from previous request, it has different method')

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
                substitutes.update({key: value})
                endpoint = endpoint.replace(f'{{{{ {key} }}}}', value)
                if name is not None:
                    name = name.replace(f'{{{{ {key} }}}}', value)
                if source is not None:
                    source = source.replace(f'{{{{ {key} }}}}', value)

        request_task = create_request_task(context, method, source, endpoint, name, substitutes=substitutes)
        if content_type is not None:
            request_task.response.content_type = content_type

        endpoint = orig_endpoint
        name = orig_name
        source = orig_source

        if in_scenario:
            grizzly.scenario.tasks.add(request_task)
        else:
            request_tasks.append((request_task, substitutes,))

    return request_tasks


def _add_response_handler(
    grizzly: GrizzlyContext,
    target: ResponseTarget,
    action: ResponseAction,
    expression: str,
    match_with: str,
    variable: Optional[str] = None,
    condition: Optional[bool] = None,
) -> None:
    if variable is not None and variable not in grizzly.state.variables:
        raise ValueError(f'variable "{variable}" has not been declared')

    if not len(grizzly.scenario.tasks()) > 0:
        raise ValueError('no request source has been added!')

    if len(expression) < 1:
        raise ValueError('expression is empty')

    if '|' in expression:
        expression, expression_arguments = split_value(expression)
        arguments = parse_arguments(expression_arguments)
        unsupported_arguments = get_unsupported_arguments(['expected_matches', 'as_json'], arguments)

        if len(unsupported_arguments) > 0:
            raise ValueError(f'unsupported arguments {", ".join(unsupported_arguments)}')

        expected_matches = int(arguments.get('expected_matches', '1'))
        as_json = True if arguments.get('as_json', 'False') == 'True' else False
    else:
        expected_matches = 1
        as_json = False

    # latest request
    request = grizzly.scenario.tasks()[-1]

    if not isinstance(request, RequestTask):
        raise ValueError('latest task was not a request')

    if request.response.content_type == TransformerContentType.UNDEFINED:
        raise ValueError('content type is not set for latest request')

    if '{{' in match_with and '}}' in match_with:
        grizzly.scenario.orphan_templates.append(match_with)

    if '{{' in expression and '}}' in expression:
        grizzly.scenario.orphan_templates.append(expression)

    handler: ResponseHandlerAction

    if action == ResponseAction.SAVE:
        if variable is None:
            raise ValueError('variable is not set')

        handler = SaveHandlerAction(
            variable,
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )
    elif action == ResponseAction.VALIDATE:
        if condition is None:
            raise ValueError('condition is not set')

        handler = ValidationHandlerAction(
            condition,
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )

    if target == ResponseTarget.METADATA:
        add_listener = request.response.handlers.add_metadata
    elif target == ResponseTarget.PAYLOAD:
        add_listener = request.response.handlers.add_payload

    add_listener(handler)


def add_save_handler(grizzly: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, variable: str) -> None:
    _add_response_handler(grizzly, target, ResponseAction.SAVE, expression=expression, match_with=match_with, variable=variable)


def add_validation_handler(grizzly: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, condition: bool) -> None:
    _add_response_handler(grizzly, target, ResponseAction.VALIDATE, expression=expression, match_with=match_with, condition=condition)


def normalize_step_name(step_name: str) -> str:
    return re.sub(r'"[^"]*"', '""', step_name)


def get_task_client(endpoint: str) -> Type[ClientTask]:
    scheme = urlparse(endpoint).scheme

    assert scheme is not None and len(scheme) > 0, f'could not find scheme in "{endpoint}"'

    task_client = client.available.get(scheme, None)

    assert task_client is not None, f'no client task registered for {scheme}'

    return task_client


def is_template(text: str) -> bool:
    return '{{' in text and '}}' in text
