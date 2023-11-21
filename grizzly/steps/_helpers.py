from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, cast
from urllib.parse import urlparse

import jinja2 as j2

from grizzly.context import GrizzlyContext
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import ClientTask, client
from grizzly.testdata.utils import resolve_variable
from grizzly.types import RequestMethod, ResponseAction, ResponseTarget
from grizzly.users.base.response_handler import ResponseHandlerAction, SaveHandlerAction, ValidationHandlerAction
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments, split_value
from grizzly_extras.transformer import TransformerContentType

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context, Row

logger = logging.getLogger(__name__)


def create_request_task(
    context: Context,
    method: RequestMethod,
    source: Optional[str],
    endpoint: str,
    name: Optional[str] = None,
    substitutes: Optional[Dict[str, str]] = None,
    content_type: Optional[TransformerContentType] = None,
) -> RequestTask:
    return _create_request_task(context.config.base_dir, method, source, endpoint, name, substitutes=substitutes, content_type=content_type)


def _create_request_task(
    base_dir: str,
    method: RequestMethod,
    source: Optional[str],
    endpoint: str,
    name: Optional[str] = None,
    substitutes: Optional[Dict[str, str]] = None,
    content_type: Optional[TransformerContentType] = None,
) -> RequestTask:
    path = Path(base_dir) / 'requests'
    j2env = j2.Environment(
        autoescape=False,
        loader=j2.FileSystemLoader(path),
    )

    template: Optional[j2.Template] = None

    if source is not None:
        original_source = source

        try:
            possible_file = path / source
            if possible_file.is_file():
                with possible_file.open() as fd:
                    try:
                        # minify json files, to increase performance when jinja creates template
                        source = json.dumps(json.load(fd))
                    except json.decoder.JSONDecodeError:
                        # not json, so just use it as is
                        fd.seek(0)
                        source = fd.read()

                template = j2env.get_template(source)

        except j2.exceptions.TemplateNotFound:
            if name is None:
                name = original_source.replace(''.join(Path(original_source).suffixes), '')

        for key, value in (substitutes or {}).items():
            source = source.replace(f'{{{{ {key} }}}}', value)

    if name is None:
        name = '<unknown>'

    request = RequestTask(method, name=name, endpoint=endpoint)
    request._source = source
    request._template = template
    if content_type is not None:
        request.response.content_type = content_type

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
    *,
    in_scenario: Optional[bool] = True,
) -> List[Tuple[RequestTask, Dict[str, str]]]:
    grizzly = cast(GrizzlyContext, context.grizzly)

    scenario_tasks_count = len(grizzly.scenario.tasks())

    request_tasks: List[Tuple[RequestTask, Dict[str, str]]] = []

    table: List[Optional[Row]]
    content_type: Optional[TransformerContentType] = None

    if endpoint is not None and ('$env::' in endpoint or '$conf::' in endpoint):
        endpoint = cast(str, resolve_variable(grizzly, endpoint, guess_datatype=False, only_grizzly=True))

    table = context.table if context.table is not None else [None]

    for row in table:
        if endpoint is None:
            if scenario_tasks_count == 0:
                message = 'no endpoint specified'
                raise ValueError(message)

            last_request = grizzly.scenario.tasks()[-1]

            if not isinstance(last_request, RequestTask):
                message = 'previous task was not a request'
                raise ValueError(message)

            if last_request.method != method:
                message = 'cannot use endpoint from previous request, it has a different request method'
                raise ValueError(message)

            endpoint = last_request.endpoint
            content_type = last_request.response.content_type
        else:
            parsed = urlparse(endpoint)
            if len(parsed.netloc) > 0:
                message = f'endpoints should only contain path relative to {grizzly.scenario.context["host"]}'
                raise ValueError(message)

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
            request_tasks.append((request_task, substitutes))

    return request_tasks


def _add_response_handler(  # noqa: C901, PLR0912
    grizzly: GrizzlyContext,
    target: ResponseTarget,
    action: ResponseAction,
    expression: str,
    match_with: str,
    variable: Optional[str] = None,
    condition: Optional[bool] = None,
) -> None:
    if variable is not None and variable not in grizzly.state.variables:
        message = f'variable "{variable}" has not been declared'
        raise ValueError(message)

    if not len(grizzly.scenario.tasks()) > 0:
        message = 'no request source has been added!'
        raise ValueError(message)

    if len(expression) < 1:
        message = 'expression is empty'
        raise ValueError(message)

    if '|' in expression:
        expression, expression_arguments = split_value(expression)
        arguments = parse_arguments(expression_arguments)
        unsupported_arguments = get_unsupported_arguments(['expected_matches', 'as_json'], arguments)

        if len(unsupported_arguments) > 0:
            message = f'unsupported arguments {", ".join(unsupported_arguments)}'
            raise ValueError(message)

        expected_matches = arguments.get('expected_matches', '1')
        as_json = arguments.get('as_json', 'False') == 'True'
    else:
        expected_matches = '1'
        as_json = False

    # latest request
    request = grizzly.scenario.tasks()[-1]

    if not isinstance(request, RequestTask):
        message = 'latest task was not a request'
        raise TypeError(message)

    if request.response.content_type == TransformerContentType.UNDEFINED:
        message = 'content type is not set for latest request'
        raise ValueError(message)

    if '{{' in match_with and '}}' in match_with:
        grizzly.scenario.orphan_templates.append(match_with)

    if '{{' in expression and '}}' in expression:
        grizzly.scenario.orphan_templates.append(expression)

    handler: ResponseHandlerAction

    if action == ResponseAction.SAVE:
        if variable is None:
            message = 'variable is not set'
            raise ValueError(message)

        handler = SaveHandlerAction(
            variable,
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
        )
    elif action == ResponseAction.VALIDATE:
        if condition is None:
            message = 'condition is not set'
            raise ValueError(message)

        handler = ValidationHandlerAction(
            condition=condition,
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


def add_validation_handler(grizzly: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, *, condition: bool) -> None:
    _add_response_handler(grizzly, target, ResponseAction.VALIDATE, expression=expression, match_with=match_with, condition=condition)


def normalize_step_name(step_name: str) -> str:
    return re.sub(r'"[^"]*"', '""', step_name)


def get_task_client(grizzly: GrizzlyContext, endpoint: str) -> Type[ClientTask]:
    scheme = urlparse(endpoint).scheme

    if not (scheme is not None and len(scheme) > 0):
        message = f'could not find scheme in "{endpoint}"'
        raise AssertionError(message)

    task_client = client.available.get(scheme, None)

    assert task_client is not None, f'no client task registered for {scheme}'

    task_client.__scenario__ = grizzly.scenario

    return task_client
