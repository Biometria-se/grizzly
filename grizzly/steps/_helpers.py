from __future__ import annotations

import json
import logging
import re
from errno import ENAMETOOLONG
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union, cast
from urllib.parse import urlparse

import jinja2 as j2

from grizzly.context import GrizzlyContext
from grizzly.events.response_handler import ResponseHandlerAction, SaveHandlerAction, ValidationHandlerAction
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import ClientTask, HttpClientTask, client
from grizzly.testdata.utils import resolve_variable
from grizzly.types import RequestMethod, ResponseAction, ResponseTarget
from grizzly.utils import has_template
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments, split_value
from grizzly_extras.text import has_separator
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
    substitutes: Optional[dict[str, str]] = None,
    content_type: Optional[TransformerContentType] = None,
) -> RequestTask:
    path = Path(context.config.base_dir) / 'requests'

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

                if name is None:
                    name, _ = possible_file.name.split('.', 1)
        except (j2.exceptions.TemplateNotFound, OSError) as e:
            # `TemplateNotFound` inherits `OSError`...
            if not isinstance(e, j2.exceptions.TemplateNotFound) and e.errno != ENAMETOOLONG:
                raise

            if name is None:
                name = original_source.replace(''.join(Path(original_source).suffixes), '')

        for key, value in (substitutes or {}).items():
            source = source.replace(f'{{{{ {key} }}}}', value)

    if name is None:
        name = '<unknown>'

    request = RequestTask(method, name=name, endpoint=endpoint)
    request.source = source

    if content_type is not None:
        request.response.content_type = content_type

    return request


def add_request_response_status_codes(request: Union[RequestTask, HttpClientTask], status_list: str) -> None:
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
) -> list[tuple[RequestTask, dict[str, str]]]:
    grizzly = cast(GrizzlyContext, context.grizzly)

    scenario_tasks_count = len(grizzly.scenario.tasks())

    request_tasks: list[tuple[RequestTask, dict[str, str]]] = []

    table: list[Optional[Row]]
    content_type: Optional[TransformerContentType] = None

    if endpoint is not None and ('$env::' in endpoint or '$conf::' in endpoint):
        endpoint = cast(str, resolve_variable(grizzly.scenario, endpoint, guess_datatype=False, try_template=False))

    table = context.table if context.table is not None else [None]

    for row in table:
        if endpoint is None:
            assert scenario_tasks_count > 0, 'no endpoint specified'

            last_request = grizzly.scenario.tasks()[-1]

            assert isinstance(last_request, RequestTask), 'previous task was not a request'
            assert last_request.method == method, 'cannot use endpoint from previous request, it has a different request method'

            endpoint = last_request.endpoint
            content_type = last_request.response.content_type
        else:
            parsed = urlparse(endpoint)
            assert len(parsed.netloc) < 1, f'endpoints should only contain path relative to {grizzly.scenario.context["host"]}'

        orig_endpoint = endpoint
        orig_name = name
        orig_source = source

        substitutes: dict[str, str] = {}

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


def _add_response_handler(
    grizzly: GrizzlyContext,
    target: ResponseTarget,
    action: ResponseAction,
    expression: str,
    match_with: str,
    variable: Optional[str] = None,
    condition: Optional[bool] = None,
    default_value: Optional[str] = None,
) -> None:
    if variable is not None and variable not in grizzly.scenario.variables:
        message = f'variable "{variable}" has not been declared'
        raise AssertionError(message)

    assert len(grizzly.scenario.tasks()) > 0, 'no request source has been added'

    assert len(expression) > 0, 'expression is empty'

    if has_separator('|', expression):
        expression, expression_arguments = split_value(expression)
        arguments = parse_arguments(expression_arguments)
        unsupported_arguments = get_unsupported_arguments(['expected_matches', 'as_json'], arguments)

        assert len(unsupported_arguments) < 1, f'unsupported arguments {", ".join(unsupported_arguments)}'

        expected_matches = arguments.get('expected_matches', '1')
        as_json = arguments.get('as_json', 'False') == 'True'
    else:
        expected_matches = '1'
        as_json = False

    # latest request
    request = grizzly.scenario.tasks()[-1]

    assert isinstance(request, RequestTask), 'latest task was not a request'
    assert request.response.content_type != TransformerContentType.UNDEFINED, 'content type is not set for latest request'

    if has_template(match_with):
        grizzly.scenario.orphan_templates.append(match_with)

    if has_template(expression):
        grizzly.scenario.orphan_templates.append(expression)

    handler: ResponseHandlerAction

    if action == ResponseAction.SAVE:
        assert variable is not None, 'variable is not set'

        handler = SaveHandlerAction(
            variable,
            expression=expression,
            match_with=match_with,
            expected_matches=expected_matches,
            as_json=as_json,
            default_value=default_value,
        )
    elif action == ResponseAction.VALIDATE:
        assert condition is not None, 'condition is not set'

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


def add_save_handler(grizzly: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, variable: str, default_value: Optional[str]) -> None:
    _add_response_handler(grizzly, target, ResponseAction.SAVE, expression=expression, match_with=match_with, variable=variable, default_value=default_value)


def add_validation_handler(grizzly: GrizzlyContext, target: ResponseTarget, expression: str, match_with: str, *, condition: bool) -> None:
    _add_response_handler(grizzly, target, ResponseAction.VALIDATE, expression=expression, match_with=match_with, condition=condition)


def normalize_step_name(step_name: str) -> str:
    return re.sub(r'"[^"]*"', '""', step_name)


def get_task_client(grizzly: GrizzlyContext, endpoint: str) -> type[ClientTask]:
    scheme = urlparse(endpoint).scheme

    if not (scheme is not None and len(scheme) > 0):
        message = f'could not find scheme in "{endpoint}"'
        raise AssertionError(message)

    task_client = client.available.get(scheme, None)

    assert task_client is not None, f'no client task registered for {scheme}'

    task_client.__scenario__ = grizzly.scenario

    return task_client
