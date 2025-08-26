from __future__ import annotations

import inspect
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from lsprotocol import types as lsp

from grizzly_ls.text import get_step_parts

if TYPE_CHECKING:  # pragma: no cover
    from grizzly_ls.server import GrizzlyLanguageServer


def get_step_definition(ls: GrizzlyLanguageServer, params: lsp.DefinitionParams, current_line: str) -> lsp.LocationLink | None:
    step_definition: lsp.LocationLink | None = None

    keyword, expression = get_step_parts(current_line)

    if keyword is None or expression is None:
        return None

    expression = re.sub(r'"[^"]*"', '""', expression)
    for steps in ls.steps.values():
        for step in steps:
            if step.expression != expression:
                continue

            # support projects that wraps the behave step decorators
            step_func = getattr(step.func, '__wrapped__', step.func)

            if isinstance(step_func, staticmethod):
                step_func = step_func.__func__

            file_location = inspect.getfile(step_func)
            _, lineno = inspect.getsourcelines(step_func)

            text_range = lsp.Range(
                start=lsp.Position(line=lineno, character=0),
                end=lsp.Position(line=lineno, character=0),
            )
            step_definition = lsp.LocationLink(
                target_uri=Path(file_location).resolve().as_uri(),
                target_range=text_range,
                target_selection_range=text_range,
                origin_selection_range=lsp.Range(
                    start=lsp.Position(
                        line=params.position.line,
                        character=(len(current_line) - len(current_line.lstrip())),
                    ),
                    end=lsp.Position(
                        line=params.position.line,
                        character=len(current_line),
                    ),
                ),
            )

            # we have found what we are looking for
            break

    return step_definition


def get_file_url_definition(ls: GrizzlyLanguageServer, params: lsp.DefinitionParams, current_line: str) -> list[lsp.LocationLink]:  # noqa: PLR0915
    text_document = ls.workspace.get_text_document(params.text_document.uri)
    document_directory = Path(text_document.path).parent
    definitions: list[lsp.LocationLink] = []
    matches = re.finditer(r'"([^"]*)"', current_line, re.MULTILINE)

    stripped_line = current_line.strip()
    is_expression = stripped_line[:2] == '{%' and stripped_line[-2:] == '%}'

    line_variables: list[str] = []

    for variable_match in matches:
        variable_value = variable_match.group(1)

        target_line = 0
        target_char_start = 0
        target_char_end = 0

        if 'file://' in variable_value:
            file_match = re.search(r'.*(file:\/\/)([^\$]*)', variable_value)
            if not file_match:
                continue

            file_url = f'{file_match.group(1)}{file_match.group(2)}'

            if sys.platform == 'win32':  # pragma: no cover
                file_url = file_url.replace('\\', '/')
                file_url = file_url.replace('file:///', 'file://')

            file_parsed = urlparse(file_url)

            # relative or absolute?
            if file_parsed.netloc == '.':  # relative!
                relative_path = file_parsed.path
                relative_path = relative_path.removeprefix('/')

                payload_file = document_directory / relative_path
            else:  # absolute!
                payload_file = Path(f'{file_parsed.netloc}{file_parsed.path}')

            start_offset = file_match.start(1)
            end_offset = -1 if variable_value.endswith('$') else 0
        else:
            # this is quite grizzly specific...
            if is_expression:
                base_path = Path(text_document.path).parent
                variable_path = Path(variable_value)
                payload_file = variable_path.resolve() if variable_path.is_absolute() else (base_path / variable_path).resolve()

                # scenario name is the argument before the feature file
                if len(line_variables) == 1 and payload_file.exists():
                    scenario_name = line_variables[0]

                    payload_text = payload_file.read_text()
                    marker = f'Scenario: {scenario_name}'
                    scenario_pos = payload_text.index(marker) + len(marker)
                    payload_text_suffix = payload_text[:scenario_pos].split('\n')

                    target_line = len(payload_text_suffix) - 1
                    target_char_start = target_char_end = len(payload_text_suffix[-1])

                    ls.logger.debug(f'scenario {scenario_name} found at line {target_line} in {payload_file.as_posix()}')

                line_variables.append(variable_value)
            else:
                payload_file = ls.root_path / 'features' / 'requests' / variable_value

            start_offset = 0
            end_offset = 0

        # just some text
        if not payload_file.exists():
            continue

        start = variable_match.start(1) + start_offset
        end = variable_match.end(1) + end_offset

        # don't add link definition if cursor is out side of range for that link
        if params.position.character >= start and params.position.character <= end:
            text_range = lsp.Range(
                start=lsp.Position(line=target_line, character=target_char_start),
                end=lsp.Position(line=target_line, character=target_char_end),
            )

            definitions.append(
                lsp.LocationLink(
                    target_uri=payload_file.as_uri(),
                    target_range=text_range,
                    target_selection_range=text_range,
                    origin_selection_range=lsp.Range(
                        start=lsp.Position(line=params.position.line, character=start),
                        end=lsp.Position(line=params.position.line, character=end),
                    ),
                )
            )

    return definitions
