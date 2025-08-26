from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import traceback
from contextlib import suppress
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Any, ClassVar, cast

from behave.parser import parse_feature
from jinja2.lexer import Token, TokenStream
from jinja2_simple_tags import StandaloneTag
from lsprotocol import types as lsp

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from behave.model import Scenario

    from grizzly_ls.server import GrizzlyLanguageServer

logger = logging.getLogger(__name__)


def run_command(
    command: list[str],
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> tuple[int, list[str]]:
    output: list[str] = []

    if env is None:
        env = os.environ.copy()

    if cwd is None:
        cwd = Path.cwd()

    logger.debug('executing command: "%s" in %s', ' '.join(command), cwd.as_posix())

    process = subprocess.Popen(
        command,
        env=env,
        cwd=cwd,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )

    try:
        while process.poll() is None:
            stdout = process.stdout
            if stdout is None:  # pragma: no cover
                break

            buffer = stdout.readline()
            if not buffer:
                break

            try:
                output.append(buffer.decode())
            except Exception:
                logger.exception(buffer)

        process.terminate()
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        with suppress(Exception):  # pragma: no cover
            process.kill()

    process.wait()

    return process.returncode, output


class MissingScenarioError(Exception):
    scenario: str
    feature: str

    def __init__(self, *args: Any, scenario: str, feature: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.scenario = scenario
        self.feature = feature

    def __str__(self) -> str:
        return f'Scenario "{self.scenario}" does not exist in included feature "{self.feature}"'


class LogOutputChannelLogger:
    ls: GrizzlyLanguageServer
    logger: logging.Logger
    embedded: bool

    def __init__(self, ls: GrizzlyLanguageServer) -> None:
        self.ls = ls
        self.logger = logging.getLogger(ls.__class__.__name__)
        self.embedded = os.environ.get('GRIZZLY_RUN_EMBEDDED', 'false') == 'true'

    @classmethod
    def py2lsp_level(cls, level: int) -> lsp.MessageType:
        if level == logging.INFO:
            return lsp.MessageType.Info
        if level == logging.ERROR:
            return lsp.MessageType.Error
        if level == logging.WARNING:
            return lsp.MessageType.Warning
        if level == logging.DEBUG:
            return lsp.MessageType.Debug

        return lsp.MessageType.Log

    def get_current_exception(self) -> str | None:
        _, _, trace = sys.exc_info()

        if trace is None:
            return trace

        return f'Stack trace:\n{"".join(traceback.format_tb(trace))}'

    def log(self, level: int, message: str, *, exc_info: bool, notify: bool) -> None:
        msg_type = self.py2lsp_level(level)
        if not self.embedded or self.ls.verbose:
            self.logger.log(level, message, exc_info=exc_info)

        if self.embedded:
            if exc_info:
                message = f'{message}\n{self.get_current_exception()}'
            self.ls.show_message_log(message, msg_type=msg_type)

        if notify:
            self.ls.show_message(message, msg_type=msg_type)

    def info(self, message: str, *, notify: bool = False) -> None:
        self.log(logging.INFO, message, exc_info=False, notify=notify)

    def error(self, message: str, *, notify: bool = False) -> None:
        self.log(logging.ERROR, message, exc_info=False, notify=notify)

    def debug(self, message: str, *, notify: bool = False) -> None:
        self.log(logging.DEBUG, message, exc_info=False, notify=notify)

    def warning(self, message: str, *, notify: bool = False) -> None:
        self.log(logging.WARNING, message, exc_info=False, notify=notify)

    def exception(self, message: str, *, notify: bool = False) -> None:
        self.log(logging.ERROR, message, exc_info=True, notify=notify)


class ScenarioTag(StandaloneTag):
    tags: ClassVar[set[str]] = {'scenario'}

    def preprocess(self, source: str, name: str | None, filename: str | None = None) -> str:
        self._source = source

        return cast('str', super().preprocess(source, name, filename))

    @classmethod
    def get_scenario_text(cls, name: str, file: Path) -> str:
        content = file.read_text()

        content_skel = re.sub(r'\{%.*%\}', '', content)
        content_skel = re.sub(r'\{\$.*\$\}', '', content_skel)

        assert len(content.splitlines()) == len(content_skel.splitlines()), 'oops, there is not a 1:1 match between lines!'

        feature = parse_feature(content_skel, filename=file.as_posix())
        if feature is None:
            message = f'{file.as_posix()} does not have any scenarios'
            raise ValueError(message)

        lines = content.splitlines()

        scenario_index: int = -1
        target_scenario: Scenario | None = None

        for index, scenario in enumerate(feature.scenarios):
            if scenario.name != name:
                continue

            target_scenario = scenario
            scenario_index = index
            break

        if target_scenario is None:
            raise MissingScenarioError(scenario=name, feature=file.as_posix())

        # check if there are scenarios after our scenario in the source
        next_scenario: Scenario | None = None
        with suppress(IndexError):
            next_scenario = feature.scenarios[scenario_index + 1]

        if next_scenario is None:  # last scenario, take everything until the end
            scenario_lines = lines[target_scenario.line :]
        else:  # take everything up until where the next scenario starts
            scenario_lines = lines[target_scenario.line : next_scenario.line - 1]
            if scenario_lines[-1] == '':  # if last line is an empty line, lets remove it
                scenario_lines.pop()

        # remove any scenario text/comments
        if len(scenario_lines) > 0:
            offset = 0

            if scenario_lines[0].strip() == '"""':
                try:
                    offset = scenario_lines[1:].index(scenario_lines[0]) + 1 + 1
                except:
                    offset = 0
            elif scenario_lines[0].strip().startswith('"""') and scenario_lines[0].strip().endswith('"""'):
                offset = 1

            if offset > 0:
                scenario_lines = scenario_lines[offset:]

            # first line can have incorrect indentation
            scenario_lines[0] = dedent(scenario_lines[0])

        return '\n'.join(scenario_lines)

    def render(self, scenario: str, feature: str, **variables: str) -> str:
        feature_file = Path(feature)

        # check if relative to parent feature file
        if not feature_file.exists():
            feature_file = (self.environment.feature_file.parent / feature).resolve()

        scenario_content = self.get_scenario_text(scenario, feature_file)

        ignore_errors = getattr(self.environment, 'ignore_errors', False)

        # <!-- sub-render included scenario
        errors_unused: set[str] = set()
        errors_undeclared: set[str] = set()

        # tag has specified variables, so lets "render"
        for name, value in variables.items():
            variable_template = f'{{$ {name} $}}'
            if variable_template not in scenario_content:
                errors_unused.add(name)
                continue

            scenario_content = scenario_content.replace(variable_template, str(value))

        # look for sub-variables that has not been rendered
        if not ignore_errors:
            if '{$' in scenario_content and '$}' in scenario_content:
                matches = re.finditer(r'\{\$ ([^$]+) \$\}', scenario_content, re.MULTILINE)

                for match in matches:
                    errors_undeclared.add(match.group(1))

            if len(errors_undeclared) + len(errors_unused) > 0:
                scenario_identifier = f'{feature}#{scenario}'
                buffer_error: list[str] = []
                if len(errors_unused) > 0:
                    errors_unused_message = '\n  '.join(errors_unused)
                    buffer_error.append(f'the following variables has been declared in scenario tag but not used in {scenario_identifier}:\n  {errors_unused_message}')
                    buffer_error.append('')

                if len(errors_undeclared) > 0:
                    errors_undeclared_message = '\n  '.join(errors_undeclared)
                    buffer_error.append(f'the following variables was used in {scenario_identifier} but was not declared in scenario tag:\n  {errors_undeclared_message}')
                    buffer_error.append('')

                message = '\n'.join(buffer_error)
                raise ValueError(message)

        # check if we have nested statements (`{% .. %}`), and render again if that is the case
        if '{%' in scenario_content and '%}' in scenario_content:
            environment = self.environment.overlay()
            environment.feature_file = feature_file
            template = environment.from_string(scenario_content)
            scenario_content = template.render()
        # // -->

        return scenario_content

    def filter_stream(self, stream: TokenStream) -> TokenStream | Iterable[Token]:  # type: ignore[return]
        """Everything outside of `{% scenario ... %}` (and `{% if ... %}...{% endif %}`) should be treated as "data", e.g. plain text.

        Overloaded from `StandaloneTag`, must match method signature, which is not `Generator`, even though we yield
        the result instead of returning.
        """
        in_scenario = False
        in_block_comment = False
        in_condition = False
        in_variable = False

        variable_begin_pos = -1
        variable_end_pos = 0
        block_begin_pos = -1
        block_end_pos = 0
        source_lines = self._source.splitlines()

        for token in stream:
            if token.type == 'block_begin':
                if stream.current.value in self.tags:  # {% scenario ... %}
                    in_scenario = True
                    current_line = source_lines[token.lineno - 1].lstrip()
                    in_block_comment = current_line.startswith('#')
                    block_begin_pos = self._source.index(token.value, block_begin_pos + 1)
                elif stream.current.value in ['if', 'endif']:  # {% if <condition> %}, {% endif %}
                    in_condition = True

            if in_scenario:
                if token.type == 'block_end' and in_block_comment:
                    in_block_comment = False
                    block_end_pos = self._source.index(token.value, block_begin_pos)
                    token_value = self._source[block_begin_pos : block_end_pos + len(token.value)]
                    filtered_token = Token(token.lineno, 'data', token_value)
                elif in_block_comment:
                    continue
                else:
                    filtered_token = token
            elif in_condition:
                filtered_token = token
            else:
                if token.type == 'variable_begin':
                    # Find variable start in the source
                    variable_begin_pos = self._source.index(token.value, variable_begin_pos + 1)
                    in_variable = True
                    continue
                elif token.type == 'variable_end':
                    # Find variable end in the source
                    variable_end_pos = self._source.index(token.value, variable_begin_pos)
                    # Extract the variable definition substring and use as token value
                    token_value = self._source[variable_begin_pos : variable_end_pos + len(token.value)]
                    in_variable = False
                elif in_variable:  # Variable templates is yielded when the whole block has been processed
                    continue
                else:
                    token_value = token.value

                filtered_token = Token(token.lineno, 'data', token_value)

            yield filtered_token

            if token.type == 'block_end':
                in_scenario = False
                in_condition = False
