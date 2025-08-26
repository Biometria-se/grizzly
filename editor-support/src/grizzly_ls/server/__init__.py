from __future__ import annotations

import platform
import re
import signal
import subprocess
import sys
from collections import deque
from contextlib import suppress
from logging import ERROR
from os import environ, fsdecode
from os.path import pathsep, sep
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from tempfile import gettempdir
from time import sleep
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

from behave.matchers import ParseMatcher
from lsprotocol import types as lsp
from pip._internal.configuration import Configuration as PipConfiguration
from pip._internal.exceptions import ConfigurationError as PipConfigurationError
from pygls.capabilities import get_capability
from pygls.server import LanguageServer

from grizzly_ls import __version__
from grizzly_ls.constants import COMMAND_REBUILD_INVENTORY, COMMAND_RENDER_GHERKIN, COMMAND_RUN_DIAGNOSTICS, FEATURE_INSTALL, LANGUAGE_ID
from grizzly_ls.text import (
    Normalizer,
    find_language,
    format_arg_line,
    get_current_line,
    get_step_parts,
)
from grizzly_ls.utils import LogOutputChannelLogger, run_command

from .commands import render_gherkin
from .features.code_actions import generate_quick_fixes
from .features.completion import (
    complete_expression,
    complete_keyword,
    complete_metadata,
    complete_step,
    complete_variable_name,
    get_trigger,
)
from .features.definition import get_file_url_definition, get_step_definition
from .features.diagnostics import validate_gherkin
from .inventory import compile_inventory, compile_keyword_inventory
from .progress import Progress

if TYPE_CHECKING:  # pragma: no cover
    from types import FrameType

    from pygls.workspace import TextDocument

    from grizzly_ls.model import Step


class GrizzlyLanguageServer(LanguageServer):
    logger: LogOutputChannelLogger

    verbose: bool
    variable_pattern: re.Pattern[str] = re.compile(r'(.*ask for value of variable "([^"]*)"$|.*value for variable "([^"]*)" is ".*?"$)')

    file_ignore_patterns: list[str]
    root_path: Path
    index_url: str | None
    behave_steps: dict[str, list[ParseMatcher]]
    steps: dict[str, list[Step]]
    keywords: list[str]
    keywords_any: ClassVar[list[str]] = []
    keywords_headers: ClassVar[list[str]] = []
    keywords_all: ClassVar[list[str]] = []
    client_settings: dict[str, Any]
    startup_messages: deque[tuple[str, int]]

    _language: str
    localizations: dict[str, list[str]]

    normalizer: Normalizer

    markup_kind: lsp.MarkupKind

    def add_startup_error_message(self, message: str) -> None:
        self.startup_messages.append((message, ERROR))

    def __init__(self) -> None:
        super().__init__('grizzly-ls', __version__)

        self.logger = LogOutputChannelLogger(self)
        self.verbose = False

        self.index_url = environ.get('PIP_EXTRA_INDEX_URL', None)
        self.behave_steps = {}
        self.steps = {}
        self.keywords = []
        self.markup_kind = lsp.MarkupKind.Markdown  # assume, until initialized request
        self.language = 'en'  # assumed default
        self.file_ignore_patterns = []
        self.client_settings = {}
        self.startup_messages = deque()

        # monkey patch functions to short-circuit them (causes problems in this context)
        with suppress(ModuleNotFoundError):
            import gevent.monkey  # noqa: PLC0415

            gevent.monkey.patch_all = lambda: None

        def _signal(signum: int | signal.Signals, frame: FrameType) -> None:  # noqa: ARG001
            return

        signal.signal = _signal  # type: ignore[assignment]

    @property
    def language(self) -> str:
        return self._language

    @language.setter
    def language(self, value: str) -> None:
        if not hasattr(self, '_language') or self._language != value:
            self._language = value
            compile_keyword_inventory(self)
            name = self.localizations.get('name', 'unknown')
            self.logger.info(f'language detected: {name} ({value})')

    def get_language_key(self, keyword: str) -> str:
        keyword = keyword.rstrip(' :')

        for key, values in self.localizations.items():
            if keyword in values:
                return key

        message = f'"{keyword}" is not a valid keyword for language "{self.language}"'
        raise ValueError(message)

    def get_base_keyword(self, position: lsp.Position, text_document: TextDocument) -> str:
        lines = list(reversed(text_document.source.splitlines()[: position.line + 1]))

        current_line = lines[0]
        base_keyword, _ = get_step_parts(current_line)

        if base_keyword is None:
            message = f'unable to get keyword from "{current_line}"'
            raise ValueError(message)

        base_keyword = base_keyword.rstrip(' :')

        if base_keyword not in self.keywords_any and base_keyword in self.keywords_all:
            return base_keyword

        for line in lines[1:]:
            step_keyword, _ = get_step_parts(line)

            if step_keyword is None:  # pragma: no cover
                continue

            step_keyword = step_keyword.rstrip(' :')

            if step_keyword not in self.keywords_all:  # pragma: no cover
                continue

            if step_keyword not in self.keywords_any:
                return step_keyword

        return base_keyword  # pragma: no cover

    def _normalize_step_expression(self, step: ParseMatcher | str) -> list[str]:
        pattern = step.pattern if isinstance(step, ParseMatcher) else step

        return self.normalizer(pattern)

    def _find_help(self, line: str) -> str | None:
        keyword, expression = get_step_parts(line)

        if expression is None or keyword is None:
            return None

        possible_help: dict[str, str] = {}

        key = self.get_language_key(keyword)
        expression = re.sub(r'"[^"]*"', '""', expression)

        for steps in self.steps.values():
            for step in steps:
                if step.expression.strip() == expression.strip() and (key in (keyword.lower(), 'step')):
                    return step.help
                if step.expression.startswith(expression) and step.help is not None:
                    possible_help.update({step.expression: step.help})

        if len(possible_help) < 1:
            return None

        return possible_help[sorted(possible_help.keys(), reverse=True)[0]]


class InstallError(Exception):
    def __init__(self, *args: Any, backend: str | None = None, stdout: str | bytes | None = None, stderr: str | bytes | None = None) -> None:
        super().__init__(*args)

        self.stdout = stdout
        self.stderr = stderr
        self.backend = backend


class ConfigurationError(Exception):
    pass


def _run_uv(cmd: list[str]) -> CompletedProcess:  # pragma: no cover
    from uv import find_uv_bin  # noqa: PLC0415

    uv = fsdecode(find_uv_bin())
    env = environ.copy()
    env.update({'UV_INTERNAL__PARENT_INTERPRETER': sys.executable})

    return subprocess.run([uv, *cmd], check=False)


def _run_venv(path: Path, *, with_pip: bool) -> None:  # pragma: no cover
    from venv import create as venv_create  # noqa: PLC0415

    venv_create(path, with_pip=with_pip)


def _create_virtual_environment(path: Path, python_version: str) -> None:
    # prefer uv over virtualenv
    try:
        rc = _run_uv(['venv', '--managed-python', '--python', python_version, path.as_posix()])

        if rc.returncode != 0:
            raise InstallError(backend='uv', stdout=rc.stdout, stderr=rc.stderr)
    except ModuleNotFoundError:
        try:
            _run_venv(path, with_pip=True)
        except CalledProcessError as e:
            raise InstallError(backend='virtualenv', stdout=e.stdout, stderr=e.stderr) from None


def use_virtual_environment(ls: GrizzlyLanguageServer, project_name: str, env: dict[str, str]) -> Path | None:
    virtual_environment = Path(gettempdir()) / f'grizzly-ls-{project_name}'
    has_venv = virtual_environment.exists()
    python_version = '.'.join(str(v) for v in sys.version_info[:2])

    ls.logger.debug(f'looking for venv at {virtual_environment!s}, {has_venv=}')

    if not has_venv:
        try:
            _create_virtual_environment(virtual_environment, python_version)
        except InstallError as e:
            ls.logger.exception(f'failed to create virtual environment with {e.backend}', notify=True)

            error: list[str] = []
            if e.stderr is not None:
                stderr = e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr
                error.append(f'stderr={stderr}')

            if e.stdout is not None:
                stdout = e.stdout.decode() if isinstance(e.stdout, bytes) else e.stdout
                error.append(f'stdout={stdout}')

            if error:
                ls.logger.error('\n'.join(error))  # noqa: TRY400

            raise InstallError from None

    bin_dir = 'Scripts' if platform.system() == 'Windows' else 'bin'

    paths = [
        str(virtual_environment / bin_dir),
        env.get('PATH', ''),
    ]
    env.update(
        {
            'PATH': pathsep.join(paths),
            'VIRTUAL_ENV': str(virtual_environment),
            'PYTHONPATH': str(ls.root_path / 'features'),
        }
    )

    rc, output = run_command(['python', '-m', 'ensurepip'], env=env)

    if rc != 0:
        ls.logger.error(f'failed to ensure pip is installed for venv {project_name}', notify=True)
        ls.logger.error(f'ensurepip error:\n{"".join(output)}')

        raise InstallError

    if ls.index_url is not None:
        index_url_parsed = urlparse(ls.index_url)
        if index_url_parsed.username is None or index_url_parsed.password is None:
            ls.logger.error(
                'global.index-url does not contain username and/or password, check your configuration!',
                notify=True,
            )
            raise InstallError

        env.update(
            {
                'PIP_EXTRA_INDEX_URL': ls.index_url,
            }
        )

    # modify sys.path to use modules from virtual environment when compiling inventory
    venv_sys_path = virtual_environment / 'lib' / f'python{python_version}/site-packages'
    sys.path.append(venv_sys_path.as_posix())

    return virtual_environment


def pip_install_upgrade(ls: GrizzlyLanguageServer, project_name: str, executable: str, requirements_file: Path, env: dict[str, str]) -> None:
    project_path = Path(gettempdir()) / f'grizzly-ls-{project_name}'
    project_age_file = project_path / '.age'

    if project_age_file.exists() and requirements_file.lstat().st_mtime <= project_age_file.lstat().st_mtime:
        ls.logger.debug(f'{requirements_file.as_posix()} is not newer than {project_age_file.as_posix()}, no need to install or upgrade')
        return

    action = 'install' if not project_age_file.exists() else 'upgrade'

    ls.logger.debug(f'{action} from {requirements_file.as_posix()}')

    rc, output = run_command(
        [
            executable,
            '-m',
            'pip',
            'install',
            '--upgrade',
            '-r',
            requirements_file.as_posix(),
        ],
        env=env,
        cwd=project_path,
    )

    for line in output:
        if line.strip().startswith('ERROR:'):
            _, line = line.split('ERROR:', 1)  # noqa: PLW2901
            log_method = ls.logger.error
        elif rc == 0:
            log_method = ls.logger.debug
        else:
            log_method = ls.logger.warning

        if len(line.strip()) > 1:
            log_method(line.strip())

    ls.logger.debug(f'{action} done {rc=}')

    if rc != 0:
        ls.logger.error(
            f'failed to {action} from {requirements_file.as_posix()}',
            notify=True,
        )
        raise InstallError

    project_age_file.parent.mkdir(parents=True, exist_ok=True)
    project_age_file.touch()


server = GrizzlyLanguageServer()


@server.feature(FEATURE_INSTALL)
def install(ls: GrizzlyLanguageServer, *_args: Any) -> None:
    """See https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#initialize.

    > Until the server has responded to the initialize request with an InitializeResult, the client must not send any
    > additional requests or notifications to the server. In addition the server is not allowed to send any requests
    > or notifications to the client until it has responded with an InitializeResult

    This custom feature handles being able to send progress report of the, slow, process of installing dependencies needed
    for it to function properly on the project it is being used.
    """
    ls.logger.debug(f'{FEATURE_INSTALL}: installing')

    try:
        with Progress(ls, 'grizzly-ls') as progress:
            progress.report('loading extension', 1)
            # <!-- should a virtual environment be used?
            use_venv = ls.client_settings.get('use_virtual_environment', True)
            executable = 'python' if use_venv else sys.executable
            # // -->

            ls.logger.debug(f'workspace root: {ls.root_path.as_posix()} (use virtual environment: {use_venv!r})')

            env = environ.copy()
            project_name = ls.root_path.stem

            if use_venv:
                progress.report('setting up virtual environment', 10)
                virtual_environment = use_virtual_environment(ls, project_name, env)
            else:
                virtual_environment = None

            progress.report('virtual environment done', 40)

            requirements_file = ls.root_path / 'requirements.txt'
            if not requirements_file.exists():
                ls.logger.error(
                    f'project "{project_name}" does not have a requirements.txt in {ls.root_path.as_posix()}',
                    notify=True,
                )
                raise InstallError

            # pip install (slow operation) if:
            # - age file does not exist
            # - requirements file has been modified since age file was last touched
            progress.report('preparing step dependencies', 60)
            pip_install_upgrade(ls, project_name, executable, requirements_file, env)

            try:
                # <!-- compile inventory
                progress.report('building step inventory', 80)
                compile_inventory(ls)
                # // ->
            except ModuleNotFoundError:
                ls.logger.exception(
                    'failed to create step inventory',
                    notify=True,
                )
                raise InstallError from None
            finally:
                if use_venv and virtual_environment is not None and virtual_environment.as_posix() in sys.path[-1]:
                    # always restore to original value
                    sys.path.pop()

            progress.report('extension done', 100)
    except Exception as e:
        if not isinstance(e, InstallError):
            ls.logger.exception('failed to install extension, check output', notify=True)

        return

    # validate all open text documents after extension has been installed
    try:
        for text_document in ls.workspace.text_documents.values():
            if text_document.language_id != LANGUAGE_ID:
                continue

            diagnostics = validate_gherkin(ls, text_document)
            ls.publish_diagnostics(text_document.uri, diagnostics)
    except:
        ls.logger.exception('failed to run diagnostics on all opened files', notify=True)


def _configuration_index_url(ls: GrizzlyLanguageServer) -> None:
    """Command-line argument > pip configuration > vscode extension."""
    # no index-url specified as argument, check if we have it in pip configuration
    if ls.index_url is None:
        pip_config = PipConfiguration(isolated=False)
        with suppress(PipConfigurationError):
            pip_config.load()
            ls.index_url = pip_config.get_value('global.index-url')

    # no index-url specified in pip config, check if we have it in extension configuration
    if ls.index_url is None:
        ls.index_url = ls.client_settings.get('pip_extra_index_url', None)
        if ls.index_url is not None and len(ls.index_url.strip()) < 1:  # setting was an empty string
            ls.index_url = None

    if ls.index_url is not None:
        ls.logger.info(f'using pip extra index url: {ls.index_url}')


def _configuration_variable_pattern(ls: GrizzlyLanguageServer) -> None:
    variable_patterns = ls.client_settings.get('variable_pattern', [])
    if len(variable_patterns) < 1:
        return

    # validate and normalize patterns
    normalized_variable_patterns: set[str] = set()
    try:
        for variable_pattern in variable_patterns:
            original_variable_pattern = variable_pattern
            if not variable_pattern.startswith('.*') and not variable_pattern.startswith('^'):
                variable_pattern = f'.*{variable_pattern}'  # noqa: PLW2901

            if not variable_pattern.startswith('^'):
                variable_pattern = f'^{variable_pattern}'  # noqa: PLW2901

            if not variable_pattern.endswith('$'):
                variable_pattern = f'{variable_pattern}$'  # noqa: PLW2901

            pattern = re.compile(variable_pattern)

            if pattern.groups != 1:
                ls.logger.warning(f'variable pattern "{original_variable_pattern}" contains {pattern.groups} match groups, it must be exactly one', notify=True)
                continue

            normalized_variable_patterns.add(variable_pattern)
    except Exception:
        ls.logger.exception(
            f'variable pattern "{original_variable_pattern}" is not valid, check grizzly.variable_pattern setting',
            notify=True,
        )
        raise ConfigurationError from None

    if len(normalized_variable_patterns) > 0:
        variable_pattern = f'({"|".join(sorted(normalized_variable_patterns))})'
        ls.variable_pattern = re.compile(variable_pattern)


@server.feature(lsp.INITIALIZE)
def initialize(ls: GrizzlyLanguageServer, params: lsp.InitializeParams) -> None:
    run_mode = 'embedded' if environ.get('GRIZZLY_RUN_EMBEDDED', 'false').lower() == 'true' else 'standalone'
    ls.logger.info(f'initializing language server {__version__} ({run_mode})')

    if params.root_path is None and params.root_uri is None:
        ls.logger.error(
            'neither root path or uri was received from client',
            notify=True,
        )
        return

    while ls.startup_messages:
        msg, level = ls.startup_messages.popleft()
        ls.logger.log(level, msg, exc_info=False, notify=True)

    try:
        parsed = urlparse(cast('str', params.root_uri))
        root_path = Path(unquote(url2pathname(parsed.path)) if params.root_uri is not None else cast('str', params.root_path))

        # fugly as hell
        if (not root_path.exists() and root_path.as_posix()[0] == sep and root_path.as_posix()[2] == ':') or (
            sys.platform == 'win32' and not root_path.exists() and root_path.as_posix()[0] == '/'
        ):
            root_path = Path(root_path.as_posix()[1:])

        ls.root_path = root_path

        client_settings = params.initialization_options
        if client_settings is not None:
            ls.client_settings = cast('dict[str, Any]', client_settings)

        markup_supported: list[lsp.MarkupKind] = get_capability(
            params.capabilities,
            'text_document.completion.completion_item.documentation_format',
            [lsp.MarkupKind.Markdown],
        )
        ls.markup_kind = lsp.MarkupKind.PlainText if len(markup_supported) < 1 else markup_supported[0]

        _configuration_index_url(ls)

        _configuration_variable_pattern(ls)

        # <!-- set file ignore patterns
        ls.file_ignore_patterns = ls.client_settings.get('file_ignore_patterns', [])
        # // -->

        # <!-- quick fix structure
        quick_fix = ls.client_settings.get('quick_fix', None)
        if quick_fix is None:
            ls.client_settings.update({'quick_fix': {}})
        # // -->

        # <!-- missing step impl template
        step_impl_template = ls.client_settings['quick_fix'].get('step_impl_template', None)
        if step_impl_template is None or step_impl_template.strip() == '':
            step_impl_template = "@{keyword}('{expression}')"
            ls.client_settings['quick_fix'].update({'step_impl_template': step_impl_template})
        # // ->

        ls.logger.info('done initializing extension')
    except Exception as e:
        if isinstance(e, ConfigurationError):
            return

        ls.logger.exception('failed to initialize extension', notify=True)


@server.feature(lsp.TEXT_DOCUMENT_COMPLETION)
def text_document_completion(
    ls: GrizzlyLanguageServer,
    params: lsp.CompletionParams,
) -> lsp.CompletionList:
    items: list[lsp.CompletionItem] = []

    if len(ls.steps.values()) < 1:
        ls.logger.error('no steps in inventory', notify=True)
        return lsp.CompletionList(
            is_incomplete=False,
            items=items,
        )

    try:
        text_document = ls.workspace.get_text_document(params.text_document.uri)
        line = get_current_line(text_document, params.position)

        trigger = line[: params.position.character]

        ls.logger.debug(f'{line=}, {params.position=}, {trigger=}')

        for trigger_characters, completion_func in [
            ('{{', complete_variable_name),
            ('{%', complete_expression),
        ]:
            if trigger_characters not in trigger:
                continue

            partial_value = get_trigger(trigger, trigger_characters)
            ls.logger.debug(f'{trigger_characters=}, {partial_value=}')

            if not isinstance(partial_value, bool):
                items = completion_func(ls, line, text_document, params.position, partial=partial_value)
                break

        if len(items) < 1:
            if line.strip().startswith('#'):
                items = complete_metadata(line, params.position)
            else:
                keyword, text = get_step_parts(line)

                if keyword is not None and keyword in ls.keywords:
                    base_keyword = ls.get_base_keyword(params.position, text_document)

                    ls.logger.debug(f'{keyword=}, {base_keyword=}, {text=}, {ls.keywords=}')
                    items = complete_step(ls, keyword, params.position, text, base_keyword=base_keyword)
                else:
                    ls.logger.debug(f'{keyword=}, {text=}, {ls.keywords=}')
                    items = complete_keyword(ls, keyword, params.position, text_document)
    except:
        ls.logger.exception('failed to complete step expression', notify=True)

    return lsp.CompletionList(
        is_incomplete=False,
        items=items,
    )


@server.feature(lsp.WORKSPACE_DID_CHANGE_CONFIGURATION)
def workspace_did_change_configuration(
    ls: GrizzlyLanguageServer,
    params: lsp.DidChangeConfigurationParams,
) -> None:
    ls.logger.debug(f'{lsp.WORKSPACE_DID_CHANGE_CONFIGURATION}: {params=}')  # pragma: no cover


@server.feature(lsp.TEXT_DOCUMENT_HOVER)
def text_document_hover(ls: GrizzlyLanguageServer, params: lsp.HoverParams) -> lsp.Hover | None:
    hover: lsp.Hover | None = None
    help_text: str | None = None
    text_document = ls.workspace.get_text_document(params.text_document.uri)
    current_line = get_current_line(text_document, params.position)
    keyword, step = get_step_parts(current_line)

    abort: bool = False

    try:
        abort = step is None or keyword is None or (ls.get_language_key(keyword) not in ls.steps and keyword not in ls.keywords_any)
    except:
        abort = True

    if abort or keyword is None:
        return None

    try:
        start = current_line.index(keyword)
        end = len(current_line) - 1

        help_text = ls._find_help(current_line)

        if help_text is None:
            return None

        if 'Args:' in help_text:
            pre, post = help_text.split('Args:', 1)
            text = '\n'.join([format_arg_line(arg_line) for arg_line in post.strip().split('\n')])

            help_text = f'{pre}Args:\n\n{text}\n'

        contents = lsp.MarkupContent(kind=ls.markup_kind, value=help_text)
        text_range = lsp.Range(
            start=lsp.Position(line=params.position.line, character=start),
            end=lsp.Position(line=params.position.line, character=end),
        )
        hover = lsp.Hover(contents=contents, range=text_range)
    except:
        ls.logger.exception('failed to get step expression help on hover', notify=True)

    return hover


@server.feature(lsp.TEXT_DOCUMENT_DID_CHANGE)
def text_document_did_change(ls: GrizzlyLanguageServer, params: lsp.DidChangeTextDocumentParams) -> None:
    text_document = ls.workspace.get_text_document(params.text_document.uri)

    try:
        ls.language = find_language(text_document.source)
    except ValueError:
        ls.language = 'en'


@server.feature(lsp.TEXT_DOCUMENT_DID_OPEN)
def text_document_did_open(ls: GrizzlyLanguageServer, params: lsp.DidOpenTextDocumentParams) -> None:
    text_document = ls.workspace.get_text_document(params.text_document.uri)

    if text_document.language_id != LANGUAGE_ID:
        return

    try:
        ls.language = find_language(text_document.source)
    except ValueError:
        ls.language = 'en'

    # if only validating on save, we should definitely do it now
    if ls.client_settings.get('diagnostics_on_save_only', True):
        try:
            diagnostics = validate_gherkin(ls, text_document)
            ls.publish_diagnostics(text_document.uri, diagnostics)
        except:
            ls.logger.exception('failed to run diagnostics on opened file', notify=True)


@server.feature(lsp.TEXT_DOCUMENT_DID_CLOSE)
def text_document_did_close(ls: GrizzlyLanguageServer, params: lsp.DidCloseTextDocumentParams) -> None:
    # always clear diagnostics when file is closed
    try:
        ls.publish_diagnostics(params.text_document.uri, None)
    except:
        ls.logger.exception('failed to clear diagnostics for closed file', notify=True)


@server.feature(lsp.TEXT_DOCUMENT_DID_SAVE)
def text_document_did_save(ls: GrizzlyLanguageServer, params: lsp.DidSaveTextDocumentParams) -> None:
    text_document = ls.workspace.get_text_document(params.text_document.uri)

    if text_document.language_id != LANGUAGE_ID:
        return

    # if only validating on save, we should definitely do it now
    if ls.client_settings.get('diagnostics_on_save_only', True):
        try:
            diagnostics = validate_gherkin(ls, text_document)
            ls.publish_diagnostics(text_document.uri, diagnostics)
        except:
            ls.logger.exception('failed to run diagnostics on save', notify=True)


@server.feature(lsp.TEXT_DOCUMENT_DEFINITION)
def text_document_definition(
    ls: GrizzlyLanguageServer,
    params: lsp.DefinitionParams,
) -> list[lsp.LocationLink] | None:
    text_document = ls.workspace.get_text_document(params.text_document.uri)
    current_line = get_current_line(text_document, params.position)
    definitions: list[lsp.LocationLink] = []

    ls.logger.debug(f'{lsp.TEXT_DOCUMENT_DEFINITION}: {params=}')

    try:
        file_url_definitions = get_file_url_definition(ls, params, current_line)

        if len(file_url_definitions) > 0:
            definitions = file_url_definitions
        else:
            step_definition = get_step_definition(ls, params, current_line)
            if step_definition is not None:
                definitions = [step_definition]
    except:
        ls.logger.exception('failed to get document definitions', notify=True)

    return definitions if len(definitions) > 0 else None


@server.feature(
    lsp.TEXT_DOCUMENT_DIAGNOSTIC,
    lsp.DiagnosticOptions(
        identifier='behave',
        inter_file_dependencies=False,
        workspace_diagnostics=True,
    ),
)
def text_document_diagnostic(
    ls: GrizzlyLanguageServer,
    params: lsp.DocumentDiagnosticParams,
) -> lsp.DocumentDiagnosticReport:
    items: list[lsp.Diagnostic] = []
    if not ls.client_settings.get('diagnostics_on_save_only', True):
        try:
            text_document = ls.workspace.get_text_document(params.text_document.uri)
            items = validate_gherkin(ls, text_document)
        except:
            ls.logger.exception('failed to run document diagnostics', notify=True)

    return lsp.RelatedFullDocumentDiagnosticReport(
        items=items,
        kind=lsp.DocumentDiagnosticReportKind.Full,
    )


@server.feature(lsp.WORKSPACE_DIAGNOSTIC)
def workspace_diagnostic(ls: GrizzlyLanguageServer, *_args: Any, **_kwargs: Any) -> lsp.WorkspaceDiagnosticReport:
    report = lsp.WorkspaceDiagnosticReport(items=[])

    try:
        items: list[lsp.Diagnostic] = []
        try:
            first_text_document = next(iter(ls.workspace.text_documents.keys()))
        except:
            raise FileNotFoundError from None

        text_document = ls.workspace.get_text_document(first_text_document)

        if not ls.client_settings.get('diagnostics_on_save_only', True):
            items = validate_gherkin(ls, text_document)

        report.items = [
            lsp.WorkspaceFullDocumentDiagnosticReport(
                uri=text_document.uri,
                items=items,
                kind=lsp.DocumentDiagnosticReportKind.Full,
            )
        ]
    except FileNotFoundError:
        pass
    except:
        ls.logger.exception('failed to run workspace diagnostics', notify=True)

    return report


@server.feature(lsp.TEXT_DOCUMENT_CODE_ACTION)
def text_document_code_action(ls: GrizzlyLanguageServer, params: lsp.CodeActionParams) -> list[lsp.CodeAction] | None:
    try:
        diagnostics = params.context.diagnostics
        text_document = ls.workspace.get_text_document(params.text_document.uri)

        return generate_quick_fixes(ls, text_document, diagnostics) if len(diagnostics) > 0 else None
    except:
        ls.logger.exception('failed to generate quick fixes', notify=True)
        return None


@server.command(COMMAND_REBUILD_INVENTORY)
def command_rebuild_inventory(ls: GrizzlyLanguageServer, *_args: Any, **_kwargs: Any) -> None:
    ls.logger.info(f'executing command: {COMMAND_REBUILD_INVENTORY}')

    try:
        sleep(1.0)  # uuhm, some race condition?
        compile_inventory(ls)

        for text_document in ls.workspace.text_documents.values():
            if text_document.language_id != LANGUAGE_ID:
                continue

            diagnostics = validate_gherkin(ls, text_document)
            ls.publish_diagnostics(text_document.uri, diagnostics)
    except:
        ls.logger.exception('failed to rebuild inventory', notify=True)


@server.command(COMMAND_RUN_DIAGNOSTICS)
def command_run_diagnostics(ls: GrizzlyLanguageServer, *args: Any, **_kwargs: Any) -> None:
    uri = '<unknown>'
    try:
        arg = args[0][0]
        uri = arg.get('uri', {}).get('external', None)

        text_document = ls.workspace.get_text_document(uri)

        diagnostics = validate_gherkin(ls, text_document)
        ls.publish_diagnostics(text_document.uri, diagnostics)
    except Exception:
        ls.logger.exception(f'failed to run diagnostics on {uri}', notify=True)


@server.command(COMMAND_RENDER_GHERKIN)
def command_render_gherkin(ls: GrizzlyLanguageServer, *args: Any, **_kwargs: Any) -> tuple[bool, str | None]:
    options = cast('dict[str, str]', args[0][0])

    content = options.get('content', None)
    path = options.get('path', None)
    on_the_fly = options.get('on_the_fly', False)

    if content is None or path is None:
        content = 'no content to preview'
        ls.logger.error(content, notify=True)
        return False, content

    try:
        return True, render_gherkin(path, content)
    except Exception:
        if not on_the_fly:
            ls.logger.exception(f'failed to render {path}', notify=True)
            return False, f'failed to render\n{ls.logger.get_current_exception()}'

        return False, None
