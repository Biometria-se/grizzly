"""Test command-line-interface pytest fixtures."""

from __future__ import annotations

import inspect
import re
import socket
import sys
from contextlib import closing, suppress
from cProfile import Profile
from getpass import getuser
from hashlib import sha1
from os import environ, linesep
from os.path import pathsep, sep
from pathlib import Path
from tempfile import gettempdir
from textwrap import dedent, indent
from typing import TYPE_CHECKING, Any, Literal, cast

from grizzly_cli.utils import rm_rf
from typing_extensions import Self

from test_cli.helpers import run_command

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from _pytest.tmpdir import TempPathFactory
    from behave.model import Feature
    from behave.runner import Context

__all__ = [
    'End2EndFixture',
]


BehaveKeyword = Literal['Then', 'Given', 'And', 'When']


class End2EndValidator:
    name: str
    implementation: Any
    table: list[dict[str, str]] | None

    def __init__(
        self,
        name: str,
        implementation: Callable[[Context], None],
        table: list[dict[str, str]] | None = None,
    ) -> None:
        self.name = name
        self.implementation = implementation
        self.table = table

    @property
    def expression(self) -> str:
        lines: list[str] = [f'Then run validator {self.name}_{self.implementation.__name__}']
        if self.table is not None and len(self.table) > 0:
            lines.append(f'  | {" | ".join(list(self.table[0].keys()))} |')
            lines.extend(f'  | {" | ".join(list(row.values()))} |' for row in self.table)

        return '\n'.join(lines)

    @property
    def impl(self) -> str:
        source_lines = inspect.getsource(self.implementation).split('\n')
        source_lines[0] = dedent(source_lines[0].replace('def ', f'def {self.name}_'))
        source = '\n'.join(source_lines)

        return f"""@then(u'run validator {self.name}_{self.implementation.__name__}')
def {self.name}_{self.implementation.__name__}_wrapper(context: Context) -> None:
    {dedent(source)}
    if on_local(context) or on_worker(context):
        {self.name}_{self.implementation.__name__}(context)
"""


class End2EndFixture:
    _tmp_path_factory: TempPathFactory
    _env: dict[str, str]
    _validators: dict[str | None, list[End2EndValidator]]
    _distributed: bool

    _after_features: dict[str, Callable[[Context, Feature], None]]
    _before_features: dict[str, Callable[[Context, Feature], None]]

    _root: Path | None
    _port: int | None = None

    cwd: Path

    profile: Profile | None

    def __init__(self, tmp_path_factory: TempPathFactory, *, distributed: bool) -> None:
        self._tmp_path_factory = tmp_path_factory
        self.cwd = Path.cwd()
        self._env = environ.copy() if sys.platform == 'win32' else {}
        self._validators = {}
        self._root = None
        self._after_features = {}
        self._before_features = {}
        self._distributed = distributed
        self.profile = None

        temp_dir = environ.get('GRIZZLY_TMP_DIR', gettempdir())

        self.log_file = Path(temp_dir) / 'grizzly.log'
        self.log_file.unlink(missing_ok=True)

    @property
    def mode_root(self) -> Path:
        if self._root is None:
            message = 'root is not set'
            raise AttributeError(message)

        if self._distributed:
            return Path('/srv/grizzly')

        return self._root

    @property
    def root(self) -> Path:
        if self._root is None:
            message = 'root is not set'
            raise AttributeError(message)

        return self._root

    @property
    def mode(self) -> str:
        return 'dist' if self._distributed else 'local'

    @property
    def webserver_port(self) -> int:
        if self._port is None:
            self._port = self.find_free_port()

        return self._port

    @property
    def host(self) -> str:
        host = 'master' if self._distributed else '127.0.0.1'

        return f'{host}:{self.webserver_port}'

    def find_free_port(self) -> int:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.bind(('', 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return cast('int', sock.getsockname()[1])

    def inject_webserver_module(self, path: Path) -> None:
        assert self._tmp_path_factory._basetemp is not None

        # create webserver module
        webserver_source = self._tmp_path_factory._basetemp.parent / 'tests' / 'test_cli' / 'webserver.py'
        webserver_destination = path / 'features' / 'steps' / 'webserver.py'
        webserver_destination.write_text(webserver_source.read_text())

    def start_webserver_step_impl(self, port: int) -> str:
        return f"""

@then(u'start webserver on master port "{port}"')
def step_start_webserver(context: Context) -> None:
    from grizzly.locust import on_worker
    if on_worker(context):
        return

    import logging
    from importlib.machinery import SourceFileLoader

    logger = logging.getLogger('webserver')

    w = SourceFileLoader(
        'steps.webserver',
        'features/steps/webserver.py',
    ).load_module('steps.webserver')

    webserver = w.Webserver({port})
    logger.info('starting webserver')
    webserver.start(logger)
    logger.info('webserver started')
"""

    def __enter__(self) -> Self:
        if environ.get('PROFILE', None) is not None:
            self.profile = Profile()
            self.profile.enable()
            self._env.update({'PROFILE': 'true'})

        self._root = self._tmp_path_factory.mktemp('test_context')

        virtual_env = environ.get('VIRTUAL_ENV')

        if virtual_env is None or f'{sep}hatch{sep}env{sep}virtual' not in virtual_env:
            virtual_env_path = self.root / 'venv'

            # create virtualenv
            rc, output = run_command(
                [sys.executable, '-m', 'venv', virtual_env_path.name],
                cwd=self.root,
            )

            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))

                with self.log_file.open('a+') as fd:
                    fd.write(''.join(output))

                raise
        else:
            virtual_env_path = Path(virtual_env)

        path = environ.get('PATH', '')

        virtual_env_bin_dir = 'Scripts' if sys.platform == 'win32' else 'bin'

        self._env.update(
            {
                'PATH': f'{virtual_env_path!s}{sep}{virtual_env_bin_dir}{pathsep}{path}',
                'VIRTUAL_ENV': f'{virtual_env_path!s}',
                'PYTHONPATH': environ.get('PYTHONPATH', '.'),
                'HOME': environ.get('HOME', '/'),
            }
        )

        if sys.platform == 'win32':
            self._env.update(
                {
                    'SYSTEMROOT': environ['SYSTEMROOT'],
                    'SYSTEMDRIVE': environ['SYSTEMDRIVE'],
                    'USERPROFILE': environ['USERPROFILE'],
                    'PYTHONIOENCODING': 'utf-8',
                    'PYTHONUTF8': '1',
                }
            )

        for env_key in ['SSH_AUTH_SOCK', 'GRIZZLY_MOUNT_CONTEXT']:
            env_value = environ.get(env_key, None)
            if env_value is not None:
                self._env.update({env_key: env_value})

        repo_root_path = (Path(__file__).parent / '..' / '..' / '..').resolve()
        command = ['uv', 'sync', '--active', '--locked', '--package', 'grizzly-loadtester']
        rc, output = run_command(
            command,
            cwd=repo_root_path,
            env=self._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            with self.log_file.open('a+') as fd:
                fd.write(''.join(output))
            raise

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        if exc is None:
            if environ.get('KEEP_FILES', None) is None:
                with suppress(AttributeError):
                    rm_rf(self.root)
            else:
                print(self._root)

        if self.profile is not None:
            self.profile.disable()
            self.profile.dump_stats('grizzly-cli-e2e-tests.hprof')

        return True

    def add_validator(
        self,
        implementation: Callable[[Context], None],
        scenario: str | None = None,
        table: list[dict[str, str]] | None = None,
    ) -> None:
        callee = inspect.stack()[1].function

        if self._validators.get(scenario, None) is None:
            self._validators[scenario] = []

        self._validators[scenario].append(End2EndValidator(callee, implementation, table))

    def add_after_feature(self, implementation: Callable[[Context, Feature], None]) -> None:
        callee = inspect.stack()[1].function
        self._after_features[callee] = implementation

    def add_before_feature(self, implementation: Callable[[Context, Feature], None]) -> None:
        callee = inspect.stack()[1].function
        self._before_features[callee] = implementation

    def modify_feature(self, feature_lines: list[str]) -> list[str]:
        scenario: str | None = None
        indentation = '    '
        modified_feature_lines: list[str] = []
        offset = 0  # number of added steps

        for nr, line in enumerate(feature_lines):
            modified_feature_lines.append(line)

            last_line = nr == len(feature_lines) - 1
            scenario_definition = line.strip().startswith('Scenario:')

            if scenario_definition or last_line:
                if scenario is not None:
                    validators = self._validators.get(scenario, self._validators.get(None, None))
                    if validators is not None:
                        for validator in validators:
                            validator_expression = indent(f'{validator.expression}', prefix=indentation * 2)
                            index = nr + offset
                            while modified_feature_lines[index].strip() == '' or 'Scenario:' in modified_feature_lines[index]:
                                index -= 1

                            index += 1
                            modified_feature_lines.insert(index, validator_expression)

                            offset += 1

                if scenario_definition:
                    scenario = line.replace('Scenario:', '').strip()
                    indentation, _ = line.split('Scenario:', 1)

        modified_feature_lines.append('')

        return modified_feature_lines

    def create_feature(self, contents: str, name: str | None = None, identifier: str | None = None) -> str:
        if name is None:
            name = inspect.stack()[1].function

        if identifier is not None:
            identifier = sha1(identifier.encode()).hexdigest()[:8]  # noqa: S324
            name = f'{name}_{identifier}'

        feature_lines = contents.strip().split('\n')
        feature_lines[0] = f'Feature: {name}'
        steps_file = self.root / 'features' / 'steps' / 'steps.py'
        environment_file = self.root / 'features' / 'environment.py'

        modified_feature_lines = self.modify_feature(feature_lines)

        contents = '\n'.join(modified_feature_lines)

        # write feature file
        with (self.root / 'features' / f'{name}.feature').open('w+') as fd:
            fd.write(contents)

        feature_file_name = fd.name.replace(f'{self.root}/', '')

        # cache current step implementations
        with steps_file.open('r') as fd:
            steps_impl = fd.read()

        # add step implementations
        with steps_file.open('a') as fd:
            added_validators: list[str] = []
            for validators in self._validators.values():
                for validator in validators:
                    # write expression and step implementation to steps/steps.py
                    if validator.impl not in steps_impl and validator.impl not in added_validators:
                        fd.write(f'\n\n{validator.impl}')
                        added_validators.append(validator.impl)

            added_validators = []

        # add after_feature hook, always write all of 'em
        with environment_file.open('w') as fd:
            fd.write('from typing import Any, Tuple, Dict, cast\n\n')
            fd.write('from behave.runner import Context\n')
            fd.write('from behave.model import Feature\n')
            fd.write('from grizzly.context import GrizzlyContext\n')
            fd.write(
                'from grizzly.environment import before_feature as grizzly_before_feature, '
                'after_feature as grizzly_after_feature, before_scenario, after_scenario, before_step\n\n',
            )

            fd.write('def before_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:\n')
            if len(self._before_features) > 0:
                for feature_name in self._before_features:
                    fd.write(f'    if feature.name == "{feature_name}":\n')
                    fd.write(f'        {feature_name}_before_feature(context, feature)\n\n')
            fd.write('    grizzly_before_feature(context, feature)\n\n')

            for key, before_feature_impl in self._before_features.items():
                source_lines = dedent(inspect.getsource(before_feature_impl)).split('\n')
                source_lines[0] = re.sub(r'^def .*?\(', f'def {key}_before_feature(', source_lines[0])
                source = '\n'.join(source_lines)

                fd.write(source + '\n\n')

            fd.write('def after_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:\n')
            fd.write('    grizzly_after_feature(context, feature)\n\n')
            if len(self._after_features) > 0:
                for feature_name in self._after_features:
                    fd.write(f'    if feature.name == "{feature_name}":\n')
                    fd.write(f'        {feature_name}_after_feature(context, feature)\n\n')

            for key, after_feature_impl in self._after_features.items():
                source_lines = dedent(inspect.getsource(after_feature_impl)).split('\n')
                source_lines[0] = re.sub(r'^def .*?\(', f'def {key}_after_feature(', source_lines[0])
                source = '\n'.join(source_lines)

                fd.write(source + '\n\n')

        # step validators are are now "burned"...
        self._validators.clear()

        return feature_file_name

    def execute(
        self,
        feature_file: Path,
        env_conf_file: str | None = None,
        testdata: dict[str, str] | None = None,
        cwd: Path | None = None,
        arguments: list[str] | None = None,
    ) -> tuple[int, list[str]]:
        if arguments is None:
            arguments = []

        command: list[str] = [
            'grizzly-cli',
            self.mode,
            'run',
            *arguments,
            '--yes',
            '--verbose',
            feature_file.as_posix(),
        ]

        if self._distributed:
            command = [*command[:2], '--project-name', self.root.name, *command[2:]]

        if env_conf_file is not None:
            command += ['-e', env_conf_file]

        if testdata is not None:
            for key, value in testdata.items():
                command += ['-T', f'{key}={value}']

        rc, output = run_command(
            command,
            cwd=cwd or self.mode_root,
            env=self._env,
        )

        if sys.platform == 'win32':
            output = [line.replace(linesep, '\n') for line in output]

        if rc != 0:
            print(''.join(output))

            with self.log_file.open('a+') as fd:
                fd.write(''.join(output))

                for container in ['master', 'worker'] if self._distributed else []:
                    command = ['docker', 'container', 'logs', f'{self.root.name}-{getuser()}_{container}_1']
                    _, output = run_command(
                        command,
                        cwd=self.root,
                        env=self._env,
                    )

                    print(''.join(output))
                    fd.write(''.join(output))

        return rc, output
