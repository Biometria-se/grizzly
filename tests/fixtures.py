"""Fixtures used in tests."""
from __future__ import annotations

import inspect
import re
from contextlib import nullcontext, suppress
from cProfile import Profile
from getpass import getuser
from hashlib import sha1
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from shutil import rmtree
from tempfile import NamedTemporaryFile
from textwrap import dedent, indent
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Tuple, Type, cast
from urllib.parse import urlparse

import yaml
from behave.configuration import Configuration
from behave.model import Background
from behave.runner import Runner as BehaveRunner
from behave.step_registry import registry as step_registry
from geventhttpclient.header import Headers
from geventhttpclient.response import HTTPSocketPoolResponse
from jinja2.filters import FILTERS
from locust.contrib.fasthttp import FastRequest, FastResponse
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from pytest_mock.plugin import MockerFixture
from requests.models import CaseInsensitiveDict

from grizzly.context import GrizzlyContext
from grizzly.tasks import RequestTask
from grizzly.testdata.variables import destroy_variables
from grizzly.types import RequestMethod
from grizzly.types.behave import Context as BehaveContext
from grizzly.types.behave import Feature, Scenario, Step
from grizzly.types.locust import Environment, LocustRunner
from grizzly.utils import create_scenario_class_type, create_user_class_type

from .helpers import TestScenario, TestUser, onerror, run_command

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType
    from unittest.mock import MagicMock

    from _pytest.tmpdir import TempPathFactory

    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import Self
    from grizzly.users import GrizzlyUser

    from .webserver import Webserver


__all__ = [
    'AtomicVariableCleanupFixture',
    'LocustFixture',
    'BehaveFixture',
    'RequestTaskFixture',
    'GrizzlyFixture',
    'NoopZmqFixture',
    'MockerFixture',
]


class AtomicVariableCleanupFixture:
    def __call__(self) -> None:
        with suppress(Exception):
            GrizzlyContext.destroy()

        destroy_variables()

        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']


class LocustFixture:
    _test_context_root: Path
    _tmp_path_factory: TempPathFactory

    environment: Environment
    runner: LocustRunner

    def __init__(self, tmp_path_factory: TempPathFactory) -> None:
        self._tmp_path_factory = tmp_path_factory

    def __enter__(self) -> Self:
        test_context = self._tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        self._test_context_root = test_context.parent

        environ['GRIZZLY_CONTEXT_ROOT'] = str(self._test_context_root)
        self.environment = Environment()
        self.runner = self.environment.create_local_runner()

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']

        rmtree(self._test_context_root)

        return True


class BehaveFixture:
    locust: LocustFixture
    context: BehaveContext

    def __init__(self, locust_fixture: LocustFixture) -> None:
        self.locust = locust_fixture

    @property
    def grizzly(self) -> GrizzlyContext:
        return cast(GrizzlyContext, self.context.grizzly)

    def create_scenario(self, name: str) -> Scenario:
        return Scenario(filename=None, line=None, keyword='', name=name)

    def __enter__(self) -> Self:
        runner = BehaveRunner(
            config=Configuration(
                command_args=[],
                load_config=False,
            ),
        )
        context = BehaveContext(runner)
        context._runner = runner
        context.config.base_dir = self.locust._test_context_root
        context.feature = Feature(filename=None, line=None, keyword='Feature', name='BehaveFixtureFeature')
        context.scenario = Scenario(filename=None, line=None, keyword='Scenario', name='BehaveFixtureScenario')
        context.step = Step(filename=None, line=None, keyword='Step', step_type='step', name='')
        context.scenario.steps = [context.step]
        context.scenario.background = Background(filename=None, line=None, keyword='', steps=[context.step], name='')
        context._runner.step_registry = step_registry
        grizzly = GrizzlyContext()
        grizzly.state.locust = self.locust.runner
        context.grizzly = grizzly
        context.exceptions = {}

        self.context = context

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        with suppress(ValueError):
            GrizzlyContext.destroy()

        return True


REQUEST_TASK_TEMPLATE_CONTENTS = """{
    "result": {
        "id": "ID-{{ AtomicIntegerIncrementer.messageID }}",
        "date": "{{ AtomicDate.now }}",
        "variable": "{{ messageID }}",
        "item": {
            "description": "this is just a description"
        }
    }
}"""


class RequestTaskFixture:
    _tmp_path_factory: TempPathFactory
    context_root: str
    relative_path: str
    request: RequestTask
    test_context: Path
    behave_fixture: BehaveFixture

    def __init__(self, tmp_path_factory: TempPathFactory, behave_fixture: BehaveFixture) -> None:
        self._tmp_path_factory = tmp_path_factory
        self.behave_fixture = behave_fixture

    def __enter__(self) -> Self:
        self.test_context = self._tmp_path_factory.mktemp('example_payload') / 'requests'
        self.test_context.mkdir()
        request_file = self.test_context / 'payload.j2.json'
        request_file.touch()
        request_file.write_text(REQUEST_TASK_TEMPLATE_CONTENTS)
        request_path = str(request_file.parent)

        request = RequestTask(RequestMethod.POST, endpoint='/api/test', name='request_task')
        request.source = REQUEST_TASK_TEMPLATE_CONTENTS

        self.context_root = request_path
        self.request = request
        self.relative_path = str(request_file).replace(f'{request_path}/', '')

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        rmtree(Path(self.context_root).parent)

        return True


class GrizzlyFixture:
    request_task: RequestTaskFixture
    grizzly: GrizzlyContext
    behave: BehaveFixture

    def __init__(self, request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> None:
        self.request_task = request_task
        self.behave = behave_fixture

    @property
    def test_context(self) -> Path:
        return self.request_task.test_context.parent

    def __enter__(self) -> Self:
        environ['GRIZZLY_CONTEXT_ROOT'] = Path(self.request_task.context_root).parent.as_posix()
        self.grizzly = GrizzlyContext()
        self.grizzly.scenarios.clear()
        self.grizzly.scenarios.create(self.behave.create_scenario('test scenario'))
        self.grizzly.scenario.user.class_name = 'TestUser'
        self.grizzly.scenario.context['host'] = 'http://example.com'
        self.grizzly.scenario.tasks.add(self.request_task.request)
        self.grizzly.state.verbose = True

        return self

    def __call__(
        self,
        host: str = '',
        user_type: Optional[Type[GrizzlyUser]] = None,
        scenario_type: Optional[Type[GrizzlyScenario]] = None,
        *,
        no_tasks: Optional[bool] = False,
    ) -> GrizzlyScenario:
        if user_type is None:
            user_type = TestUser

        user_class_name = user_type.__name__ if user_type.__module__.startswith('grizzly.users') else f'{user_type.__module__}.{user_type.__name__}'

        if scenario_type is None:
            scenario_type = TestScenario

        scenario_class_name = scenario_type.__name__ if scenario_type.__module__ == 'grizzly.scenarios' else f'{scenario_type.__module__}.{scenario_type.__name__}'

        self.grizzly.scenario.user.class_name = user_class_name
        self.grizzly.scenario.context['host'] = host
        self.request_task.request.name = scenario_type.__name__

        scenario_type = create_scenario_class_type(scenario_class_name, self.grizzly.scenario)
        user_type = create_user_class_type(self.grizzly.scenario)

        self.behave.locust.environment = Environment(
            host=host,
            user_classes=[user_type],
        )

        user_type.host = host
        user = user_type(self.behave.locust.environment)

        if not no_tasks:
            user_type.tasks = [scenario_type]
        else:
            user_type.tasks = []

        scenario = scenario_type(parent=user)

        self.grizzly.state.locust = self.behave.locust.environment.create_local_runner()

        return scenario

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']

        with suppress(Exception):
            GrizzlyContext.destroy()

        # clean up filters, since we might've added custom ones
        filter_keys = reversed(list(FILTERS.keys()))
        for key in filter_keys:
            if key == 'tojson':
                break

            with suppress(KeyError):
                del FILTERS[key]

        return True


class ResponseContextManagerFixture:
    # borrowed from geventhttpclient.client._build_request
    def _build_request(self, method: str, request_url: str, body: Optional[str] = '', headers: Optional[Dict[str, Any]] = None) -> str:  # noqa: ARG002
        parsed = urlparse(request_url)

        request = method + ' ' + parsed.path + ' HTTP/1.1\r\n'

        for field, value in (headers or {}).items():
            request += field + ': ' + str(value) + '\r\n'
        request += '\r\n'

        return request

    def __call__(
        self,
        status_code: int,
        response_body: Optional[Any] = None,
        response_headers: Optional[Dict[str, Any]] = None,
        request_method: Optional[str] = None,
        request_body: Optional[Any] = None,
        request_headers: Optional[Dict[str, Any]] = None,
        url: Optional[str] = None,
        **kwargs: Dict[str, Any],
    ) -> FastResponseContextManager:
        name = kwargs['name']

        _build_request = self._build_request
        request_url = url

        class FakeGhcResponse(HTTPSocketPoolResponse):
            _headers_index: Optional[Headers]
            _sent_request: str
            _sock: Any

            def __init__(self) -> None:
                self._headers_index = None

                body: Optional[Any] = None
                if request_headers is not None and CaseInsensitiveDict(**request_headers).get('Content-Type', None) in [None, 'application/json']:
                    body = jsondumps(request_body or '')
                else:
                    body = request_body

                self._sent_request = _build_request(
                    request_method or '',
                    request_url or '',
                    body=body or '',
                    headers=request_headers,
                )
                self._sock = None

            def get_code(self) -> int:
                return status_code

        request = FastRequest(url, method=request_method, headers=Headers(), payload=request_body)
        for key, value in (request_headers or {}).items():
            request.headers.add(key, value)

        response = FastResponse(FakeGhcResponse(), request)
        response.headers = Headers()
        for key, value in (response_headers or {}).items():
            response.headers.add(key, value)

        if response_body is not None:
            response._cached_content = jsondumps(response_body).encode('utf-8')
        else:
            response._cached_content = None

        if request_body is not None:
            response.request_body = request_body

        response_context_manager = FastResponseContextManager(response, None, {})
        response_context_manager._entered = True
        response_context_manager.request_meta = {
            'method': None,
            'name': name,
            'response_time': 1.0,
            'content_size': 1337,
            'exception': None,
        }

        return response_context_manager


class NoopZmqFixture:
    _mocker: MockerFixture

    _mocks: Dict[str, MagicMock]

    def __init__(self, mocker: MockerFixture) -> None:
        self._mocker = mocker
        self._mocks = {}

    def __call__(self, prefix: str) -> None:
        targets = [
            'zmq.Context.term',
            'zmq.Context.__del__',
            'zmq.Socket.bind',
            'zmq.Socket.connect',
            'zmq.Socket.close',
            'zmq.Socket.send_json',
            'zmq.Socket.send',
            'zmq.Socket.recv_json',
            'zmq.Socket.recv_multipart',
            'zmq.Socket.send_multipart',
            'zmq.Socket.disconnect',
            'zmq.Socket.setsockopt',
            'zmq.Socket.send_string',
            'zmq.Poller.poll',
            'zmq.Poller.register',
            'gsleep',
        ]

        for target in targets:
            try:
                self._mocks.update({target: self._mocker.patch(
                    f'{prefix}.{target}',
                )})
            except AttributeError as e:  # noqa: PERF203
                if 'gsleep' in str(e):
                    continue

                raise

    def get_mock(self, attr: str) -> MagicMock:
        mock = self._mocks.get(attr, None)

        if mock is not None:
            return mock

        for full_attr, mock in self._mocks.items():
            _, last_part = full_attr.rsplit('.', 1)

            if last_part == attr:
                return mock

        message = f'no mocks for {attr}'
        raise AttributeError(message)


BehaveKeyword = Literal['Then', 'Given', 'And', 'When']


class End2EndValidator:
    name: str
    implementation: Any
    table: Optional[List[Dict[str, str]]]

    def __init__(
        self,
        name: str,
        implementation: Callable[[BehaveContext], None],
        table: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        self.name = name
        self.implementation = implementation
        self.table = table

    @property
    def expression(self) -> str:
        lines: List[str] = [f'Then run validator {self.name}_{self.implementation.__name__}']
        if self.table is not None and len(self.table) > 0:
            lines.append(f'  | {" | ".join(list(self.table[0].keys()))} |')
            lines.extend([f'  | {" | ".join(list(row.values()))} |' for row in self.table])

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
    step_start_webserver = """

@then(u'start webserver on master port "{{port:d}}"')
def step_start_webserver(context: Context, port: int) -> None:
    from grizzly.locust import on_master
    if not on_master(context):
        return

    from importlib.machinery import SourceFileLoader

    w = SourceFileLoader(
        'steps.webserver',
        '{}/features/steps/webserver.py',
    ).load_module('steps.webserver')

    webserver = w.Webserver(port)
    webserver.start()
"""

    _tmp_path_factory: TempPathFactory
    _env: Dict[str, str]
    _validators: Dict[Optional[str], List[End2EndValidator]]
    _distributed: bool

    _after_features: Dict[str, Callable[[BehaveContext, Feature], None]]
    _before_features: Dict[str, Callable[[BehaveContext, Feature], None]]

    _root: Optional[Path]

    _has_pymqi: Optional[bool]

    cwd: Path
    test_tmp_dir: Path
    _tmp_path_factory_basetemp: Optional[Path]
    webserver: Webserver
    profile: Optional[Profile]

    def __init__(self, tmp_path_factory: TempPathFactory, webserver: Webserver, *, distributed: bool) -> None:
        self.test_tmp_dir = (Path(__file__) / '..' / '..' / '.pytest_tmp').resolve()
        self.test_tmp_dir.mkdir(exist_ok=True)
        self._tmp_path_factory_basetemp = tmp_path_factory._basetemp
        self.webserver = webserver
        tmp_path_factory._basetemp = self.test_tmp_dir

        self._tmp_path_factory = tmp_path_factory
        self.cwd = Path.cwd()
        self._env = {}
        self._validators = {}
        self._root = None
        self._after_features = {}
        self._before_features = {}
        self._distributed = distributed
        self._has_pymqi = None
        self.profile = None

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
    def host(self) -> str:
        hostname = 'master' if self._distributed else 'localhost'

        return f'{hostname}:{self.webserver.port}'

    def has_pymqi(self) -> bool:
        if self._has_pymqi is None:
            requirements_file = self.root / 'requirements.txt'

            self._has_pymqi = '[mq]' in requirements_file.read_text()

        return self._has_pymqi

    def __enter__(self) -> Self:  # noqa: PLR0912, PLR0915
        if environ.get('PROFILE', None) is not None:
            self.profile = Profile()
            self.profile.enable()

        project_name = 'test-project'
        test_context = self._tmp_path_factory.mktemp('test_context')

        virtual_env_path = test_context / 'grizzly-venv'

        # create virtualenv
        rc, output = run_command(
            ['python3', '-m', 'venv', virtual_env_path.name],
            cwd=str(test_context),
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))

            raise

        path = environ.get('PATH', '')

        self._env.update({
            'PATH': f'{virtual_env_path!s}/bin:{path}',
            'VIRTUAL_ENV': str(virtual_env_path),
            'PYTHONPATH': environ.get('PYTHONPATH', '.'),
            'HOME': environ.get('HOME', '/'),
        })

        for env_key in ['SSH_AUTH_SOCK', 'GRIZZLY_MOUNT_CONTEXT']:
            env_value = environ.get(env_key, None)
            if env_value is not None:
                self._env.update({env_key: env_value})

        # install grizzly-cli
        rc, output = run_command(
            ['python3', '-m', 'pip', 'install', 'git+https://github.com/biometria-se/grizzly-cli.git@main'],
            cwd=str(test_context),
            env=self._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            raise

        # create grizzly project
        rc, output = run_command(
            ['grizzly-cli', 'init', '--yes', project_name],
            cwd=str(test_context),
            env=self._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            raise

        self._root = test_context / project_name

        assert self._root.is_dir()

        (self._root / 'features' / f'{project_name}.feature').unlink()

        # create base test-project ... steps.py
        with (self.root / 'features' / 'steps' / 'steps.py').open('w') as fd:
            fd.write('from importlib import import_module\n')
            fd.write('from typing import cast, Callable, Any\n\n')
            fd.write('from grizzly.types.behave import Context, then\n')
            fd.write('from grizzly.locust import on_master, on_worker, on_local\n')
            fd.write('from grizzly.context import GrizzlyContext, GrizzlyContextScenario\n')
            fd.write('from grizzly.tasks import GrizzlyTask\n')
            fd.write('from grizzly.scenarios import GrizzlyScenario\n')
            fd.write('from grizzly.steps import *\n')

        if self._distributed:
            # rewrite test requirements.txt to point to local code
            with (self.root / 'requirements.txt').open('r+') as fd:
                fd.truncate(0)
                fd.flush()
                fd.write('grizzly-loadtester\n')

            with (self.root / 'features' / 'steps' / 'steps.py').open('a') as fd:
                fd.write(
                    self.step_start_webserver.format(
                        str(self.root).replace(str(self.root), '/srv/grizzly'),
                    ),
                )

            # create steps/webserver.py
            webserver_source = self.test_tmp_dir.parent / 'tests' / 'webserver.py'
            webserver_destination = self.root / 'features' / 'steps' / 'webserver.py'
            webserver_destination.write_text(webserver_source.read_text())

            command = ['grizzly-cli', 'dist', '--project-name', self.root.name, 'build', '--no-cache', '--local-install']
            rc, output = run_command(
                command,
                cwd=str(self.cwd),
                env=self._env,
            )
            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                raise
        else:
            # install dependencies, in local venv
            grizzly_package = '.'
            if self.has_pymqi():
                grizzly_package = f'{grizzly_package}[mq]'
                self._env.update({'LD_LIBRARY_PATH': environ.get('LD_LIBRARY_PATH', '')})

            rc, output = run_command(
                ['python3', '-m', 'pip', 'install', grizzly_package],
                cwd=str(self.test_tmp_dir.parent),
                env=self._env,
            )

            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                raise

        return self

    @property
    def keep_files(self) -> bool:
        return environ.get('KEEP_FILES', None) is not None

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        # reset fixture basetemp
        self._tmp_path_factory._basetemp = self._tmp_path_factory_basetemp

        if self.profile is not None:
            self.profile.disable()
            self.profile.dump_stats('grizzly-e2e-tests.hprof')

        if exc is None:
            if self._distributed and not self.keep_files:
                rc, output = run_command(
                    ['grizzly-cli', 'dist', '--project-name', self.root.name, 'clean'],
                    cwd=str(self.root),
                    env=self._env,
                )

                if rc != 0:
                    print(''.join(output))

            if not self.keep_files:
                with suppress(AttributeError):
                    rmtree(self.root.parent, onerror=onerror)
            else:
                print(self._root)

        return True

    def add_validator(
        self,
        implementation: Callable[[BehaveContext], None],
        /,
        scenario: Optional[str] = None,
        table: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        callee = inspect.stack()[1].function

        if self._validators.get(scenario, None) is None:
            self._validators[scenario] = []

        self._validators[scenario].append(End2EndValidator(callee, implementation, table))

    def add_after_feature(self, implementation: Callable[[BehaveContext, Feature], None]) -> None:
        callee = inspect.stack()[1].function
        self._after_features[callee] = implementation

    def add_before_feature(self, implementation: Callable[[BehaveContext, Feature], None]) -> None:
        callee = inspect.stack()[1].function
        self._before_features[callee] = implementation

    def test_steps(self, /, scenario: Optional[List[str]] = None, background: Optional[List[str]] = None, identifier: Optional[str] = None) -> str:
        callee = inspect.stack()[1].function
        contents: List[str] = ['Feature:']
        add_user_count_step = True
        add_user_type_step = True
        add_spawn_rate_step = True

        if background is None:
            background = []

        if scenario is None:
            scenario = []

        # check required steps
        for step in background + scenario:
            if re.match(r'Given "[^"]*" user[s]?', step) is not None:
                add_user_count_step = False

            if re.match(r'Given.*user[s]? of type "[^"]*"', step) is not None:
                add_user_type_step = False

            if re.match(r'(And|Given) spawn rate is "[^"]*" user[s]? per second', step) is not None:
                add_spawn_rate_step = False

        if add_user_count_step:
            background.insert(0, 'Given "1" user')
            if add_spawn_rate_step:
                background.insert(1, 'And spawn rate is "1" user per second')

        if add_user_type_step:
            scenario.insert(0, f'Given a user of type "RestApi" load testing "http://{self.host}"')

        if add_spawn_rate_step and not add_user_count_step:
            background.append('Given spawn rate is "1" user per second')

        if self._distributed and not any(step.strip().startswith('Then start webserver on master port') for step in background):
            background.append(f'Then start webserver on master port "{self.webserver.port}"')

        contents.append('  Background: common configuration')
        contents.extend([f'    {step}' for step in background])
        contents.append('')

        contents.append(f'  Scenario: {callee}')
        contents.extend([f'    {step}' for step in scenario or []])
        contents.append('    Then log message "dummy"\n')

        return self.create_feature(
            '\n'.join(contents),
            name=callee,
            identifier=identifier,
        )

    def create_feature(self, contents: str, name: Optional[str] = None, identifier: Optional[str] = None) -> str:  # noqa: C901, PLR0915
        if name is None:
            name = inspect.stack()[1].function

        if identifier is not None:
            identifier = sha1(identifier.encode()).hexdigest()[:8]  # noqa: S324
            name = f'{name}_{identifier}'

        feature_lines = contents.strip().split('\n')
        feature_lines[0] = f'Feature: {name}'
        steps_file = self.root / 'features' / 'steps' / 'steps.py'
        environment_file = self.root / 'features' / 'environment.py'

        scenario: Optional[str] = None
        indentation = '    '
        modified_feature_lines: List[str] = []
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
                            nr += offset  # noqa: PLW2901
                            validator_expression = indent(f'{validator.expression}', prefix=indentation * 2)
                            index = nr
                            while modified_feature_lines[index].strip() == '' or 'Scenario:' in modified_feature_lines[index]:
                                index -= 1

                            index += 1
                            modified_feature_lines.insert(index, validator_expression)

                            offset += 1

                if scenario_definition:
                    scenario = line.replace('Scenario:', '').strip()
                    indentation, _ = line.split('Scenario:', 1)

        modified_feature_lines.append('')

        contents = '\n'.join(modified_feature_lines)

        # write feature file
        with (self.root / 'features' / f'{name}.feature').open('w+') as fd:
            fd.write(contents)

        feature_file_name = fd.name.replace(f'{self.root}/', '')

        # cache current step implementations
        steps_impl = steps_file.read_text()

        # add step implementations
        with steps_file.open('a') as fd:
            added_validators: List[str] = []
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
            fd.write('from grizzly.types.behave import Context, Feature\n')
            fd.write('from grizzly.context import GrizzlyContext\n')
            fd.write(
                'from grizzly.behave import before_feature as grizzly_before_feature, '
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

                # "render" references to myself
                source = source.replace('{e2e_fixture.host}', self.host).replace('{e2e_fixture.webserver.auth_provider_uri}', self.webserver.auth_provider_uri)

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
        feature_file: str,
        env_conf: Optional[Dict[str, Any]] = None,
        testdata: Optional[Dict[str, str]] = None,
        project_name: Optional[str] = None,
        *,
        dry_run: bool = False,
    ) -> Tuple[int, List[str]]:
        env_conf_fd: Any
        if env_conf is not None:
            prefix = Path(feature_file).stem
            env_conf_fd = NamedTemporaryFile(delete=not self.keep_files, prefix=prefix, suffix='.yaml', dir=(self.root / 'environments'))
        else:
            env_conf_fd = nullcontext()

        if project_name is None:
            project_name = self.root.name

        with env_conf_fd as env_conf_file:
            command = [
                'grizzly-cli',
                self.mode,
                'run',
                '--yes',
                '--verbose',
                '-l', '/tmp/grizzly.log',  # noqa: S108
                feature_file,
            ]

            if dry_run:
                command.append('--dry-run')

            if self._distributed:
                command = command[:2] + ['--project-name', project_name] + command[2:]

            if env_conf is not None:
                env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
                env_conf_file.flush()
                env_conf_path = str(env_conf_file.name).replace(f'{self.root}/', '')
                command += ['-e', env_conf_path]

            if testdata is not None:
                for key, value in testdata.items():
                    command += ['-T', f'{key}={value}']

            rc, output = run_command(
                command,
                cwd=str(self.root),
                env=self._env,
            )

            if rc != 0:
                print('-' * 100)

                if self._distributed:
                    # get docker compose project
                    validate_command = command[:2] + ['--validate-config'] + command[2:]
                    _, output = run_command(
                        validate_command,
                        cwd=str(self.root),
                        env=self._env,
                    )
                    print(''.join(output))
                    print('-' * 100)

                    output = []

                    for container in ['master', 'worker']:
                        command = ['docker', 'container', 'logs', f'{project_name}-{getuser()}-{container}-1']
                        _, o = run_command(
                            command,
                            cwd=str(self.root),
                            env=self._env,
                        )

                        output += o

                print(''.join(output))
                print('-' * 100)

            return rc, output
