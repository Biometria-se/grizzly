from __future__ import annotations

import asyncio
import os
import sys
from contextlib import suppress
from importlib import reload as reload_module
from logging import DEBUG
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, Any, Literal

from grizzly_ls.server.progress import Progress
from grizzly_ls.utils import LogOutputChannelLogger
from lsprotocol import types as lsp
from lsprotocol.types import EXIT
from pip._internal.configuration import Configuration as PipConfiguration
from pygls.server import LanguageServer
from pygls.workspace import Workspace
from pytest_mock.plugin import MockerFixture
from typing_extensions import Self

from test_ls.helpers import rm_rf

if TYPE_CHECKING:
    from types import TracebackType
    from unittest.mock import MagicMock

    from _pytest.tmpdir import TempPathFactory
    from grizzly_ls.server import GrizzlyLanguageServer

__all__ = ['MockerFixture']


class CwdFixture:
    cwd: Path
    old_cwd: Path

    def __call__(self, cwd: Path) -> Self:
        self.cwd = cwd

        return self

    def __enter__(self) -> Self:
        self.old_cwd = Path.cwd()

        os.chdir(self.cwd)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        os.chdir(self.old_cwd)

        return exc is None


class DummyClient(LanguageServer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        @self.feature('window/workDoneProgress/create')
        def window_work_done_progress_create(*_args: Any, **_kwargs: Any) -> None:
            return

        @self.feature('textDocument/publishDiagnostics')
        def text_document_publish_diagnostics(*_args: Any, **_kwargs: Any) -> None:
            return


class LspFixture:
    client: LanguageServer
    server: GrizzlyLanguageServer

    _server_thread: Thread
    _client_thread: Thread

    datadir: Path

    def _reset_behave_runtime(self) -> None:
        from behave import step_registry

        step_registry.setup_step_decorators(None, step_registry.registry)

        import parse

        reload_module(parse)

    def __enter__(self) -> Self:
        self._reset_behave_runtime()
        cstdio, cstdout = os.pipe()
        sstdio, sstdout = os.pipe()

        def start(ls: LanguageServer, fdr: int, fdw: int) -> None:
            with suppress(Exception):
                ls.start_io(os.fdopen(fdr, 'rb'), os.fdopen(fdw, 'wb'))  # type: ignore[arg-type]

        from grizzly_ls.server import server

        server.logger.logger.setLevel(DEBUG)

        server.loop.close()
        server._owns_loop = False
        asyncio.set_event_loop(None)

        server.loop = asyncio.new_event_loop()

        self.server = server
        self.server.language = 'en'
        self._server_thread = Thread(target=start, args=(self.server, cstdio, sstdout), daemon=True)
        self._server_thread.start()

        self.client = DummyClient(loop=asyncio.new_event_loop(), name='dummy client', version='0.0.0')
        self._client_thread = Thread(target=start, args=(self.client, sstdio, cstdout), daemon=True)
        self._client_thread.start()

        self.datadir = (Path(__file__).parent / '..' / '..' / 'tests' / 'project').resolve()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        self.server.send_notification(EXIT)
        self.client.send_notification(EXIT)

        self._server_thread.join(timeout=2.0)
        self._client_thread.join(timeout=2.0)

        return True


class GrizzlyTestFixture:
    mocks: list[MagicMock]

    def done(self) -> None: ...

    def reset_mocks(self) -> None:
        for mock in self.mocks:
            mock.reset_mock()

    def _provide(self, fixture: GrizzlyTestFixture) -> None:
        self.__dict__.update({k: v for k, v in fixture.__dict__.items() if not k.startswith('_')})


class ServerPipInstallUpgrade(GrizzlyTestFixture):
    project_name: str
    test_context: Path
    ls: GrizzlyLanguageServer
    venv_path: Path
    requirements_file: Path
    project_age_file: Path
    logger_mock: MagicMock
    run_command_mock: MagicMock


class ServerPipInstallUpgradeFixture(ServerPipInstallUpgrade):
    def __init__(self, lsp_fixture: LspFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
        self.project_name = 'unit-test-project'
        self.test_context = tmp_path_factory.mktemp('test-context')
        self.ls = lsp_fixture.server

        self.venv_path = self.test_context / f'grizzly-ls-{self.project_name}'
        self.venv_path.mkdir()

        self.requirements_file = self.test_context / 'requirements.txt'
        self.requirements_file.touch()

        self.project_age_file = self.venv_path / '.age'
        self.project_age_file.touch()

        mocker.patch('grizzly_ls.server.gettempdir', return_value=self.test_context.as_posix())

        self.logger_mock = mocker.patch.object(self.ls, 'logger', spec=LogOutputChannelLogger)
        self.run_command_mock = mocker.patch('grizzly_ls.server.run_command', return_value=None)

        self.mocks = [self.logger_mock, self.run_command_mock]

    def done(self) -> None:
        rm_rf(self.test_context)


class ServerInstall(GrizzlyTestFixture):
    project_name: str
    test_context: Path
    ls: GrizzlyLanguageServer
    progress_class_mock: MagicMock
    progress_mock: MagicMock
    logger_mock: MagicMock
    use_virtual_environment_mock: MagicMock
    pip_install_upgrade_mock: MagicMock
    compile_inventory_mock: MagicMock
    validate_gherkin_mock: MagicMock
    ls_publish_diagnostics: MagicMock
    requirements_file: Path


class ServerInstallFixture(ServerInstall):
    def __init__(self, lsp_fixture: LspFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
        self.project_name = 'unit-test-project'
        self.test_context = tmp_path_factory.mktemp('test-context')
        self.ls = lsp_fixture.server
        self.ls.root_path = self.test_context
        self.progress_class_mock = mocker.patch('grizzly_ls.server.Progress', spec=Progress)
        self.progress_mock = self.progress_class_mock.return_value.__enter__.return_value
        self.logger_mock = mocker.patch.object(self.ls, 'logger', spec=LogOutputChannelLogger)
        self.use_virtual_environment_mock = mocker.patch('grizzly_ls.server.use_virtual_environment', return_value=self.test_context)
        self.pip_install_upgrade_mock = mocker.patch('grizzly_ls.server.pip_install_upgrade', return_value=None)
        self.compile_inventory_mock = mocker.patch('grizzly_ls.server.compile_inventory', return_value=None)
        self.validate_gherkin_mock = mocker.patch('grizzly_ls.server.validate_gherkin', return_value=None)
        self.ls_publish_diagnostics = mocker.patch.object(self.ls, 'publish_diagnostics', return_value=None)
        self.requirements_file = self.test_context / 'requirements.txt'
        self.ls.lsp._workspace = Workspace(root_uri=(self.test_context / f'grizzly-ls-{self.project_name}').as_posix())

        self.mocks = [
            self.progress_class_mock,
            self.progress_mock,
            self.logger_mock,
            self.use_virtual_environment_mock,
            self.pip_install_upgrade_mock,
            self.compile_inventory_mock,
            self.validate_gherkin_mock,
            self.ls_publish_diagnostics,
        ]

        mocker.patch('grizzly_ls.server.environ.copy', return_value={})

    def done(self) -> None:
        rm_rf(self.test_context)
        if self.test_context.as_posix() in sys.path[-1]:
            sys.path.pop()


class ServerUseVirtualEnvironment(GrizzlyTestFixture):
    project_name: str
    test_context: Path
    ls: GrizzlyLanguageServer
    python_version: str
    venv_path: Path
    logger_mock: MagicMock
    create_virtual_environment_mock: MagicMock
    run_command_mock: MagicMock
    env: dict[str, str]


class ServerUseVirtualEnvironmentFixture(ServerUseVirtualEnvironment):
    def __init__(self, lsp_fixture: LspFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
        self.project_name = 'unit-test-project'
        self.test_context = tmp_path_factory.mktemp('test-context')
        self.ls = lsp_fixture.server
        self.ls.root_path = self.test_context
        self.python_version = '.'.join(str(v) for v in sys.version_info[:2])
        self.venv_path = self.test_context / f'grizzly-ls-{self.project_name}'
        self.venv_path.mkdir()
        self.env = {'PATH': '/bin'}

        mocker.patch('grizzly_ls.server.gettempdir', return_value=self.test_context.as_posix())
        self.logger_mock = mocker.patch.object(self.ls, 'logger', return_value=None)
        self.create_virtual_environment_mock = mocker.patch('grizzly_ls.server._create_virtual_environment', return_value=None)
        self.run_command_mock = mocker.patch('grizzly_ls.server.run_command', return_value=None)

        self.mocks = [self.logger_mock, self.create_virtual_environment_mock, self.run_command_mock]

    def done(self) -> None:
        rm_rf(self.test_context)

        if self.venv_path.as_posix() in sys.path[-1]:
            sys.path.pop()


class ServerConfigurationIndexUrl(GrizzlyTestFixture):
    ls: GrizzlyLanguageServer
    pip_configuration_mock: MagicMock
    pip_config_mock: MagicMock


class ServerConfigurationIndexUrlFixture(ServerConfigurationIndexUrl):
    def __init__(self, lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
        self.ls = lsp_fixture.server
        self.pip_configuration_mock = mocker.patch('grizzly_ls.server.PipConfiguration', spec=PipConfiguration)
        self.pip_config_mock = self.pip_configuration_mock.return_value
        self.mocks = [self.pip_config_mock, self.pip_configuration_mock]

    def done(self) -> None:
        self.ls.client_settings.clear()
        self.ls.index_url = None


class ServerInitialize(GrizzlyTestFixture):
    ls: GrizzlyLanguageServer
    logger_mock: MagicMock
    get_capability_mock: MagicMock
    params: lsp.InitializeParams
    env: dict[str, str]


class ServerInitializeFixture(ServerInitialize):
    def __init__(self, lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
        self.ls = lsp_fixture.server

        self.env = {}
        mocker.patch('grizzly_ls.server.environ', self.env)
        self.logger_mock = mocker.patch.object(self.ls, 'logger', spec=LogOutputChannelLogger)
        self.get_capability_mock = mocker.patch('grizzly_ls.server.get_capability', return_value=None)

        self.mocks = [self.logger_mock, self.get_capability_mock]

        self.params = lsp.InitializeParams(capabilities=self.get_client_capabilities(), root_path=None, root_uri=None, initialization_options=None)

    def get_client_capabilities(self) -> lsp.ClientCapabilities:
        return lsp.ClientCapabilities(
            text_document=lsp.TextDocumentClientCapabilities(
                completion=lsp.CompletionClientCapabilities(completion_item=lsp.CompletionClientCapabilitiesCompletionItemType(documentation_format=[]))
            ),
        )

    def done(self) -> None:
        self.ls.client_settings = {}
        self.env = {}
        self.params.root_path = None
        self.params.root_uri = None
        self.params.capabilities = self.get_client_capabilities()

        with suppress(AttributeError):
            delattr(self.ls, 'root_path')


class ServerTextDocumentCompletion(GrizzlyTestFixture):
    ls: GrizzlyLanguageServer
    params: lsp.CompletionParams
    logger_mock: MagicMock
    ls_get_text_document_mock: MagicMock
    get_current_line_mock: MagicMock
    complete_variable_name_mock: MagicMock
    complete_expression_mock: MagicMock
    complete_metadata_mock: MagicMock
    complete_step_mock: MagicMock
    complete_keyword_mock: MagicMock
    mocks: list[MagicMock]


class ServerTextDocumentCompletionFixture(ServerTextDocumentCompletion):
    def __init__(self, lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
        self.ls = lsp_fixture.server
        self.ls.lsp._workspace = Workspace(root_uri='file:///tmp/grizzly-ls-unit-test')

        self.logger_mock = mocker.patch.object(self.ls, 'logger', spec=LogOutputChannelLogger)
        self.ls_get_text_document_mock = mocker.patch.object(self.ls.workspace, 'get_text_document', return_value=None)
        self.get_current_line_mock = mocker.patch('grizzly_ls.server.get_current_line', return_value=None)
        self.complete_variable_name_mock = mocker.patch('grizzly_ls.server.complete_variable_name', return_value=None)
        self.complete_expression_mock = mocker.patch('grizzly_ls.server.complete_expression', return_value=None)
        self.complete_metadata_mock = mocker.patch('grizzly_ls.server.complete_metadata', return_value=None)
        self.complete_step_mock = mocker.patch('grizzly_ls.server.complete_step', return_value=None)
        self.complete_keyword_mock = mocker.patch('grizzly_ls.server.complete_keyword', return_value=None)

        self.mocks = [
            self.logger_mock,
            self.ls_get_text_document_mock,
            self.get_current_line_mock,
            self.complete_variable_name_mock,
            self.complete_expression_mock,
            self.complete_metadata_mock,
            self.complete_step_mock,
            self.complete_keyword_mock,
        ]

        text_document = lsp.TextDocumentIdentifier(uri='file:///hello.world.txt')
        position = lsp.Position(line=0, character=0)
        self.params = lsp.CompletionParams(text_document=text_document, position=position)

    def done(self) -> None:
        pass
