from __future__ import annotations

import logging
import sys
from collections import deque
from contextlib import suppress
from os import pathsep, utime
from os.path import sep
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from unittest.mock import call

import gevent.monkey
import pytest
from pip._internal.exceptions import ConfigurationError as PipConfigurationError

# monkey patch functions to short-circuit them (causes problems in this context)
gevent.monkey.patch_all = lambda: None

from typing import TYPE_CHECKING

from behave.matchers import ParseMatcher
from grizzly_ls import __version__
from grizzly_ls.constants import LANGUAGE_ID
from grizzly_ls.model import Step
from grizzly_ls.server import (
    ConfigurationError,
    InstallError,
    _configuration_index_url,
    _configuration_variable_pattern,
    _create_virtual_environment,
    initialize,
    install,
    pip_install_upgrade,
    text_document_completion,
    use_virtual_environment,
)
from grizzly_ls.server.inventory import compile_inventory
from grizzly_ls.utils import LogOutputChannelLogger
from lsprotocol import types as lsp
from pygls.workspace import TextDocument

from test_ls.conftest import GRIZZLY_PROJECT
from test_ls.fixtures import ServerConfigurationIndexUrl, ServerInitialize, ServerInstall, ServerPipInstallUpgrade, ServerTextDocumentCompletion, ServerUseVirtualEnvironment
from test_ls.helpers import SOME, rm_rf

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture

    from test_ls.fixtures import (
        LspFixture,
        MockerFixture,
        ServerConfigurationIndexUrlFixture,
        ServerInitializeFixture,
        ServerInstallFixture,
        ServerPipInstallUpgradeFixture,
        ServerTextDocumentCompletionFixture,
        ServerUseVirtualEnvironmentFixture,
    )


class TestGrizzlyLanguageServer:
    @pytest.mark.parametrize(
        ('language', 'words'),
        [
            (
                'en',
                {
                    'keywords': [
                        'Scenario',
                        'Scenario Outline',
                        'Scenario Template',
                        'Example',
                        'Examples',
                        'Scenarios',
                        'And',
                        'But',
                    ],
                    'keywords_any': ['*', 'But', 'And'],
                    'keywords_once': ['Feature', 'Background', 'Ability', 'Business Need'],
                },
            ),
            (
                'sv',
                {
                    'keywords': [
                        'Scenario',
                        'Abstrakt Scenario',
                        'Scenariomall',
                        'Exempel',
                        'Och',
                        'Men',
                    ],
                    'keywords_any': ['*', 'Men', 'Och'],
                    'keywords_once': ['Egenskap', 'Bakgrund'],
                },
            ),
            (
                'de',
                {
                    'keywords': [
                        'Szenario',
                        'Szenarien',
                        'Szenariogrundriss',
                        'Beispiel',
                        'Beispiele',
                        'Und',
                        'Aber',
                    ],
                    'keywords_any': ['*', 'Und', 'Aber'],
                    'keywords_once': ['Grundlage', 'Funktionalit\xe4t', 'Funktion', 'Hintergrund', 'Voraussetzungen', 'Vorbedingungen'],
                },
            ),
        ],
    )
    def test___init__(self, language: str, words: dict[str, list[str]], lsp_fixture: LspFixture) -> None:
        ls = lsp_fixture.server
        try:
            ls.steps.clear()
            with suppress(ValueError):
                ls.language = 'dummy'

            ls.language = language

            assert ls.name == 'grizzly-ls'
            assert ls.version == __version__
            assert sorted(ls.keywords) == sorted(
                words.get('keywords', []),
            )
            assert sorted(ls.keywords_any) == sorted(words.get('keywords_any', []))

            assert isinstance(ls.logger, LogOutputChannelLogger)
            assert ls.logger.logger.name == 'GrizzlyLanguageServer'
        finally:
            ls.language = 'en'

    def test_add_startup_error_message(self, lsp_fixture: LspFixture) -> None:
        ls = lsp_fixture.server

        try:
            assert ls.startup_messages == deque([])

            ls.add_startup_error_message('something is foobar')

            assert ls.startup_messages == deque([('something is foobar', logging.ERROR)])
        finally:
            ls.startup_messages.clear()

    def test__normalize_step_expression(self, lsp_fixture: LspFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        mocker.patch('parse.Parser.__init__', return_value=None)
        ls = lsp_fixture.server

        ls.steps.clear()

        assert ls.steps == {}

        ls.root_path = GRIZZLY_PROJECT

        compile_inventory(ls)

        noop = lambda: None  # noqa: E731

        step = ParseMatcher(noop, 'hello world')

        assert ls._normalize_step_expression(step) == ['hello world']

        step = ParseMatcher(noop, 'hello "{world}"! how "{are:d}" you')

        assert ls._normalize_step_expression(step) == ['hello ""! how "" you']

        step = ParseMatcher(noop, 'you have "{count}" {grammar:UserGramaticalNumber}')

        assert sorted(ls._normalize_step_expression(step)) == sorted(
            [
                'you have "" users',
                'you have "" user',
            ]
        )

        step = ParseMatcher(noop, 'send from {from_node:MessageDirection} to {to_node:MessageDirection}')

        assert sorted(ls._normalize_step_expression(step)) == sorted(
            [
                'send from client to server',
                'send from server to client',
            ]
        )

        assert sorted(
            ls._normalize_step_expression(
                'send to {to_node:MessageDirection} from {from_node:MessageDirection} for "{iterations}" {grammar:IterationGramaticalNumber}',
            )
        ) == sorted(
            [
                'send to server from client for "" iteration',
                'send to server from client for "" iterations',
                'send to client from server for "" iteration',
                'send to client from server for "" iterations',
            ]
        )

        assert sorted(
            ls._normalize_step_expression(
                'send {direction:Direction} {node:MessageDirection}',
            )
        ) == sorted(
            [
                'send from server',
                'send from client',
                'send to server',
                'send to client',
            ]
        )

        step = ParseMatcher(
            noop,
            'Then save {target:ResponseTarget} as "{content_type:ContentType}" in "{variable}" for "{count}" {grammar:UserGramaticalNumber}',
        )
        actual = sorted(ls._normalize_step_expression(step))
        assert actual == sorted(
            [
                'Then save payload as "undefined" in "" for "" user',
                'Then save payload as "undefined" in "" for "" users',
                'Then save metadata as "undefined" in "" for "" user',
                'Then save metadata as "undefined" in "" for "" users',
                'Then save payload as "json" in "" for "" user',
                'Then save payload as "json" in "" for "" users',
                'Then save metadata as "json" in "" for "" user',
                'Then save metadata as "json" in "" for "" users',
                'Then save payload as "xml" in "" for "" user',
                'Then save payload as "xml" in "" for "" users',
                'Then save metadata as "xml" in "" for "" user',
                'Then save metadata as "xml" in "" for "" users',
                'Then save payload as "plain" in "" for "" user',
                'Then save payload as "plain" in "" for "" users',
                'Then save metadata as "plain" in "" for "" user',
                'Then save metadata as "plain" in "" for "" users',
                'Then save metadata as "multipart_form_data" in "" for "" user',
                'Then save metadata as "multipart_form_data" in "" for "" users',
                'Then save payload as "multipart_form_data" in "" for "" user',
                'Then save payload as "multipart_form_data" in "" for "" users',
                'Then save metadata as "octet_stream_utf8" in "" for "" user',
                'Then save metadata as "octet_stream_utf8" in "" for "" users',
                'Then save payload as "octet_stream_utf8" in "" for "" user',
                'Then save payload as "octet_stream_utf8" in "" for "" users',
            ]
        )

        assert sorted(
            ls._normalize_step_expression(
                'python {condition:Condition} cool',
            )
        ) == sorted(
            [
                'python is cool',
                'python is not cool',
            ]
        )

        assert sorted(ls._normalize_step_expression('{method:Method} {direction:Direction} endpoint "{endpoint:s}"')) == sorted(
            [
                'send to endpoint ""',
                'send from endpoint ""',
                'post to endpoint ""',
                'post from endpoint ""',
                'put to endpoint ""',
                'put from endpoint ""',
                'receive to endpoint ""',
                'receive from endpoint ""',
                'get to endpoint ""',
                'get from endpoint ""',
            ]
        )

        caplog.clear()

        show_message_mock = mocker.patch.object(ls, 'show_message', autospec=True)

        with caplog.at_level(logging.ERROR):
            assert sorted(
                ls._normalize_step_expression(
                    'unhandled type {test:Unknown} for {target:ResponseTarget}',
                )
            ) == sorted(
                [
                    'unhandled type {test:Unknown} for metadata',
                    'unhandled type {test:Unknown} for payload',
                ]
            )

        assert caplog.messages == []
        show_message_mock.assert_not_called()

        with caplog.at_level(logging.ERROR):
            assert sorted(
                ls._normalize_step_expression(
                    'unhandled type "{test:Unknown}" for {target:ResponseTarget}',
                )
            ) == sorted(
                [
                    'unhandled type "" for metadata',
                    'unhandled type "" for payload',
                ]
            )

        assert caplog.messages == []
        show_message_mock.assert_not_called()

    def test__find_help(self, lsp_fixture: LspFixture) -> None:
        ls = lsp_fixture.server

        def noop() -> None:
            pass

        ls.steps = {
            'then': [
                Step('Then', 'hello world', noop, 'this is the help for hello world'),
            ],
            'step': [
                Step(
                    'And',
                    'hello ""',
                    noop,
                    'this is the help for hello world parameterized',
                ),
                Step('But', 'foo bar', noop, 'this is the help for foo bar'),
                Step('But', '"" bar', noop, 'this is the help for foo bar parameterized'),
            ],
        }

        assert ls._find_help('Then hello world') == 'this is the help for hello world'
        assert ls._find_help('Then hello') == 'this is the help for hello world'
        assert ls._find_help('asdfasdf') is None
        assert ls._find_help('And hello') == 'this is the help for hello world'
        assert ls._find_help('And hello "world"') == 'this is the help for hello world parameterized'
        assert ls._find_help('But foo') == 'this is the help for foo bar'
        assert ls._find_help('But "foo" bar') == 'this is the help for foo bar parameterized'
        assert ls._find_help('When you are not alone') is None

    def test_get_language_key(self, lsp_fixture: LspFixture) -> None:
        ls = lsp_fixture.server

        try:
            ls.language = 'sv'
            assert ls.get_language_key('Egenskap:') == 'feature'
            assert ls.get_language_key('Och') == 'and'
            assert ls.get_language_key('Givet') == 'given'
            assert ls.get_language_key('Scenariomall:') == 'scenario_outline'
            assert ls.get_language_key('När') == 'when'
            assert ls.get_language_key('Så') == 'then'
            assert ls.get_language_key('Exempel') == 'examples'
            assert ls.get_language_key('Bakgrund') == 'background'
            assert ls.get_language_key('Men') == 'but'
            with pytest.raises(ValueError, match='"Feature" is not a valid keyword for language "sv"'):
                ls.get_language_key('Feature')

            ls.language = 'en'
            assert ls.get_language_key('Feature') == 'feature'
            assert ls.get_language_key('And') == 'and'
            assert ls.get_language_key('Given') == 'given'
            assert ls.get_language_key('Scenario Template:') == 'scenario_outline'
            assert ls.get_language_key('When') == 'when'
            assert ls.get_language_key('Then') == 'then'
            assert ls.get_language_key('Examples') == 'examples'
            assert ls.get_language_key('Background') == 'background'
            assert ls.get_language_key('But') == 'but'
            with pytest.raises(ValueError, match='"Egenskap" is not a valid keyword for language "en"'):
                ls.get_language_key('Egenskap')
        finally:
            ls.language = 'en'

    def test_get_base_keyword(self, lsp_fixture: LspFixture) -> None:
        feature_file = lsp_fixture.datadir / 'features' / 'test_get_base_keyword.feature'
        try:
            feature_file.write_text(
                """Feature:
Scenario: test scenario
    Given a user of type "RestApi" load testing "dummy://test"
    And value of variable "hello" is "world"
    And repeat for "1" iterations

    Then parse date "{{ datetime.now() }} and save in variable "foo"
    But fail scenario"""
            )
            ls = lsp_fixture.server

            text_document = TextDocument(feature_file.as_uri())

            assert ls.get_base_keyword(lsp.Position(line=7, character=0), text_document) == 'Then'
            assert ls.get_base_keyword(lsp.Position(line=6, character=0), text_document) == 'Then'

            assert ls.get_base_keyword(lsp.Position(line=4, character=0), text_document) == 'Given'

            assert ls.get_base_keyword(lsp.Position(line=3, character=0), text_document) == 'Given'

            assert ls.get_base_keyword(lsp.Position(line=2, character=0), text_document) == 'Given'

            with pytest.raises(ValueError, match='unable to get keyword from ""'):
                ls.get_base_keyword(lsp.Position(line=5, character=5), text_document)
        finally:
            with suppress(Exception):
                feature_file.unlink()


def test_install_error_exception() -> None:
    e = InstallError('something went wrong', backend='uv', stdout=b'', stderr=b'major unrecoverable error')
    assert isinstance(e, Exception)
    assert str(e) == 'something went wrong'
    assert e.backend == 'uv'
    assert e.stdout == b''
    assert e.stderr == b'major unrecoverable error'


class Test_CreateVirtualEnvironment:
    """Tests for grizzly_ls.server._create_virtual_environment."""

    def test_venv(self, mocker: MockerFixture) -> None:
        _run_uv_mock = mocker.patch('grizzly_ls.server._run_uv', side_effect=[ModuleNotFoundError])
        _run_venv_mock = mocker.patch('grizzly_ls.server._run_venv', return_value=None)

        _create_virtual_environment(Path.cwd(), '3.12')
        _run_uv_mock.assert_called_once()
        _run_uv_mock.reset_mock()

        _run_venv_mock.assert_called_once_with(Path.cwd(), with_pip=True)
        _run_venv_mock.reset_mock()

        _run_uv_mock.side_effect = [ModuleNotFoundError]
        _run_venv_mock.side_effect = [CalledProcessError(1, cmd='rm -rf *', output=b'something', stderr=b'error')]

        with pytest.raises(InstallError) as e:
            _create_virtual_environment(Path.cwd(), '3.12')

        assert e.value == SOME(InstallError, backend='virtualenv', stdout=b'something', stderr=b'error')
        _run_uv_mock.assert_called_once()

    def test_uv(self, mocker: MockerFixture) -> None:
        _run_uv_mock = mocker.patch('grizzly_ls.server._run_uv', return_value=CompletedProcess(args='rm -rf *', returncode=0))
        _run_venv_mock = mocker.patch('grizzly_ls.server._run_venv', return_value=None)

        _create_virtual_environment(Path.cwd(), '3.12')

        _run_venv_mock.assert_not_called()
        _run_uv_mock.assert_called_once_with(['venv', '--managed-python', '--python', '3.12', Path.cwd().as_posix()])
        _run_uv_mock.reset_mock()

        _run_uv_mock.return_value = CompletedProcess(args='rm -rf *', returncode=1, stdout=b'something', stderr=b'error')

        with pytest.raises(InstallError) as e:
            _create_virtual_environment(Path.cwd(), '3.12')

        assert e.value == SOME(InstallError, backend='uv', stdout=b'something', stderr=b'error')
        _run_uv_mock.assert_called_once_with(['venv', '--managed-python', '--python', '3.12', Path.cwd().as_posix()])
        _run_venv_mock.assert_not_called()


class TestUseVirtualEnvironment(ServerUseVirtualEnvironment):
    @pytest.fixture(autouse=True)
    def _provide_fixture(self, server_use_virtual_environment_fixture: ServerUseVirtualEnvironmentFixture) -> None:
        self._provide(server_use_virtual_environment_fixture)

    def test_venv_does_not_exist(self) -> None:
        self.create_virtual_environment_mock.side_effect = [InstallError('asdf', backend='uv', stdout='hello', stderr='world')]

        rm_rf(self.venv_path)

        with pytest.raises(InstallError):
            use_virtual_environment(self.ls, self.project_name, {})

        assert self.logger_mock.mock_calls == [
            call.debug(f'looking for venv at {self.venv_path!s}, has_venv=False'),
            call.exception('failed to create virtual environment with uv', notify=True),
            call.error('stderr=world\nstdout=hello'),
        ]
        self.create_virtual_environment_mock.assert_called_once_with(self.test_context / f'grizzly-ls-{self.project_name}', self.python_version)

    def test_ensurepip_failed(self) -> None:
        self.run_command_mock.return_value = (1, ['this is the error'])

        with pytest.raises(InstallError):
            use_virtual_environment(self.ls, self.project_name, self.env)

        self.run_command_mock.assert_called_once_with(['python', '-m', 'ensurepip'], env=self.env)
        assert self.logger_mock.mock_calls == [
            call.debug(f'looking for venv at {self.venv_path!s}, has_venv=True'),
            call.error('failed to ensure pip is installed for venv unit-test-project', notify=True),
            call.error('ensurepip error:\nthis is the error'),
        ]
        executables_dir = 'bin' if sys.platform != 'win32' else 'Scripts'
        assert self.env == {
            'PATH': f'{self.venv_path!s}{sep}{executables_dir}{pathsep}/bin',
            'VIRTUAL_ENV': f'{self.venv_path!s}',
            'PYTHONPATH': f'{(self.test_context / "features")!s}',
        }

    def test_index_url(self) -> None:
        self.ls.index_url = 'https://example.com/pypi'
        self.run_command_mock.return_value = (0, [])

        with pytest.raises(InstallError):
            use_virtual_environment(self.ls, 'unit-test-project', self.env)

        self.run_command_mock.assert_called_once_with(['python', '-m', 'ensurepip'], env=self.env)
        assert self.logger_mock.mock_calls == [
            call.debug(f'looking for venv at {self.venv_path!s}, has_venv=True'),
            call.error('global.index-url does not contain username and/or password, check your configuration!', notify=True),
        ]
        executables_dir = 'bin' if sys.platform != 'win32' else 'Scripts'
        assert self.env == {
            'PATH': f'{self.venv_path!s}{sep}{executables_dir}{pathsep}/bin',
            'VIRTUAL_ENV': f'{self.venv_path!s}',
            'PYTHONPATH': f'{(self.test_context / "features")!s}',
        }

        self.reset_mocks()

        self.env = {'PATH': '/bin'}

        self.ls.index_url = 'https://user:pass@example.com/pypi'

        assert use_virtual_environment(self.ls, self.project_name, self.env) == self.venv_path
        assert sys.path[-1] == f'{self.venv_path.as_posix()}/lib/python{self.python_version}/site-packages'
        assert self.env == {
            'PATH': f'{self.venv_path!s}{sep}{executables_dir}{pathsep}/bin',
            'VIRTUAL_ENV': f'{self.venv_path!s}',
            'PYTHONPATH': f'{(self.test_context / "features")!s}',
            'PIP_EXTRA_INDEX_URL': 'https://user:pass@example.com/pypi',
        }
        assert self.logger_mock.mock_calls == [call.debug(f'looking for venv at {self.venv_path!s}, has_venv=True')]
        self.run_command_mock.assert_called_once()


class TestPipInstallUpgrade(ServerPipInstallUpgrade):
    """Tests for grizzly_ls.server.pip_install_upgrade."""

    @pytest.fixture(autouse=True)
    def _provide_fixture(self, server_pip_install_upgrade_fixture: ServerPipInstallUpgradeFixture) -> None:
        self._provide(server_pip_install_upgrade_fixture)

    def test_noop(self) -> None:
        """Test when project age file exists, and requirements file has older or same modified time, in that case nothing should be done."""
        self.requirements_file.touch()
        utime(self.project_age_file, (self.requirements_file.lstat().st_atime, self.requirements_file.lstat().st_mtime))

        pip_install_upgrade(self.ls, self.project_name, '/opt/python/bin/python', self.requirements_file, {})

        self.run_command_mock.assert_not_called()
        assert self.logger_mock.mock_calls == [
            call.debug(f'{self.requirements_file.as_posix()} is not newer than {self.project_age_file.as_posix()}, no need to install or upgrade'),
        ]

    def test_install(self) -> None:
        """Test if age file does not exist, action = install."""
        # <!-- successful (rc=0)
        self.project_age_file.unlink(missing_ok=True)
        self.run_command_mock.return_value = (0, ['hello world  ', '  ERROR: just kidding'])

        pip_install_upgrade(self.ls, self.project_name, '/opt/python/bin/python', self.requirements_file, {})

        assert self.logger_mock.mock_calls == [
            call.debug(f'install from {self.requirements_file.as_posix()}'),
            call.debug('hello world'),
            call.error('just kidding'),
            call.debug('install done rc=0'),
        ]
        self.run_command_mock.assert_called_once_with(
            [
                '/opt/python/bin/python',
                '-m',
                'pip',
                'install',
                '--upgrade',
                '-r',
                self.requirements_file.as_posix(),
            ],
            env={},
            cwd=self.venv_path,
        )
        assert self.project_age_file.exists()
        assert self.project_age_file.lstat().st_mtime >= self.requirements_file.lstat().st_mtime
        # // ->

        self.reset_mocks()

        # <!-- unsuccessful (rc=1)
        self.project_age_file.unlink(missing_ok=True)
        self.run_command_mock.return_value = (1, ['hello world  ', '  ERROR: just kidding'])

        with pytest.raises(InstallError):
            pip_install_upgrade(self.ls, self.project_name, '/opt/python/bin/python', self.requirements_file, {})

        assert self.logger_mock.mock_calls == [
            call.debug(f'install from {self.requirements_file.as_posix()}'),
            call.warning('hello world'),
            call.error('just kidding'),
            call.debug('install done rc=1'),
            call.error(f'failed to install from {self.requirements_file.as_posix()}', notify=True),
        ]
        self.run_command_mock.assert_called_once_with(
            [
                '/opt/python/bin/python',
                '-m',
                'pip',
                'install',
                '--upgrade',
                '-r',
                self.requirements_file.as_posix(),
            ],
            env={},
            cwd=self.venv_path,
        )
        assert not self.project_age_file.exists()
        # // ->

    def test_upgrade(self) -> None:
        """Test when requirements file has newer modified time than existing project age file, in which case we should update."""
        # <!-- age file exist, but has older modified time than requirements file, action = update and is successful (0)
        utime(self.project_age_file, (self.requirements_file.lstat().st_atime, self.requirements_file.lstat().st_mtime - 3600))
        self.run_command_mock.return_value = (0, ['hello world  ', '  ERROR: just kidding'])

        project_age_file_lstat = self.project_age_file.lstat()

        pip_install_upgrade(self.ls, self.project_name, '/opt/python/bin/python', self.requirements_file, {})

        assert self.logger_mock.mock_calls == [
            call.debug(f'upgrade from {self.requirements_file.as_posix()}'),
            call.debug('hello world'),
            call.error('just kidding'),
            call.debug('upgrade done rc=0'),
        ]
        self.run_command_mock.assert_called_once_with(
            [
                '/opt/python/bin/python',
                '-m',
                'pip',
                'install',
                '--upgrade',
                '-r',
                self.requirements_file.as_posix(),
            ],
            env={},
            cwd=self.venv_path,
        )
        assert self.project_age_file.lstat().st_mtime > project_age_file_lstat.st_mtime
        assert self.project_age_file.lstat().st_mtime >= self.requirements_file.lstat().st_mtime
        # // ->

        self.reset_mocks()

        # <!-- age file exist, action = upgrade and is unsuccessful (1)
        utime(self.project_age_file, (self.requirements_file.lstat().st_atime, self.requirements_file.lstat().st_mtime - 3600))
        self.run_command_mock.return_value = (1, ['hello world  ', '  ERROR: just kidding'])

        project_age_file_lstat = self.project_age_file.lstat()

        with pytest.raises(InstallError):
            pip_install_upgrade(self.ls, self.project_name, '/opt/python/bin/python', self.requirements_file, {})

        assert self.logger_mock.mock_calls == [
            call.debug(f'upgrade from {self.requirements_file.as_posix()}'),
            call.warning('hello world'),
            call.error('just kidding'),
            call.debug('upgrade done rc=1'),
            call.error(f'failed to upgrade from {self.requirements_file.as_posix()}', notify=True),
        ]
        self.run_command_mock.assert_called_once_with(
            [
                '/opt/python/bin/python',
                '-m',
                'pip',
                'install',
                '--upgrade',
                '-r',
                self.requirements_file.as_posix(),
            ],
            env={},
            cwd=self.venv_path,
        )
        assert self.project_age_file.lstat().st_mtime == project_age_file_lstat.st_mtime
        assert self.project_age_file.lstat().st_mtime <= self.requirements_file.lstat().st_mtime
        # // ->


class TestInstall(ServerInstall):
    @pytest.fixture(autouse=True)
    def _provide_fixture(self, server_install_fixture: ServerInstallFixture) -> None:
        self._provide(server_install_fixture)

    def test_no_requirements_file(self) -> None:
        """Test if requirements file does not exist, and use_venv is True."""
        self.ls.client_settings.update({'use_virtual_environment': True})

        assert not self.requirements_file.exists()

        install(self.ls)

        self.progress_class_mock.assert_called_once_with(self.ls, 'grizzly-ls')
        assert self.logger_mock.mock_calls == [
            call.debug('grizzly-ls/install: installing'),
            call.debug(f'workspace root: {self.test_context.as_posix()} (use virtual environment: True)'),
            call.error(f'project "{self.test_context.stem}" does not have a requirements.txt in {self.test_context.as_posix()}', notify=True),
        ]
        self.use_virtual_environment_mock.assert_called_once_with(self.ls, self.test_context.stem, {})
        self.pip_install_upgrade_mock.assert_not_called()
        self.compile_inventory_mock.assert_not_called()
        self.progress_mock.assert_not_called()

    def test_compile_inventory_fails(self) -> None:
        """Test if compile_inventory fails, and use_venv is False."""
        self.ls.client_settings.update({'use_virtual_environment': False})
        self.compile_inventory_mock.side_effect = [ModuleNotFoundError]

        self.requirements_file.touch()

        assert self.requirements_file.exists()

        install(self.ls)

        self.progress_class_mock.assert_called_once_with(self.ls, 'grizzly-ls')
        assert self.logger_mock.mock_calls == [
            call.debug('grizzly-ls/install: installing'),
            call.debug(f'workspace root: {self.test_context.as_posix()} (use virtual environment: False)'),
            call.exception('failed to create step inventory', notify=True),
        ]
        self.use_virtual_environment_mock.assert_not_called()
        self.pip_install_upgrade_mock.assert_called_once_with(self.ls, self.test_context.stem, sys.executable, self.requirements_file, {})
        self.compile_inventory_mock.assert_called_once_with(self.ls)
        assert self.progress_mock.report.mock_calls == [
            call('loading extension', 1),
            call('virtual environment done', 40),
            call('preparing step dependencies', 60),
            call('building step inventory', 80),
        ]

    def test_non_install_error_exception(self) -> None:
        """Test if something in the grizzly-ls/install command fails with something other than InstallError."""
        self.ls.client_settings.update({'use_virtual_environment': False})

        self.progress_class_mock.return_value.__enter__.side_effect = [ValueError]

        install(self.ls)

        self.progress_class_mock.assert_called_once_with(self.ls, 'grizzly-ls')
        assert self.logger_mock.mock_calls == [call.debug('grizzly-ls/install: installing'), call.exception('failed to install extension, check output', notify=True)]
        self.use_virtual_environment_mock.assert_not_called()
        self.pip_install_upgrade_mock.assert_not_called()
        self.compile_inventory_mock.assert_not_called()

    def test_extension_done_publish_diagnostics(self) -> None:
        """Test when install is successful, and has_venv is True."""
        text_documents: dict[str, TextDocument] = {
            'first.feature': TextDocument(uri='first.feature', language_id=LANGUAGE_ID),
            'second.txt': TextDocument(uri='second.feature', language_id='text-plain'),
        }
        self.ls.client_settings.update({'use_virtual_environment': True})
        self.ls.workspace._text_documents = text_documents
        self.requirements_file.touch()
        self.validate_gherkin_mock.return_value = []

        # <!-- all good
        assert self.requirements_file.exists()

        python_sys_path = self.test_context.as_posix()
        sys.path.append(python_sys_path)

        install(self.ls)

        assert python_sys_path not in sys.path[-1]
        self.progress_class_mock.assert_called_once_with(self.ls, 'grizzly-ls')
        self.progress_mock.report.assert_has_calls(
            [
                call('loading extension', 1),
                call('setting up virtual environment', 10),
                call('virtual environment done', 40),
                call('preparing step dependencies', 60),
                call('building step inventory', 80),
                call('extension done', 100),
            ]
        )
        assert self.logger_mock.mock_calls == [
            call.debug('grizzly-ls/install: installing'),
            call.debug(f'workspace root: {self.test_context.as_posix()} (use virtual environment: True)'),
        ]
        self.use_virtual_environment_mock.assert_called_once_with(self.ls, self.test_context.stem, {})
        self.pip_install_upgrade_mock.assert_called_once_with(self.ls, self.test_context.stem, 'python', self.requirements_file, {})
        self.compile_inventory_mock.assert_called_once_with(self.ls)
        self.validate_gherkin_mock.assert_called_once_with(self.ls, text_documents['first.feature'])
        self.ls_publish_diagnostics.assert_called_once_with(text_documents['first.feature'].uri, [])
        # // -->

        self.reset_mocks()

        # <!-- validate_gherkin throws exception
        self.validate_gherkin_mock.side_effect = [ValueError]
        assert self.requirements_file.exists()

        install(self.ls)

        assert self.logger_mock.mock_calls == [
            call.debug('grizzly-ls/install: installing'),
            call.debug(f'workspace root: {self.test_context.as_posix()} (use virtual environment: True)'),
            call.exception('failed to run diagnostics on all opened files', notify=True),
        ]
        self.use_virtual_environment_mock.assert_called_once_with(self.ls, self.test_context.stem, {})
        self.pip_install_upgrade_mock.assert_called_once_with(self.ls, self.test_context.stem, 'python', self.requirements_file, {})
        self.compile_inventory_mock.assert_called_once_with(self.ls)
        self.validate_gherkin_mock.assert_called_once_with(self.ls, text_documents['first.feature'])
        self.ls_publish_diagnostics.assert_not_called()
        # // -->


class Test_ConfigurationIndexUrl(ServerConfigurationIndexUrl):
    @pytest.fixture(autouse=True)
    def _provide_fixture(self, server_configuration_index_url_fixture: ServerConfigurationIndexUrlFixture) -> None:
        self._provide(server_configuration_index_url_fixture)

    def test_via_cli_argument(self) -> None:
        # <!-- command line argument
        self.ls.client_settings.update({'pip_extra_index_url': 'https://user:pass@example.arg/pypi'})
        self.ls.index_url = 'https://user:pass@example.cli/pypi'
        self.pip_config_mock.get_value.return_value = 'https://user:pass@example.pip/pypi'

        _configuration_index_url(self.ls)

        assert self.ls.index_url == 'https://user:pass@example.cli/pypi'
        self.pip_configuration_mock.assert_not_called()
        self.pip_config_mock.assert_not_called()
        # // -->

    def test_not_provided(self) -> None:
        assert self.ls.index_url is None
        assert 'pip_extra_index_url' not in self.ls.client_settings

        # <!-- unable to load pip config file, no vscode extension settings
        self.pip_config_mock.load.side_effect = [PipConfigurationError]

        _configuration_index_url(self.ls)

        assert getattr(self.ls, 'index_url', '') is None
        self.pip_configuration_mock.assert_called_once_with(isolated=False)
        assert self.pip_config_mock.mock_calls == [call.load()]
        # // -->

    def test_vscode_extension_setting(self) -> None:
        # <!-- vscode extension setting set
        self.pip_config_mock.load.side_effect = [PipConfigurationError]
        self.ls.client_settings.update({'pip_extra_index_url': 'https://user:pass@example.arg/pypi'})

        _configuration_index_url(self.ls)

        assert self.ls.index_url == 'https://user:pass@example.arg/pypi'
        self.pip_configuration_mock.assert_called_once_with(isolated=False)
        assert self.pip_config_mock.mock_calls == [call.load()]
        # // -->

        self.reset_mocks()

        # <!-- vscode extension setting set, but empty string
        self.ls.index_url = None
        self.pip_config_mock.load.side_effect = [PipConfigurationError]
        self.ls.client_settings.update({'pip_extra_index_url': ''})

        _configuration_index_url(self.ls)

        assert getattr(self.ls, 'index_url', '') is None
        self.pip_configuration_mock.assert_called_once_with(isolated=False)
        assert self.pip_config_mock.mock_calls == [call.load()]
        # // -->

    def test_pip_config(self) -> None:
        # <!-- value from pip config, no vscode extension setting
        self.pip_config_mock.load.side_effect = None
        self.pip_config_mock.load.return_value = None
        self.pip_config_mock.get_value.return_value = 'https://user:pass@example.pip/pypi'

        _configuration_index_url(self.ls)

        assert self.ls.index_url == 'https://user:pass@example.pip/pypi'
        self.pip_configuration_mock.assert_called_once_with(isolated=False)
        assert self.pip_config_mock.mock_calls == [call.load(), call.get_value('global.index-url')]
        # // -->

    def test_pip_config_and_vscode_setting(self) -> None:
        # <!-- both set
        self.ls.client_settings.update({'pip_extra_index_url': 'https://user:pass@example.arg/pypi'})
        self.pip_config_mock.get_value.return_value = 'https://user:pass@example.pip/pypi'

        _configuration_index_url(self.ls)

        assert self.ls.index_url == 'https://user:pass@example.pip/pypi'
        self.pip_configuration_mock.assert_called_once_with(isolated=False)
        assert self.pip_config_mock.mock_calls == [call.load(), call.get_value('global.index-url')]
        # // -->


def test__configuration_variable_pattern(lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
    ls = lsp_fixture.server
    logger_mock = mocker.patch.object(ls, 'logger', spec=LogOutputChannelLogger)

    original_variable_pattern = ls.variable_pattern

    # <!-- no variable patterns, only default
    assert ls.client_settings.get('variable_pattern', []) == []

    _configuration_variable_pattern(ls)

    assert ls.variable_pattern.pattern == r'(.*ask for value of variable "([^"]*)"$|.*value for variable "([^"]*)" is ".*?"$)'
    assert logger_mock.mock_calls == []
    # // -->

    # <!-- variable pattern, missing groups or has more than one group
    ls.client_settings.update({'variable_pattern': ['hello world', '.*(he.*) (.*rld)']})

    _configuration_variable_pattern(ls)

    assert ls.variable_pattern.pattern == r'(.*ask for value of variable "([^"]*)"$|.*value for variable "([^"]*)" is ".*?"$)'
    assert logger_mock.mock_calls == [
        call.warning('variable pattern "hello world" contains 0 match groups, it must be exactly one', notify=True),
        call.warning('variable pattern ".*(he.*) (.*rld)" contains 2 match groups, it must be exactly one', notify=True),
    ]
    # // -->

    logger_mock.reset_mock()

    # <!-- valid patterns, each not matching at least one of the conditions for normalization
    ls.client_settings.update({'variable_pattern': ['hello (world)!', '^foo (bar)', 'bar (foo)$', '.*foo (baz)']})

    _configuration_variable_pattern(ls)

    assert ls.variable_pattern.pattern == r'(^.*bar (foo)$|^.*foo (baz)$|^.*hello (world)!$|^foo (bar)$)'
    assert logger_mock.mock_calls == []
    # // -->

    # <!-- invalid pattern
    ls.variable_pattern = original_variable_pattern
    ls.client_settings.update({'variable_pattern': ['hello (world!']})

    with pytest.raises(ConfigurationError):
        _configuration_variable_pattern(ls)

    assert ls.variable_pattern.pattern == r'(.*ask for value of variable "([^"]*)"$|.*value for variable "([^"]*)" is ".*?"$)'
    assert logger_mock.mock_calls == [call.exception('variable pattern "hello (world!" is not valid, check grizzly.variable_pattern setting', notify=True)]
    # // -->


class TestInitialize(ServerInitialize):
    @pytest.fixture(autouse=True)
    def _provide_fixture(self, server_initialize_fixture: ServerInitializeFixture) -> None:
        self._provide(server_initialize_fixture)

    def test_no_root(self) -> None:
        assert self.params.root_path is None
        assert self.params.root_uri is None

        initialize(self.ls, self.params)

        assert self.logger_mock.mock_calls == [
            call.info(f'initializing language server {__version__} (standalone)'),
            call.error('neither root path or uri was received from client', notify=True),
        ]

    def test_failed_to_initialize_extension(self) -> None:
        if sys.platform == 'win32':
            self.params.root_path = 'c:\\opt\\test'
            root_uri = self.params.root_path.replace('\\', '/')
            self.params.root_uri = f'file:///{root_uri}'
        else:
            self.params.root_path = '/opt/test'
            self.params.root_uri = f'file://{self.params.root_path}'

        # <!-- fail with ConfigurationError
        self.env.update({'GRIZZLY_RUN_EMBEDDED': 'false'})
        self.ls.add_startup_error_message('foobar')
        self.ls.add_startup_error_message('barfoo')
        self.get_capability_mock.side_effect = [ConfigurationError]

        assert getattr(self.ls, 'root_path', None) is None

        initialize(self.ls, self.params)

        assert str(self.ls.root_path).lower() == self.params.root_path.lower()
        assert [mock_call for mock_call in self.logger_mock.mock_calls if mock_call[0] != 'debug'] == [
            call.info(f'initializing language server {__version__} (standalone)'),
            call.log(logging.ERROR, 'foobar', exc_info=False, notify=True),
            call.log(logging.ERROR, 'barfoo', exc_info=False, notify=True),
        ]
        self.get_capability_mock.assert_called_once_with(self.params.capabilities, 'text_document.completion.completion_item.documentation_format', [lsp.MarkupKind.Markdown])
        # // -->

        self.reset_mocks()

        # <!-- fail with other exception
        self.get_capability_mock.side_effect = [RuntimeError]
        self.env.update({'GRIZZLY_RUN_EMBEDDED': 'true'})

        initialize(self.ls, self.params)

        assert [mock_call for mock_call in self.logger_mock.mock_calls if mock_call[0] != 'debug'] == [
            call.info(f'initializing language server {__version__} (embedded)'),
            call.exception('failed to initialize extension', notify=True),
        ]
        self.get_capability_mock.assert_called_once_with(self.params.capabilities, 'text_document.completion.completion_item.documentation_format', [lsp.MarkupKind.Markdown])
        # // -->

        self.reset_mocks()

    def test_successful(self) -> None:
        self.params.root_uri = f'file://{sep}c:{sep}workspaces{sep}grizzly-project'
        self.params.initialization_options = {'file_ignore_patterns': ['**/*.py']}
        self.get_capability_mock.return_value = [lsp.MarkupKind.PlainText]

        assert self.ls.client_settings == {}
        assert self.ls.markup_kind.value == lsp.MarkupKind.Markdown.value

        initialize(self.ls, self.params)

        assert [mock_call for mock_call in self.logger_mock.mock_calls if mock_call[0] != 'debug'] == [
            call.info(f'initializing language server {__version__} (standalone)'),
            call.info('done initializing extension'),
        ]
        assert self.ls.client_settings == {
            'file_ignore_patterns': ['**/*.py'],
            'quick_fix': {'step_impl_template': "@{keyword}('{expression}')"},
        }
        assert self.ls.markup_kind.value == lsp.MarkupKind.PlainText


class TestTextDocumentCompletion(ServerTextDocumentCompletion):
    @pytest.fixture(autouse=True)
    def _provide_fixture(self, server_text_document_completion_fixture: ServerTextDocumentCompletionFixture) -> None:
        self._provide(server_text_document_completion_fixture)

    def test_no_steps(self) -> None:
        self.ls.steps.clear()

        assert self.ls.steps == {}
        assert text_document_completion(self.ls, self.params) == SOME(lsp.CompletionList, is_incomplete=False, items=[])

        self.logger_mock.error.assert_called_once_with('no steps in inventory', notify=True)

    def test_unhandled_exception(self) -> None:
        """No files open in workspace, would trigger an exception."""
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        assert text_document_completion(self.ls, self.params) == SOME(lsp.CompletionList, is_incomplete=False, items=[])

        self.logger_mock.exception.assert_called_once_with('failed to complete step expression', notify=True)

    def test_complete_variable_name(self) -> None:
        # <!-- no partial variable name
        text_document = TextDocument(
            uri='file:///test.feature',
            source="""Feature: test
    Scenario: test
        Given value for variable "foobar" is "foobar"
        And value for variable "foobaz" is "foobaz"
        And value for variable "hello" is "world"
        Then log message "{{
""",  #                    ^
        )  #               +----------------------------------+
        #                                                     v
        self.params.position = lsp.Position(line=5, character=28)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar'), lsp.CompletionItem(label='hello')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_variable_name_mock.assert_called_once_with(self.ls, actual_line, text_document, self.params.position, partial=None)
        self.complete_expression_mock.assert_not_called()
        self.complete_metadata_mock.assert_not_called()
        self.complete_step_mock.assert_not_called()
        self.complete_keyword_mock.assert_not_called()
        # // -->

        self.reset_mocks()

        # <!-- partial variable name
        text_document = TextDocument(
            uri='file:///test.feature',
            source="""Feature: test
    Scenario: test
        Given value for variable "foobar" is "foobar"
        And value for variable "foobaz" is "foobaz"
        And value for variable "hello" is "world"
        Then log message "{{ foo
""",  #                        ^
        )  #                   +------------------------------+
        #                                                     v
        self.params.position = lsp.Position(line=5, character=32)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar'), lsp.CompletionItem(label='hello')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_variable_name_mock.assert_called_once_with(self.ls, actual_line, text_document, self.params.position, partial='foo')
        self.complete_expression_mock.assert_not_called()
        self.complete_metadata_mock.assert_not_called()
        self.complete_step_mock.assert_not_called()
        self.complete_keyword_mock.assert_not_called()
        # // -->

    def test_complete_expression(self) -> None:
        # <!-- no partial variable name
        text_document = TextDocument(
            uri='file:///test.feature',
            source="""Feature: test
    Scenario: test
        Given value for variable "foobar" is "foobar"
        And value for variable "foobaz" is "foobaz"
        And value for variable "hello" is "world"
        {%
""",  #   ^__,
        )  # +------------------------------------------------+
        #                                                     v
        self.params.position = lsp.Position(line=5, character=10)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar'), lsp.CompletionItem(label='hello')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_expression_mock.assert_called_once_with(self.ls, actual_line, text_document, self.params.position, partial=None)
        self.complete_variable_name_mock.assert_not_called()
        self.complete_metadata_mock.assert_not_called()
        self.complete_step_mock.assert_not_called()
        self.complete_keyword_mock.assert_not_called()
        # // -->

        self.reset_mocks()

        # <!-- partial expression name
        text_document = TextDocument(
            uri='file:///test.feature',
            source="""Feature: test
    Scenario: test
        Given value for variable "foobar" is "foobar"
        And value for variable "foobaz" is "foobaz"
        And value for variable "hello" is "world"
        {% sce
""",  #       ^
        )  #  +-----------------------------------------------+
        #                                                     v
        self.params.position = lsp.Position(line=5, character=14)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar'), lsp.CompletionItem(label='hello')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_expression_mock.assert_called_once_with(self.ls, actual_line, text_document, self.params.position, partial='sce')
        self.complete_variable_name_mock.assert_not_called()
        self.complete_metadata_mock.assert_not_called()
        self.complete_step_mock.assert_not_called()
        self.complete_keyword_mock.assert_not_called()
        # // -->

    def test_complete_metadata(self) -> None:
        text_document = TextDocument(
            uri='file:///test.feature',
            source='# ',
        )
        self.params.position = lsp.Position(line=0, character=2)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_expression_mock.assert_not_called()
        self.complete_variable_name_mock.assert_not_called()
        self.complete_metadata_mock.assert_called_once_with(actual_line, self.params.position)
        self.complete_step_mock.assert_not_called()
        self.complete_keyword_mock.assert_not_called()

    def test_complete_step(self) -> None:
        text_document = TextDocument(
            uri='file:///test.feature',
            source="""Feature: test
    Scenario: test
        Given value for variable "foobar" is "foobar"
        And value for variable "foobaz" is "foobaz"
        And value f
""",  #            ^
        )  #       +------------------------------------------+
        #                                                     v
        self.params.position = lsp.Position(line=4, character=20)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_expression_mock.assert_not_called()
        self.complete_variable_name_mock.assert_not_called()
        self.complete_metadata_mock.assert_not_called()
        self.complete_step_mock.assert_called_once_with(self.ls, 'And', self.params.position, 'value f', base_keyword='Given')
        self.complete_keyword_mock.assert_not_called()

    def test_complete_keyword(self) -> None:
        text_document = TextDocument(
            uri='file:///test.feature',
            source="""Feature: test
    Scenario: test
        Given value for variable "foobar" is "foobar"
        And value for variable "foobaz" is "foobaz"
        A
""",  #  ^_________,
        )  #       +------------------------------------------+
        #                                                     v
        self.params.position = lsp.Position(line=4, character=10)
        self.ls_get_text_document_mock.return_value = text_document
        actual_line = text_document.source.strip().split('\n')[-1]
        self.get_current_line_mock.return_value = actual_line
        self.complete_variable_name_mock.return_value = [lsp.CompletionItem(label='foobar')]
        self.ls.steps.update({'then': [Step('then', 'this step actually exists!', func=lambda _: _)]})

        text_document_completion(self.ls, self.params)

        self.complete_expression_mock.assert_not_called()
        self.complete_variable_name_mock.assert_not_called()
        self.complete_metadata_mock.assert_not_called()
        self.complete_step_mock.assert_not_called()
        self.complete_keyword_mock.assert_called_once_with(self.ls, 'A', self.params.position, text_document)
