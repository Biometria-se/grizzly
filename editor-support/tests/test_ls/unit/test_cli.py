from __future__ import annotations

from argparse import Namespace
from typing import TYPE_CHECKING
from unittest.mock import call

from colorama import Fore
from grizzly_ls.cli import _get_severity_color, diagnostic_to_text, lint, render
from lsprotocol import types as lsp
from pygls.workspace import TextDocument

from test_ls.helpers import ANY, SOME, rm_rf

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory

    from test_ls.fixtures import CwdFixture, LspFixture, MockerFixture


def test__get_severity_color() -> None:
    assert _get_severity_color(None) == Fore.RESET
    assert _get_severity_color(lsp.DiagnosticSeverity.Error) == Fore.RED
    assert _get_severity_color(lsp.DiagnosticSeverity.Information) == Fore.BLUE
    assert _get_severity_color(lsp.DiagnosticSeverity.Warning) == Fore.YELLOW
    assert _get_severity_color(lsp.DiagnosticSeverity.Hint) == Fore.CYAN


def test_diagnostic_to_text() -> None:
    diagnostic = lsp.Diagnostic(
        range=lsp.Range(
            start=lsp.Position(line=9, character=10),
            end=lsp.Position(line=9, character=15),
        ),
        message='foobar',
        severity=lsp.DiagnosticSeverity.Warning,
        source='test_diagnostic_to_text',
    )

    assert diagnostic_to_text('foobar.feature', diagnostic, max_length=14) == f'foobar.feature:10:11    {Fore.YELLOW}warning{Fore.RESET} foobar'


def test_lint(lsp_fixture: LspFixture, tmp_path_factory: TempPathFactory, cwd_fixture: CwdFixture, mocker: MockerFixture, capsys: CaptureFixture) -> None:
    ls = lsp_fixture.server
    test_context = tmp_path_factory.mktemp('test-context')

    try:
        colorama_init_mock = mocker.patch('grizzly_ls.cli.colorama_init', return_value=None)
        compile_inventory_mock = mocker.patch('grizzly_ls.cli.compile_inventory', return_value=None)
        validate_gherkin_mock = mocker.patch('grizzly_ls.cli.validate_gherkin', return_value=[])

        with cwd_fixture(test_context):
            for file_name in ['test1.feature', 'dir/test2.feature', 'test3.feature']:
                test_feature = test_context / file_name
                test_feature.parent.mkdir(exist_ok=True)
                test_feature.touch()

            capsys.readouterr()
            # <!-- validate all files in cwd, passes
            args = Namespace(files=['.'])

            assert lint(ls, args) == 0

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == ''
            colorama_init_mock.assert_called_once_with()
            compile_inventory_mock.assert_called_once_with(ls, standalone=True)
            assert validate_gherkin_mock.mock_calls == [
                call(ls, ANY(TextDocument)),
                call(ls, ANY(TextDocument)),
                call(ls, ANY(TextDocument)),
            ]
            # // -->

            validate_gherkin_mock.reset_mock()

            # <!-- validate one specific file, does not pass
            args = Namespace(files=['dir/test2.feature'])
            validate_gherkin_mock.return_value = [
                lsp.Diagnostic(
                    range=lsp.Range(start=lsp.Position(line=1, character=2), end=lsp.Position(line=1, character=4)), message='invalid format', severity=lsp.DiagnosticSeverity.Error
                )
            ]

            assert lint(ls, args) == 1

            assert validate_gherkin_mock.mock_calls == [call(ls, SOME(TextDocument, uri=(test_context / 'dir/test2.feature').as_uri()))]
            capture = capsys.readouterr()
            assert capture.out == f'dir/test2.feature:2:3      {Fore.RED}error{Fore.RESET}   invalid format\n'
            assert capture.err == ''
            # // ->
    finally:
        rm_rf(test_context)


def test_render(tmp_path_factory: TempPathFactory, cwd_fixture: CwdFixture, mocker: MockerFixture, capsys: CaptureFixture) -> None:
    test_context = tmp_path_factory.mktemp('test-context')

    try:
        render_gherkin_mock = mocker.patch('grizzly_ls.cli.render_gherkin', return_value=None)
        with cwd_fixture(test_context):
            capsys.readouterr()

            # <!-- no file specified
            args = Namespace(file=[])
            assert render(args) == 1

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'no file specified\n'
            render_gherkin_mock.assert_not_called()
            # // -->

            # <!-- file does not exist
            args = Namespace(file=['test.feature'])
            assert render(args) == 1

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'test.feature does not exist\n'
            render_gherkin_mock.assert_not_called()
            # // -->

            test_feature = test_context / 'test.feature'
            test_feature.touch()

            # <!-- failed to render
            args = Namespace(file=['test.feature'])
            render_gherkin_mock.side_effect = [RuntimeError('no dice')]
            assert render(args) == 1

            capture = capsys.readouterr()
            assert capture.out == ''
            assert 'no dice' in capture.err
            render_gherkin_mock.assert_called_once_with(test_feature.relative_to(test_context).as_posix(), test_feature.read_text(), raw=True)
            # // -->

            render_gherkin_mock.reset_mock()

            # <!-- render OK
            render_gherkin_mock.side_effect = None
            render_gherkin_mock.return_value = 'all good!'

            assert render(args) == 0
            capture = capsys.readouterr()
            assert capture.out == 'all good!\n'
            assert capture.err == ''
            # // -->
    finally:
        rm_rf(test_context)
