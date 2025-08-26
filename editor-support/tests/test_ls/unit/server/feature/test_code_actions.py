from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from grizzly_ls.constants import (
    MARKER_LANG_NOT_VALID,
    MARKER_LANG_WRONG_LINE,
    MARKER_LANGUAGE,
    MARKER_NO_STEP_IMPL,
)
from grizzly_ls.server.features.code_actions import (
    generate_quick_fixes,
    quick_fix_lang_not_valid,
    quick_fix_lang_wrong_line,
    quick_fix_no_step_impl,
)
from lsprotocol import types as lsp
from pygls.workspace import TextDocument

from test_ls.conftest import GRIZZLY_PROJECT
from test_ls.helpers import SOME

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from test_ls.fixtures import LspFixture


def test_quick_fix_no_step_impl(lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT
    expected_quick_fix_file = GRIZZLY_PROJECT / 'steps' / 'steps.py'

    feature_file = lsp_fixture.datadir / 'features' / 'test_quick_fix_no_step_impl.feature'

    text_document = TextDocument(feature_file.as_uri())

    def assert_quick_fix_edit(quick_fix: lsp.CodeAction, new_text: str) -> None:
        actual_edit = quick_fix.edit
        assert isinstance(actual_edit, lsp.WorkspaceEdit)
        assert actual_edit.changes is not None

        actual_changes = actual_edit.changes.get(expected_quick_fix_file.as_uri(), None)
        assert actual_changes is not None
        assert len(actual_changes) == 1
        actual_text_edit = actual_changes[0]
        assert actual_text_edit.new_text == new_text

        source = expected_quick_fix_file.read_text().splitlines()
        expected_position = lsp.Position(line=len(source), character=0)

        assert actual_text_edit.range == lsp.Range(start=expected_position, end=expected_position)

    try:
        # <!-- "And"
        feature_file.write_text(
            """Given foobar
"""
        )
        diagnostic = lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=1, character=0),
                end=lsp.Position(line=1, character=0),
            ),
            message=f'{MARKER_NO_STEP_IMPL}:\nAnd hello world',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )

        # no template
        with suppress(KeyError):
            del ls.client_settings['quick_fix']

        assert quick_fix_no_step_impl(ls, diagnostic, text_document) is None

        ls.client_settings.update({'quick_fix': {'step_impl_template': "@{keyword}(u'{expression}')"}})

        # no quick fix file
        ls.root_path = Path('/tmp/asdf')  # noqa: S108
        assert quick_fix_no_step_impl(ls, diagnostic, text_document) is None

        # all good
        ls.root_path = GRIZZLY_PROJECT

        quick_fix = quick_fix_no_step_impl(ls, diagnostic, text_document)
        assert quick_fix == SOME(
            lsp.CodeAction,
            title='Create step implementation',
            kind=lsp.CodeActionKind.QuickFix,
            diagnostics=[diagnostic],
            command=lsp.Command('Rebuild step inventory', 'grizzly.server.inventory.rebuild'),
        )

        assert_quick_fix_edit(
            quick_fix,
            """
@given(u'hello world')
def step_impl(context: Context) -> None:
    raise NotImplementedError('no step implementation')
""",
        )
        # // -->

        # <!-- Given
        diagnostic = lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            message=f'{MARKER_NO_STEP_IMPL}:\nGiven a whole lot of cash',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )

        ls.client_settings.update({'quick_fix': {'step_impl_template': "@step({keyword}, en=u'{expression}')"}})

        quick_fix = quick_fix_no_step_impl(ls, diagnostic, text_document)
        assert quick_fix == SOME(
            lsp.CodeAction,
            title='Create step implementation',
            kind=lsp.CodeActionKind.QuickFix,
            diagnostics=[diagnostic],
            command=lsp.Command('Rebuild step inventory', 'grizzly.server.inventory.rebuild'),
        )

        assert_quick_fix_edit(
            quick_fix,
            """
@step(given, en=u'a whole lot of cash')
def step_impl(context: Context) -> None:
    raise NotImplementedError('no step implementation')
""",
        )
        # // -->

        # <!-- no a valid gherkin expression
        diagnostic = lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            message=f'{MARKER_NO_STEP_IMPL}:\nIf',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )

        assert quick_fix_no_step_impl(ls, diagnostic, text_document) is None
        # // -->

        # <!-- with arguments
        mocker.patch(
            'random_word.RandomWords.get_random_word',
            return_value='foobar',
        )
        diagnostic = lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            message=f'{MARKER_NO_STEP_IMPL}:\nGiven a "book" with "100" pages',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )

        quick_fix = quick_fix_no_step_impl(ls, diagnostic, text_document)
        assert quick_fix == SOME(
            lsp.CodeAction,
            title='Create step implementation',
            kind=lsp.CodeActionKind.QuickFix,
            diagnostics=[diagnostic],
            command=lsp.Command('Rebuild step inventory', 'grizzly.server.inventory.rebuild'),
        )

        assert_quick_fix_edit(
            quick_fix,
            """
@step(given, en=u'a "{book}" with "{foobar}" pages')
def step_impl(context: Context, book: str, foobar: str) -> None:
    raise NotImplementedError('no step implementation')
""",
        )
        # // -->

        # <!-- error...
        mocker.patch.object(ls, 'get_language_key', side_effect=ValueError)

        assert quick_fix_no_step_impl(ls, diagnostic, text_document) is None
        # // -->
    finally:
        feature_file.unlink()


def test_quick_fix_lang_not_valid() -> None:
    text_document = TextDocument(uri='file:///test.feature', source='')

    def create_diagnostic(message: str) -> lsp.Diagnostic:
        return lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            message=message,
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )

    # <!-- default to 'en'
    diagnostic = create_diagnostic(f'"huggabugga" {MARKER_LANG_NOT_VALID}')

    quick_fix = quick_fix_lang_not_valid(text_document, diagnostic)

    assert [
        SOME(
            lsp.CodeAction,
            title='Change language to "en"',
            kind=lsp.CodeActionKind.QuickFix,
            edit=SOME(lsp.WorkspaceEdit, changes={'file:///test.feature': [SOME(lsp.TextEdit, new_text='en')]}),
        )
    ] == quick_fix
    # // -->

    # <!-- long typed name to short
    diagnostic = create_diagnostic(f'"swedish" {MARKER_LANG_NOT_VALID}')

    quick_fix = quick_fix_lang_not_valid(text_document, diagnostic)

    assert [
        SOME(
            lsp.CodeAction,
            title='Change language to "sv"',
            kind=lsp.CodeActionKind.QuickFix,
            edit=SOME(lsp.WorkspaceEdit, changes={'file:///test.feature': [SOME(lsp.TextEdit, new_text='sv')]}),
        )
    ] == quick_fix
    # // -->

    # <!-- long typed native to short
    diagnostic = create_diagnostic(f'"Svenska" {MARKER_LANG_NOT_VALID}')

    quick_fix = quick_fix_lang_not_valid(text_document, diagnostic)

    assert [
        SOME(
            lsp.CodeAction,
            title='Change language to "sv"',
            kind=lsp.CodeActionKind.QuickFix,
            edit=SOME(lsp.WorkspaceEdit, changes={'file:///test.feature': [SOME(lsp.TextEdit, new_text='sv')]}),
        )
    ] == quick_fix
    # // -->

    # <!-- closes match
    diagnostic = create_diagnostic(f'"Cyrl" {MARKER_LANG_NOT_VALID}')

    quick_fix = quick_fix_lang_not_valid(text_document, diagnostic)

    assert [
        SOME(
            lsp.CodeAction,
            title='Change language to "sr-Cyrl"',
            kind=lsp.CodeActionKind.QuickFix,
            edit=SOME(lsp.WorkspaceEdit, changes={'file:///test.feature': [SOME(lsp.TextEdit, new_text='sr-Cyrl')]}),
        ),
        SOME(
            lsp.CodeAction,
            title='Change language to "mk-Cyrl"',
            kind=lsp.CodeActionKind.QuickFix,
            edit=SOME(lsp.WorkspaceEdit, changes={'file:///test.feature': [SOME(lsp.TextEdit, new_text='mk-Cyrl')]}),
        ),
    ] == quick_fix
    # // -->


def test_quick_fix_lang_wrong_line() -> None:
    diagnostic = lsp.Diagnostic(
        range=lsp.Range(
            start=lsp.Position(line=2, character=0),
            end=lsp.Position(line=2, character=14),
        ),
        message=f'"{MARKER_LANGUAGE}" {MARKER_LANG_WRONG_LINE}',
        severity=lsp.DiagnosticSeverity.Warning,
        source='Dummy',
    )

    # <!-- unable to move text around
    text_document = TextDocument(
        uri='file:///test.feature',
        source='',
    )

    assert quick_fix_lang_wrong_line(text_document, diagnostic) is None
    # // -->

    text_document = TextDocument(
        uri='file:///test.feature',
        source="""
Feature: hello
# language: en
    Scenario: test
        Given sure
""",
    )

    quick_fix = quick_fix_lang_wrong_line(text_document, diagnostic)
    assert quick_fix is not None
    actual_edit = quick_fix.edit
    assert actual_edit is not None
    actual_changes = actual_edit.changes
    assert actual_changes is not None
    assert len(actual_changes) == 1
    actual_text_edits = actual_changes.get('file:///test.feature', None)
    assert actual_text_edits is not None
    assert len(actual_text_edits) == 1
    assert (
        actual_text_edits[0].new_text
        == """# language: en

Feature: hello
    Scenario: test
        Given sure"""
    )


def test_generate_quick_fixes(lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT

    diagnostics: list[lsp.Diagnostic] = []

    quick_fix_no_step_impl_mock = mocker.patch('grizzly_ls.server.features.code_actions.quick_fix_no_step_impl')
    quick_fix_lang_not_valid_mock = mocker.patch('grizzly_ls.server.features.code_actions.quick_fix_lang_not_valid')
    quick_fix_lang_wrong_line_mock = mocker.patch('grizzly_ls.server.features.code_actions.quick_fix_lang_wrong_line')

    text_document = TextDocument(
        uri='file:///test.feature',
        source="""
Feature: hello
# language: en
    Scenario: test
        Given sure
""",
    )

    # <!-- no quick fixes
    assert generate_quick_fixes(ls, text_document, []) is None
    quick_fix_no_step_impl_mock.assert_not_called()
    quick_fix_lang_not_valid_mock.assert_not_called()
    quick_fix_lang_wrong_line_mock.assert_not_called()
    # // -->

    # <!-- all the quick fixes
    # language wrong line
    diagnostics.append(
        lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=2, character=0),
                end=lsp.Position(line=2, character=14),
            ),
            message=f'"{MARKER_LANGUAGE}" {MARKER_LANG_WRONG_LINE}',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )
    )

    # language invalid
    diagnostics.append(
        lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            message=f'"en-" {MARKER_LANG_NOT_VALID}',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )
    )

    # no step implementation
    diagnostics.append(
        lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            message=f'{MARKER_NO_STEP_IMPL}:\nGiven a "book" with "100" pages',
            severity=lsp.DiagnosticSeverity.Warning,
            source='Dummy',
        )
    )

    quick_fixes = generate_quick_fixes(ls, text_document, diagnostics)
    assert quick_fixes is not None
    assert len(quick_fixes) == 3
    quick_fix_no_step_impl_mock.assert_called_once_with(ls, diagnostics[2], text_document)
    quick_fix_lang_wrong_line_mock.assert_called_once_with(text_document, diagnostics[0])
    quick_fix_lang_not_valid_mock.assert_called_once_with(text_document, diagnostics[1])
    # // -->
