from __future__ import annotations

import logging
import sys
from inspect import getsourcelines
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING

from grizzly_ls.model import Step
from grizzly_ls.server.features.definition import (
    get_file_url_definition,
    get_step_definition,
)
from lsprotocol import types as lsp
from pygls.workspace import Workspace

from test_ls.conftest import GRIZZLY_PROJECT
from test_ls.helpers import SOME

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture

    from test_ls.fixtures import LspFixture


def test_get_step_definition(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server

    text_document = lsp.TextDocumentIdentifier('file:///hello.feature')
    position = lsp.Position(line=0, character=0)
    params = lsp.DefinitionParams(text_document, position)

    assert get_step_definition(ls, params, '') is None
    assert get_step_definition(ls, params, 'Then ') is None

    def step_impl() -> None:  # <!-- lineno
        pass

    _, lineno = getsourcelines(step_impl)

    ls.steps.update(
        {
            'given': [
                Step('given', 'foobar', step_impl, 'todo'),
                Step('given', 'hello world!', step_impl, 'todo'),
            ]
        }
    )
    params.position.character = 5
    actual_definition = get_step_definition(ls, params, 'Given hello world!')

    assert actual_definition is not None
    assert actual_definition.target_uri == Path(__file__).as_uri()
    assert actual_definition.target_range == lsp.Range(
        start=lsp.Position(line=lineno, character=0),
        end=lsp.Position(line=lineno, character=0),
    )


def test_get_file_url_definition(lsp_fixture: LspFixture, caplog: LogCaptureFixture) -> None:
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT
    ls.lsp._workspace = Workspace(ls.root_path.as_uri())

    test_feature_file = ls.root_path / 'features' / 'empty.feature'
    test_file = ls.root_path / 'features' / 'requests' / 'test.txt'
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch()

    test_feature_file_included = ls.root_path / 'features' / 'included.feature'
    test_feature_file_included.write_text(
        """Feature: test feature
    Background: common steps

    Scenario: world
        Then log message "boo"

    Scenario: hello
        Then log message "yay"

    Scenario: foo
        Then log messge "bar"
"""
    )

    def get_platform_uri(uri: str) -> object:
        class WrappedPlatformUri(str):
            __slots__: list[str] = []

            def __hash__(self) -> int:
                return hash(self)

            def __eq__(self, other: object) -> bool:
                nonlocal uri
                # windows is case-insensitive, and drive letter can be different case...
                # and drive latters in uri's from LSP seems to be in lower-case...
                if not isinstance(other, str):
                    return False

                if sys.platform == 'win32':
                    uri = uri.lower()
                    other = other.lower()

                return uri == other

            def __ne__(self, other: object) -> bool:
                return not self.__eq__(other)

        return WrappedPlatformUri()

    try:
        text_document = lsp.TextDocumentIdentifier(test_feature_file.as_uri())
        position = lsp.Position(line=0, character=0)
        params = lsp.DefinitionParams(text_document, position)

        # no files
        assert (
            get_file_url_definition(
                ls,
                params,
                'Then this is a variable "hello" and this is also a variable "world"',
            )
            == []
        )

        # `file://` in a "variable"
        position.character = 26
        actual_definitions = get_file_url_definition(
            ls,
            params,
            f'Then this is a variable "file://./requests/test.txt" and this is also a variable "$include::{test_file.as_uri()}$"',
        )  # .character =               ^- 26                    ^- 51

        assert len(actual_definitions) == 1
        assert actual_definitions[0] == SOME(
            lsp.LocationLink,
            target_uri=get_platform_uri(test_file.as_uri()),
            target_range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            target_selection_range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            origin_selection_range=lsp.Range(
                start=lsp.Position(line=0, character=25),
                end=lsp.Position(line=0, character=51),
            ),
        )

        # `$include::file://..$` in a "variable"
        position.character = 95
        expression = f'Then this is a variable "file://./requests/test.txt" and this is also a variable "$include::{test_file.as_uri()}$"'
        actual_definitions = get_file_url_definition(
            ls,
            params,
            expression,
        )

        assert len(actual_definitions) == 1
        assert actual_definitions[0] == SOME(
            lsp.LocationLink,
            target_uri=get_platform_uri(test_file.as_uri()),
            target_range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            target_selection_range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            origin_selection_range=lsp.Range(
                start=lsp.Position(line=0, character=92),
                end=lsp.Position(line=0, character=92 + len(test_file.as_uri())),
            ),
        )

        # classic (relative to grizzly requests directory)
        position.character = 16
        actual_definitions = get_file_url_definition(ls, params, 'Then send file "test.txt"')  # .character =                ^- 16   ^- 24

        assert len(actual_definitions) == 1
        assert actual_definitions[0] == SOME(
            lsp.LocationLink,
            target_uri=get_platform_uri(test_file.as_uri()),
            target_range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            target_selection_range=lsp.Range(
                start=lsp.Position(line=0, character=0),
                end=lsp.Position(line=0, character=0),
            ),
            origin_selection_range=lsp.Range(
                start=lsp.Position(line=0, character=16),
                end=lsp.Position(line=0, character=24),
            ),
        )

        # {% scenario ... %}
        position.character = 30
        for path in ['', './', f'{test_feature_file_included.parent.as_posix()}/']:
            feature_argument = f'{path}included.feature'
            position = lsp.Position(line=0, character=32)
            params = lsp.DefinitionParams(text_document, position)
            with caplog.at_level(logging.DEBUG):
                actual_definitions = get_file_url_definition(ls, params, f'{{% scenario "hello", feature="{feature_argument}" %}}')

            assert len(actual_definitions) == 1
            assert actual_definitions[0] == SOME(
                lsp.LocationLink,
                target_uri=get_platform_uri(test_feature_file_included.as_uri()),
                target_range=lsp.Range(
                    start=lsp.Position(line=6, character=19),
                    end=lsp.Position(line=6, character=19),
                ),
                target_selection_range=lsp.Range(
                    start=lsp.Position(line=6, character=19),
                    end=lsp.Position(line=6, character=19),
                ),
                origin_selection_range=lsp.Range(
                    start=lsp.Position(line=0, character=30),
                    end=lsp.Position(line=0, character=30 + len(feature_argument)),
                ),
            )
    finally:
        test_feature_file_included.unlink()
        rmtree(test_file.parent)
