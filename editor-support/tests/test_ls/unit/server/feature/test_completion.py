from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from grizzly_ls.constants import MARKER_LANGUAGE
from grizzly_ls.server.features.completion import (
    complete_expression,
    complete_keyword,
    complete_metadata,
    complete_step,
    complete_variable_name,
    get_trigger,
)
from grizzly_ls.server.inventory import compile_inventory
from lsprotocol import types as lsp
from pygls.workspace import TextDocument

from test_ls.conftest import GRIZZLY_PROJECT
from test_ls.helpers import normalize_completion_item, normalize_completion_text_edit

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture

    from test_ls.fixtures import LspFixture


@pytest.mark.parametrize('trigger_char', ['{{', '{%'])
def test_get_trigger(trigger_char: str) -> None:
    assert get_trigger('Then what up', trigger_char) is False
    assert get_trigger(f'Then what up you "{trigger_char}', trigger_char) is None
    assert get_trigger(f'Then what up you "{trigger_char} foo', trigger_char) == 'foo'
    assert get_trigger(f'    Then try to complete variable "{trigger_char} f', trigger_char) == 'f'


def test_complete_keyword(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT
    compile_inventory(ls)

    text_document = TextDocument(
        uri='dummy.feature',
        source='',
    )

    null_position = lsp.Position(line=0, character=0)

    assert normalize_completion_item(
        complete_keyword(ls, None, null_position, text_document),
        lsp.CompletionItemKind.Keyword,
    ) == [
        'Ability',
        'Business Need',
        'Feature',
    ]

    text_document = TextDocument(
        uri='dummy.feature',
        source='Feature:',
    )

    assert sorted(
        normalize_completion_item(
            complete_keyword(ls, None, null_position, text_document),
            lsp.CompletionItemKind.Keyword,
        )
    ) == sorted(
        [
            'Background',
            'Example',
            'Scenario',
            'Scenario Outline',
            'Scenario Template',
        ]
    )

    text_document = TextDocument(
        uri='dummy.feature',
        source="""Feature:
Scenario:
""",
    )

    assert sorted(
        normalize_completion_item(
            complete_keyword(ls, None, null_position, text_document),
            lsp.CompletionItemKind.Keyword,
        )
    ) == sorted(
        [
            'And',
            'Background',
            'But',
            'Example',
            'Examples',
            'Given',
            'Scenario',
            'Scenario Outline',
            'Scenario Template',
            'Scenarios',
            'Then',
            'When',
        ]
    )

    text_document = TextDocument(
        uri='dummy.feature',
        source="""Feature:
Background:
    Given a bunch of stuff
Scenario:
""",
    )

    assert sorted(
        normalize_completion_item(
            complete_keyword(ls, None, null_position, text_document),
            lsp.CompletionItemKind.Keyword,
        )
    ) == sorted(
        [
            'And',
            'But',
            'Example',
            'Examples',
            'Given',
            'Scenario',
            'Scenario Outline',
            'Scenario Template',
            'Scenarios',
            'Then',
            'When',
        ]
    )

    assert sorted(
        normalize_completion_item(
            complete_keyword(ls, 'EN', lsp.Position(line=0, character=2), text_document),
            lsp.CompletionItemKind.Keyword,
        )
    ) == sorted(
        [
            'Given',
            'Scenario',
            'Scenario Template',
            'Scenario Outline',
            'Scenarios',
            'Then',
            'When',
        ]
    )

    assert normalize_completion_item(
        complete_keyword(ls, 'Giv', lsp.Position(line=0, character=4), text_document),
        lsp.CompletionItemKind.Keyword,
    ) == [
        'Given',
    ]


def test_complete_step(lsp_fixture: LspFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT
    compile_inventory(ls)

    with caplog.at_level(logging.DEBUG):
        matched_steps = normalize_completion_item(
            complete_step(
                ls,
                'Given',
                lsp.Position(line=0, character=6),
                'variable',
                base_keyword='Given',
            ),
            lsp.CompletionItemKind.Function,
        )

        for expected_step in [
            'set context variable "" to ""',
            'ask for value of variable ""',
            'set alias "" for variable ""',
            'value for variable "" is ""',
        ]:
            assert expected_step in matched_steps

        matched_steps = sorted(
            normalize_completion_item(
                complete_step(
                    ls,
                    'Then',
                    lsp.Position(line=0, character=5),
                    'save',
                    base_keyword='Then',
                ),
                lsp.CompletionItemKind.Function,
            )
        )

        for expected_step in sorted(
            [
                'save response metadata "" in variable ""',
                'save response payload "" in variable ""',
                'save response payload "" that matches "" in variable ""',
                'save response metadata "" that matches "" in variable ""',
                'get from "" with name "" and save response payload in ""',
                'parse date "" and save in variable ""',
                'parse "" as "undefined" and save value of "" in variable ""',
                'parse "" as "plain" and save value of "" in variable ""',
                'parse "" as "xml" and save value of "" in variable ""',
                'parse "" as "json" and save value of "" in variable ""',
                'parse "" as "octet_stream_utf8" and save value of "" in variable ""',
            ]
        ):
            assert expected_step in matched_steps

        suggested_steps = complete_step(
            ls,
            'Then',
            lsp.Position(line=0, character=35),
            'save response metadata "hello"',
            base_keyword='Then',
        )
        matched_steps = normalize_completion_item(suggested_steps, lsp.CompletionItemKind.Function)

        for expected_step in [
            'save response metadata "hello" in variable ""',
            'save response metadata "hello" that matches "" in variable ""',
        ]:
            assert expected_step in matched_steps

        for suggested_step in suggested_steps:
            if suggested_step.label == 'save response metadata "hello" that matches "" in variable ""':
                assert suggested_step.text_edit is not None
                assert suggested_step.text_edit.new_text.endswith(' that matches "$1" in variable "$2"')
            elif suggested_step.label == 'save response metadata "hello" in variable ""':
                assert suggested_step.text_edit is not None
                assert suggested_step.text_edit.new_text.endswith(' in variable "$1"')
            else:
                message = f'"{suggested_step.label}" was an unexpected suggested step'
                raise AssertionError(message)

        matched_steps = normalize_completion_item(
            complete_step(ls, 'When', lsp.Position(line=0, character=4), None, base_keyword='When'),
            lsp.CompletionItemKind.Function,
        )

        for expected_step in [
            'condition "" with name "" is true, execute these tasks',
            'fail ratio is greater than ""% fail scenario',
            'average response time is greater than "" milliseconds fail scenario',
            'response time percentile ""% is greater than "" milliseconds fail scenario',
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ]:
            assert expected_step in matched_steps

        matched_steps = normalize_completion_item(
            complete_step(
                ls,
                'When',
                lsp.Position(line=0, character=13),
                'response ',
                base_keyword='When',
            ),
            lsp.CompletionItemKind.Function,
        )

        for expected_step in [
            'response time percentile ""% is greater than "" milliseconds fail scenario',
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ]:
            assert expected_step in matched_steps

        matched_steps = normalize_completion_item(
            complete_step(
                ls,
                'When',
                lsp.Position(line=0, character=25),
                'response fail request',
                base_keyword='When',
            ),
            lsp.CompletionItemKind.Function,
        )

        for expected_step in [
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ]:
            assert expected_step in matched_steps

        matched_steps = normalize_completion_item(
            complete_step(
                ls,
                'When',
                lsp.Position(line=0, character=39),
                'response payload "" is fail request',
                base_keyword='When',
            ),
            lsp.CompletionItemKind.Function,
        )

        for expected_step in [
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
        ]:
            assert expected_step in matched_steps

        matched_steps = normalize_completion_item(
            complete_step(
                ls,
                'And',
                lsp.Position(line=0, character=50),
                'a user of type "RestApi" with weight "1" load',
                base_keyword='Given',
            ),
            lsp.CompletionItemKind.Function,
        )

        assert len(matched_steps) == 1
        assert matched_steps[0] == 'a user of type "RestApi" with weight "1" load testing ""'

        actual_completed_steps = complete_step(
            ls,
            'And',
            lsp.Position(line=0, character=20),
            'repeat for "1" it',
            base_keyword='Given',
        )

        matched_steps = normalize_completion_item(
            actual_completed_steps,
            lsp.CompletionItemKind.Function,
        )

        assert sorted(matched_steps) == sorted(['repeat for "1" iterations', 'repeat for "1" iteration'])

        matched_text_edit = normalize_completion_text_edit(actual_completed_steps, lsp.CompletionItemKind.Function)

        assert sorted(matched_text_edit) == sorted(['repeat for "1" iteration', 'repeat for "1" iterations'])

        actual_completed_steps = complete_step(
            ls,
            'And',
            lsp.Position(line=0, character=16),
            'repeat for "1"',
            base_keyword='Given',
        )

        matched_steps = normalize_completion_item(
            actual_completed_steps,
            lsp.CompletionItemKind.Function,
        )

        assert sorted(matched_steps) == sorted(['repeat for "1" iterations', 'repeat for "1" iteration'])

        matched_text_edit = normalize_completion_text_edit(actual_completed_steps, lsp.CompletionItemKind.Function)

        assert sorted(matched_text_edit) == sorted(['repeat for "1" iteration', 'repeat for "1" iterations'])

        actual_completed_steps = complete_step(
            ls,
            'And',
            lsp.Position(line=0, character=17),
            'repeat for "1" ',
            base_keyword='Given',
        )

        matched_steps = normalize_completion_item(
            actual_completed_steps,
            lsp.CompletionItemKind.Function,
        )

        assert sorted(matched_steps) == sorted(['repeat for "1" iterations', 'repeat for "1" iteration'])

        matched_text_edit = normalize_completion_text_edit(actual_completed_steps, lsp.CompletionItemKind.Function)

        assert sorted(matched_text_edit) == sorted(['repeat for "1" iteration', 'repeat for "1" iterations'])

        actual_completed_steps = complete_step(
            ls,
            'And',
            lsp.Position(line=0, character=38),
            'parse date "{{ datetime.now() }}" ',
            base_keyword='Then',
        )
        assert len(actual_completed_steps) == 1
        actual_completed_step = actual_completed_steps[0]
        assert actual_completed_step.text_edit is not None
        assert actual_completed_step.text_edit.new_text.endswith('and save in variable "$1"')

        actual_completed_steps = complete_step(
            ls,
            'But',
            lsp.Position(line=0, character=37),
            'parse date "{{ datetime.now() }}"',
            base_keyword='Then',
        )
        assert len(actual_completed_steps) == 1
        actual_completed_step = actual_completed_steps[0]
        assert actual_completed_step.text_edit is not None
        assert actual_completed_step.text_edit.new_text.endswith(' and save in variable "$1"')


def test_complete_metadata() -> None:
    position = lsp.Position(line=0, character=2)

    # <!-- complete metadata attribute
    actual_items = complete_metadata('#', position)

    assert len(actual_items) == 1
    actual_item = actual_items[0]
    assert actual_item.label == MARKER_LANGUAGE
    assert actual_item.kind == lsp.CompletionItemKind.Property
    assert isinstance(actual_item.text_edit, lsp.TextEdit)
    assert actual_item.text_edit.new_text == f'{MARKER_LANGUAGE} '
    assert actual_item.text_edit.range == lsp.Range(start=lsp.Position(line=position.line, character=0), end=position)
    # // -->

    # <!-- complete all the languages
    position = lsp.Position(line=0, character=12)
    actual_items = complete_metadata('# language: ', position)

    # Asturian is the 6th language in behave.i18n.languages
    assert len(actual_items) > 10
    actual_item = actual_items[5]
    assert actual_item.label == 'ast'
    assert actual_item.kind == lsp.CompletionItemKind.Property
    assert isinstance(actual_item.text_edit, lsp.TextEdit)
    assert actual_item.text_edit.new_text == 'ast'
    assert actual_item.text_edit.range == lsp.Range(
        start=position,
        end=position,
    )
    # // -->


def test_complete_variable_name(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT

    text_document = TextDocument(
        uri='file:///test.feature',
        source="""Feature:
    Scenario:

        Given value for variable "foo" is "bar"
        And what about it?
        And value for variable "bar" is "foo"
        And value for variable "foobar" is "barfoo"
""",
    )

    position = lsp.Position(line=7, character=19)

    actual_items = complete_variable_name(ls, 'Then log message "{{', text_document, position)
    assert len(actual_items) == 3
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted([' foo }}"', ' bar }}"', ' foobar }}"'])

    actual_items = complete_variable_name(ls, 'Then log message "{{"', text_document, position)

    assert len(actual_items) == 3
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted([' foo }}', ' bar }}', ' foobar }}'])

    position = lsp.Position(line=7, character=21)
    actual_items = complete_variable_name(ls, 'Then log message "{{ }}"', text_document, position)

    assert len(actual_items) == 3
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted(['foo ', 'bar ', 'foobar '])

    position = lsp.Position(line=7, character=21)
    actual_items = complete_variable_name(ls, 'Then log message "{{ f', text_document, position, partial='f')

    assert len(actual_items) == 2
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted(['foo }}"', 'foobar }}"'])


def test_complete_expression(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server
    ls.root_path = GRIZZLY_PROJECT

    text_document = TextDocument(
        uri='file:///test.feature',
        source="""Feature:
    Scenario:
        {%
""",
    )

    position = lsp.Position(line=2, character=10)

    actual_items = complete_expression(ls, '{%', text_document, position, partial=None)
    assert len(actual_items) == 1
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted([' scenario "$1", feature="$2" %}'])

    actual_items = complete_expression(ls, '{% ', text_document, position, partial=None)
    assert len(actual_items) == 1
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted(['scenario "$1", feature="$2" %}'])

    actual_items = complete_expression(ls, '{% sce', text_document, position, partial='sce')
    assert len(actual_items) == 1
    actual_text_edits = sorted([actual_item.text_edit.new_text for actual_item in actual_items if actual_item.text_edit is not None])
    assert actual_text_edits == sorted(['scenario "$1", feature="$2" %}'])

    text_document = TextDocument(
        uri='file:///test.feature',
        source="""Feature:
    Scenario:
        {% scenario
""",
    )

    actual_items = complete_expression(ls, '{% scenario', text_document, position, partial='scenario')
    assert len(actual_items) == 0

    text_document = TextDocument(
        uri='file:///test.feature',
        source="""Feature:
    Scenario:
        {% %}
""",
    )

    actual_items = complete_expression(ls, '{% %}', text_document, position, partial=None)
    assert len(actual_items) == 0
