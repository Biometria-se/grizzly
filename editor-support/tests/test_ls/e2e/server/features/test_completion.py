from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lsprotocol import types as lsp

from test_ls.e2e.server.features import initialize, open_text_document

if TYPE_CHECKING:
    from pathlib import Path

    from pygls.server import LanguageServer

    from test_ls.fixtures import LspFixture


def completion(
    client: LanguageServer,
    path: Path,
    content: str,
    options: dict[str, str] | None = None,
    context: lsp.CompletionContext | None = None,
    position: lsp.Position | None = None,
) -> lsp.CompletionList | None:
    path = path / 'features' / 'project.feature'

    initialize(client, path, options)
    open_text_document(client, path, content)

    lines = content.split('\n')
    line = len(lines) - 1
    character = len(lines[-1])

    character = max(character, 0)

    if position is None:
        position = lsp.Position(line=line, character=character)

    params = lsp.CompletionParams(
        text_document=lsp.TextDocumentIdentifier(
            uri=path.as_uri(),
        ),
        position=position,
        context=context,
        partial_result_token=None,
        work_done_token=None,
    )

    response = client.lsp.send_request(lsp.TEXT_DOCUMENT_COMPLETION, params).result(timeout=3)

    assert response is None or isinstance(response, lsp.CompletionList)

    return response


def test_completion_keywords(lsp_fixture: LspFixture) -> None:
    client = lsp_fixture.client

    def filter_keyword_properties(
        items: list[lsp.CompletionItem],
    ) -> list[dict[str, Any]]:
        return [
            {
                'label': item.label,
                'kind': item.kind,
                'text_edit': item.text_edit.new_text if item.text_edit is not None else None,
            }
            for item in items
        ]

    # partial match, keyword containing 'B'
    response = completion(
        client,
        lsp_fixture.datadir,
        """Feature:
    Scenario:
        B""",
        options=None,
    )

    assert response is not None
    assert not response.is_incomplete
    assert filter_keyword_properties(response.items) == [
        {'label': 'Background', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Background: '},
        {'label': 'But', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'But '},
    ]

    # partial match, keyword containing 'en'
    response = completion(
        client,
        lsp_fixture.datadir,
        """Feature:
    Scenario:
        en""",
        options=None,
    )
    assert response is not None
    assert not response.is_incomplete
    assert filter_keyword_properties(response.items) == [
        {'label': 'Given', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Given '},
        {'label': 'Scenario', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Scenario: '},
        {'label': 'Scenario Outline', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Scenario Outline: '},
        {'label': 'Scenario Template', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Scenario Template: '},
        {'label': 'Scenarios', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Scenarios: '},
        {'label': 'Then', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Then '},
        {'label': 'When', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'When '},
    ]

    # all keywords
    response = completion(client, lsp_fixture.datadir, '', options=None)
    assert response is not None
    assert not response.is_incomplete
    assert filter_keyword_properties(response.items) == [
        {'label': 'Ability', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Ability: '},
        {'label': 'Business Need', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Business Need: '},
        {'label': 'Feature', 'kind': lsp.CompletionItemKind.Keyword, 'text_edit': 'Feature: '},
    ]


def test_completion_steps(lsp_fixture: LspFixture) -> None:
    client = lsp_fixture.client

    # all Given/And steps
    for keyword in ['Given', 'And']:
        response = completion(
            client,
            lsp_fixture.datadir,
            f"""Feature:
    Scenario:
        Given a user of type "RestApiUser" load testing "dummy://test"
        {keyword}""",
            options=None,
            position=lsp.Position(line=3, character=8 + len(keyword)),
        )
        assert response is not None
        assert not response.is_incomplete
        unexpected_kinds = list(
            filter(
                lambda s: s != 3,
                (s.kind for s in response.items),
            )
        )
        assert len(unexpected_kinds) == 0

        labels = [s.text_edit.new_text for s in response.items if s.text_edit is not None]
        assert all(label is not None for label in labels)

        assert ' ask for value of variable "$1"' in labels
        assert ' spawn rate is "$1" user per second' in labels
        assert ' spawn rate is "$1" users per second' in labels
        assert ' a user of type "$1" with weight "$2" load testing "$3"' in labels

        response = completion(
            client,
            lsp_fixture.datadir,
            f"""Feature:
    Scenario:
        Given a user of type "RestApiUser" load testing "dummy://test"
        {keyword} """,
            options=None,
            position=lsp.Position(line=3, character=8 + len(keyword) + 1),
        )
        assert response is not None
        assert not response.is_incomplete
        unexpected_kinds = list(
            filter(
                lambda s: s != 3,
                (s.kind for s in response.items),
            )
        )
        assert len(unexpected_kinds) == 0

        labels = [s.text_edit.new_text for s in response.items if s.text_edit is not None]
        assert all(label is not None for label in labels)

        assert 'ask for value of variable "$1"' in labels
        assert 'spawn rate is "$1" user per second' in labels
        assert 'spawn rate is "$1" users per second' in labels
        assert 'a user of type "$1" with weight "$2" load testing "$3"' in labels

    response = completion(client, lsp_fixture.datadir, 'Given value', options=None)
    assert response is not None
    assert not response.is_incomplete
    unexpected_kinds = list(
        filter(
            lambda s: s != 3,
            (s.kind for s in response.items),
        )
    )
    assert len(unexpected_kinds) == 0

    labels = [s.label for s in response.items]
    assert all(label is not None for label in labels)

    assert 'ask for value of variable ""' in labels
    assert 'value for variable "" is ""' in labels

    response = completion(client, lsp_fixture.datadir, 'Given a user of')
    assert response is not None
    assert not response.is_incomplete
    unexpected_kinds = list(
        filter(
            lambda s: s != 3,
            (s.kind for s in response.items),
        )
    )
    assert len(unexpected_kinds) == 0

    labels = [s.label for s in response.items]
    assert all(label is not None for label in labels)

    assert sorted(labels) == sorted(
        [
            'a user of type "" with weight "" load testing ""',
            'a user of type "" load testing ""',
        ]
    )

    response = completion(client, lsp_fixture.datadir, 'Then parse date "{{ datetime.now() }}"')
    assert response is not None
    assert not response.is_incomplete

    labels = [s.label for s in response.items]
    new_texts = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert labels == ['parse date "{{ datetime.now() }}" and save in variable ""']
    assert new_texts == ['parse date "{{ datetime.now() }}" and save in variable "$1"']


def test_completion_variable_names(lsp_fixture: LspFixture) -> None:  # noqa: PLR0915
    client = lsp_fixture.client

    content = """Feature: test
    Scenario: test
        Given a user of type "Dummy" load testing "dummy://test"
        And value for variable "price" is "200"
        And value for variable "foo" is "bar"
        And value for variable "test" is "False"
        And ask for value of variable "bar"

        Then parse date "{{"""
    response = completion(client, lsp_fixture.datadir, content)

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted([' price }}"', ' foo }}"', ' test }}"', ' bar }}"'])
    assert sorted(labels) == sorted(['price', 'foo', 'test', 'bar'])

    content = '''Feature: test
    Scenario: test1
        Given a user of type "Dummy" load testing "dummy://test"
        And value for variable "price" is "200"
        And value for variable "foo" is "bar"
        And value for variable "test" is "False"
        And ask for value of variable "bar"

        Then log message "{{

    Scenario: test2
        Given a user of type "Dummy" load testing "dummy://test"
        And value for variable "weight1" is "200"
        And value for variable "hello1" is "bar"
        And value for variable "test1" is "False"
        And ask for value of variable "world1"

        Then log message "{{ "
        Then log message "{{ w"

    Scenario: test3
        Given a user of type "Dummy" load testing "dummy://test"
        And value for variable "weight2" is "200"
        And value for variable "hello2" is "bar"
        And value for variable "test2" is "False"
        And ask for value of variable "world2"

        Then log message "{{ }}"
        Then log message "{{ w}}"'''

    # <!-- Scenario: test1
    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=8, character=28),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted([' price }}"', ' foo }}"', ' test }}"', ' bar }}"'])
    assert sorted(labels) == sorted(['price', 'foo', 'test', 'bar'])
    # // -->

    # <!-- Scenario: test2
    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=17, character=28),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted([' weight1 }}', ' hello1 }}', ' test1 }}', ' world1 }}'])
    assert sorted(labels) == sorted(['weight1', 'hello1', 'test1', 'world1'])

    # partial variable name
    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=18, character=30),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted(['weight1 }}', 'world1 }}'])
    assert sorted(labels) == sorted(['weight1', 'world1'])
    # // -->

    # <!-- Scenario: test3
    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=27, character=28),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted([' weight2', ' hello2', ' test2', ' world2'])
    assert sorted(labels) == sorted(['weight2', 'hello2', 'test2', 'world2'])

    # partial variable name
    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=28, character=30),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted(['weight2 ', 'world2 '])
    assert sorted(labels) == sorted(['weight2', 'world2'])
    # // -->

    content = '''Feature: test
    Scenario: test
        Given a user of type "Dummy" load testing "dummy://test"
        And value for variable "price" is "200"
        And value for variable "foo" is "bar"
        And value for variable "test" is "False"
        And ask for value of variable "bar"

        Then parse date "{{" and save in variable ""'''

    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=8, character=27),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted([' price }}', ' foo }}', ' test }}', ' bar }}'])
    assert sorted(labels) == sorted(['price', 'foo', 'test', 'bar'])

    content = '''Feature: test
    Scenario: test
        Given a user of type "Dummy" load testing "dummy://test"
        And value for variable "price" is "200"
        And value for variable "foo" is "bar"
        And value for variable "test" is "False"
        And ask for value of variable "bar"

        Then send request "test/request.j2.json" with name "{{" to endpoint ""
        Then send request "{{}}" with name "" to endpoint ""'''

    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=8, character=62),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted([' price }}', ' foo }}', ' test }}', ' bar }}'])
    assert sorted(labels) == sorted(['price', 'foo', 'test', 'bar'])

    response = completion(
        client,
        lsp_fixture.datadir,
        content,
        position=lsp.Position(line=9, character=29),
    )

    assert response is not None

    labels = [s.label for s in response.items]
    text_edits = [s.text_edit.new_text for s in response.items if s.text_edit is not None]

    assert sorted(text_edits) == sorted(
        [
            ' price ',
            ' foo ',
            ' test ',
            ' bar ',
        ]
    )
    assert sorted(labels) == sorted(['price', 'foo', 'test', 'bar'])
