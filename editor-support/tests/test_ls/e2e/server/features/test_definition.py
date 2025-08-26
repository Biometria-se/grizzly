from __future__ import annotations

import inspect
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, cast

from lsprotocol import types as lsp

from test_ls.e2e.server.features import initialize, open_text_document

if TYPE_CHECKING:
    from pygls.server import LanguageServer

    from test_ls.fixtures import LspFixture


def definition(
    client: LanguageServer,
    path: Path,
    position: lsp.Position,
    content: str | None = None,
) -> list[lsp.LocationLink] | None:
    path = path / 'features' / 'project.feature'

    initialize(client, path, options=None)
    open_text_document(client, path, content)

    params = lsp.DefinitionParams(
        text_document=lsp.TextDocumentIdentifier(
            uri=path.as_uri(),
        ),
        position=position,
    )

    response = client.lsp.send_request(lsp.TEXT_DOCUMENT_DEFINITION, params).result(timeout=3)

    assert response is None or isinstance(response, list)

    return cast('list[lsp.LocationLink] | None', response)


def test_definition(lsp_fixture: LspFixture) -> None:
    client = lsp_fixture.client

    content = """Feature:
    Scenario: test
        Given a user of type "RestApi" load testing "http://localhost"
        Then post request "test/test.txt" with name "test request" to endpoint "/api/test"
"""
    # <!-- hover "Scenario", no definition
    response = definition(
        client,
        lsp_fixture.datadir,
        lsp.Position(line=1, character=9),
        content,
    )

    assert response is None
    # // -->

    # <!-- hover the first variable in "Given a user of type..."
    response = definition(
        client,
        lsp_fixture.datadir,
        lsp.Position(line=2, character=30),
        content,
    )

    assert response is not None
    assert len(response) == 1
    actual_definition = response[0]

    from grizzly.steps.scenario.user import step_user_type

    file_location = Path(inspect.getfile(step_user_type.__wrapped__))
    _, lineno = inspect.getsourcelines(step_user_type)

    assert actual_definition.target_uri == file_location.as_uri()
    assert actual_definition.target_range == lsp.Range(
        start=lsp.Position(line=lineno, character=0),
        end=lsp.Position(line=lineno, character=0),
    )
    assert actual_definition.target_range == actual_definition.target_selection_range
    assert actual_definition.origin_selection_range == lsp.Range(
        start=lsp.Position(line=2, character=8),
        end=lsp.Position(line=2, character=70),
    )
    # // -->

    # <!-- hover "test/test.txt" in "Then post a request..."
    request_payload_dir = lsp_fixture.datadir / 'features' / 'requests' / 'test'
    request_payload_dir.mkdir(exist_ok=True, parents=True)
    try:
        test_txt_file = request_payload_dir / 'test.txt'
        test_txt_file.write_text('hello world!')
        response = definition(
            client,
            lsp_fixture.datadir,
            lsp.Position(line=3, character=27),
            content,
        )
        assert response is not None
        assert len(response) == 1
        actual_definition = response[0]
        assert actual_definition.target_uri == test_txt_file.as_uri()
        assert actual_definition.target_range == lsp.Range(
            start=lsp.Position(line=0, character=0),
            end=lsp.Position(line=0, character=0),
        )
        assert actual_definition.target_selection_range == actual_definition.target_range
        assert actual_definition.origin_selection_range is not None
        assert actual_definition.origin_selection_range == lsp.Range(
            start=lsp.Position(line=3, character=27),
            end=lsp.Position(line=3, character=40),
        )
    # // -->
    finally:
        rmtree(request_payload_dir)
