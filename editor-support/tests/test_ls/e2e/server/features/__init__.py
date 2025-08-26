from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from grizzly_ls.constants import FEATURE_INSTALL
from lsprotocol import types as lsp

if TYPE_CHECKING:
    from pathlib import Path

    from pygls.server import LanguageServer


def initialize(
    client: LanguageServer,
    root: Path,
    options: dict[str, Any] | None = None,
) -> None:
    assert root.is_file()

    root = root.parent.parent
    params = lsp.InitializeParams(
        process_id=1337,
        root_uri=root.as_uri(),
        capabilities=lsp.ClientCapabilities(
            workspace=None,
            text_document=None,
            window=None,
            general=None,
            experimental=None,
        ),
        client_info=None,
        locale=None,
        root_path=root.as_posix(),
        initialization_options=options,
        trace=None,
        workspace_folders=None,
        work_done_token=None,
    )

    for logger_name in ['pygls', 'parse', 'pip']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)

    logger = logging.getLogger()
    level = logger.getEffectiveLevel()
    try:
        logger.setLevel(logging.DEBUG)

        # INITIALIZE takes time...
        client.lsp.send_request(
            lsp.INITIALIZE,
            params,
        ).result(timeout=299)

        client.lsp.send_request(FEATURE_INSTALL).result(timeout=299)
    finally:
        logger.setLevel(level)


def open_text_document(client: LanguageServer, path: Path, text: str | None = None) -> None:
    if text is None:
        text = path.read_text()

    client.lsp.notify(
        lsp.TEXT_DOCUMENT_DID_OPEN,
        lsp.DidOpenTextDocumentParams(
            text_document=lsp.TextDocumentItem(
                uri=path.as_uri(),
                language_id='grizzly-gherkin',
                version=1,
                text=text,
            ),
        ),
    )
