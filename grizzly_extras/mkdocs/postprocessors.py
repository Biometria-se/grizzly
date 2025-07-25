"""grizzly-mkdocs plugin postprocessor implementations."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Protocol, cast

import lxml.html

from grizzly_extras.mkdocs import transform_step_header

if TYPE_CHECKING:
    from mkdocs.structure.pages import Page

    from grizzly_extras.mkdocs.log import MkdocsPluginLogger


class PostProcessor(Protocol):
    def __init__(self, logger: MkdocsPluginLogger) -> None: ...

    def __call__(self, page: Page, text: str) -> str | None: ...


class StepHeaders(PostProcessor):
    def __init__(self, logger: MkdocsPluginLogger) -> None:
        self.logger = logger

    def __call__(self, page: Page, html: str) -> str | None:  # noqa: ARG002
        tree = lxml.html.fromstring(html)

        headers = tree.cssselect('h2.doc.doc-heading')

        for header in headers:
            header_text = transform_step_header(header.text_content().strip())

            # remove children
            for child in header:
                child.getparent().remove(child)

            header.text = header_text

        return cast('str | None', lxml.html.tostring(tree).decode())
