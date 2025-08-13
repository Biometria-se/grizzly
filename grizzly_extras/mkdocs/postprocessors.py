"""grizzly-mkdocs plugin postprocessor implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol, cast

import lxml.html
from lxml.builder import E

from grizzly_extras.mkdocs import transform_step_header

if TYPE_CHECKING:
    from mkdocs.structure.pages import Page

    from grizzly_extras.mkdocs.log import MkdocsPluginLogger


TextFormat = Literal['html', 'markdown']


class PostProcessor(Protocol):
    text_format: TextFormat

    def __init__(self, logger: MkdocsPluginLogger) -> None: ...

    def __call__(self, page: Page, text: str | None) -> str | None: ...


class StepHeaders(PostProcessor):
    text_format = 'html'

    def __init__(self, logger: MkdocsPluginLogger) -> None:
        self.logger = logger

    def __call__(self, page: Page, html: str | None) -> str | None:
        if html is None:
            return html

        tree = lxml.html.fromstring(html)

        module = page.meta.get('module', None)

        headers = tree.cssselect('h2.doc.doc-heading')

        for header in headers:
            header_text = transform_step_header(header.text_content().strip(), module)

            # remove children
            for child in header:
                child.getparent().remove(child)

            header.text = header_text

        return cast('str | None', lxml.html.tostring(tree).decode())


class SubHeaders(PostProcessor):
    text_format = 'html'

    def __init__(self, logger: MkdocsPluginLogger) -> None:
        self.logger = logger

    def __call__(self, page: Page, html: str | None) -> str | None:  # noqa: ARG002
        if html is None:
            return html

        tree = lxml.html.fromstring(html)

        if 'Parameters:' in html:
            """
            <p><span class="doc-section-title">Parameters:</span></p> ->
            <h3 id="<namespace>--arguments">Arguments</h3>
            """

            headers = tree.cssselect('p > span.doc-section-title:contains("Parameters:")')

            for span in headers:
                p = span.getparent()
                parent = p.getparent()
                header_text = span.text_content().strip()

                if header_text != 'Parameters:':
                    continue

                grandparent = parent.getparent()

                main_header = next(iter(grandparent.cssselect('h2.doc-heading')))
                main_id = main_header.get('id')

                new_header = E.h3('Arguments', {'id': f'{main_id}--arguments'})
                parent.replace(p, new_header)

        if 'Example:' in html:
            """
            <p>Example: </p> ->
            <h3 id="<namespace>--example">Example</h3>
            """
            headers = tree.cssselect('p:contains("Example:")')

            for p in headers:
                parent = p.getparent()
                header_text = p.text_content().strip()

                if header_text != 'Example:':
                    continue

                grandparent = parent.getparent()

                main_header = next(iter(grandparent.cssselect('h2.doc-heading')))
                main_id = main_header.get('id')

                new_header = E.h3('Example', {'id': f'{main_id}--example'})
                parent.replace(p, new_header)

        return cast('str | None', lxml.html.tostring(tree).decode())
