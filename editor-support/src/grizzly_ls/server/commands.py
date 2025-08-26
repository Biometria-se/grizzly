from __future__ import annotations

from contextlib import suppress
from pathlib import Path

from jinja2 import Environment

from grizzly_ls.text import remove_if_statements
from grizzly_ls.utils import ScenarioTag


def render_gherkin(path: str, content: str, *, raw: bool = False) -> str:
    feature_file = Path(path)
    environment = Environment(autoescape=False, extensions=[ScenarioTag])
    environment.extend(feature_file=feature_file, ignore_errors=True)

    content = remove_if_statements(content)
    template = environment.from_string(content)
    content = template.render()

    if not raw:
        # <!-- sanatize content
        buffer: list[str] = []
        for line in content.splitlines():
            buffer_line = line
            # make any html tag characters in comments are replaced with respective html entity code
            with suppress(Exception):
                if buffer_line.lstrip().startswith('#'):
                    buffer_line = buffer_line.replace('<', '&lt;')
                    buffer_line = buffer_line.replace('>', '&gt;')

            buffer.append(buffer_line)
        # // -->
        return '\n'.join(buffer)

    return content
