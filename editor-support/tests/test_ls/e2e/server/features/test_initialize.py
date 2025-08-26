from __future__ import annotations

import re
from pathlib import Path
from shutil import rmtree
from tempfile import gettempdir
from typing import TYPE_CHECKING

from test_ls.e2e.server.features import initialize

if TYPE_CHECKING:
    from test_ls.fixtures import LspFixture


def test_initialize(lsp_fixture: LspFixture) -> None:
    client = lsp_fixture.client
    server = lsp_fixture.server

    server.steps.clear()

    assert server.steps == {}

    virtual_environment = Path(gettempdir()) / 'grizzly-ls-project'

    if virtual_environment.exists():
        rmtree(virtual_environment)

    original_settings = server.client_settings.copy()

    try:
        initialize(
            client,
            lsp_fixture.datadir / 'features' / 'project.feature',
            options={
                'variable_pattern': [
                    'hello "([^"]*)"!$',
                    'foo bar is a (nice|bad) word',
                    '.*and they lived (happy|unfortunate) ever after',
                    '^foo(bar)$',
                ]
            },
        )

        assert server.steps != {}

        assert isinstance(server.variable_pattern, re.Pattern)
        assert '^.*hello "([^"]*)"!$' in server.variable_pattern.pattern
        assert '^.*foo bar is a (nice|bad) word$' in server.variable_pattern.pattern
        assert '^.*and they lived (happy|unfortunate) ever after$' in server.variable_pattern.pattern
        assert '^foo(bar)$' in server.variable_pattern.pattern
        assert server.variable_pattern.pattern.count('^') == 4 + 1  # first pattern has ^ in the pattern...
        assert server.variable_pattern.pattern.count('(') == 5
        assert server.variable_pattern.pattern.count(')') == 5

        keywords = list(server.steps.keys())

        for keyword in ['given', 'then', 'when']:
            assert keyword in keywords

        assert 'Feature' not in server.keywords  # already used once in feature file
        assert 'Background' not in server.keywords  # - " -
        assert 'And' in server.keywords  # just an alias for Given, but we need it
        assert 'Scenario' in server.keywords  # can be used multiple times
        assert 'Given' in server.keywords  # - " -
        assert 'When' in server.keywords
    finally:
        server.client_settings = original_settings
        server.variable_pattern = server.__class__.variable_pattern
