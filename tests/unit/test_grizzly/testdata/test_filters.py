"""Unit tests for grizzly.testdata.filters."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from jinja2.filters import FILTERS

from grizzly.testdata.filters import templatingfilter

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import GrizzlyFixture


def test_templatingfilter(grizzly_fixture: GrizzlyFixture) -> None:
    parent = grizzly_fixture()

    def testuppercase(value: str) -> str:
        return value.upper()

    assert FILTERS.get('testuppercase', None) is None

    templatingfilter(testuppercase)

    assert FILTERS.get('testuppercase', None) is testuppercase

    actual = parent.user._scenario.jinja2.from_string('{{ variable | testuppercase }}').render(variable='foobar')

    assert actual == 'FOOBAR'

    def _testuppercase(value: str) -> str:
        return value.upper()

    uc = _testuppercase
    uc.__name__ = 'testuppercase'

    with pytest.raises(AssertionError, match='testuppercase is already registered as a filter'):
        templatingfilter(uc)

    assert FILTERS.get('testuppercase', None) is testuppercase
