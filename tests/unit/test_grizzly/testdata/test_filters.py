"""Unit tests for grizzly.testdata.filters."""
from __future__ import annotations

from base64 import b64encode as base64_b64encode
from collections import namedtuple
from typing import TYPE_CHECKING

import pytest
from jinja2.filters import FILTERS

from grizzly.testdata.filters import b64decode, b64encode, fromtestdata, stringify, templatingfilter

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

    del FILTERS['testuppercase']


def test_fromtestdata() -> None:
    sub_value = namedtuple('Testdata', ['foz', 'baz'])(**{'foz': 'foo', 'baz': 'bar'})  # noqa: PYI024, PIE804

    value = namedtuple('Testdata', ['foo', 'bar'])(**{'foo': sub_value, 'bar': 'bar'})  # noqa: PYI024, PIE804

    assert fromtestdata(value) == {
        'foo': {
            'foz': 'foo',
            'baz': 'bar',
        },
        'bar': 'bar',
    }


def test_stringify() -> None:
    assert stringify('foobar') == '"foobar"'
    assert stringify(1337) == '1337'
    assert stringify(0.1337) == '0.1337'
    assert stringify(['foo', 'bar']) == '["foo", "bar"]'
    assert stringify({'foo': 'bar'}) == '{"foo": "bar"}'
    assert stringify(None) == 'null'


def test_b64encode() -> None:
    assert b64encode('foobar') == base64_b64encode(b'foobar').decode()


def test_b64decode() -> None:
    assert b64decode(base64_b64encode(b'foobar').decode()) == 'foobar'
