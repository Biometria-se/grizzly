"""Unit tests for grizzly.testdata.filters."""

from __future__ import annotations

from base64 import b64encode as base64_b64encode
from collections import namedtuple
from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from grizzly.testdata.filters import templatingfilter
from jinja2.filters import FILTERS

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture


def test_templatingfilter(grizzly_fixture: GrizzlyFixture) -> None:
    parent = grizzly_fixture()

    try:

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
    finally:
        with suppress(Exception):
            del FILTERS['testuppercase']


def test_fromtestdata() -> None:
    func = FILTERS.get('fromtestdata', None)
    assert func is not None

    sub_value = namedtuple('Testdata', ['foz', 'baz'])(**{'foz': 'foo', 'baz': 'bar'})  # noqa: PYI024, PIE804
    value = namedtuple('Testdata', ['foo', 'bar'])(**{'foo': sub_value, 'bar': 'bar'})  # noqa: PYI024, PIE804

    assert func(value) == {
        'foo': {
            'foz': 'foo',
            'baz': 'bar',
        },
        'bar': 'bar',
    }


def test_stringify() -> None:
    func = FILTERS.get('stringify', None)
    assert func is not None

    assert func('foobar') == '"foobar"'
    assert func(1337) == '1337'
    assert func(0.1337) == '0.1337'
    assert func(['foo', 'bar']) == '["foo", "bar"]'
    assert func({'foo': 'bar'}) == '{"foo": "bar"}'
    assert func(None) == 'null'


def test_b64encode() -> None:
    func = FILTERS.get('b64encode', None)
    assert func is not None
    assert func('foobar') == base64_b64encode(b'foobar').decode()


def test_b64decode() -> None:
    func = FILTERS.get('b64decode', None)
    assert func is not None
    assert func(base64_b64encode(b'foobar').decode()) == 'foobar'


def test_literal_eval() -> None:
    func = FILTERS.get('literal_eval', None)
    assert func is not None
    assert func("{'hello': 'world'}") == {'hello': 'world'}
    assert func('True')
    assert func('10.3') == 10.3
