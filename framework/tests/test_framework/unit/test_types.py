"""Unit tests of grizzly.types."""

from __future__ import annotations

import pytest
from grizzly.types import RequestDirection, RequestMethod, RequestType, bool_type, int_rounded_float_type, list_type, optional_str_lower_type


class TestRequestDirection:
    def test_from_string(self) -> None:
        for direction in RequestDirection:
            assert RequestDirection.from_string(direction.name.lower()) == direction

        with pytest.raises(AssertionError, match='"ASDF" is not a valid value of RequestDirection'):
            RequestDirection.from_string('asdf')

    def test_methods(self) -> None:
        for method in RequestMethod:
            if method.direction == RequestDirection.FROM:
                assert method in RequestDirection.FROM.methods
            elif method.direction == RequestDirection.TO:
                assert method in RequestDirection.TO.methods
            else:
                pytest.fail(f'{method.name} does not have a direction registered')


class TestRequestType:
    def test___str__(self) -> None:
        for custom_type in RequestType:
            assert str(custom_type) == custom_type.value[0]
            assert custom_type.weight >= 0

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (RequestMethod.GET, 'GET'),
            (RequestMethod.RECEIVE, 'RECV'),
            (RequestMethod.PUT, 'PUT'),
            (RequestMethod.SEND, 'SEND'),
        ],
    )
    def test_from_method(self, value: RequestMethod, expected: str) -> None:
        assert RequestType.from_method(value) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [(e.name, e.alias) for e in RequestType]
        + [(e.name, e.name) for e in RequestMethod if getattr(RequestType, e.name, None) is None]
        + [(e.alias, e.alias) for e in RequestType],
    )
    def test_from_string(self, value: str, expected: str) -> None:
        assert RequestType.from_string(value) == expected

        with pytest.raises(AssertionError, match='foobar does not exist'):
            RequestType.from_string('foobar')

    @pytest.mark.parametrize('value', list(RequestType))
    def test___call___and___str__(self, value: RequestType) -> None:
        assert value() == value.value[0] == str(value)

    def test_weight(self) -> None:
        assert RequestType.AUTH.weight == 0
        assert RequestType.SCENARIO.weight == 1
        assert RequestType.TESTDATA.weight > RequestType.SCENARIO.weight
        assert RequestType.UNTIL.weight > RequestType.TESTDATA.weight
        assert RequestType.VARIABLE.weight == RequestType.UNTIL.weight
        assert RequestType.ASYNC_GROUP.weight == RequestType.VARIABLE.weight
        assert RequestType.CLIENT_TASK.weight == RequestType.ASYNC_GROUP.weight
        assert RequestType.HELLO.weight == RequestType.CLIENT_TASK.weight
        assert RequestType.RECEIVE.weight == RequestType.HELLO.weight
        assert RequestType.CONNECT.weight == RequestType.RECEIVE.weight

    def test_get_method_weight(self) -> None:
        assert RequestType.get_method_weight('ASDF') == RequestType.get_method_weight('GET')
        assert RequestType.get_method_weight('GET') == RequestType.get_method_weight('POST')
        assert RequestType.get_method_weight('AUTH') == 0
        assert RequestType.get_method_weight('SCEN') == 1
        assert RequestType.get_method_weight('TSTD') == 2
        assert RequestType.get_method_weight('PACE') == 3

        for request_type in RequestType:
            if request_type.weight < 10:
                continue

            assert RequestType.get_method_weight(request_type.alias) == 10

        for request_method in RequestMethod:
            assert RequestType.get_method_weight(request_method.name) == 10

    @pytest.mark.parametrize(('alias', 'request_type'), [(request_type.alias, request_type) for request_type in RequestType])
    def test_from_alias(self, alias: str, request_type: RequestType) -> None:
        assert RequestType.from_alias(alias) == request_type

    def test_from_alias_non_existing(self) -> None:
        with pytest.raises(AssertionError, match='no request type with alias ASDF'):
            RequestType.from_alias('ASDF')


class TestRequestMethod:
    def test_from_string(self) -> None:
        for method in RequestMethod:
            assert RequestMethod.from_string(method.name.lower()) == method

        with pytest.raises(AssertionError, match='"ASDF" is not a valid value of RequestMethod'):
            RequestMethod.from_string('asdf')

    def test_direction(self) -> None:
        for method in RequestMethod:
            assert method.value.wrapped == method.direction
            assert method in method.direction.methods


def test_bool_typed() -> None:
    assert bool_type('True')
    assert not bool_type('False')

    with pytest.raises(ValueError, match='asdf is not a valid boolean'):
        bool_type('asdf')


def test_int_rounded_float_typed() -> None:
    assert int_rounded_float_type('1') == 1
    assert int_rounded_float_type('1.337') == 1
    assert int_rounded_float_type('1.51') == 2

    with pytest.raises(ValueError, match="could not convert string to float: 'asdf'"):
        int_rounded_float_type('asdf')

    with pytest.raises(ValueError, match="could not convert string to float: '0xbeef'"):
        int_rounded_float_type('0xbeef')

    with pytest.raises(ValueError, match="could not convert string to float: '1,5'"):
        int_rounded_float_type('1,5')


def test_optional_str_lower_type() -> None:
    assert optional_str_lower_type(None) is None
    assert optional_str_lower_type('asdf') == 'asdf'
    assert optional_str_lower_type('ASDF') == 'asdf'


def test_list_type() -> None:
    assert list_type('foobar') == ['foobar']
    assert list_type('foo,bar') == ['foo', 'bar']
