import pytest

from grizzly.types import RequestType, RequestDirection, RequestMethod, bool_typed, int_rounded_float_typed


class TestRequestDirection:
    def test_from_string(self) -> None:
        for direction in RequestDirection:
            assert RequestDirection.from_string(direction.name.lower()) == direction

        with pytest.raises(ValueError):
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
            assert str(custom_type) == custom_type.value

    @pytest.mark.parametrize('input,expected', [
        (RequestMethod.GET, 'GET',),
        (RequestMethod.RECEIVE, 'RECV',),
        (RequestMethod.PUT, 'PUT',),
        (RequestMethod.SEND, 'SEND',),
    ])
    def test_from_method(self, input: RequestMethod, expected: str) -> None:
        assert RequestType.from_method(input) == expected

    @pytest.mark.parametrize('input,expected', [
        (e.name, e.value,) for e in RequestType
    ] + [
        (e.name, e.name,) for e in RequestMethod if getattr(RequestType, e.name, None) is None
    ] + [
        (e.value, e.value,) for e in RequestType
    ])
    def test_from_string(self, input: str, expected: str) -> None:
        assert RequestType.from_string(input) == expected

        with pytest.raises(AttributeError) as ae:
            RequestType.from_string('foobar')
        assert str(ae.value) == 'foobar does not exist'

    @pytest.mark.parametrize('input', [e for e in RequestType])
    def test___call___and___str__(self, input: RequestType) -> None:
        assert input() == input.value == str(input)


class TestRequestMethod:
    def test_from_string(self) -> None:
        for method in RequestMethod:
            assert RequestMethod.from_string(method.name.lower()) == method

        with pytest.raises(ValueError):
            RequestMethod.from_string('asdf')

    def test_direction(self) -> None:
        for method in RequestMethod:
            assert method.value == method.direction
            assert method in method.direction.methods


def test_bool_typed() -> None:
    assert bool_typed('True')
    assert not bool_typed('False')

    with pytest.raises(ValueError):
        bool_typed('asdf')


def test_int_rounded_float_typed() -> None:
    assert int_rounded_float_typed('1') == 1
    assert int_rounded_float_typed('1.337') == 1
    assert int_rounded_float_typed('1.51') == 2

    with pytest.raises(ValueError):
        int_rounded_float_typed('asdf')

    with pytest.raises(ValueError):
        int_rounded_float_typed('0xbeef')

    with pytest.raises(ValueError):
        int_rounded_float_typed('1,5')
