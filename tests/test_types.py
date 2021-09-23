import pytest

from grizzly.types import RequestDirection, RequestMethod, bool_typed, int_rounded_float_typed


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
