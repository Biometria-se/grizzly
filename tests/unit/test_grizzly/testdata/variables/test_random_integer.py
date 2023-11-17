"""Unit tests for grizzly.testdata.variables.random_integer."""
from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest

from grizzly.testdata.variables import AtomicRandomInteger
from grizzly.testdata.variables.random_integer import atomicrandominteger__base_type__

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import AtomicVariableCleanupFixture


def test_atomicrandominteger__base_type__() -> None:
    with pytest.raises(ValueError, match='10 is not a valid value format, must be: "a..b"'):
        atomicrandominteger__base_type__('10')

    with pytest.raises(ValueError, match='a is not a valid integer'):
        atomicrandominteger__base_type__('a..b')

    with pytest.raises(ValueError, match='first value needs to be less than second value'):
        atomicrandominteger__base_type__('10..1')

    atomicrandominteger__base_type__('1..10')


class TestAtomicRandomInteger:
    def test_generate_random(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t1 = AtomicRandomInteger('random', '1..10')
            v = t1['random']
            assert v >= 1
            assert v <= 10
            v = t1['random']
            assert v >= 1
            assert v <= 10
            t2 = AtomicRandomInteger('test', '100..200')
            assert t2 is t1
            v = t2['test']
            assert v >= 100
            assert v <= 200
            v = t2['test']
            assert v >= 100
            assert v <= 200

            assert len(t2._max.keys()) == 2
            assert 'test' in t2._max
            assert 'random' in t2._max
        finally:
            cleanup()

    def test_clear_and_destroy(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            with suppress(Exception):
                AtomicRandomInteger.destroy()

            with pytest.raises(ValueError, match='AtomicRandomInteger is not instantiated'):
                AtomicRandomInteger.destroy()

            with pytest.raises(ValueError, match='AtomicRandomInteger is not instantiated'):
                AtomicRandomInteger.clear()

            instance = AtomicRandomInteger('dummy', '25..50')

            assert len(instance._values.keys()) == 1
            assert len(instance._max.keys()) == 1

            AtomicRandomInteger.clear()

            assert len(instance._values.keys()) == 0
            assert len(instance._max.keys()) == 0

            AtomicRandomInteger.destroy()

            with pytest.raises(ValueError, match='AtomicRandomInteger is not instantiated'):
                AtomicRandomInteger.destroy()
        finally:
            cleanup()

    def test_set_and_del(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            instance = AtomicRandomInteger('random', '1337..31337')
            v = instance['random']
            assert v >= 1337
            assert v <= 31337
            assert len(instance._max) == 1

            with pytest.raises(NotImplementedError, match='has not implemented "__setitem__"'):
                instance['value'] = 20
            assert len(instance._max) == 1

            v = instance['random']
            assert v >= 1337
            assert v <= 31337

            del instance['random']
            assert len(instance._max) == 0
            del instance['random']

        finally:
            cleanup()
