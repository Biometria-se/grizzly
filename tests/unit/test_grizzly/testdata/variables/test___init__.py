"""Unit tests of grizzly.testdata.variables."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.testdata.variables import AtomicIntegerIncrementer, AtomicRandomString, AtomicVariable, destroy_variables
from tests.helpers import AtomicCustomVariable

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import AtomicVariableCleanupFixture


def test_destroy_variables(cleanup: AtomicVariableCleanupFixture) -> None:
    try:
        t1 = AtomicRandomString('test1', '%s%s%d%d')
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test1'] is not None
        assert t2['test2'] == 1337

        with pytest.raises(AttributeError, match='object already has attribute'):
            AtomicRandomString('test1', '%s%d')

        with pytest.raises(AttributeError, match='object already has attribute'):
            AtomicIntegerIncrementer('test2', 1338)

        destroy_variables()

        t1 = AtomicRandomString('test1', '%s%s%d%d')
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test1'] is not None
        assert t2['test2'] == 1337
    finally:
        cleanup()


class TestAtomicVariable:
    def test_dont_instantiate(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            with pytest.raises(TypeError, match="Can't instantiate abstract class AtomicVariable"):
                AtomicVariable('dummy')

            with pytest.raises(TypeError, match="Can't instantiate abstract class AtomicVariable"):
                AtomicVariable[int]('dummy')
        finally:
            cleanup()

    def test_get(self) -> None:
        with pytest.raises(ValueError, match='is not instantiated'):
            AtomicVariable.get()

    def test_destroy(self) -> None:
        with pytest.raises(ValueError, match='is not instantiated'):
            AtomicVariable.destroy()

    def test_clear(self) -> None:
        with pytest.raises(ValueError, match='is not instantiated'):
            AtomicVariable.clear()

    def test_obtain(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            v = AtomicCustomVariable.obtain('foo', 'hello')
            assert v['foo'] == 'hello'
            v = AtomicCustomVariable.obtain('foo', 'world')
            assert v['foo'] == 'hello'
            v = AtomicCustomVariable.obtain('bar', 'world')
            assert v['foo'] == 'hello'
            assert v['bar'] == 'world'
        finally:
            cleanup()

    def test___getitem___and___setitem__(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = AtomicCustomVariable('hello', 'value')

            with pytest.raises(AttributeError, match='AtomicCustomVariable object has no attribute "foo"'):
                t['foo']

            assert t['hello'] == 'value'

            with pytest.raises(NotImplementedError, match='AtomicCustomVariable has not implemented "__setitem__"'):
                t['foo'] = 'bar'

            with pytest.raises(NotImplementedError, match='AtomicCustomVariable has not implemented "__setitem__"'):
                t['hello'] = 'bar'
        finally:
            cleanup()
