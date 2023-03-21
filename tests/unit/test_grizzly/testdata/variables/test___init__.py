import pytest

from grizzly.testdata.variables import AtomicVariable, AtomicIntegerIncrementer, AtomicRandomString, destroy_variables

from tests.fixtures import AtomicVariableCleanupFixture
from tests.helpers import AtomicCustomVariable


def test_destroy_variables(cleanup: AtomicVariableCleanupFixture) -> None:
    try:
        t1 = AtomicRandomString('test1', '%s%s%d%d')
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test1'] is not None
        assert t2['test2'] == 1337

        with pytest.raises(AttributeError):
            AtomicRandomString('test1', '%s%d')

        with pytest.raises(AttributeError):
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
            with pytest.raises(TypeError):
                AtomicVariable('dummy')

            with pytest.raises(TypeError):
                AtomicVariable[int]('dummy')
        finally:
            cleanup()

    def test_get(self) -> None:
        with pytest.raises(ValueError) as ve:
            AtomicVariable.get()
        assert 'is not instantiated' in str(ve)

    def test_destroy(self) -> None:
        with pytest.raises(ValueError) as ve:
            AtomicVariable.destroy()
        assert 'is not instantiated' in str(ve)

    def test_clear(self) -> None:
        with pytest.raises(ValueError) as ve:
            AtomicVariable.clear()
        assert 'is not instantiated' in str(ve)

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

            with pytest.raises(AttributeError) as ae:
                t['foo']
            assert 'AtomicCustomVariable object has no attribute "foo"' == str(ae.value)

            assert t['hello'] == 'value'

            with pytest.raises(NotImplementedError) as nie:
                t['foo'] = 'bar'
            assert str(nie.value) == 'AtomicCustomVariable has not implemented "__setitem__"'

            with pytest.raises(NotImplementedError) as nie:
                t['hello'] = 'bar'
            assert str(nie.value) == 'AtomicCustomVariable has not implemented "__setitem__"'
        finally:
            cleanup()
