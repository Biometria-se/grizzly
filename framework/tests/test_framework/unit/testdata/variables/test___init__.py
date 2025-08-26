"""Unit tests of grizzly.testdata.variables."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from grizzly.testdata.variables import AtomicIntegerIncrementer, AtomicRandomString, AtomicVariable, destroy_variables

from test_framework.helpers import AtomicCustomVariable

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


def test_destroy_variables(grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
    try:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        t1 = AtomicRandomString(scenario=scenario1, variable='test1', value='A%s%s%d%dA')
        t2 = AtomicIntegerIncrementer(scenario=scenario2, variable='test2', value=1337)
        test1 = t1['test1']
        assert test1 is not None
        assert test1[:: len(test1) - 1] == 'AA'
        assert t2['test2'] == 1337

        with pytest.raises(AttributeError, match='object already has attribute'):
            AtomicRandomString(scenario=scenario1, variable='test1', value='%s%d')

        with pytest.raises(AttributeError, match='object already has attribute'):
            AtomicIntegerIncrementer(scenario=scenario2, variable='test2', value=1338)

        destroy_variables()

        t1 = AtomicRandomString(scenario=scenario1, variable='test1', value='A%s%s%d%dA')
        t2 = AtomicIntegerIncrementer(scenario=scenario2, variable='test2', value=1337)
        test1 = t1['test1']
        assert test1 is not None
        assert test1[:: len(test1) - 1] == 'AA'
        assert t2['test2'] == 1337
    finally:
        cleanup()


class TestAtomicVariable:
    def test_dont_instantiate(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly

            with pytest.raises(TypeError, match="Can't instantiate abstract class AtomicVariable"):
                AtomicVariable(scenario=grizzly.scenario, variable='dummy')

            with pytest.raises(TypeError, match="Can't instantiate abstract class AtomicVariable"):
                AtomicVariable[int](scenario=grizzly.scenario, variable='dummy')
        finally:
            cleanup()

    def test_get(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        with pytest.raises(ValueError, match=f'is not instantiated for {grizzly.scenario.name}'):
            AtomicVariable.get(grizzly.scenario)

    def test_destroy(self) -> None:
        with pytest.raises(ValueError, match='is not instantiated'):
            AtomicVariable.destroy()

    def test_clear(self) -> None:
        with pytest.raises(ValueError, match='is not instantiated'):
            AtomicVariable.clear()

    def test_obtain(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly

            scenario1 = grizzly.scenario
            scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('scenario2'))

            v = AtomicCustomVariable.obtain(scenario=scenario1, variable='foo', value='hello')
            assert v['foo'] == 'hello'
            v = AtomicCustomVariable.obtain(scenario=scenario1, variable='foo', value='world')
            assert v['foo'] == 'hello'
            v = AtomicCustomVariable.obtain(scenario=scenario1, variable='bar', value='world')
            assert v['foo'] == 'hello'
            assert v['bar'] == 'world'

            v = AtomicCustomVariable.obtain(scenario=scenario2, variable='foo', value='foo')
            assert v['foo'] == 'foo'
            v = AtomicCustomVariable.obtain(scenario=scenario1, variable='foo', value='asdf')
            assert v['foo'] == 'hello'
        finally:
            cleanup()

    def test___getitem___and___setitem__(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly
            t = AtomicCustomVariable(scenario=grizzly.scenario, variable='hello', value='value')

            with pytest.raises(AttributeError, match='AtomicCustomVariable object has no attribute "foo"'):
                t['foo']

            assert t['hello'] == 'value'

            with pytest.raises(NotImplementedError, match='AtomicCustomVariable has not implemented "__setitem__"'):
                t['foo'] = 'bar'

            with pytest.raises(NotImplementedError, match='AtomicCustomVariable has not implemented "__setitem__"'):
                t['hello'] = 'bar'

            grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

            t = AtomicCustomVariable(scenario=grizzly.scenario, variable='hello', value='world')
            assert t['hello'] == 'world'
        finally:
            cleanup()
