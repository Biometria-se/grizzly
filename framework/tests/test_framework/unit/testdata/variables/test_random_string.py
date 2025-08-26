"""Unit tests of grizzly.testdata.variables.random_string."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import gevent
import pytest
from grizzly.testdata.variables import AtomicRandomString

if TYPE_CHECKING:  # pragma: no cover
    from gevent.greenlet import Greenlet

    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


class TestAtomicRandomString:
    def test_variable(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:  # noqa: PLR0915
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            with pytest.raises(ValueError, match='no string pattern specified'):
                t = AtomicRandomString(scenario=scenario1, variable='test1', value='')

            with pytest.raises(ValueError, match='specified string pattern does not contain any generators'):
                t = AtomicRandomString(scenario=scenario1, variable='test1', value='AAABBB')

            t = AtomicRandomString(scenario=scenario1, variable='test1', value='%s')

            # count will be 1
            t = AtomicRandomString(scenario=scenario1, variable='testXX', value='%s | count=-1')
            assert t['testXX'] is not None
            assert t.__getitem__('testXX') is None
            del t['testXX']

            with pytest.raises(NotImplementedError, match='format "f" is not implemented'):
                AtomicRandomString(scenario=scenario2, variable='testformat', value='%s%d%fa | count=1')

            with pytest.raises(ValueError, match='argument length is not allowed'):
                AtomicRandomString(scenario=scenario1, variable='testformat', value='%s%da | length=1')

            v = t['test1']
            assert v is not None
            assert len(v) == 1
            assert t.__getitem__('test1') is None

            t = AtomicRandomString(scenario=scenario1, variable='test2', value='a%s%d | count=5')

            for _ in range(5):
                v = t['test2']
                assert v is not None, f'iteration {_}'
                assert len(v) == 3
                assert v[0] == 'a'
                try:
                    int(v[-1])
                except:
                    pytest.fail(f'{v[-1]} is not an integer')

            assert t.__getitem__('test2') is None

            t = AtomicRandomString(scenario=scenario2, variable='test3', value='a%s%d | count=8, upper=True')

            for _ in range(8):
                v = t['test3']
                assert v is not None, f'iteration {_}'
                assert len(v) == 3
                assert v[0] == 'A'
                try:
                    int(v[-1])
                except:
                    pytest.fail(f'{v[-1]} is not an integer')

            assert t.__getitem__('test3') is None

            t = AtomicRandomString(scenario=scenario1, variable='regnr', value='%sA%s1%d%d | count=10, upper=True')

            for _ in range(10):
                rn = t['regnr']
                assert rn is not None, f'iteration {_}'
                assert len(rn) == 6
                assert rn[1] == 'A'
                assert rn[3] == '1'
                try:
                    int(rn[4:])
                except:
                    pytest.fail(f'"{rn[4:]}" is not an integer')

            assert t.__getitem__('regnr') is None

            with pytest.raises(NotImplementedError, match='AtomicRandomString has not implemented "__setitem__"'):
                t['regnr'] = 'ABC123'
            assert t['regnr'] is None

            assert len(t._strings) == 3
            assert 'regnr' in t._strings
            del t['regnr']
            del t['regnr']
            assert len(t._strings) == 2
            assert 'regnr' not in t._strings

            AtomicRandomString.clear()
            assert len(t._strings) == 0

            t = AtomicRandomString(scenario=scenario1, variable='regnr', value='%sA%s1%d%d | count=10000, upper=True')

            assert len(t._strings['regnr']) == 10000
            # check that all are unique
            assert sorted(t._strings['regnr']) == sorted(set(t._strings['regnr']))

            t = AtomicRandomString(scenario=scenario2, variable='uuid', value='%g | count=3')

            assert len(t._strings['uuid']) == 3
            regex = re.compile(r'^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.IGNORECASE)
            for _ in range(3):
                uuid = t['uuid']
                assert uuid is not None
                assert regex.match(uuid)

            assert t['uuid'] is None

            with pytest.raises(ValueError, match='AtomicRandomString: %g cannot be combined with other formats'):
                AtomicRandomString(scenario=scenario2, variable='uuid4', value='%s%g')
        finally:
            cleanup()

    def test_multi_greenlet(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly

            num_greenlets: int = 20
            num_iterations: int = 100
            count = num_greenlets * num_iterations

            t = AtomicRandomString(scenario=grizzly.scenario, variable='greenlet_var', value=f'%s%s%s%d%d%d | count={count}, upper=True')

            def exception_handler(greenlet: gevent.Greenlet) -> None:
                message = f'func1 did not validate for {greenlet}'
                raise RuntimeError(message)

            values: set[str] = set()

            def func1() -> None:
                for _ in range(num_iterations):
                    value = t['greenlet_var']
                    assert value is not None, f'iteration {_}'
                    assert value not in values
                    values.add(value)

            greenlets: list[Greenlet] = []
            for _ in range(num_greenlets):
                greenlet = gevent.spawn(func1)
                greenlet.link_exception(exception_handler)
                greenlets.append(greenlet)

            try:
                gevent.joinall(greenlets)

                for greenlet in greenlets:
                    greenlet.get()
            except RuntimeError as e:
                pytest.fail(str(e))

            assert t['greenlet_var'] is None
        finally:
            cleanup()
