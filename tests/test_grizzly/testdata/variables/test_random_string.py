from typing import Callable, List, Set

import pytest
import gevent

from gevent.greenlet import Greenlet

from grizzly.testdata.variables import AtomicRandomString

from ..fixtures import cleanup  # pylint: disable=unused-import


class TestAtomicRandomString:
    @pytest.mark.usefixtures('cleanup')
    def test(self, cleanup: Callable) -> None:
        try:
            with pytest.raises(ValueError) as ve:
                t = AtomicRandomString('test1', '')
            assert 'no string pattern specified' in str(ve)

            with pytest.raises(ValueError) as ve:
                t = AtomicRandomString('test1', 'AAABBB')
            assert 'specified string pattern does not contain any generators' in str(ve)

            t = AtomicRandomString('test1', '%s')

            t = AtomicRandomString('testXX', '%s | count=-1')
            assert t['testXX'] is not None
            assert t['testXX'] == None
            del t['testXX']

            with pytest.raises(NotImplementedError) as nie:
                AtomicRandomString('testformat', '%s%d%fa | count=1')
            assert 'format "f" is not implemented' in str(nie)

            with pytest.raises(ValueError) as ve:
                AtomicRandomString('testformat', '%s%da | length=1')
            assert 'argument length is not allowed' in str(ve)

            v = t['test1']
            assert v is not None
            assert len(v) == 1
            assert t['test1'] == None

            t = AtomicRandomString('test2', 'a%s%d | count=5')

            for _ in range(0, 5):
                v = t['test2']
                assert v is not None, f'iteration {_}'
                assert len(v) == 3
                assert v[0] == 'a'
                try:
                    int(v[-1])
                except:
                    pytest.fail()

            assert t['test2'] == None

            t = AtomicRandomString('test3', 'a%s%d | count=8, upper=True')

            for _ in range(0, 8):
                v = t['test3']
                assert v is not None, f'iteration {_}'
                assert len(v) == 3
                assert v[0] == 'A'
                try:
                    int(v[-1])
                except:
                    pytest.fail()

            assert t['test3'] == None

            t = AtomicRandomString('regnr', '%sA%s1%d%d | count=10, upper=True')

            for _ in range(0, 10):
                rn = t['regnr']
                assert rn is not None, f'iteration {_}'
                assert len(rn) == 6
                assert rn[1] == 'A'
                assert rn[3] == '1'

            assert t['regnr'] == None

            t['regnr'] = 'ABC123'
            assert t['regnr'] == None

            assert len(t._strings) == 4
            assert 'regnr' in t._strings
            del t['regnr']
            del t['regnr']
            assert len(t._strings) == 3
            assert 'regnr' not in t._strings

            AtomicRandomString.clear()
            assert len(t._strings) == 0

            t = AtomicRandomString('regnr', '%sA%s1%d%d | count=10000, upper=True')

            assert len(t._strings['regnr']) == 10000
            assert sorted(t._strings['regnr']) == sorted(list(set(t._strings['regnr'])))
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_multi_greenlet(self, cleanup: Callable) -> None:
        try:
            num_greenlets: int = 20
            num_iterations: int = 100
            count = num_greenlets * num_iterations

            t = AtomicRandomString('greenlet_var', f'%s%s%s%d%d%d | count={count}, upper=True')

            def exception_handler(greenlet: gevent.Greenlet) -> None:
                raise RuntimeError(f'func1 did not validate for {greenlet}')

            values: Set[str] = set()

            def func1() -> None:
                for _ in range(num_iterations):
                    # pylint: disable=pointless-statement
                    value = t['greenlet_var']
                    assert value is not None, f'iteration {_}'
                    assert value not in values
                    values.add(value)

            greenlets: List[Greenlet] = []
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
