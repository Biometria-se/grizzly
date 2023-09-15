from gevent import Greenlet, getcurrent

import pytest

from grizzly.gevent import GreenletWithExceptionCatching

from tests.fixtures import MockerFixture


def func(a: str, i: int) -> None:
    raise RuntimeError(f'func error, {a=}, {i=}')


class TestGreenletWithExceptionCatching:
    def test___init__(self) -> None:
        g = GreenletWithExceptionCatching()

        assert g.started_from == getcurrent()
        assert isinstance(g, Greenlet)

    def test_handle_exception(self) -> None:
        g = GreenletWithExceptionCatching()

        with pytest.raises(RuntimeError) as re:
            g.handle_exception(RuntimeError('error'))
        assert str(re.value) == 'error'

    def test_spawn(self, mocker: MockerFixture) -> None:
        g = GreenletWithExceptionCatching()
        wrap_exceptions_spy = mocker.spy(g, 'wrap_exceptions')

        with pytest.raises(RuntimeError) as re:
            func_g = g.spawn(func, 'hello', i=1)
            wrap_exceptions_spy.assert_called_once_with(func)
            wrap_exceptions_spy.reset_mock()
            func_g.join()
        assert str(re.value) == "func error, a='hello', i=1"

        wrap_exceptions_spy.assert_called_once_with(g.handle_exception)

    def test_spawn_later(self, mocker: MockerFixture) -> None:
        g = GreenletWithExceptionCatching()
        wrap_exceptions_spy = mocker.spy(g, 'wrap_exceptions')

        with pytest.raises(RuntimeError) as re:
            func_g = g.spawn_later(1, func, 'foobar', i=1337)
            wrap_exceptions_spy.assert_called_once_with(func)
            wrap_exceptions_spy.reset_mock()
            func_g.join()
        assert str(re.value) == "func error, a='foobar', i=1337"

        wrap_exceptions_spy.assert_called_once_with(g.handle_exception)
