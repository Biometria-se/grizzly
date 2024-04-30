"""Unit tests of grizzly.gevent."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from gevent import Greenlet, getcurrent

from grizzly.gevent import GreenletWithExceptionCatching

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import MockerFixture


def func(a: str, i: int) -> None:
    msg = f'func error, {a=}, {i=}'
    raise RuntimeError(msg)


class TestGreenletWithExceptionCatching:
    def test___init__(self) -> None:
        g = GreenletWithExceptionCatching()

        assert g.started_from == getcurrent()
        assert isinstance(g, Greenlet)

    def test_handle_exception(self) -> None:
        g = GreenletWithExceptionCatching()

        with pytest.raises(RuntimeError, match='error'):
            g.handle_exception(RuntimeError('error'))

    def test_spawn(self, mocker: MockerFixture) -> None:
        g = GreenletWithExceptionCatching()
        wrap_exceptions_spy = mocker.spy(g, 'wrap_exceptions')

        with pytest.raises(RuntimeError, match="func error, a='hello', i=1"):  # noqa: PT012
            func_g = g.spawn(func, 'hello', i=1)
            wrap_exceptions_spy.assert_called_once_with(func)
            wrap_exceptions_spy.reset_mock()
            func_g.join()

        wrap_exceptions_spy.assert_called_once_with(g.handle_exception)

    def test_spawn_blocking(self) -> None:
        def fail() -> None:
            msg = 'foobar'
            raise RuntimeError(msg)

        def ok() -> None:
            pass

        factory = GreenletWithExceptionCatching()

        with pytest.raises(RuntimeError, match='foobar'):
            factory.spawn_blocking(fail)

        factory.spawn_blocking(ok)

    def test_spawn_later(self, mocker: MockerFixture) -> None:
        g = GreenletWithExceptionCatching()
        wrap_exceptions_spy = mocker.spy(g, 'wrap_exceptions')

        with pytest.raises(RuntimeError, match="func error, a='foobar', i=1337"):  # noqa: PT012
            func_g = g.spawn_later(1, func, 'foobar', i=1337)
            wrap_exceptions_spy.assert_called_once_with(func)
            wrap_exceptions_spy.reset_mock()
            func_g.join()

        wrap_exceptions_spy.assert_called_once_with(g.handle_exception)
