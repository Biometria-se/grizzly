"""Unit tests of grizzly.exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import call

import pytest
from grizzly.exceptions import retry

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    from test_framework.fixtures import MockerFixture


def test_retry(mocker: MockerFixture) -> None:  # noqa: PLR0915
    uniform_mock = mocker.patch('grizzly.exceptions.uniform', return_value=1.0)
    sleep_mock = mocker.patch('grizzly.exceptions.gsleep', return_value=None)

    def raise_exception(*, retries: int, exception: type[Exception], return_value: Any) -> MagicMock:
        return mocker.MagicMock(side_effect=([exception] * retries) + [return_value])  # type: ignore[no-any-return]

    func = raise_exception(retries=2, exception=RuntimeError, return_value='foobar')
    with retry(retries=3, exceptions=(RuntimeError,), backoff=None) as context:
        result = context.execute(func, 'hello', foobar='world')
        assert result == 'foobar'

    assert func.call_count == 3
    for i in range(func.call_count):
        assert func.call_args_list[i] == call('hello', foobar='world')

    sleep_mock.assert_not_called()
    uniform_mock.assert_not_called()

    # make sure time between retries is used
    func = raise_exception(retries=2, exception=RuntimeError, return_value='foobar')
    with retry(retries=3, exceptions=(RuntimeError,), backoff=1.0) as context:
        result = context.execute(func, 'hello', foobar='world')
        assert result == 'foobar'

    assert func.call_count == 3
    assert sleep_mock.call_count == 2
    assert sleep_mock.call_args_list[0] == call(2.0)
    assert sleep_mock.call_args_list[1] == call(4.0)
    sleep_mock.reset_mock()

    assert uniform_mock.call_count == 2
    for i in range(uniform_mock.call_count):
        assert uniform_mock.call_args_list[i] == call(0.5, 1.5)
    uniform_mock.reset_mock()

    # func raises exception when number of retires is exceeded
    func = raise_exception(retries=4, exception=RuntimeError, return_value='foobar')
    with pytest.raises(RuntimeError), retry(retries=3, exceptions=(RuntimeError,), backoff=1.0) as context:
        result = context.execute(func, 'hello', foobar='world')

    assert func.call_count == 3
    assert sleep_mock.call_count == 2
    assert sleep_mock.call_args_list[0] == call(2.0)
    assert sleep_mock.call_args_list[1] == call(4.0)
    sleep_mock.reset_mock()

    assert uniform_mock.call_count == 2
    for i in range(uniform_mock.call_count):
        assert uniform_mock.call_args_list[i] == call(0.5, 1.5)
    uniform_mock.reset_mock()

    # func raises RuntimeError when number of retires is exceeded, but retry raises AssertionError
    func = raise_exception(retries=4, exception=RuntimeError, return_value='foobar')
    with pytest.raises(AssertionError), retry(retries=3, exceptions=(RuntimeError,), backoff=1.0, failure_exception=AssertionError) as context:
        result = context.execute(func, 'hello', foobar='world')

    assert func.call_count == 3
    assert sleep_mock.call_count == 2
    assert sleep_mock.call_args_list[0] == call(2.0)
    assert sleep_mock.call_args_list[1] == call(4.0)
    sleep_mock.reset_mock()

    assert uniform_mock.call_count == 2
    for i in range(uniform_mock.call_count):
        assert uniform_mock.call_args_list[i] == call(0.5, 1.5)
    uniform_mock.reset_mock()

    # func raises RuntimeError when number of retires is exceeded, but retry raises AssertionError with specific message
    func = raise_exception(retries=4, exception=RuntimeError, return_value='foobar')
    with pytest.raises(AssertionError, match='Big trouble'), retry(retries=3, exceptions=(RuntimeError,), backoff=1.0, failure_exception=AssertionError('Big trouble')) as context:
        result = context.execute(func, 'hello', foobar='world')

    assert func.call_count == 3
    assert sleep_mock.call_count == 2
    assert sleep_mock.call_args_list[0] == call(2.0)
    assert sleep_mock.call_args_list[1] == call(4.0)
    sleep_mock.reset_mock()

    assert uniform_mock.call_count == 2
    for i in range(uniform_mock.call_count):
        assert uniform_mock.call_args_list[i] == call(0.5, 1.5)
    uniform_mock.reset_mock()
