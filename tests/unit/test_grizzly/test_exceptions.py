"""Unit tests of grizzly.exceptions."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import call

import pytest

from grizzly.exceptions import RestartScenario, RetryTask, StopUser, failure_handler, retry
from grizzly.types import FailureAction

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from tests.fixtures import GrizzlyFixture, MockerFixture


def test_failure_handler(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly = grizzly_fixture.grizzly

    scenario = grizzly.scenario

    assert scenario.failure_handling == {}

    scenario.failure_handling.update({
        None: StopUser,
        '504 gateway timeout': RetryTask,
    })

    failure_handler(None, scenario)

    with pytest.raises(StopUser):
        failure_handler(RuntimeError('foobar'), scenario)

    with pytest.raises(RetryTask):
        failure_handler(RuntimeError('504 gateway timeout'), scenario)

    del scenario.failure_handling[None]

    failure_handler(RuntimeError('foobar'), scenario)

    with pytest.raises(RetryTask):
        failure_handler(RuntimeError('504 gateway timeout'), scenario)

    scenario.failure_handling.update({AttributeError: RestartScenario})

    with pytest.raises(StopUser):
        failure_handler(AttributeError('foobaz'), scenario)

    scenario.failure_handling.update({MemoryError: RestartScenario})

    with pytest.raises(RestartScenario):
        failure_handler(MemoryError('0% free'), scenario)

    for exception in FailureAction.get_failure_exceptions():
        with pytest.raises(exception):
            failure_handler(exception(), scenario)


def test_retry(mocker: MockerFixture) -> None:
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

