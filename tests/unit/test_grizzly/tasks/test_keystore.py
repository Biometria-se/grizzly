"""Unit tests of grizzly.tasks.keystore."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.exceptions import RestartScenario
from grizzly.tasks import KeystoreTask
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import GrizzlyFixture, MockerFixture


class TestKeystoreTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        with pytest.raises(AssertionError, match='action context for get must be a string'):
            KeystoreTask('foobar', 'get', None)

        with pytest.raises(AssertionError, match='foobar has not been initialized'):
            KeystoreTask('foobar', 'get', 'foobar')

        grizzly_fixture.grizzly.state.variables.update({'foobar': 'none'})

        task = KeystoreTask('foobar', 'get', 'foobar')

        assert task.key == 'foobar'
        assert task.action == 'get'
        assert task.action_context == 'foobar'
        assert task.default_value is None

        task = KeystoreTask('foobar', 'get', 'foobar', ['hello', 'world'])

        assert task.key == 'foobar'
        assert task.action == 'get'
        assert task.action_context == 'foobar'
        assert task.default_value == ['hello', 'world']

        with pytest.raises(AssertionError, match='action context for set cannot be None'):
            KeystoreTask('foobar', 'set', None)

        task = KeystoreTask('foobar', 'set', {'hello': 'world'})

        assert task.key == 'foobar'
        assert task.action == 'set'
        assert task.action_context == {'hello': 'world'}
        assert task.default_value is None

        with pytest.raises(AssertionError, match='unknown is not a valid action'):
            KeystoreTask('foobar', 'unknown', None)  # type: ignore[arg-type]

    def test___call___get(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        behave = grizzly_fixture.behave.context
        grizzly = cast(GrizzlyContext, behave.grizzly)

        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.consumer = consumer_mock

        grizzly.state.variables.update({'foobar': 'none'})

        # key does not exist in keystore
        setattr(parent.consumer.keystore_get, 'return_value', None)  # noqa: B010
        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        task_factory = KeystoreTask('foobar', 'get', 'foobar')
        task = task_factory()

        parent.user._scenario.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            task(parent)

        assert parent.user._context['variables'].get('foobar', None) is None

        request_spy.assert_called_once_with(
            request_type='KEYS',
            name='001 foobar',
            response_time=0,
            response_length=1,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='key foobar does not exist in keystore'),
        )
        request_spy.reset_mock()

        # key exist in keystore
        setattr(parent.consumer.keystore_get, 'return_value', ['hello', 'world'])  # noqa: B010

        task(parent)

        request_spy.assert_not_called()

        assert parent.user._context['variables'].get('foobar', None) == ['hello', 'world']

        # key does not exist in keystore, but has a default value
        setattr(parent.consumer.keystore_get, 'return_value', None)  # noqa: B010

        task_factory = KeystoreTask('foobar', 'get', 'foobar', {'hello': 'world'})
        assert task_factory.default_value is not None
        task = task_factory()

        task(parent)

        request_spy.assert_not_called()
        consumer_mock.keystore_set.assert_called_with('foobar', {'hello': 'world'})
        assert parent.user._context['variables'].get('foobar', None) == {'hello': 'world'}

    def test___call___set(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        setattr(parent, 'consumer', consumer_mock)  # noqa: B010

        task_factory = KeystoreTask('foobar', 'set', {'hello': '{{ world }}'})
        task = task_factory()

        task(parent)

        consumer_mock.keystore_set.assert_called_once_with('foobar', {'hello': '{{ world }}'})
        consumer_mock.reset_mock()
