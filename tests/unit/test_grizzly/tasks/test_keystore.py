from typing import cast
from unittest.mock import ANY

import pytest

from grizzly.tasks import KeystoreTask
from grizzly.context import GrizzlyContext
from grizzly.exceptions import RestartScenario

from tests.fixtures import GrizzlyFixture, MockerFixture


class TestKeystoreTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        with pytest.raises(AssertionError) as ae:
            KeystoreTask('foobar', 'get', None)
        assert str(ae.value) == 'action context for get must be a string'

        with pytest.raises(AssertionError) as ae:
            KeystoreTask('foobar', 'get', 'foobar')
        assert str(ae.value) == 'foobar has not been initialized'

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

        with pytest.raises(AssertionError) as ae:
            KeystoreTask('foobar', 'set', None)
        assert str(ae.value) == 'action context for set cannot be None'

        task = KeystoreTask('foobar', 'set', {'hello': 'world'})

        assert task.key == 'foobar'
        assert task.action == 'set'
        assert task.action_context == {'hello': 'world'}
        assert task.default_value is None

        with pytest.raises(AssertionError) as ae:
            KeystoreTask('foobar', 'unknown', None)  # type: ignore
        assert str(ae.value) == 'unknown is not a valid action'

    def test___call___get(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        behave = grizzly_fixture.behave.context
        grizzly = cast(GrizzlyContext, behave.grizzly)

        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        setattr(parent, 'consumer', consumer_mock)

        grizzly.state.variables.update({'foobar': 'none'})

        # key does not exist in keystore
        setattr(parent.consumer.keystore_get, 'return_value', None)
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
            exception=ANY,
        )

        _, kwargs = request_spy.call_args_list[-1]
        actual_exception = kwargs.get('exception', None)
        assert isinstance(actual_exception, RuntimeError)
        assert str(actual_exception) == 'key foobar does not exist in keystore'

        request_spy.reset_mock()

        # key exist in keystore
        setattr(parent.consumer.keystore_get, 'return_value', ['hello', 'world'])

        task(parent)

        request_spy.assert_not_called()

        assert parent.user._context['variables'].get('foobar', None) == ['hello', 'world']

        # key does not exist in keystore, but has a default value
        setattr(parent.consumer.keystore_get, 'return_value', None)

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
        setattr(parent, 'consumer', consumer_mock)

        task_factory = KeystoreTask('foobar', 'set', {'hello': '{{ world }}'})
        task = task_factory()

        task(parent)

        consumer_mock.keystore_set.assert_called_once_with('foobar', {'hello': '{{ world }}'})
        consumer_mock.reset_mock()
