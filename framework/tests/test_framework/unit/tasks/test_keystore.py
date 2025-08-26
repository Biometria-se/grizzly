"""Unit tests of grizzly.tasks.keystore."""

from __future__ import annotations

from json import loads as jsonloads
from typing import TYPE_CHECKING, Any

import pytest
from grizzly.exceptions import RestartScenario
from grizzly.tasks import KeystoreTask

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class TestKeystoreTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        with pytest.raises(AssertionError, match='action context for "get" must be a string'):
            KeystoreTask('foobar', 'get', None)

        with pytest.raises(AssertionError, match='variable "foobar" has not been initialized'):
            KeystoreTask('foobar', 'get', 'foobar')

        grizzly_fixture.grizzly.scenario.variables.update({'foobar': 'none'})

        task = KeystoreTask('foobar', 'get', 'foobar')

        assert task.key == 'foobar'
        assert task.action == 'get'
        assert task.action_context == 'foobar'
        assert task.default_value is None
        assert task.arguments == {}

        task = KeystoreTask('foobar', 'get', 'foobar', "['hello', 'world']")

        assert task.key == 'foobar'
        assert task.action == 'get'
        assert task.action_context == 'foobar'
        assert task.default_value == ['hello', 'world']
        assert task.arguments == {}

        with pytest.raises(AssertionError, match='action context for "set" must be declared'):
            KeystoreTask('foobar', 'set', None)

        grizzly = grizzly_fixture.grizzly
        grizzly.state.configuration.update({'keystore.wait': 100, 'keystore.poll.interval': 1.0})
        task = KeystoreTask('foobar | wait=$conf::keystore.wait$', 'set', '{"hello": "world"} | interval=$conf::keystore.poll.interval$')

        assert task.key == 'foobar'
        assert task.action == 'set'
        assert task.action_context == {'hello': 'world'}
        assert task.default_value is None
        assert task.arguments == {'wait': 100, 'interval': 1.0}

        assert task.__template_attributes__ == {'action_context', 'key'}

        with pytest.raises(AssertionError, match='"unknown" is not a valid action'):
            KeystoreTask('foobar', 'unknown', None)  # type: ignore[arg-type]

    def test___call___get(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        grizzly.scenario.variables.update({'foobar': 'none'})
        parent.user.variables.update({'foobar': 'none'})

        # key does not exist in keystore
        consumer_mock.keystore_get.return_value = None
        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        task_factory = KeystoreTask('foobar', 'get', 'foobar')
        task = task_factory()

        parent.user._scenario.failure_handling.update({None: RestartScenario})

        with pytest.raises(RestartScenario):
            task(parent)

        assert parent.user.variables.get('foobar', None) == 'none'
        consumer_mock.keystore_get.assert_called_once_with('foobar', remove=False)
        consumer_mock.reset_mock()

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
        consumer_mock.keystore_get.return_value = ['hello', 'world']

        task(parent)

        request_spy.assert_not_called()
        consumer_mock.keystore_get.assert_called_once_with('foobar', remove=False)
        consumer_mock.reset_mock()

        assert parent.user.variables.get('foobar', None) == ['hello', 'world']

        # key does not exist in keystore, but has a default value
        consumer_mock.keystore_get.return_value = None

        task_factory = KeystoreTask('foobar', 'get', 'foobar', '{"hello": "world"}')
        assert task_factory.default_value is not None
        task = task_factory()

        task(parent)

        request_spy.assert_not_called()
        consumer_mock.keystore_set.assert_called_with('foobar', {'hello': 'world'})
        consumer_mock.reset_mock()

        assert parent.user.variables.get('foobar', None) == {'hello': 'world'}

        # remove key entry from keystore after retreiving value
        consumer_mock.keystore_get.return_value = 'foobar'

        task_factory = KeystoreTask('foobar', 'get_del', 'foobar')
        assert task_factory.default_value is None
        task = task_factory()

        task(parent)

        request_spy.assert_not_called()
        consumer_mock.keystore_get.assert_called_once_with('foobar', remove=True)
        consumer_mock.reset_mock()

        assert parent.user.variables.get('foobar', None) == 'foobar'

    def test___call___set(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        task_factory = KeystoreTask('foobar', 'set', "{'hello': '{{ world }}'}")
        task = task_factory()

        task(parent)

        consumer_mock.keystore_set.assert_called_once_with('foobar', {'hello': '{{ world }}'})
        consumer_mock.reset_mock()

    def test___call__inc(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        consumer_mock.keystore_inc.return_value = 1

        parent.user._scenario.variables.update({'counter': 'none'})
        task_factory = KeystoreTask('foobar', 'inc', 'counter')
        task = task_factory()

        task(parent)
        assert parent.user.variables.get('counter', None) == 1
        consumer_mock.keystore_inc.assert_called_once_with('foobar', step=1)
        consumer_mock.reset_mock()

    def test___call__dec(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        consumer_mock.keystore_dec.return_value = 1

        parent.user._scenario.variables.update({'counter': 'none'})
        task_factory = KeystoreTask('foobar', 'dec', 'counter')
        task = task_factory()

        task(parent)
        assert parent.user.variables.get('counter', None) == 1
        consumer_mock.keystore_dec.assert_called_once_with('foobar', step=1)
        consumer_mock.reset_mock()

    @pytest.mark.parametrize(('actual', 'expected'), [('hello', 'hello'), ('True', True), ('10', 10), ('13.7', 13.7)])
    def test___call__push(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, actual: str, expected: Any) -> None:
        parent = grizzly_fixture()
        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        consumer_mock.keystore_push.return_value = None

        task_factory = KeystoreTask('foobar', 'push', actual)
        task = task_factory()

        task(parent)

        consumer_mock.keystore_push.assert_called_once_with('foobar', expected)
        consumer_mock.reset_mock()

    def test___call__push_complex(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()
        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        value = """{
    'name': '{{ name }}',
    'location': '{{ location }}',
    'receiver': '{{ receiver }}',
    'product': '{{ product }}',
    'parcels': '{{ parcels }}'
} | render=True"""

        parent.user.variables.update(
            {
                'name': 'AMD RADEON 7900 XTX',
                'location': 'Storage 2',
                'receiver': 'Grizzly',
                'product': 'GPU1337',
                'parcels': 1,
            },
        )

        expected, _ = value.replace("'", '"').split(' | ', 1)
        expected = jsonloads(parent.user.render(expected))

        consumer_mock.keystore_push.return_value = None

        task_factory = KeystoreTask('foobar', 'push', value)
        task = task_factory()

        task(parent)

        consumer_mock.keystore_push.assert_called_once_with('foobar', expected)
        consumer_mock.reset_mock()

    def test___call__pop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()
        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock

        consumer_mock.keystore_pop.return_value = None

        with pytest.raises(AssertionError, match='variable "foobar" has not been initialized'):
            KeystoreTask('foobar', 'pop', 'foobar')

        grizzly_fixture.grizzly.scenario.variables.update({'foobar': 'none'})
        parent.user.set_variable('foobar', 'none')
        parent.user.set_variable('key', 'hello')
        task_factory = KeystoreTask('foobar::{{ key }}', 'pop', 'foobar')

        assert sorted(task_factory.get_templates()) == sorted(['foobar::{{ key }}'])

        task = task_factory()

        task(parent)

        assert parent.user.variables.get('foobar', None) == 'none'
        consumer_mock.keystore_pop.assert_called_once_with('foobar::hello', wait=-1, poll_interval=1.0)
        consumer_mock.reset_mock()

        consumer_mock.keystore_pop.return_value = 'hello'
        task_factory.arguments.update({'wait': 2, 'interval': 0.5})
        task(parent)

        assert parent.user.variables.get('foobar', None) == 'hello'
        consumer_mock.keystore_pop.assert_called_once_with('foobar::hello', wait=2, poll_interval=0.5)
        consumer_mock.reset_mock()

    def test___call__del(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()
        assert parent is not None

        consumer_mock = mocker.MagicMock()
        parent.__class__._consumer = consumer_mock
        consumer_mock.keystore_del.return_value = None

        with pytest.raises(AssertionError, match='action context for "del" cannot be declared'):
            KeystoreTask('foobar', 'del', 'baz')

        task_factory = KeystoreTask('foobar', 'del', None)
        task = task_factory()

        task(parent)

        consumer_mock.keystore_del.assert_called_once_with('foobar')
        consumer_mock.reset_mock()
