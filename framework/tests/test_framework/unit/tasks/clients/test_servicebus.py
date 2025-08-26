"""Unit tests for grizzly.tasks.clients.servicebus."""

from __future__ import annotations

import logging
from json import dumps as jsondumps
from typing import TYPE_CHECKING

import pytest
from grizzly.tasks.clients import ServiceBusClientTask
from grizzly.types import RequestDirection, StrDict

from test_framework.helpers import SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from async_messaged import AsyncMessageRequest

    from test_framework.fixtures import GrizzlyFixture, MockerFixture, NoopZmqFixture


class TestServiceBusClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        context_mock = mocker.patch('grizzly.tasks.clients.servicebus.zmq.Context', autospec=True)

        task_type = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        # <!-- connection string
        task = task_type(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': '',
            'message_wait': None,
            'consume': False,
            'username': None,
            'password': None,
            'tenant': None,
            'unique': True,
            'verbose': False,
            'forward': False,
        }
        assert task._state == {}
        assert task.text is None
        assert task.should_empty
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
        context_mock.reset_mock()
        # // -->

        # <!-- credential
        task = task_type(RequestDirection.FROM, 'sb://bob@example.com:secret@my-sbns/#Tenant=example.com&Empty=False&Unique=False&Verbose=True&Forward=True', 'test')

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net'
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': '',
            'message_wait': None,
            'consume': False,
            'username': 'bob@example.com',
            'password': 'secret',
            'tenant': 'example.com',
            'unique': False,
            'verbose': True,
            'forward': True,
        }
        assert task._state == {}
        assert task.text is None
        assert not task.should_empty
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
        context_mock.reset_mock()
        # // -->

        task = task_type(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/queue:my-queue;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            source='hello world!',
        )

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'sender',
            'endpoint': 'queue:my-queue',
            'message_wait': None,
            'consume': False,
            'username': None,
            'password': None,
            'tenant': None,
            'unique': True,
            'verbose': False,
            'forward': False,
        }
        assert task.text is None
        assert task.source == 'hello world!'
        assert task._state == {}
        assert task.__template_attributes__ == {'endpoint', 'destination', 'source', 'name', 'variable_template', 'context'}
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        grizzly = grizzly_fixture.grizzly
        grizzly.state.configuration.update({'sbns.host': 'sb.windows.net', 'sbns.key.name': 'KeyName', 'sbns.key.secret': 'SeCrEtKeY=='})
        grizzly.scenario.variables.update({'foobar': 'none'})

        task = task_type(
            RequestDirection.FROM,
            (
                'sb://$conf::sbns.host$/topic:my-topic/subscription:"my-subscription-{{ id }}"/expression:$.hello.world'
                ';SharedAccessKeyName=$conf::sbns.key.name$;SharedAccessKey=$conf::sbns.key.secret$#Consume=True&MessageWait=300&ContentType=json'
            ),
            'test',
            text='foobar',
            payload_variable='foobar',
        )

        assert task.endpoint == 'sb://sb.windows.net/;SharedAccessKeyName=KeyName;SharedAccessKey=SeCrEtKeY=='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': 'topic:my-topic, subscription:"my-subscription-{{ id }}", expression:$.hello.world',
            'consume': True,
            'message_wait': 300,
            'content_type': 'JSON',
            'username': None,
            'password': None,
            'tenant': None,
            'unique': True,
            'verbose': False,
            'forward': False,
        }
        assert task.text == 'foobar'
        assert task.payload_variable == 'foobar'
        assert task.metadata_variable is None
        assert task.source is None
        assert task._state == {}
        assert sorted(task.get_templates()) == sorted(['topic:my-topic, subscription:"my-subscription-{{ id }}", expression:$.hello.world', '{{ foobar }}'])
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        grizzly.scenario.variables.update({'barfoo': 'none'})

        task = task_type(
            RequestDirection.FROM,
            (
                'sb://$conf::sbns.host$/topic:my-topic/subscription:"my-subscription-{{ id }}"/expression:$.hello.world'
                ';SharedAccessKeyName=$conf::sbns.key.name$;SharedAccessKey=$conf::sbns.key.secret$#Consume=True&MessageWait=300&ContentType=json'
            ),
            'test',
            text='foobar',
            payload_variable='foobar',
            metadata_variable='barfoo',
        )

        assert task.endpoint == 'sb://sb.windows.net/;SharedAccessKeyName=KeyName;SharedAccessKey=SeCrEtKeY=='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': 'topic:my-topic, subscription:"my-subscription-{{ id }}", expression:$.hello.world',
            'consume': True,
            'message_wait': 300,
            'content_type': 'JSON',
            'username': None,
            'password': None,
            'tenant': None,
            'unique': True,
            'verbose': False,
            'forward': False,
        }
        assert task.text == 'foobar'
        assert task.payload_variable == 'foobar'
        assert task.metadata_variable == 'barfoo'
        assert task.source is None
        assert task._state == {}
        assert sorted(task.get_templates()) == sorted(['topic:my-topic, subscription:"my-subscription-{{ id }}", expression:$.hello.world', '{{ foobar }} {{ barfoo }}'])
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        task = task_type(
            RequestDirection.FROM,
            (
                'sb://my-sbns/topic:my-topic/subscription:my-subscription/expression:$.name|=\'["hello", "world"]\''
                ';SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=120&ContentType=json&Unique=False'
            ),
            'test',
        )

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'connection': 'receiver',
            'endpoint': 'topic:my-topic, subscription:my-subscription, expression:$.name|=\'["hello", "world"]\'',
            'message_wait': 120,
            'consume': False,
            'content_type': 'JSON',
            'username': None,
            'password': None,
            'tenant': None,
            'unique': False,
            'verbose': False,
            'forward': False,
        }
        assert task._state == {}
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        with pytest.raises(AssertionError, match='MessageWait parameter in endpoint fragment is not a valid integer'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=foo',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='foo is not a valid boolean'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Consume=foo',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(ValueError, match='"foo" is an unknown response content type'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#ContentType=foo',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='Tenant fragment in endpoint is not allowed when using connection string'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Tenant=example.com',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='no query string found in endpoint'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription#ContentType=xml',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='SharedAccessKey not found in query string of endpoint'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='SharedAccessKeyName not found in query string of endpoint'):
            task_type(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKey=AccessKey',
                'test',
            )
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='Tenant not found in fragment of endpoint'):
            task_type(RequestDirection.FROM, 'sb://bob@example.com:secret@my-sbns/', 'test')
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='query string found in endpoint, which is not allowed when using credential authentication'):
            task_type(RequestDirection.FROM, 'sb://bob@example.com:secret@my-sbns/;SharedAccessKeyName=key;SharedAccessKey=asdf#Tenant=example.com', 'test')
        context_mock.assert_not_called()

        with pytest.raises(AssertionError, match='subscription name is too long, max length is 50 characters'):
            task_type(
                RequestDirection.FROM,
                (
                    'sb://my-sbns/topic:my-topic/subscription:my-subscriptionaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                    ';SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=120&ContentType=json'
                ),
                'test',
            )

        with pytest.raises(AssertionError, match='subscription name is too long, max length is 42 characters'):
            task_type(
                RequestDirection.FROM,
                (
                    'sb://my-sbns/topic:my-topic/subscription:my-subscriptionaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                    ';SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=120&ContentType=json'
                ),
                'test',
                text='rule text',
            )

        with pytest.raises(AssertionError, match='asdf is not a valid boolean'):
            task_type(
                RequestDirection.FROM,
                (
                    'sb://my-sbns/topic:my-topic/subscription:my-subscription'
                    ';SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=120&ContentType=json&Verbose=asdf'
                ),
                'test',
                text='rule text',
            )

    def test_text(self, grizzly_fixture: GrizzlyFixture) -> None:
        task_type = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})
        task = task_type(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.text is None

        task.text = 'hello'

        assert task.text == 'hello'

        task.text = """
                hello
        """

        assert task.text == 'hello'

    def test_connect(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        noop_zmq('grizzly.tasks.clients.servicebus')

        async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request')

        grizzly_fixture.grizzly.state.configuration.update({'sbns.key.secret': 'fooBARfoo'})

        task_type = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})
        task = task_type(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/queue:my-queue;SharedAccessKeyName=AccessKey;SharedAccessKey=$conf::sbns.key.secret$',
            'test',
        )

        # successfully connected
        assert task._state == {}
        async_message_request_mock.return_value = {'success': True, 'worker': 'foo-bar-baz-foo'}

        task.connect(parent)

        state = task.get_state(parent)

        assert state.worker == 'foo-bar-baz-foo'
        assert state.parent is parent
        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=fooBARfoo'

        async_message_request_mock.assert_called_once_with(
            state.client,
            {
                'worker': None,
                'client': state.parent_id,
                'action': 'HELLO',
                'context': state.context,
            },
        )

    def test_disconnect(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request')
        zmq_disconnect_mock = mocker.patch('grizzly.tasks.clients.servicebus.zmq_disconnect')

        task_type = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})
        task = task_type(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/queue:my-queue;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        # not connected, don't do anything
        task.disconnect(parent)

        async_message_request_mock.assert_not_called()

        # connected
        state = task.get_state(parent)
        state.worker = 'foo-bar-baz-foo'
        state.client = client_mock

        task.disconnect(parent)

        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz-foo',
                'client': state.parent_id,
                'action': 'DISCONNECT',
                'context': state.context,
            },
        )
        zmq_disconnect_mock.assert_called_once_with(client_mock, destroy_context=False)

        assert task._state == {}

    def test_subscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        sha256_mock = mocker.patch('grizzly.tasks.clients.servicebus.sha256')
        sha256_mock.return_value.hexdigest.return_value = 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'

        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request', return_value={'message': 'foobar!'})

        task_type = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})
        task = task_type(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:"my-subscription-{{ id }}";SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        state = task.get_state(parent)

        state.worker = 'foo-bar-baz'
        state.client = client_mock
        task._text = '1={{ condition }}'

        parent.user.variables.update({'id': 'baz-bar-foo', 'condition': '2'})
        expected_context = state.context.copy()
        expected_context['endpoint'] = expected_context['endpoint'].replace('{{ id }}', 'baz-bar-foo')

        with caplog.at_level(logging.INFO):
            task.subscribe(parent)

        assert caplog.messages == ['foobar!']
        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz',
                'client': state.parent_id,
                'action': 'SUBSCRIBE',
                'context': expected_context,
                'payload': '1=2',
            },
        )
        caplog.clear()
        async_message_request_mock.reset_mock()

        task = task_type(
            RequestDirection.FROM,
            (
                "sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:'my-subscription-{{ id }}'/expression:'$.`this`[bar='foo' && bar='foo']';"
                'SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
            ),
            'test',
        )
        task._text = '1=1'

        state = task.get_state(parent)

        state.worker = 'foo-bar-baz'
        state.client = client_mock

        expected_context = state.context.copy()
        expected_context['endpoint'] = expected_context['endpoint'].replace('{{ id }}', 'baz-bar-foo')

        with caplog.at_level(logging.INFO):
            task.subscribe(parent)

        assert caplog.messages == ['foobar!']
        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz',
                'client': state.parent_id,
                'action': 'SUBSCRIBE',
                'context': {
                    'url': expected_context['url'],
                    'connection': 'receiver',
                    'endpoint': "topic:my-topic, subscription:'my-subscription-baz-bar-foodeadbeef', expression:'$.`this`[bar='foo' && bar='foo']'",
                    'message_wait': None,
                    'consume': False,
                    'username': None,
                    'password': None,
                    'tenant': None,
                    'unique': True,
                    'verbose': False,
                    'forward': False,
                },
                'payload': '1=1',
            },
        )

        async_message_request_mock.reset_mock()
        caplog.clear()

        task = task_type(
            RequestDirection.FROM,
            (
                "sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:'my-subscription-{{ id }}'/"
                "expression:'$.`this`[?bar='foo' & bar='foo']';SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Unique=False&Verbose=True&Forward=True"
            ),
            'test',
        )
        task._text = '1=1'

        state = task.get_state(parent)

        state.worker = 'foo-bar-baz'
        state.client = client_mock

        expected_context = state.context.copy()
        expected_context['endpoint'] = expected_context['endpoint'].replace('{{ id }}', 'baz-bar-foo')

        with caplog.at_level(logging.INFO):
            task.subscribe(parent)

        assert caplog.messages == ['foobar!']
        print(async_message_request_mock.call_args_list)
        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz',
                'client': state.parent_id,
                'action': 'SUBSCRIBE',
                'context': {
                    'url': expected_context['url'],
                    'connection': 'receiver',
                    'endpoint': "topic:my-topic, subscription:'my-subscription-baz-bar-foo', expression:'$.`this`[?bar='foo' & bar='foo']'",
                    'message_wait': None,
                    'consume': False,
                    'username': None,
                    'password': None,
                    'tenant': None,
                    'unique': False,
                    'verbose': True,
                    'forward': True,
                },
                'payload': '1=1',
            },
        )

    def test_unsubscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request', return_value={'message': 'hello world!'})

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            text='1=1',
        )

        state = task.get_state(parent)
        state.client = client_mock
        state.worker = 'foo-bar-baz'

        with caplog.at_level(logging.INFO):
            task.unsubscribe(parent)

        assert caplog.messages == ['hello world!']

        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz',
                'client': id(parent.user),
                'action': 'UNSUBSCRIBE',
                'context': state.context,
            },
        )

    def test_empty(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request', return_value={'message': ''})

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            text='1=1',
        )

        state = task.get_state(parent)
        state.client = client_mock
        state.worker = 'foo-bar-baz'

        with caplog.at_level(logging.INFO):
            task.empty(parent)

        assert caplog.messages == []
        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz',
                'client': id(parent.user),
                'action': 'EMPTY',
                'context': state.context,
            },
        )

        async_message_request_mock.reset_mock()
        async_message_request_mock.return_value = {'message': 'removed 100 messages which took 1.3 seconds'}

        with caplog.at_level(logging.INFO):
            task.empty(parent)

        assert caplog.messages == ['removed 100 messages which took 1.3 seconds']
        async_message_request_mock.assert_called_once_with(
            client_mock,
            {
                'worker': 'foo-bar-baz',
                'client': id(parent.user),
                'action': 'EMPTY',
                'context': state.context,
            },
        )

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        sha256_mock = mocker.patch('grizzly.tasks.clients.servicebus.sha256')
        sha256_mock.return_value.hexdigest.return_value = 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        # no text
        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        connect_mock = mocker.patch.object(task, 'connect', return_value=None)
        subscribe_mock = mocker.patch.object(task, 'subscribe', return_value=None)

        state = task.get_state(parent)

        assert task._text is None

        task.on_start(parent)

        connect_mock.assert_called_once_with(parent)
        subscribe_mock.assert_not_called()
        assert task.context.get('endpoint', None) == 'topic:my-topic, subscription:my-subscription'

        connect_mock.reset_mock()
        subscribe_mock.reset_mock()

        # text
        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            text='1=1',
        )

        connect_mock = mocker.patch.object(task, 'connect', return_value=None)
        subscribe_mock = mocker.patch.object(task, 'subscribe', return_value=None)

        state = task.get_state(parent)

        task.on_start(parent)

        connect_mock.assert_called_once_with(parent)
        subscribe_mock.assert_called_once_with(parent)
        assert state.context.get('endpoint', None) == 'topic:my-topic, subscription:my-subscriptiondeadbeef'

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        disconnect_mock = mocker.patch.object(task, 'disconnect', return_value=None)
        unsubscribe_mock = mocker.patch.object(task, 'unsubscribe', return_value=None)

        # no text
        assert task._text is None

        task.on_stop(parent)

        disconnect_mock.assert_called_once_with(parent)
        unsubscribe_mock.assert_not_called()

        disconnect_mock.reset_mock()
        unsubscribe_mock.reset_mock()

        # text
        task._text = '1=1'

        task.on_stop(parent)

        disconnect_mock.assert_called_once_with(parent)
        unsubscribe_mock.assert_called_once_with(parent)

    def test_on_iteration(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        empty_mock = mocker.patch.object(task, 'empty', return_value=None)

        # no text
        assert task._text is None

        task.on_iteration(parent)

        empty_mock.assert_not_called()
        task._text = '1=1'

        task.on_iteration(parent)

        empty_mock.assert_called_once_with(parent)

        # do not empty
        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Empty=False',
            'test',
        )

        empty_mock = mocker.patch.object(task, 'empty', return_value=None)
        task._text = '1=1'

        task.on_iteration(parent)
        empty_mock.assert_not_called()

    def test_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        sha256_mock = mocker.patch('grizzly.tasks.clients.servicebus.sha256')
        sha256_mock.return_value.hexdigest.return_value = 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'

        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request', return_value={'message': 'foobar!'})

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        state = task.get_state(parent)
        state.client = client_mock
        action_mock = mocker.MagicMock()
        meta_mock: StrDict = {}
        action_mock.__enter__.return_value = meta_mock
        context_mock = mocker.patch.object(task, 'action', return_value=action_mock)

        # got response, empty payload
        request: AsyncMessageRequest = {
            'context': task.context,
        }

        assert task.request(parent, request) == {'message': 'foobar!'}

        context_mock.assert_called_once_with(state.parent)
        request.update({'client': state.parent_id})
        async_message_request_mock.assert_called_once_with(state.client, request)
        del request['client']

        assert meta_mock == {
            'action': state.context.get('endpoint', None),
            'request': request,
            'response_length': 0,
            'response': {'message': 'foobar!'},
        }

        context_mock.reset_mock()
        async_message_request_mock.reset_mock()
        meta_mock.clear()
        action_mock.reset_mock()

        # got response, some payload
        del request['context']['url']
        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request_wrapper', return_value={'message': 'foobar!', 'payload': '1234567890'})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            text='1=1',
        )
        state = task.get_state(parent)
        state.client = client_mock
        action_mock = mocker.MagicMock()
        meta_mock = {}
        action_mock.__enter__.return_value = meta_mock
        context_mock = mocker.patch.object(task, 'action', return_value=action_mock)

        assert task.request(parent, request) == {'message': 'foobar!', 'payload': '1234567890'}

        context_mock.assert_called_once_with(state.parent)
        async_message_request_mock.assert_called_once_with(state.parent, state.client, request)
        assert meta_mock == {
            'action': 'topic:my-topic, subscription:my-subscriptiondeadbeef',
            'request': request,
            'response_length': 10,
            'response': {'message': 'foobar!', 'payload': '1234567890'},
        }

    def test_request_from(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()
        parent.user._context = {'variables': {}}

        cls_task = type('ServiceBusClientTaskTest', (ServiceBusClientTask,), {'__scenario__': grizzly_fixture.grizzly.scenario})

        task = cls_task(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        state = task.get_state(parent)
        state.client = client_mock
        state.worker = 'foo-bar'

        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': None, 'payload': 'foobar'})

        # no variables
        task.payload_variable = None

        assert task.request_from(parent) == (None, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'RECEIVE',
                'worker': 'foo-bar',
                'context': state.context,
                'payload': None,
            },
        )

        assert parent.user.variables == {}

        request_mock.reset_mock()

        # with payload variable
        task.payload_variable = 'foobaz'

        assert task.request_from(parent) == (None, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'RECEIVE',
                'worker': 'foo-bar',
                'context': state.context,
                'payload': None,
            },
        )

        assert parent.user.variables.get('foobaz', None) == 'foobar'

        # with payload and metadata variable
        task.payload_variable = 'foobaz'
        task.metadata_variable = 'bazfoo'
        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': {'x-foo-bar': 'hello'}, 'payload': 'foobar'})

        assert task.request_from(parent) == ({'x-foo-bar': 'hello'}, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'RECEIVE',
                'worker': 'foo-bar',
                'context': state.context,
                'payload': None,
            },
        )

        assert parent.user.variables == SOME(dict, {'foobaz': 'foobar', 'bazfoo': jsondumps({'x-foo-bar': 'hello'})})

    def test_request_to(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        scenario = grizzly_fixture()

        service_bus_client_task = type('ServiceBusClientTask_001', (ServiceBusClientTask,), {'_context': {'variables': {}}, '__scenario__': scenario.user._scenario})

        client_mock = mocker.MagicMock()
        scenario.user._context = {'variables': {}}

        task = service_bus_client_task(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            source='hello world',
        )
        state = task.get_state(scenario)
        state.client = client_mock
        state.worker = 'foo-baz'

        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': None, 'payload': 'foobar'})

        # inline source, no file
        assert task.request_to(scenario) == (None, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'SEND',
                'worker': 'foo-baz',
                'context': state.context,
                'payload': 'hello world',
            },
        )

        request_mock.reset_mock()

        # inline source, template
        task.source = '{{ foobar }}'
        scenario.user.set_variable('foobar', 'hello world')

        assert task.request_to(scenario) == (None, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'SEND',
                'worker': 'foo-baz',
                'context': state.context,
                'payload': 'hello world',
            },
        )

        request_mock.reset_mock()
        del scenario.user.variables['foobar']

        # source file
        (grizzly_fixture.test_context / 'requests' / 'source.json').write_text('hello world')
        task.source = 'source.json'

        assert task.request_to(scenario) == (None, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'SEND',
                'worker': 'foo-baz',
                'context': state.context,
                'payload': 'hello world',
            },
        )

        request_mock.reset_mock()

        # source file, with template
        scenario.user.variables.update({'foobar': 'hello world', 'filename': 'source.j2.json'})
        (grizzly_fixture.test_context / 'requests' / 'source.j2.json').write_text('{{ foobar }}')
        task.source = '{{ filename }}'

        assert task.request_to(scenario) == (None, 'foobar')

        request_mock.assert_called_once_with(
            state.parent,
            {
                'action': 'SEND',
                'worker': 'foo-baz',
                'context': state.context,
                'payload': 'hello world',
            },
        )

        request_mock.reset_mock()
