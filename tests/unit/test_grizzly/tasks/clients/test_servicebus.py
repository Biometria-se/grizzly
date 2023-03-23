import logging

from typing import Dict, Any
import pytest

from _pytest.logging import LogCaptureFixture
from zmq.sugar.constants import LINGER as ZMQ_LINGER
from grizzly.tasks.clients import ServiceBusClientTask
from grizzly.types import RequestDirection
from grizzly_extras.async_message import AsyncMessageRequest

from tests.fixtures import GrizzlyFixture, NoopZmqFixture, MockerFixture


class TestServiceBusClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': '',
            'message_wait': None,
            'consume': False,
        }
        assert task.worker_id is None
        assert task.text is None

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/queue:my-queue;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            text='foobar',
        )

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': 'queue:my-queue',
            'message_wait': None,
            'consume': False,
        }
        assert task.worker_id is None
        assert task.text == 'foobar'

        task = ServiceBusClientTask(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/queue:my-queue;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454+24=#Consume=True;MessageWait=300',
            'test',
            source='hello world!'
        )

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454+24='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'sender',
            'endpoint': 'queue:my-queue',
            'consume': True,
            'message_wait': 300,
        }
        assert task.worker_id is None

        task = ServiceBusClientTask(
            RequestDirection.FROM, (
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription'
                ';SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=120&ContentType=json'
            ),
            'test',
        )

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': 'topic:my-topic, subscription:my-subscription',
            'message_wait': 120,
            'consume': False,
            'content_type': 'JSON',
        }
        assert task.worker_id is None

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=foo',
                'test',
            )
        assert str(ve.value) == 'MessageWait parameter in endpoint fragment is not a valid integer'

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Consume=foo',
                'test',
            )
        assert str(ve.value) == 'Consume parameter in endpoint fragment is not a valid boolean'

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#ContentType=foo',
                'test',
            )
        assert str(ve.value) == '"foo" is an unknown response content type'

    def test_text(self, grizzly_fixture: GrizzlyFixture) -> None:
        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.text is None

        task.text = 'hello'

        assert task.text == 'hello'

    def test_connect(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:
        noop_zmq('grizzly.tasks.clients.servicebus')

        zmq_connect_mock = noop_zmq.get_mock('zmq.Socket.connect')
        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request')

        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        # already connected
        setattr(task, '_client', True)

        task.connect()

        zmq_connect_mock.assert_not_called()

        # successfully connected
        setattr(task, '_client', None)
        async_message_request_mock.return_value = {'success': True, 'worker': 'foo-bar-baz-foo'}

        task.connect()

        assert task.worker_id == 'foo-bar-baz-foo'

        zmq_connect_mock.assert_called_once_with('tcp://127.0.0.1:5554')
        async_message_request_mock.assert_called_once_with(task.client, {
            'worker': None,
            'action': 'HELLO',
            'context': task.context,
        })

    def test_disconnect(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request')

        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        # not connected
        with pytest.raises(ConnectionError) as ce:
            task.disconnect()
        assert str(ce.value) == 'not connected'

        client_mock.close.assert_not_called()

        # connected
        task._client = client_mock
        task.worker_id = 'foo-bar-baz-foo'

        print(task._client)

        task.disconnect()

        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz-foo',
            'action': 'DISCONNECT',
            'context': {
                'endpoint': task.context['endpoint'],
            },
        })
        client_mock.setsockopt.assert_called_once_with(ZMQ_LINGER, 0)
        client_mock.close.assert_called_once_with()

        assert getattr(task, 'worker_id', True) is None
        assert task._client is None

    def test_subscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request', return_value={'message': 'foobar!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        task._client = client_mock
        task.worker_id = 'foo-bar-baz'
        task._text = '1=1'

        with caplog.at_level(logging.INFO):
            task.subscribe()

        assert caplog.messages == ['foobar!']
        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz',
            'action': 'SUBSCRIBE',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': '1=1'
        })

    def test_unsubscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request', return_value={'message': 'hello world!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        task._client = client_mock
        task.worker_id = 'foo-bar-baz'

        with caplog.at_level(logging.INFO):
            task.unsubscribe()

        assert caplog.messages == ['hello world!']

        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz',
            'action': 'UNSUBSCRIBE',
            'context': {
                'endpoint': task.context['endpoint'],
            },
        })

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        connect_mock = mocker.patch.object(task, 'connect', return_value=None)
        subscribe_mock = mocker.patch.object(task, 'subscribe', return_value=None)

        # no text
        assert task._text is None

        task.on_start()

        connect_mock.assert_called_once_with()
        subscribe_mock.assert_not_called()

        connect_mock.reset_mock()
        subscribe_mock.reset_mock()

        # text
        task._text = '1=1'

        task.on_start()

        connect_mock.assert_called_once_with()
        subscribe_mock.assert_called_once_with()

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        disconnect_mock = mocker.patch.object(task, 'disconnect', return_value=None)
        unsubscribe_mock = mocker.patch.object(task, 'unsubscribe', return_value=None)

        # no text
        assert task._text is None

        task.on_stop()

        disconnect_mock.assert_called_once_with()
        unsubscribe_mock.assert_not_called()

        disconnect_mock.reset_mock()
        unsubscribe_mock.reset_mock()

        # text
        task._text = '1=1'

        task.on_stop()

        disconnect_mock.assert_called_once_with()
        unsubscribe_mock.assert_called_once_with()

    def test_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request', return_value={'message': 'foobar!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        task._client = client_mock

        parent_mock = mocker.MagicMock()
        action_mock = mocker.MagicMock()
        meta_mock: Dict[str, Any] = {}
        action_mock.__enter__.return_value = meta_mock
        context_mock = mocker.patch.object(task, 'action', return_value=action_mock)

        # got response, empty payload
        request: AsyncMessageRequest = {}

        assert task.request(parent_mock, request) == {'message': 'foobar!'}

        context_mock.assert_called_once_with(parent_mock)
        async_message_request_mock.assert_called_once_with(client_mock, request)
        assert meta_mock == {
            'action': 'topic:my-topic, subscription:my-subscription',
            'request': request,
            'response_length': 0,
            'response': {'message': 'foobar!'}
        }

        context_mock.reset_mock()
        async_message_request_mock.reset_mock()
        meta_mock.clear()

        # got response, some payload
        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request', return_value={'message': 'foobar!', 'payload': '1234567890'})

        assert task.request(parent_mock, request) == {'message': 'foobar!', 'payload': '1234567890'}

        context_mock.assert_called_once_with(parent_mock)
        async_message_request_mock.assert_called_once_with(client_mock, request)
        assert meta_mock == {
            'action': 'topic:my-topic, subscription:my-subscription',
            'request': request,
            'response_length': 10,
            'response': {'message': 'foobar!', 'payload': '1234567890'}
        }

    def test_get(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        client_mock = mocker.MagicMock()
        parent_mock = mocker.MagicMock()
        parent_mock.user._context = {'variables': {}}

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        task._client = client_mock
        task.worker_id = 'foo-bar'

        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': None, 'payload': 'foobar'})

        # no variable
        task.variable = None

        assert task.get(parent_mock) == (None, 'foobar',)

        request_mock.assert_called_once_with(parent_mock, {
            'action': 'GET',
            'worker': 'foo-bar',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': None,
        })

        assert parent_mock.user._context['variables'] == {}

        request_mock.reset_mock()

        # with variable
        task.variable = 'foobaz'

        assert task.get(parent_mock) == (None, 'foobar',)

        request_mock.assert_called_once_with(parent_mock, {
            'action': 'GET',
            'worker': 'foo-bar',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': None,
        })

        assert parent_mock.user._context['variables'] == {'foobaz': 'foobar'}

    def test_put(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        client_mock = mocker.MagicMock()
        parent_mock = mocker.MagicMock()
        scenario.user._context = {'variables': {}}
        parent_mock.render = scenario.render

        task = ServiceBusClientTask(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            source='hello world',
        )
        task._client = client_mock
        task.worker_id = 'foo-baz'

        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': None, 'payload': 'foobar'})

        # inline source, no file
        assert task.put(parent_mock) == (None, 'foobar',)

        request_mock.assert_called_once_with(parent_mock, {
            'action': 'PUT',
            'worker': 'foo-baz',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': 'hello world',
        })

        request_mock.reset_mock()

        # inline source, template
        task.source = '{{ foobar }}'
        scenario.user._context['variables'].update({'foobar': 'hello world'})

        assert task.put(parent_mock) == (None, 'foobar',)

        request_mock.assert_called_once_with(parent_mock, {
            'action': 'PUT',
            'worker': 'foo-baz',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': 'hello world',
        })

        request_mock.reset_mock()
        del scenario.user._context['variables']['foobar']

        # source file
        (grizzly_fixture.test_context / 'source.json').write_text('hello world')
        task.source = 'source.json'

        assert task.put(parent_mock) == (None, 'foobar',)

        request_mock.assert_called_once_with(parent_mock, {
            'action': 'PUT',
            'worker': 'foo-baz',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': 'hello world',
        })

        request_mock.reset_mock()

        # source file, with template
        scenario.user._context['variables'].update({'foobar': 'hello world', 'filename': 'source.j2.json'})
        (grizzly_fixture.test_context / 'source.j2.json').write_text('{{ foobar }}')
        task.source = '{{ filename }}'

        assert task.put(parent_mock) == (None, 'foobar',)

        request_mock.assert_called_once_with(parent_mock, {
            'action': 'PUT',
            'worker': 'foo-baz',
            'context': {
                'endpoint': task.context['endpoint'],
            },
            'payload': 'hello world',
        })

        request_mock.reset_mock()
