import pytest

from zmq.sugar.constants import REQ as ZMQ_REQ, LINGER as ZMQ_LINGER
from grizzly.tasks.clients import ServiceBusClientTask
from grizzly.types import RequestDirection

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
        setattr(task, 'client', True)

        task.connect()

        zmq_connect_mock.assert_not_called()

        setattr(task, 'client', None)

        # successfully connected
        setattr(task, 'client', None)
        async_message_request_mock.return_value = {'success': True, 'worker': 'foo-bar-baz-foo'}

        task.connect()

        assert task.worker_id == 'foo-bar-baz-foo'

        zmq_connect_mock.assert_called_once_with('tcp://127.0.0.1:5554')
        async_message_request_mock.assert_called_once_with(task.client, {
            'worker': None,
            'action': 'HELLO',
            'context': task.context,
        })

    def test_disconnect(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:
        noop_zmq('grizzly.tasks.clients.servicebus')

        zmq_close_mock = noop_zmq.get_mock('zmq.Socket.close')
        zmq_setsockopt_mock = noop_zmq.get_mock('zmq.Socket.setsockopt')
        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request')

        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        # not connected
        task.disconnect()

        zmq_close_mock.assert_not_called()

        # connected
        task.client = task._zmq_context.socket(ZMQ_REQ)
        task.worker_id = 'foo-bar-baz-foo'

        task.disconnect()

        async_message_request_mock.assert_called_once_with(task.client, {
            'worker': 'foo-bar-baz-foo',
            'action': 'DISCONNECT',
            'context': task.context,
        })
        zmq_setsockopt_mock.assert_called_once_with(ZMQ_LINGER, 0)
        zmq_close_mock.assert_called_once_with()

        assert task.worker_id is None
