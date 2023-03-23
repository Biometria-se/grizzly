import pytest

from zmq.sugar.constants import LINGER as ZMQ_LINGER
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
            'context': task.context,
        })
        client_mock.setsockopt.assert_called_once_with(ZMQ_LINGER, 0)
        client_mock.close.assert_called_once_with()

        assert getattr(task, 'worker_id', True) is None
        assert task._client is None
