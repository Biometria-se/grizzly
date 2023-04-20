import logging

from typing import Dict, Any
from json import dumps as jsondumps

import pytest

from _pytest.logging import LogCaptureFixture
from zmq.sugar.constants import LINGER as ZMQ_LINGER, REQ as ZMQ_REQ
from grizzly.tasks.clients import ServiceBusClientTask
from grizzly.types import RequestDirection
from grizzly_extras.async_message import AsyncMessageRequest

from tests.fixtures import GrizzlyFixture, NoopZmqFixture, MockerFixture


class TestServiceBusClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        context_mock = mocker.patch('grizzly.tasks.clients.servicebus.zmq.Context', autospec=True)

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
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_called_once_with(ZMQ_REQ)
        context_mock.return_value.socket.return_value.connect.assert_called_once_with('tcp://127.0.0.1:5554')
        context_mock.reset_mock()

        task = ServiceBusClientTask(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/queue:my-queue;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            source='hello world!'
        )

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'sender',
            'endpoint': 'queue:my-queue',
            'message_wait': None,
            'consume': False,
        }
        assert task.text is None
        assert task.source == 'hello world!'
        assert task.worker_id is None
        assert task.__template_attributes__ == {'endpoint', 'destination', 'source', 'name', 'variable_template', 'context'}
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_called_once_with(ZMQ_REQ)
        context_mock.return_value.socket.return_value.connect.assert_called_once_with('tcp://127.0.0.1:5554')
        context_mock.reset_mock()

        grizzly = grizzly_fixture.grizzly
        grizzly.state.configuration.update({'sbns.host': 'sb.windows.net', 'sbns.key.name': 'KeyName', 'sbns.key.secret': 'SeCrEtKeY=='})
        grizzly.state.variables.update({'foobar': 'none'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            (
                'sb://$conf::sbns.host$/topic:my-topic/subscription:my-subscription-{{ id }}/expression:$.hello.world'
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
            'endpoint': 'topic:my-topic, subscription:my-subscription-{{ id }}, expression:$.hello.world',
            'consume': True,
            'message_wait': 300,
            'content_type': 'JSON'
        }
        assert task.text == 'foobar'
        assert task.payload_variable == 'foobar'
        assert task.metadata_variable is None
        assert task.source is None
        assert task.worker_id is None
        assert sorted(task.get_templates()) == sorted(['topic:my-topic, subscription:my-subscription-{{ id }}, expression:$.hello.world', '{{ foobar }}'])
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_called_once_with(ZMQ_REQ)
        context_mock.return_value.socket.return_value.connect.assert_called_once_with('tcp://127.0.0.1:5554')
        context_mock.reset_mock()

        grizzly.state.variables.update({'barfoo': 'none'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            (
                'sb://$conf::sbns.host$/topic:my-topic/subscription:my-subscription-{{ id }}/expression:$.hello.world'
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
            'endpoint': 'topic:my-topic, subscription:my-subscription-{{ id }}, expression:$.hello.world',
            'consume': True,
            'message_wait': 300,
            'content_type': 'JSON'
        }
        assert task.text == 'foobar'
        assert task.payload_variable == 'foobar'
        assert task.metadata_variable == 'barfoo'
        assert task.source is None
        assert task.worker_id is None
        assert sorted(task.get_templates()) == sorted(['topic:my-topic, subscription:my-subscription-{{ id }}, expression:$.hello.world', '{{ foobar }} {{ barfoo }}'])
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_called_once_with(ZMQ_REQ)
        context_mock.return_value.socket.return_value.connect.assert_called_once_with('tcp://127.0.0.1:5554')
        context_mock.reset_mock()

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
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_called_once_with(ZMQ_REQ)
        context_mock.return_value.socket.return_value.connect.assert_called_once_with('tcp://127.0.0.1:5554')
        context_mock.reset_mock()

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=foo',
                'test',
            )
        assert str(ve.value) == 'MessageWait parameter in endpoint fragment is not a valid integer'
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_not_called()
        context_mock.reset_mock()

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Consume=foo',
                'test',
            )
        assert str(ve.value) == 'Consume parameter in endpoint fragment is not a valid boolean'
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_not_called()
        context_mock.reset_mock()

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#ContentType=foo',
                'test',
            )
        assert str(ve.value) == '"foo" is an unknown response content type'
        context_mock.assert_called_once_with()
        context_mock.return_value.socket.assert_not_called()
        context_mock.reset_mock()

    def test_text(self, grizzly_fixture: GrizzlyFixture) -> None:
        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.text is None

        task.text = 'hello'

        assert task.text == 'hello'

        task.text = '''
                hello
        '''

        assert task.text == 'hello'

    def test_connect(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        noop_zmq('grizzly.tasks.clients.servicebus')

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request')

        grizzly_fixture.grizzly.state.configuration.update({'sbns.key.secret': 'fooBARfoo'})

        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=$conf::sbns.key.secret$', 'test')

        task._parent = scenario

        # already connected
        task.worker_id = 'foo-bar'

        task.connect()

        async_message_request_mock.assert_not_called()

        # successfully connected
        task.worker_id = None
        async_message_request_mock.return_value = {'success': True, 'worker': 'foo-bar-baz-foo'}

        task.connect()

        assert task.worker_id == 'foo-bar-baz-foo'
        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=fooBARfoo'

        async_message_request_mock.assert_called_once_with(task.client, {
            'worker': None,
            'client': id(scenario.user),
            'action': 'HELLO',
            'context': task.context,
        })

    def test_disconnect(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request')

        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')
        task._parent = scenario

        # not connected, don't do anything
        task._client = None
        task.disconnect()

        client_mock.close.assert_not_called()

        # connected
        task._client = client_mock
        task.worker_id = 'foo-bar-baz-foo'

        task.disconnect()

        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz-foo',
            'client': id(task.parent.user),
            'action': 'DISCONNECT',
            'context': task.context,
        })
        client_mock.setsockopt.assert_called_once_with(ZMQ_LINGER, 0)
        client_mock.close.assert_called_once_with()

        assert getattr(task, 'worker_id', True) is None
        assert task._client is None

    def test_subscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value={'message': 'foobar!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription-{{ id }};SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        task._parent = scenario

        task._client = client_mock
        task.worker_id = 'foo-bar-baz'
        task._text = '1={{ condition }}'

        scenario.user._context['variables'].update({'id': 'baz-bar-foo', 'condition': '2'})
        expected_context = task.context.copy()
        expected_context['endpoint'] = expected_context['endpoint'].replace('{{ id }}', 'baz-bar-foo')

        with caplog.at_level(logging.INFO):
            task.subscribe()

        assert caplog.messages == ['foobar!']
        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz',
            'client': id(scenario.user),
            'action': 'SUBSCRIBE',
            'context': expected_context,
            'payload': '1=2'
        })

    def test_unsubscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value={'message': 'hello world!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        task._parent = scenario

        task._client = client_mock
        task.worker_id = 'foo-bar-baz'

        with caplog.at_level(logging.INFO):
            task.unsubscribe()

        assert caplog.messages == ['hello world!']

        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz',
            'client': id(task.parent.user),
            'action': 'UNSUBSCRIBE',
            'context': task.context,
        })

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        connect_mock = mocker.patch.object(task, 'connect', return_value=None)
        subscribe_mock = mocker.patch.object(task, 'subscribe', return_value=None)

        # no text
        assert task._text is None

        task.on_start(scenario)

        connect_mock.assert_called_once_with()
        subscribe_mock.assert_not_called()
        assert task.context.get('endpoint', None) == 'topic:my-topic, subscription:my-subscription'

        connect_mock.reset_mock()
        subscribe_mock.reset_mock()

        # text
        task._text = '1=1'

        task.on_start(scenario)

        connect_mock.assert_called_once_with()
        subscribe_mock.assert_called_once_with()
        assert task.context.get('endpoint', None) == f'topic:my-topic, subscription:my-subscription_{id(scenario)}'

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        disconnect_mock = mocker.patch.object(task, 'disconnect', return_value=None)
        unsubscribe_mock = mocker.patch.object(task, 'unsubscribe', return_value=None)

        # no text
        assert task._text is None

        task.on_stop(scenario)

        disconnect_mock.assert_called_once_with()
        unsubscribe_mock.assert_not_called()

        disconnect_mock.reset_mock()
        unsubscribe_mock.reset_mock()

        # text
        task._text = '1=1'

        task.on_stop(scenario)

        disconnect_mock.assert_called_once_with()
        unsubscribe_mock.assert_called_once_with()

    def test_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value={'message': 'foobar!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        task._client = client_mock
        task._parent = scenario

        action_mock = mocker.MagicMock()
        meta_mock: Dict[str, Any] = {}
        action_mock.__enter__.return_value = meta_mock
        context_mock = mocker.patch.object(task, 'action', return_value=action_mock)

        # got response, empty payload
        request: AsyncMessageRequest = {
            'context': task.context,
        }

        assert task.request(task.parent, request) == {'message': 'foobar!'}

        context_mock.assert_called_once_with(task.parent)
        request.update({'client': id(task.parent.user)})
        async_message_request_mock.assert_called_once_with(client_mock, request)
        del request['client']

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
        del request['context']['url']
        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request_wrapper', return_value={'message': 'foobar!', 'payload': '1234567890'})

        assert task.request(task.parent, request) == {'message': 'foobar!', 'payload': '1234567890'}

        context_mock.assert_called_once_with(task.parent)
        async_message_request_mock.assert_called_once_with(task.parent, client_mock, request)
        assert meta_mock == {
            'action': 'topic:my-topic, subscription:my-subscription',
            'request': request,
            'response_length': 10,
            'response': {'message': 'foobar!', 'payload': '1234567890'}
        }

    def test_get(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        client_mock = mocker.MagicMock()
        scenario.user._context = {'variables': {}}

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        task._client = client_mock
        task._parent = scenario
        task.worker_id = 'foo-bar'

        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': None, 'payload': 'foobar'})

        # no variables
        task.payload_variable = None

        assert task.get(task.parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'RECEIVE',
            'worker': 'foo-bar',
            'context': task.context,
            'payload': None,
        })

        assert task.parent.user._context['variables'] == {}

        request_mock.reset_mock()

        # with payload variable
        task.payload_variable = 'foobaz'

        assert task.get(task.parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'RECEIVE',
            'worker': 'foo-bar',
            'context': task.context,
            'payload': None,
        })

        assert task.parent.user._context['variables'] == {'foobaz': 'foobar'}

        # with payload and metadata variable
        task.payload_variable = 'foobaz'
        task.metadata_variable = 'bazfoo'
        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': {'x-foo-bar': 'hello'}, 'payload': 'foobar'})

        assert task.get(task.parent) == ({'x-foo-bar': 'hello'}, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'RECEIVE',
            'worker': 'foo-bar',
            'context': task.context,
            'payload': None,
        })

        assert task.parent.user._context['variables'] == {'foobaz': 'foobar', 'bazfoo': jsondumps({'x-foo-bar': 'hello'})}

    def test_put(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        client_mock = mocker.MagicMock()
        scenario.user._context = {'variables': {}}

        task = ServiceBusClientTask(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            source='hello world',
        )
        task._client = client_mock
        task._parent = scenario
        task.worker_id = 'foo-baz'

        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': None, 'payload': 'foobar'})

        # inline source, no file
        assert task.put(task.parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': task.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()

        # inline source, template
        task.source = '{{ foobar }}'
        scenario.user._context['variables'].update({'foobar': 'hello world'})

        assert task.put(task.parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': task.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()
        del scenario.user._context['variables']['foobar']

        # source file
        (grizzly_fixture.test_context / 'source.json').write_text('hello world')
        task.source = 'source.json'

        assert task.put(task.parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': task.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()

        # source file, with template
        scenario.user._context['variables'].update({'foobar': 'hello world', 'filename': 'source.j2.json'})
        (grizzly_fixture.test_context / 'source.j2.json').write_text('{{ foobar }}')
        task.source = '{{ filename }}'

        assert task.put(task.parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(task.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': task.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()

    def test_parent(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        client_mock = mocker.MagicMock()

        task = ServiceBusClientTask(
            RequestDirection.TO,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
            source='hello world',
        )

        task._client = client_mock

        with pytest.raises(AttributeError) as ae:
            _ = task.parent
        assert str(ae.value) == 'no parent set'

        task.parent = mocker.MagicMock()

        _ = task.parent

        with pytest.raises(AttributeError) as ae:
            task.parent = scenario
        assert str(ae.value) == 'parent already set, why are a different parent being set?'
