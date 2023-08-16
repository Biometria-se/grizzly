import logging

from typing import Dict, Any
from json import dumps as jsondumps

import pytest
import zmq.green as zmq

from _pytest.logging import LogCaptureFixture
from grizzly.tasks.clients import ServiceBusClientTask
from grizzly.types import RequestDirection
from grizzly_extras.async_message import AsyncMessageRequest

from tests.fixtures import GrizzlyFixture, NoopZmqFixture, MockerFixture


class TestServiceBusClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        context_mock = mocker.patch('grizzly.tasks.clients.servicebus.zmq.Context', autospec=True)

        ServiceBusClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.endpoint == 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324='
        assert task.context == {
            'url': task.endpoint,
            'connection': 'receiver',
            'endpoint': '',
            'message_wait': None,
            'consume': False,
        }
        assert task._state == {}
        assert task.text is None
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
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
        assert task._state == {}
        assert task.__template_attributes__ == {'endpoint', 'destination', 'source', 'name', 'variable_template', 'context'}
        assert task.get_templates() == []
        context_mock.assert_called_once_with()
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
        assert task._state == {}
        assert sorted(task.get_templates()) == sorted(['topic:my-topic, subscription:my-subscription-{{ id }}, expression:$.hello.world', '{{ foobar }}'])
        context_mock.assert_called_once_with()
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
        assert task._state == {}
        assert sorted(task.get_templates()) == sorted(['topic:my-topic, subscription:my-subscription-{{ id }}, expression:$.hello.world', '{{ foobar }} {{ barfoo }}'])
        context_mock.assert_called_once_with()
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
        assert task._state == {}
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#MessageWait=foo',
                'test',
            )
        assert str(ve.value) == 'MessageWait parameter in endpoint fragment is not a valid integer'
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#Consume=foo',
                'test',
            )
        assert str(ve.value) == 'Consume parameter in endpoint fragment is not a valid boolean'
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

        with pytest.raises(ValueError) as ve:
            ServiceBusClientTask(
                RequestDirection.FROM,
                'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=#ContentType=foo',
                'test',
            )
        assert str(ve.value) == '"foo" is an unknown response content type'
        context_mock.assert_called_once_with()
        context_mock.reset_mock()

    def test_text(self, grizzly_fixture: GrizzlyFixture) -> None:
        ServiceBusClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        task = ServiceBusClientTask(RequestDirection.FROM, 'sb://my-sbns.servicebus.windows.net/;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=', 'test')

        assert task.text is None

        task.text = 'hello'

        assert task.text == 'hello'

        task.text = '''
                hello
        '''

        assert task.text == 'hello'

    def test_connect(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        noop_zmq('grizzly.tasks.clients.servicebus')

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request')

        grizzly_fixture.grizzly.state.configuration.update({'sbns.key.secret': 'fooBARfoo'})

        ServiceBusClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        task = ServiceBusClientTask(
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

        async_message_request_mock.assert_called_once_with(state.client, {
            'worker': None,
            'client': state.parent_id,
            'action': 'HELLO',
            'context': state.context,
        })

    def test_disconnect(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request')

        ServiceBusClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        task = ServiceBusClientTask(
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

        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz-foo',
            'client': state.parent_id,
            'action': 'DISCONNECT',
            'context': state.context,
        })
        client_mock.setsockopt.assert_called_once_with(zmq.LINGER, 0)
        client_mock.close.assert_called_once_with()

        assert task._state == {}

    def test_subscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value={'message': 'foobar!'})

        ServiceBusClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:"my-subscription-{{ id }}";SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        state = task.get_state(parent)

        state.worker = 'foo-bar-baz'
        state.client = client_mock
        task._text = '1={{ condition }}'

        parent.user._context['variables'].update({'id': 'baz-bar-foo', 'condition': '2'})
        expected_context = state.context.copy()
        expected_context['endpoint'] = expected_context['endpoint'].replace('{{ id }}', 'baz-bar-foo')

        with caplog.at_level(logging.INFO):
            task.subscribe(parent)

        assert caplog.messages == ['foobar!']
        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz',
            'client': state.parent_id,
            'action': 'SUBSCRIBE',
            'context': expected_context,
            'payload': '1=2'
        })

    def test_unsubscribe(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value={'message': 'hello world!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        state = task.get_state(parent)
        state.client = client_mock
        state.worker = 'foo-bar-baz'

        with caplog.at_level(logging.INFO):
            task.unsubscribe(parent)

        assert caplog.messages == ['hello world!']

        async_message_request_mock.assert_called_once_with(client_mock, {
            'worker': 'foo-bar-baz',
            'client': id(parent.user),
            'action': 'UNSUBSCRIBE',
            'context': state.context,
        })

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )

        connect_mock = mocker.patch.object(task, 'connect', return_value=None)
        subscribe_mock = mocker.patch.object(task, 'subscribe', return_value=None)

        state = task.get_state(parent)

        # no text
        assert task._text is None

        task.on_start(parent)

        connect_mock.assert_called_once_with(parent)
        subscribe_mock.assert_not_called()
        assert task.context.get('endpoint', None) == 'topic:my-topic, subscription:my-subscription'

        connect_mock.reset_mock()
        subscribe_mock.reset_mock()

        # text
        task._text = '1=1'

        task.on_start(parent)

        connect_mock.assert_called_once_with(parent)
        subscribe_mock.assert_called_once_with(parent)
        assert state.context.get('endpoint', None) == f'topic:my-topic, subscription:my-subscription_{id(parent.user)}'

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        task = ServiceBusClientTask(
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

    def test_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()

        async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value={'message': 'foobar!'})

        task = ServiceBusClientTask(
            RequestDirection.FROM,
            'sb://my-sbns.servicebus.windows.net/topic:my-topic/subscription:my-subscription;SharedAccessKeyName=AccessKey;SharedAccessKey=37aabb777f454324=',
            'test',
        )
        state = task.get_state(parent)
        state.client = client_mock
        action_mock = mocker.MagicMock()
        meta_mock: Dict[str, Any] = {}
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
            'response': {'message': 'foobar!'}
        }

        context_mock.reset_mock()
        async_message_request_mock.reset_mock()
        meta_mock.clear()

        # got response, some payload
        del request['context']['url']
        async_message_request_mock = mocker.patch('grizzly.tasks.clients.servicebus.async_message_request_wrapper', return_value={'message': 'foobar!', 'payload': '1234567890'})

        assert task.request(parent, request) == {'message': 'foobar!', 'payload': '1234567890'}

        context_mock.assert_called_once_with(state.parent)
        async_message_request_mock.assert_called_once_with(state.parent, state.client, request)
        assert meta_mock == {
            'action': f'topic:my-topic, subscription:my-subscription_{state.parent_id}',
            'request': request,
            'response_length': 10,
            'response': {'message': 'foobar!', 'payload': '1234567890'}
        }

    def test_get(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        client_mock = mocker.MagicMock()
        parent.user._context = {'variables': {}}

        task = ServiceBusClientTask(
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

        assert task.get(parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'RECEIVE',
            'worker': 'foo-bar',
            'context': state.context,
            'payload': None,
        })

        assert parent.user._context['variables'] == {}

        request_mock.reset_mock()

        # with payload variable
        task.payload_variable = 'foobaz'

        assert task.get(parent) == (None, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'RECEIVE',
            'worker': 'foo-bar',
            'context': state.context,
            'payload': None,
        })

        assert parent.user._context['variables'] == {'foobaz': 'foobar'}

        # with payload and metadata variable
        task.payload_variable = 'foobaz'
        task.metadata_variable = 'bazfoo'
        request_mock = mocker.patch.object(task, 'request', return_value={'metadata': {'x-foo-bar': 'hello'}, 'payload': 'foobar'})

        assert task.get(parent) == ({'x-foo-bar': 'hello'}, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'RECEIVE',
            'worker': 'foo-bar',
            'context': state.context,
            'payload': None,
        })

        assert parent.user._context['variables'] == {'foobaz': 'foobar', 'bazfoo': jsondumps({'x-foo-bar': 'hello'})}

    def test_put(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        scenario = grizzly_fixture()

        client_mock = mocker.MagicMock()
        scenario.user._context = {'variables': {}}

        task = ServiceBusClientTask(
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
        assert task.put(scenario) == (None, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': state.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()

        # inline source, template
        task.source = '{{ foobar }}'
        scenario.user._context['variables'].update({'foobar': 'hello world'})

        assert task.put(scenario) == (None, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': state.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()
        del scenario.user._context['variables']['foobar']

        # source file
        (grizzly_fixture.test_context / 'source.json').write_text('hello world')
        task.source = 'source.json'

        assert task.put(scenario) == (None, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': state.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()

        # source file, with template
        scenario.user._context['variables'].update({'foobar': 'hello world', 'filename': 'source.j2.json'})
        (grizzly_fixture.test_context / 'source.j2.json').write_text('{{ foobar }}')
        task.source = '{{ filename }}'

        assert task.put(scenario) == (None, 'foobar',)

        request_mock.assert_called_once_with(state.parent, {
            'action': 'SEND',
            'worker': 'foo-baz',
            'context': state.context,
            'payload': 'hello world',
        })

        request_mock.reset_mock()
