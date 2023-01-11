from typing import Dict, Any, Optional, cast

import pytest
from zmq.sugar.constants import REQ as ZMQ_REQ
from zmq.error import ZMQError, Again as ZMQAgain
import zmq.green as zmq

from pytest_mock import MockerFixture

from grizzly.testdata.variables.servicebus import AtomicServiceBus, atomicservicebus_url, atomicservicebus_endpoint, atomicservicebus__base_type__
from grizzly.context import GrizzlyContext
from grizzly_extras.async_message import AsyncMessageResponse
from grizzly_extras.transformer import TransformerContentType

from ....fixtures import NoopZmqFixture


def test_atomicservicebus__base_type() -> None:
    with pytest.raises(ValueError) as ve:
        atomicservicebus__base_type__('documents-in')
    assert 'AtomicServiceBus: initial value must contain arguments' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicservicebus__base_type__('|')
    assert 'AtomicServiceBus: incorrect format in arguments: ""' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicservicebus__base_type__('| url=""')
    assert 'AtomicServiceBus: endpoint name is not valid: ""' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicservicebus__base_type__('queue:documents-in | argument=False')
    assert 'AtomicServiceBus: url parameter must be specified' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicservicebus__base_type__(
            'queue:documents-in | url="sb://sb.example.com/;SharedAccessKeyName=name;SharedAccessKey=asdf-asdf-asdf=", argument=False',
        )
    assert 'AtomicServiceBus: argument argument is not allowed' in str(ve)

    safe_value = atomicservicebus__base_type__(
        'queue:documents-in| url="sb://sb.example.com/;SharedAccessKeyName=name;SharedAccessKey=asdf-asdf-asdf="',
    )
    assert safe_value == 'queue:documents-in | url="sb://sb.example.com/;SharedAccessKeyName=name;SharedAccessKey=asdf-asdf-asdf="'


def test_atomicservicebus_url() -> None:
    url = 'sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm'
    assert atomicservicebus_url(url) == url

    url = 'Endpoint=sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm'
    assert atomicservicebus_url(url) == url

    url = 'mq://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_url(url)
    assert 'AtomicServiceBus: "mq" is not a supported scheme' in str(ve)

    url = 'sb://sb.example.com/'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_url(url)
    assert 'AtomicServiceBus: SharedAccessKeyName and SharedAccessKey must be in the query string' in str(ve)

    url = 'sb://sb.example.com/;SharedAccessKeyName=authorization-key'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_url(url)
    assert 'AtomicServiceBus: SharedAccessKey must be in the query string' in str(ve)

    url = 'sb://sb.example.com/;SharedAccessKey=c2VjcmV0LXN0dWZm'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_url(url)
    assert 'AtomicServiceBus: SharedAccessKeyName must be in the query string' in str(ve)

    url = '$conf::sb.url$'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_url(url)
    assert 'AtomicServiceBus: configuration variable "sb.url" is not set' in str(ve)

    try:
        grizzly = GrizzlyContext()
        grizzly.state.configuration['sb.url'] = 'Endpoint=sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm'

        assert atomicservicebus_url(url) == grizzly.state.configuration['sb.url']
    finally:
        try:
            GrizzlyContext.destroy()
        except:
            pass


def test_atomicservicebus_endpoint() -> None:
    endpoint = 'documents-in'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: documents-in does not specify queue: or topic:' in str(ve)

    endpoint = 'asdf:document-in'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: only support endpoint types queue and topic, not asdf'

    endpoint = 'queue:document-in'
    assert atomicservicebus_endpoint(endpoint) == endpoint

    endpoint = 'topic:document-in'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: endpoint needs to include subscription when receiving messages from a topic' in str(ve)

    endpoint = 'topic:document-in, queue:document-in'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: cannot specify both topic: and queue: in endpoint' in str(ve)

    endpoint = 'topic:document-in, asdf:subscription'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: arguments asdf is not supported' in str(ve)

    endpoint = 'topic:document-in, subscription:'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: invalid value for argument "subscription"' in str(ve)

    endpoint = 'queue:document-in, subscription:application-x'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: argument subscription is only allowed if endpoint is a topic' in str(ve)

    endpoint = 'queue:"{{ queue_name }}"'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: value contained variable "queue_name" which has not been set' in str(ve)

    endpoint = 'queue:"$conf::sb.endpoint.queue$"'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: configuration variable "sb.endpoint.queue" is not set' in str(ve)

    endpoint = 'topic:documents-in, subscription:"$conf::sb.endpoint.subscription$"'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: configuration variable "sb.endpoint.subscription" is not set' in str(ve)

    endpoint = 'topic:documents-in, subscription:application-x, expression:"{{ expression }}"'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: value contained variable "expression" which has not been set' in str(ve)

    try:
        grizzly = GrizzlyContext()

        endpoint = 'queue:"{{ queue_name }}"'
        grizzly.state.variables['queue_name'] = 'test-queue'
        assert atomicservicebus_endpoint(endpoint) == 'queue:test-queue'

        endpoint = 'queue:"$conf::sb.endpoint.queue$"'
        grizzly.state.configuration['sb.endpoint.queue'] = 'test-queue'
        assert atomicservicebus_endpoint(endpoint) == 'queue:test-queue'

        grizzly.state.configuration['sb.endpoint.subscription'] = 'test-subscription'
        grizzly.state.configuration['sb.endpoint.topic'] = 'test-topic'

        endpoint = 'topic:"$conf::sb.endpoint.topic$",subscription:"$conf::sb.endpoint.subscription$"'
        assert atomicservicebus_endpoint(endpoint) == 'topic:test-topic, subscription:test-subscription'

        endpoint = 'topic:"$conf::sb.endpoint.topic$",subscription:"$conf::sb.endpoint.subscription$",expression:"{{ queue_name }}"'
        assert atomicservicebus_endpoint(endpoint) == 'topic:test-topic, subscription:test-subscription, expression:test-queue'

    finally:
        try:
            GrizzlyContext.destroy()
        except:
            pass

    endpoint = 'queue:"$env::QUEUE_NAME$"'
    with pytest.raises(ValueError) as ve:
        atomicservicebus_endpoint(endpoint)
    assert 'AtomicServiceBus: environment variable "QUEUE_NAME" is not set' in str(ve)

    endpoint = 'topic:document-in, subscription:application-x'
    assert atomicservicebus_endpoint(endpoint) == endpoint


class TestAtomicServiceBus:
    def test___init__(self, mocker: MockerFixture) -> None:
        mocker.patch(
            'grizzly.testdata.variables.servicebus.AtomicServiceBus.create_client',
            return_value={'client': True},
        )

        try:
            v = AtomicServiceBus(
                'test1',
                'queue:documents-in | url="Endpoint=sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key"',
            )

            assert v._initialized
            assert 'test1' in v._values
            assert v._values.get('test1', None) == 'queue:documents-in'
            assert v._settings.get('test1', None) == {
                'repeat': False,
                'wait': None,
                'endpoint_name': 'queue:documents-in',
                'url': 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'context': None,
                'worker': None,
                'content_type': None,
                'consume': False,
            }
            assert v._endpoint_clients.get('test1', None) is not None
            assert v._zmq_context.__class__.__name__ == '_Context'
            assert v._zmq_context.__class__.__module__ == 'zmq.green.core'

            t = AtomicServiceBus(
                'test2',
                (
                    'topic:documents-in, subscription:application-x | '
                    'url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, content_type=json, consume=True'
                ),
            )

            assert v is t
            assert len(v._values.keys()) == 2
            assert len(v._endpoint_messages.keys()) == 2
            assert len(v._settings.keys()) == 2
            assert len(v._endpoint_clients.keys()) == 2
            assert 'test2' in v._values
            assert v._values.get('test2', None) == 'topic:documents-in, subscription:application-x'
            assert v._endpoint_messages.get('test2', None) == []
            assert v._settings.get('test2', None) == {
                'repeat': False,
                'wait': 15,
                'endpoint_name': 'topic:documents-in, subscription:application-x',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'context': None,
                'worker': None,
                'content_type': TransformerContentType.JSON,
                'consume': True,
            }
            assert v._endpoint_clients.get('test2', None) is not None

            with pytest.raises(ValueError) as ve:
                t = AtomicServiceBus(
                    'test3',
                    (
                        'topic:documents-in, subscription:application-x, expression:"$.document[?(@.id==10)]" | '
                        'url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", wait=15'
                    ),
                )
            assert 'AtomicServiceBus.test3: argument "content_type" is mandatory when "expression" is used in endpoint' in str(ve)

            t = AtomicServiceBus(
                'test4',
                (
                    'topic:documents-in, subscription:application-x, expression:"$.document[name = "tpm report"]" | '
                    'url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", wait=15, content_type=json, consume=False'
                ),
            )
            assert t is v
            assert len(t._values.keys()) == 3
            assert len(t._endpoint_messages.keys()) == 3
            assert len(t._settings.keys()) == 3
            assert len(t._endpoint_clients.keys()) == 3
            assert 'test4' in t._values
            assert t._values.get('test4', None) == 'topic:documents-in, subscription:application-x, expression:"$.document[name = "tpm report"]"'
            assert t._endpoint_messages.get('test4', None) == []
            assert t._settings.get('test4', None) == {
                'repeat': False,
                'wait': 15,
                'endpoint_name': 'topic:documents-in, subscription:application-x, expression:"$.document[name = "tpm report"]"',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'context': None,
                'worker': None,
                'content_type': TransformerContentType.JSON,
                'consume': False,
            }
            assert t._endpoint_clients.get('test4', None) is not None

        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

    def test_create_context(self) -> None:
        settings: Dict[str, Any]

        try:
            settings = {
                'url': 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'endpoint_name': 'queue:documents-in',
            }
            context = AtomicServiceBus.create_context(settings)
            assert context == {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'endpoint': 'queue:documents-in',
                'connection': 'receiver',
                'message_wait': None,
                'consume': False,
            }

            settings = {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'endpoint_name': 'topic:documents-in, subscription:application-x',
                'wait': 120,
                'content_type': TransformerContentType.JSON,
                'consume': True,
            }

            context = AtomicServiceBus.create_context(settings)
            assert context == {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'endpoint': 'topic:documents-in, subscription:application-x',
                'connection': 'receiver',
                'message_wait': 120,
                'content_type': 'json',
                'consume': True,
            }
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

    def test_create_client(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        try:
            say_hello_spy = mocker.patch(
                'grizzly.testdata.variables.servicebus.AtomicServiceBus.say_hello',
                side_effect=[None] * 2,
            )

            v = AtomicServiceBus(
                'test',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            endpoint_client = v._endpoint_clients.get('test', None)
            assert endpoint_client is not None
            assert endpoint_client.__class__.__name__ == '_Socket'
            assert endpoint_client.__class__.__module__ == 'zmq.green.core'
            assert v._settings.get('test', None) == {
                'repeat': True,
                'wait': 15,
                'worker': None,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'endpoint_name': 'topic:documents-in, subscription:application-x',
                'content_type': None,
                'consume': False,
                'context': {
                    'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                    'endpoint': 'topic:documents-in, subscription:application-x',
                    'connection': 'receiver',
                    'message_wait': 15,
                    'consume': False,
                },
            }
            assert say_hello_spy.call_count == 1
            args, _ = say_hello_spy.call_args_list[-1]
            zmq_socket = args[0]
            assert zmq_socket.__class__.__name__ == '_Socket'
            assert zmq_socket.__class__.__module__ == 'zmq.green.core'
            assert args[1] == 'test'
            assert args[0] is v._endpoint_clients.get('test', None)

            v = AtomicServiceBus(
                'test-variable',
                (
                    'queue:documents-in, expression:"$.document[?(@.name=="TPM Report")]"  | url="Endpoint=sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, content_type=json, consume=True'
                ),
            )
            endpoint_client = v._endpoint_clients.get('test-variable', None)
            assert endpoint_client is not None
            assert endpoint_client.__class__.__name__ == '_Socket'
            assert endpoint_client.__class__.__module__ == 'zmq.green.core'
            assert v._settings.get('test-variable', None) == {
                'repeat': False,
                'wait': 15,
                'worker': None,
                'url': 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                'content_type': TransformerContentType.JSON,
                'consume': True,
                'endpoint_name': 'queue:documents-in, expression:"$.document[?(@.name=="TPM Report")]"',
                'context': {
                    'url': 'sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key',
                    'endpoint': 'queue:documents-in, expression:"$.document[?(@.name=="TPM Report")]"',
                    'connection': 'receiver',
                    'message_wait': 15,
                    'content_type': 'json',
                    'consume': True,
                },
            }
            assert say_hello_spy.call_count == 2
            args, _ = say_hello_spy.call_args_list[-1]
            zmq_socket = args[0]
            assert zmq_socket.__class__.__name__ == '_Socket'
            assert zmq_socket.__class__.__module__ == 'zmq.green.core'
            assert args[1] == 'test-variable'
            assert args[0] is v._endpoint_clients.get('test-variable', None)
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

    def test_say_hello(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        def mock_response(
            client: zmq.Socket,
            response: Optional[AsyncMessageResponse],
        ) -> None:
            mocker.patch.object(client, 'recv_json', side_effect=[ZMQAgain, response])

        context: Optional[zmq.Context] = None

        try:
            # <!-- lazy way to initialize an empty AtomicServiceBus...
            try:
                AtomicServiceBus(
                    'test',
                    (
                        'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                        'wait=15, repeat=True'
                    ),
                )
            except:
                pass

            v = cast(AtomicServiceBus, AtomicServiceBus.get())
            AtomicServiceBus.clear()
            # -->

            context = zmq.Context()
            client = context.socket(ZMQ_REQ)

            v._settings['test2'] = {
                'context': {
                    'endpoint': 'topic:test-topic',
                },
                'worker': None,
            }

            send_json_spy = noop_zmq.get_mock('send_json')
            send_json_spy.reset_mock()
            gsleep_spy = noop_zmq.get_mock('gsleep')

            mock_response(client, None)

            with pytest.raises(RuntimeError) as re:
                v.say_hello(client, 'test2')
            assert 'AtomicServiceBus.test2: no response when trying to connect' in str(re)
            assert gsleep_spy.call_count == 1
            args, _ = gsleep_spy.call_args_list[0]
            assert args[0] == 0.1
            assert send_json_spy.call_count == 1
            args, _ = send_json_spy.call_args_list[0]
            assert args[1] == {
                'worker': None,
                'action': 'HELLO',
                'context': {
                    'endpoint': 'topic:test-topic',
                },
            }
            assert v._settings['test2'].get('worker', '') is None

            mock_response(client, {
                'success': False,
                'message': 'ohnoes!',
            })

            with pytest.raises(RuntimeError) as re:
                v.say_hello(client, 'test2')
            assert 'AtomicServiceBus.test2: ohnoes!' in str(re)
            assert v._settings['test2'].get('worker', '') is None
            assert gsleep_spy.call_count == 2
            assert send_json_spy.call_count == 2

            mock_response(client, {
                'success': True,
                'worker': 'asdf-asdf-asdf',
            })

            v.say_hello(client, 'test2')

            assert v._settings['test2'].get('worker', None) == 'asdf-asdf-asdf'
            assert gsleep_spy.call_count == 3
            assert send_json_spy.call_count == 3

            v.say_hello(client, 'test2')
            assert gsleep_spy.call_count == 3
            assert send_json_spy.call_count == 3
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

            if context is not None:
                context.destroy()

    def test_clear(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        try:
            say_hello_spy = mocker.patch(
                'grizzly.testdata.variables.servicebus.AtomicServiceBus.say_hello',
                side_effect=[None] * 2,
            )

            v = AtomicServiceBus(
                'test1',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            v = AtomicServiceBus(
                'test2',
                (
                    'queue:documents-in, expression:$.hello.world | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True, content_type=json'
                ),
            )

            assert say_hello_spy.call_count == 2

            assert len(v._settings.keys()) == 2
            assert len(v._endpoint_messages.keys()) == 2
            assert len(v._endpoint_clients.keys()) == 2
            assert len(v._values.keys()) == 2

            AtomicServiceBus.clear()

            assert len(v._settings.keys()) == 0
            assert len(v._endpoint_messages.keys()) == 0
            assert len(v._endpoint_clients.keys()) == 0
            assert len(v._values.keys()) == 0
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

    def test___getitem__(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        def mock_response(response: Optional[AsyncMessageResponse], repeat: int = 1) -> None:
            mocker.patch(
                'grizzly.testdata.variables.servicebus.zmq.Socket.recv_json',
                side_effect=[ZMQAgain, response] * repeat,
            )

        mocker.patch(
            'grizzly.testdata.variables.servicebus.AtomicServiceBus.say_hello',
            autospec=True
        )

        try:
            mock_response(None)
            v = AtomicServiceBus(
                'test1',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            with pytest.raises(RuntimeError) as re:
                v['test1']
            assert str(re.value) == 'AtomicServiceBus.test1: unknown error, no response'

            AtomicServiceBus.destroy()

            mock_response({
                'success': False,
                'message': 'testing testing',
            })

            v = AtomicServiceBus(
                'test1',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            with pytest.raises(RuntimeError) as re:
                v['test1']
            assert str(re.value) == 'AtomicServiceBus.test1: testing testing'

            v._settings['test1']['worker'] = '1337-aaaabbbb-beef'

            mock_response({
                'success': False,
                'message': 'no messages on topic:documents-in, subscription:application-x',
            }, 6)

            with pytest.raises(RuntimeError) as re:
                v['test1']
            assert str(re.value) == 'AtomicServiceBus.test1: no messages on topic:documents-in, subscription:application-x'

            v._endpoint_messages['test1'] += ['hello world', 'world hello']

            assert v['test1'] == 'hello world'
            assert v['test1'] == 'world hello'
            assert v['test1'] == 'hello world'
            assert v['test1'] == 'world hello'

            v._endpoint_messages['test1'].clear()

            v._settings['test1']['repeat'] = False
            with pytest.raises(RuntimeError) as re:
                v['test1']
            assert str(re.value) == 'AtomicServiceBus.test1: no messages on topic:documents-in, subscription:application-x'

            mock_response({
                'success': True,
                'payload': None,
            })

            with pytest.raises(RuntimeError) as re:
                v['test1']
            assert str(re.value) == 'AtomicServiceBus.test1: payload in response was None'

            mock_response({
                'success': True,
                'payload': '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result></test>',
            }, 4)

            assert len(v._endpoint_messages['test1']) == 0
            assert v['test1'] == '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result></test>'
            assert len(v._endpoint_messages['test1']) == 0

            v._settings['test1']['repeat'] = True
            assert v['test1'] == '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result></test>'
            assert len(v._endpoint_messages['test1']) == 1
            assert v._endpoint_messages['test1'][0] == '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result></test>'

            mock_response({
                'success': True,
                'payload': '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result><result>world hello</result></test>',
            })

            mock_response({
                'success': True,
                'payload': '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result></test>',
            })

            assert v['test1'] == '<?xml version="1.0" encoding="utf-8"?><test><result>hello world</result></test>'
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

    def test___setitem__(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        def mocked___getitem__(i: AtomicServiceBus, variable: str) -> Optional[str]:
            return i._get_value(variable)

        mocker.patch(
            'grizzly.testdata.variables.servicebus.AtomicServiceBus.__getitem__',
            mocked___getitem__,
        )

        mocker.patch(
            'grizzly.testdata.variables.servicebus.AtomicServiceBus.say_hello',
            autospec=True,
        )

        try:
            v = AtomicServiceBus(
                'test',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            assert v['test'] == 'topic:documents-in, subscription:application-x'
            v['test'] = 'we <3 azure service bus'
            assert v['test'] == 'topic:documents-in, subscription:application-x'
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass

    def test___delitem__(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        def mocked___getitem__(i: AtomicServiceBus, variable: str) -> Optional[str]:
            return i._get_value(variable)

        mocker.patch(
            'grizzly.testdata.variables.servicebus.AtomicServiceBus.__getitem__',
            mocked___getitem__,
        )

        mocker.patch(
            'grizzly.testdata.variables.servicebus.AtomicServiceBus.say_hello',
            side_effect=[None] * 2,
        )

        zmq_disconnect_spy = mocker.patch(
            'grizzly.testdata.variables.servicebus.zmq.Socket.disconnect',
            side_effect=[None] * 10,
        )

        try:
            v = AtomicServiceBus(
                'test',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            assert v['test'] == 'topic:documents-in, subscription:application-x'
            assert len(v._values.keys()) == 1
            del v['test']
            assert len(v._values.keys()) == 0
            assert zmq_disconnect_spy.call_count == 1
            args, _ = zmq_disconnect_spy.call_args_list[0]
            assert args[0] == AtomicServiceBus._zmq_url

            del v['asdf']
            assert zmq_disconnect_spy.call_count == 1

            zmq_disconnect_spy.side_effect = [ZMQError]
            v = AtomicServiceBus(
                'test',
                (
                    'topic:documents-in, subscription:application-x | url="sb://sb.example.org/;SharedAccessKeyName=name;SharedAccessKey=key", '
                    'wait=15, repeat=True'
                ),
            )
            assert len(v._values.keys()) == 1
            del v['test']
            assert len(v._values.keys()) == 0
            zmq_disconnect_spy.call_count == 2
        finally:
            try:
                AtomicServiceBus.destroy()
            except:
                pass
