from typing import Any, Dict, Tuple

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from locust.exception import StopUser
from azure.servicebus import ServiceBusMessage, TransportType, ServiceBusSender, ServiceBusClient
from jinja2 import Template

from grizzly.users.meta import ContextVariables, RequestLogger, ResponseHandler
from grizzly.users.servicebus import ServiceBusUser
from grizzly.types import RequestMethod
from grizzly.task import RequestTask
from grizzly.testdata.utils import transform
from grizzly.context import GrizzlyContextScenario

from ..fixtures import behave_context, request_task, locust_environment  # pylint: disable=unused-import


class TestServiceBusUser:
    @pytest.mark.usefixtures('locust_environment')
    def test_create(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        try:
            servicebusclient_spy = mocker.spy(ServiceBusClient, 'from_connection_string')

            ServiceBusUser.host = 'Endpoint=mq://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret='
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: "mq" is not a supported scheme' in str(e)

            assert servicebusclient_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org'
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: SharedAccessKeyName and SharedAccessKey must be in the query string' in str(e)

            assert servicebusclient_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKey=secret='
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: SharedAccessKeyName must be in the query string' in str(e)

            assert servicebusclient_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey'
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: SharedAccessKey must be in the query string' in str(e)

            assert servicebusclient_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
            user = ServiceBusUser(environment=locust_environment)
            assert issubclass(user.__class__, ContextVariables)
            assert issubclass(user.__class__, ResponseHandler)
            assert issubclass(user.__class__, RequestLogger)

            assert servicebusclient_spy.call_count == 1
            _, kwargs = servicebusclient_spy.call_args_list[0]

            assert kwargs.get('conn_str', None) == 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
            assert kwargs.get('transport_type', None) == TransportType.AmqpOverWebsocket
        finally:
            ServiceBusUser._context = {
                'message': {
                    'wait': None,
                }
            }

    def test_from_message(self) -> None:
        assert ServiceBusUser.from_message(None) == (None, None,)

        message = ServiceBusMessage('a message')
        message.raw_amqp_message.properties = None
        message.raw_amqp_message.header = None
        assert ServiceBusUser.from_message(message) == ({}, 'a message',)

        message = ServiceBusMessage('a message'.encode('utf-8'))
        metadata, payload = ServiceBusUser.from_message(message)
        assert payload == 'a message'
        assert isinstance(metadata, dict)
        assert len(metadata) > 0

    @pytest.mark.usefixtures('locust_environment')
    def test_get_sender_instance(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = ServiceBusUser(environment=locust_environment)

        queue_spy = mocker.spy(user.sb_client, 'get_queue_sender')
        topic_spy = mocker.spy(user.sb_client, 'get_topic_sender')
        task = RequestTask(RequestMethod.SEND, name='test-send-queue', endpoint='queue:test-queue')
        user.get_sender_instance(task, task.endpoint)
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 1
        _, kwargs = queue_spy.call_args_list[0]
        assert len(kwargs) == 1
        assert kwargs.get('queue_name', None) == 'test-queue'

        task = RequestTask(RequestMethod.SEND, name='test-send-topic', endpoint='topic:test-topic')
        user.get_sender_instance(task, task.endpoint)
        assert queue_spy.call_count == 1
        assert topic_spy.call_count == 1
        _, kwargs = topic_spy.call_args_list[0]
        assert len(kwargs) == 1
        assert kwargs.get('topic_name', None) == 'test-topic'

    @pytest.mark.usefixtures('locust_environment')
    def test_get_receiver_instance(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = ServiceBusUser(environment=locust_environment)

        queue_spy = mocker.spy(user.sb_client, 'get_queue_receiver')
        topic_spy = mocker.spy(user.sb_client, 'get_subscription_receiver')

        task = RequestTask(RequestMethod.RECEIVE, name='test-recv-queue', endpoint='queue:test-queue')
        user.get_receiver_instance(task, task.endpoint)
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 1
        _, kwargs = queue_spy.call_args_list[0]
        assert len(kwargs) == 1
        assert kwargs.get('queue_name', None) == 'test-queue'

        user._context['message'] = {'wait': 100}
        user.get_receiver_instance(task, task.endpoint)
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 2
        _, kwargs = queue_spy.call_args_list[1]
        assert len(kwargs) == 2
        assert kwargs.get('queue_name', None) == 'test-queue'
        assert kwargs.get('max_wait_time', None) == 100

        task = RequestTask(RequestMethod.RECEIVE, name='test-recv-topic', endpoint='topic:test-topic, subscription:test-subscription')
        user.get_receiver_instance(task, task.endpoint)
        assert topic_spy.call_count == 1
        assert queue_spy.call_count == 2
        _, kwargs = topic_spy.call_args_list[0]
        assert len(kwargs) == 3
        assert kwargs.get('topic_name', None) == 'test-topic'
        assert kwargs.get('subscription_name', None) == 'test-subscription'
        assert kwargs.get('max_wait_time', None) == 100

        user._context['message'] = {'wait': None}
        user.get_receiver_instance(task, task.endpoint)
        assert topic_spy.call_count == 2
        assert queue_spy.call_count == 2
        _, kwargs = topic_spy.call_args_list[1]
        assert len(kwargs) == 2
        assert kwargs.get('topic_name', None) == 'test-topic'
        assert kwargs.get('subscription_name', None) == 'test-subscription'

    def test_get_endpoint_details(self) -> None:
        task = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='test')
        with pytest.raises(ValueError) as ve:
            ServiceBusUser.get_endpoint_details(task, task.endpoint)
        assert 'ServiceBusUser: "test" is not prefixed with queue: or topic:' in str(ve)

        task = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='asdf:test')
        with pytest.raises(ValueError) as ve:
            ServiceBusUser.get_endpoint_details(task, task.endpoint)
        assert 'ServiceBusUser: only support endpoint types queue and topic, not asdf' in str(ve)

        task = RequestTask(RequestMethod.SEND, name='test', endpoint='topic:test, dummy:test')
        with pytest.raises(ValueError) as ve:
            ServiceBusUser.get_endpoint_details(task, task.endpoint)
        assert 'ServiceBusUser: additional arguments in endpoint is not supported for SEND' in str(ve)

        task = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='topic:test, dummy:test')
        with pytest.raises(ValueError) as ve:
            ServiceBusUser.get_endpoint_details(task, task.endpoint)
        assert 'ServiceBusUser: argument dummy is not supported' in str(ve)

        task = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='topic:test')
        with pytest.raises(ValueError) as ve:
            ServiceBusUser.get_endpoint_details(task, task.endpoint)
        assert 'ServiceBusUser: endpoint needs to include subscription when receiving messages from a topic' in str(ve)

        task = RequestTask(RequestMethod.SEND, name='test', endpoint='queue:test')
        assert ServiceBusUser.get_endpoint_details(task, task.endpoint) == ('queue', 'test', None, )

        task = RequestTask(RequestMethod.SEND, name='test', endpoint='topic:test')
        assert ServiceBusUser.get_endpoint_details(task, task.endpoint) == ('topic', 'test', None, )

        task = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='queue:test')
        assert ServiceBusUser.get_endpoint_details(task, task.endpoint) == ('queue', 'test', None, )

        task = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='topic:test, subscription:test')
        assert ServiceBusUser.get_endpoint_details(task, task.endpoint) == ('topic', 'test', 'test', )

    @pytest.mark.usefixtures('locust_environment')
    def test_request_not_implemented_method(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = ServiceBusUser(environment=locust_environment)

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        user.add_context(remote_variables)

        request_fire_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_fire_spy = mocker.spy(user.response_event, 'fire')

        task = RequestTask(RequestMethod.PUT, name='test-send-queue', endpoint='queue:test-queue')
        task.source = '{"hello": {{ messageID }}}'
        task.template = Template(task.source)

        scenario = GrizzlyContextScenario()
        scenario.name = 'test'
        scenario.add_task(task)

        with pytest.raises(StopUser):
            user.request(task)

        assert response_event_fire_spy.call_count == 1
        assert request_fire_spy.call_count == 1

        _, kwargs = response_event_fire_spy.call_args_list[0]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert metadata is None
        assert payload == '{"hello": 137}'
        assert kwargs.get('user', None) is user
        assert isinstance(kwargs.get('exception', None), NotImplementedError)

        _, kwargs = request_fire_spy.call_args_list[0]
        assert kwargs.get('request_type', None) == 'sb:PUT'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert 'has not implemented PUT' in str(exception)


    @pytest.mark.usefixtures('locust_environment')
    def test_request_sender(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        def mocked___enter__(instance: ServiceBusSender) -> ServiceBusSender:
            return instance

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        mocker.patch(
            'grizzly.users.servicebus.ServiceBusSender.__enter__',
            mocked___enter__,
        )

        mocker.patch(
            'grizzly.users.servicebus.ServiceBusSender.send_messages',
            noop,
        )

        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = ServiceBusUser(environment=locust_environment)

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        user.add_context(remote_variables)

        request_fire_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_fire_spy = mocker.spy(user.response_event, 'fire')

        task = RequestTask(RequestMethod.SEND, name='test-send-queue', endpoint='queue:test-queue')
        task.source = '{"hello": {{ messageID }}}'
        task.template = Template(task.source)

        scenario = GrizzlyContextScenario()
        scenario.name = 'test'
        scenario.add_task(task)

        user.request(task)

        assert response_event_fire_spy.call_count == 1
        assert request_fire_spy.call_count == 1

        _, kwargs = response_event_fire_spy.call_args_list[0]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert isinstance(metadata, dict)
        assert payload == '{"hello": 137}'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', None) is None

        _, kwargs = request_fire_spy.call_args_list[0]
        assert kwargs.get('request_type', None) == 'sb:SEND'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == len('{"hello": 137}')
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', None) is None

        task.name = 'test-send-topic'
        task.endpoint = 'topic:test-topic'
        task.source = '{"value": {{ AtomicIntegerIncrementer.messageID }}}'
        task.template = Template(task.source)

        user.request(task)

        assert response_event_fire_spy.call_count == 2
        assert request_fire_spy.call_count == 2

        _, kwargs = response_event_fire_spy.call_args_list[1]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert isinstance(metadata, dict)
        assert payload == '{"value": 31337}'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', None) is None

        _, kwargs = request_fire_spy.call_args_list[1]
        assert kwargs.get('request_type', None) == 'sb:SEND'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == len('{"value": 31337}')
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', None) is None

    @pytest.mark.usefixtures('locust_environment')
    def test_request_receiver(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        def mocked___enter__(instance: ServiceBusSender) -> ServiceBusSender:
            return instance

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        mocker.patch(
            'grizzly.users.servicebus.ServiceBusReceiver.__enter__',
            mocked___enter__,
        )

        mocker.patch(
            'grizzly.users.servicebus.ServiceBusReceiver.complete_message',
            noop,
        )

        mocker.patch(
            'grizzly.users.servicebus.ServiceBusReceiver.next',
            side_effect=[StopIteration, ServiceBusMessage('{"test": 137}'), ServiceBusMessage('{"value": 31337}')]
        )

        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = ServiceBusUser(environment=locust_environment)

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        user.add_context(remote_variables)

        request_fire_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_fire_spy = mocker.spy(user.response_event, 'fire')

        task = RequestTask(RequestMethod.RECEIVE, name='test-recv-queue', endpoint='queue:test-queue')

        scenario = GrizzlyContextScenario()
        scenario.name = 'test'
        scenario.add_task(task)
        scenario.stop_on_failure = True

        with pytest.raises(StopUser):
            user.request(task)

        assert response_event_fire_spy.call_count == 1
        assert request_fire_spy.call_count == 1

        _, kwargs = response_event_fire_spy.call_args_list[0]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        assert kwargs.get('context', ('', '',)) == (None, None,)
        assert kwargs.get('user', None) is user
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)

        _, kwargs = request_fire_spy.call_args_list[0]
        assert kwargs.get('request_type', None) == 'sb:RECE'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert 'no message on queue:test-queue' in str(exception)

        user.request(task)

        assert response_event_fire_spy.call_count == 2
        assert request_fire_spy.call_count == 2

        _, kwargs = response_event_fire_spy.call_args_list[1]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert isinstance(metadata, dict)
        assert payload == '{"test": 137}'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', None) is None

        _, kwargs = request_fire_spy.call_args_list[1]
        assert kwargs.get('request_type', None) == 'sb:RECE'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == len('{"test": 137}')
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', None) is None

        task.endpoint = 'topic:test-topic, subscription:test-subscription'
        user.request(task)

        assert response_event_fire_spy.call_count == 3
        assert request_fire_spy.call_count == 3

        _, kwargs = response_event_fire_spy.call_args_list[2]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert isinstance(metadata, dict)
        assert payload == '{"value": 31337}'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', None) is None

        _, kwargs = request_fire_spy.call_args_list[2]
        assert kwargs.get('request_type', None) == 'sb:RECE'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == len('{"value": 31337}')
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', None) is None
