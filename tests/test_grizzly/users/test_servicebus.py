import json

from typing import Any, Callable, Generator, Tuple, Optional, cast
from contextlib import contextmanager

import pytest

from azure.servicebus import ServiceBusMessage
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from locust.exception import StopUser

from grizzly.users.meta.context_variables import ContextVariables
from grizzly.users.servicebus import ServiceBusUser
from grizzly.types import RequestMethod
from grizzly.task import RequestTask
from grizzly.testdata.utils import transform

from ..fixtures import locust_context, request_task  # pylint: disable=unused-import
from ..helpers import ResultFailure, RequestEvent, RequestSilentFailureEvent, clone_request

import logging

# we are not interested in misleading log messages when unit testing
logging.getLogger().setLevel(logging.CRITICAL)

class DummySender:
    sent_msg: Optional[ServiceBusMessage]
    endpoint: Optional[str]
    endpoint_type: Optional[str]

    def __init__(self) -> None:
        self.sent_msg = None
        self.endpoint = None
        self.endpoint_type = None

    def send_messages(self, msg: ServiceBusMessage) -> None:
        self.sent_msg = msg


class DummyErrorSender(DummySender):
    def send_messages(self, msg: ServiceBusMessage) -> None:
        raise Exception('send_messages failed')


class DummyServiceBusClient:
    conn_str: str
    sender: DummySender

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    def set_sender(self, sender: DummySender) -> None:
        self.sender = sender

    @contextmanager
    def get_queue_sender(self, endpoint: str) -> Generator[DummySender, None, None]:
        self.sender.endpoint = endpoint
        self.sender.endpoint_type = 'queue'
        yield self.sender

    @contextmanager
    def get_topic_sender(self, endpoint: str) -> Generator[DummySender, None, None]:
        self.sender.endpoint = endpoint
        self.sender.endpoint_type = 'topic'
        yield self.sender


CONNECTION_STRING = 'Endpoint=sb://sb.ifktest.ru/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='


@pytest.fixture
def sb_user(locust_context: Callable, mocker: MockerFixture) -> Tuple[ServiceBusUser, RequestTask, Environment]:
    def sb_connect(conn_str: str, **kwargs: Any) -> DummyServiceBusClient:
        return DummyServiceBusClient(conn_str)

    mocker.patch(
        'azure.servicebus.ServiceBusClient.from_connection_string',
        sb_connect,
    )

    environment, user, _, [_, _, request] = locust_context(
        CONNECTION_STRING, ServiceBusUser)

    request.method = RequestMethod.SEND

    return user, request, environment

class TestServiceBusUser:
    def test_create(self, sb_user: Tuple[ServiceBusUser, RequestTask, Environment]) -> None:
        [user, _, environment] = sb_user
        assert CONNECTION_STRING == cast(DummyServiceBusClient, user.client).conn_str
        assert issubclass(user.__class__, ContextVariables)

        ServiceBusUser.host = 'Endpoint=mq://sb.ifktest.ru/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret='
        with pytest.raises(ValueError) as e:
            user = ServiceBusUser(environment=environment)
        assert 'is not a supported scheme for ServiceBusUser' in str(e)

        ServiceBusUser.host = 'Endpoint=sb://sb.ifktest.ru'
        with pytest.raises(ValueError) as e:
            user = ServiceBusUser(environment=environment)
        assert 'needs SharedAccessKeyName and SharedAccessKey in the query string' in str(e)

        ServiceBusUser.host = 'Endpoint=sb://sb.ifktest.ru/;SharedAccessKey=secret='
        with pytest.raises(ValueError) as e:
            user = ServiceBusUser(environment=environment)
        assert 'needs SharedAccessKeyName in the query string' in str(e)

        ServiceBusUser.host = 'Endpoint=sb://sb.ifktest.ru/;SharedAccessKeyName=RootManageSharedAccessKey'
        with pytest.raises(ValueError) as e:
            user = ServiceBusUser(environment=environment)
        assert 'needs SharedAccessKey in the query string' in str(e)

    def test_send_queue(self, sb_user: Tuple[ServiceBusUser, RequestTask, Environment], mocker: MockerFixture) -> None:
        [user, request, environment] = sb_user

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        user.add_context(remote_variables)
        request.endpoint = 'queue:some_queue_name'

        dummy_client = user.client
        dummy_sender = DummySender()
        cast(DummyServiceBusClient, dummy_client).set_sender(dummy_sender)

        user.request(request)

        assert dummy_sender.sent_msg is not None

        msg_str = ''
        for bytes in dummy_sender.sent_msg.body:
            msg_str += bytes.decode('utf-8')

        json_msg = json.loads(msg_str)
        assert json_msg['result']['id'] == 'ID-31337'
        assert 'queue' == dummy_sender.endpoint_type
        assert f'queue:{dummy_sender.endpoint}' == request.endpoint

        request_invalid_endpoint_type = clone_request('POST', request)

        request_invalid_endpoint_type.endpoint = request.endpoint.replace('queue:', '')
        with pytest.raises(ValueError) as i:
            user.request(request_invalid_endpoint_type)
        assert 'does not specify queue: or topic:' in str(i)

        request_invalid_endpoint_type.endpoint = request.endpoint.replace('queue:', 'test:')
        with pytest.raises(ValueError) as i:
            user.request(request_invalid_endpoint_type)
        assert 'supports endpoint types queue or topic only, and not test' in str(i)

        dummy_sender = DummyErrorSender()
        cast(DummyServiceBusClient, dummy_client).set_sender(dummy_sender)
        environment.events.request = RequestEvent()

        with pytest.raises(ResultFailure):
            user.request(request)

        request_error = clone_request('RECEIVE', request)

        with pytest.raises(ResultFailure) as e:
            user.request(request_error)
        assert 'has not implemented RECEIVE' in str(e)

        dummy_sender = DummyErrorSender()
        cast(DummyServiceBusClient, dummy_client).set_sender(dummy_sender)
        environment.events.request = RequestSilentFailureEvent()

        request.scenario.stop_on_failure = False
        user.request(request_error)

        request.scenario.stop_on_failure = True
        with pytest.raises(StopUser):
            user.request(request_error)

    def test_send_topic(self, sb_user: Tuple[ServiceBusUser, RequestTask, Environment], mocker: MockerFixture) -> None:
        [user, request, environment] = sb_user

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        user.add_context(remote_variables)
        request.endpoint = 'topic:some_topic_name'

        dummy_client = user.client
        dummy_sender = DummySender()
        cast(DummyServiceBusClient, dummy_client).set_sender(dummy_sender)

        user.request(request)

        assert dummy_sender.sent_msg is not None

        msg_str = ''
        for bytes in dummy_sender.sent_msg.body:
            msg_str += bytes.decode('utf-8')

        json_msg = json.loads(msg_str)
        assert json_msg['result']['id'] == 'ID-31337'
        assert 'topic' == dummy_sender.endpoint_type
        assert f'topic:{dummy_sender.endpoint}' == request.endpoint

        dummy_sender = DummyErrorSender()
        cast(DummyServiceBusClient, dummy_client).set_sender(dummy_sender)
        environment.events.request = RequestEvent()

        with pytest.raises(ResultFailure):
            user.request(request)

        request_error = clone_request('RECEIVE', request)

        with pytest.raises(ResultFailure) as e:
            user.request(request_error)
        assert 'has not implemented RECEIVE' in str(e)

        dummy_sender = DummyErrorSender()
        cast(DummyServiceBusClient, dummy_client).set_sender(dummy_sender)
        environment.events.request = RequestSilentFailureEvent()

        request.scenario.stop_on_failure = False
        user.request(request_error)

        request.scenario.stop_on_failure = True
        with pytest.raises(StopUser):
            user.request(request_error)
