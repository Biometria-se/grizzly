import os
import json

from typing import Any, Callable, Generator, Tuple, Optional, cast
from contextlib import contextmanager

import pytest

from locust.env import Environment
from locust.exception import StopUser
from azure.servicebus import ServiceBusMessage

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly.users.blobstorage import BlobStorageUser
from grizzly.users.meta.context_variables import ContextVariables
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.task import RequestTask
from grizzly.testdata.utils import transform

from ..fixtures import grizzly_context, request_task  # pylint: disable=unused-import
from ..helpers import ResultFailure, RequestEvent, RequestSilentFailureEvent

import logging

# we are not interested in misleading log messages when unit testing
logging.getLogger().setLevel(logging.CRITICAL)

class DummyBlobClient:
    blob_data: Optional[ServiceBusMessage]
    container: Optional[str]
    blob: Optional[str]

    def __init__(self) -> None:
        self.blob_data = None
        self.container = None
        self.blob = None

    def upload_blob(self, msg: str) -> None:
        self.blob_data = ServiceBusMessage(body=msg)


class DummyBlobErrorClient(DummyBlobClient):
    def upload_blob(self, msg: str) -> None:
        raise Exception('upload_blob failed')

class DummyBlobServiceClient:
    conn_str: str
    blobclient: DummyBlobClient

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    def set_blobclient(self, blobclient: DummyBlobClient) -> None:
        self.blobclient = blobclient

    @contextmanager
    def get_blob_client(self, container: str, blob: str) -> Generator[DummyBlobClient, None, None]:
        self.blobclient.container = container
        self.blobclient.blob = blob
        yield self.blobclient


CONNECTION_STRING = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=bioutvifkstorage;AccountKey=xxxyyyyzzz=='


@pytest.fixture
def bs_user(grizzly_context: Callable, mocker: MockerFixture) -> Tuple[BlobStorageUser, GrizzlyContextScenario, Environment]:
    def bs_connect(conn_str: str, **kwargs: Any) -> DummyBlobServiceClient:
        return DummyBlobServiceClient(conn_str)

    mocker.patch(
        'azure.storage.blob.BlobServiceClient.from_connection_string',
        bs_connect,
    )

    environment, user, task, [_, _, request] = grizzly_context(
        CONNECTION_STRING,
        BlobStorageUser,
    )

    request = cast(RequestTask, request)

    scenario = GrizzlyContextScenario()
    scenario.name = task.__class__.__name__
    scenario.user.class_name = 'BlobStorageUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND

    scenario.add_task(request)

    return user, scenario, environment


class TestBlobStorageUser:
    def test_create(self, bs_user: Tuple[BlobStorageUser, GrizzlyContextScenario, Environment]) -> None:
        [user, _, environment] = bs_user
        assert CONNECTION_STRING == user.client.conn_str
        assert issubclass(user.__class__, ContextVariables)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=http;EndpointSuffix=core.windows.net;AccountName=bioutvifkstorage;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError) as e:
            user = BlobStorageUser(environment)
        assert 'is not supported for BlobStorageUser' in str(e)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net'
        with pytest.raises(ValueError) as e:
            user = BlobStorageUser(environment)
        assert 'needs AccountName and AccountKey in the query string' in str(e)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError) as e:
            user = BlobStorageUser(environment)
        assert 'needs AccountName in the query string' in str(e)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=bioutvifkstorage'
        with pytest.raises(ValueError) as e:
            user = BlobStorageUser(environment)
        assert 'needs AccountKey in the query string' in str(e)


    def test_send(self, bs_user: Tuple[BlobStorageUser, GrizzlyContextScenario, Environment]) -> None:
        [user, scenario, environment] = bs_user

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        user.add_context(remote_variables)
        request = cast(RequestTask, scenario.tasks[-1])
        request.endpoint = 'some_container_name'

        dummy_client = user.client
        dummy_blobclient = DummyBlobClient()
        dummy_client.set_blobclient(dummy_blobclient)

        metadata, payload = user.request(request)

        assert dummy_blobclient.blob_data is not None
        assert metadata == {}

        msg: str = ''
        for part in dummy_blobclient.blob_data.body:
            msg += part.decode('utf-8')

        json_msg = json.loads(msg)
        payload = json.loads(msg)
        assert json_msg == payload
        assert json_msg['result']['id'] == 'ID-31337'
        assert dummy_blobclient.container == cast(RequestTask, scenario.tasks[-1]).endpoint
        assert dummy_blobclient.blob == os.path.basename(scenario.name)

        dummy_blobclient = DummyBlobErrorClient()
        dummy_client.set_blobclient(dummy_blobclient)
        environment.events.request = RequestEvent()

        with pytest.raises(ResultFailure):
            user.request(cast(RequestTask, scenario.tasks[-1]))

        request_error = cast(RequestTask, scenario.tasks[-1])
        request_error.method = RequestMethod.RECEIVE
        with pytest.raises(ResultFailure) as e:
            user.request(request_error)
        assert 'has not implemented RECEIVE' in str(e)

        dummy_blobclient = DummyBlobErrorClient()
        dummy_client.set_blobclient(dummy_blobclient)
        environment.events.request = RequestSilentFailureEvent()

        request_error.scenario.stop_on_failure = False
        user.request(request_error)

        request_error.scenario.stop_on_failure = True
        with pytest.raises(StopUser):
            user.request(request_error)
