import os
import json

from typing import Tuple, cast

import pytest

from locust.env import Environment
from locust.exception import StopUser
from azure.storage.blob._blob_client import BlobClient

from pytest_mock import MockerFixture

from grizzly.users.blobstorage import BlobStorageUser
from grizzly.users.base.grizzly_user import GrizzlyUser
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.exceptions import RestartScenario

from ...fixtures import GrizzlyFixture


CONNECTION_STRING = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='

BlobStorageScenarioFixture = Tuple[BlobStorageUser, GrizzlyContextScenario, Environment]


@pytest.fixture
def blob_storage_scenario(grizzly_fixture: GrizzlyFixture) -> BlobStorageScenarioFixture:
    environment, user, task = grizzly_fixture(
        CONNECTION_STRING,
        BlobStorageUser,
    )

    request = grizzly_fixture.request_task.request

    scenario = GrizzlyContextScenario(99)
    scenario.name = task.__class__.__name__
    scenario.user.class_name = 'BlobStorageUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND

    scenario.tasks.add(request)

    return cast(BlobStorageUser, user), scenario, environment


class TestBlobStorageUser:
    @pytest.mark.usefixtures('blob_storage_scenario')
    def test_create(self, blob_storage_scenario: BlobStorageScenarioFixture) -> None:
        user, _, environment = blob_storage_scenario
        assert user.client.account_name == 'my-storage'
        assert issubclass(user.__class__, GrizzlyUser)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=http;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='
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

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage'
        with pytest.raises(ValueError) as e:
            user = BlobStorageUser(environment)
        assert 'needs AccountKey in the query string' in str(e)

    @pytest.mark.usefixtures('blob_storage_scenario')
    def test_send(self, blob_storage_scenario: BlobStorageScenarioFixture, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        [user, scenario, _] = blob_storage_scenario

        grizzly = grizzly_fixture.grizzly

        remote_variables = {
            'variables': transform(grizzly, {
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        user.add_context(remote_variables)
        request = cast(RequestTask, scenario.tasks[-1])
        request.endpoint = 'some_container_name'

        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', autospec=True)

        expected_payload = {
            'result': {
                'id': 'ID-31337',
                'date': '',
                'variable': '137',
                'item': {
                    'description': 'this is just a description',
                }
            }
        }

        metadata, payload = user.request(request)

        assert payload is not None
        assert upload_blob.call_count == 1
        args, _ = upload_blob.call_args_list[-1]
        assert len(args) == 2
        assert args[1] == json.dumps(expected_payload, indent=4)

        blob_client = cast(BlobClient, args[0])

        assert metadata == {}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'
        assert blob_client.container_name == cast(RequestTask, scenario.tasks[-1]).endpoint
        assert blob_client.blob_name == os.path.basename(scenario.name)

        request = cast(RequestTask, scenario.tasks[-1])

        user.request(request)

        request_event = mocker.spy(user.environment.events.request, 'fire')

        request.method = RequestMethod.RECEIVE
        with pytest.raises(StopUser):
            user.request(request)

        assert request_event.call_count == 1
        _, kwargs = request_event.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'RECV'
        assert kwargs.get('name', None) == f'{request.scenario.identifier} {request.scenario.name}'
        assert kwargs.get('response_time', -1) > -1
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert 'has not implemented RECEIVE' in str(exception)

        request.scenario.failure_exception = None
        with pytest.raises(StopUser):
            user.request(request)

        request.scenario.failure_exception = StopUser
        with pytest.raises(StopUser):
            user.request(request)

        request.scenario.failure_exception = RestartScenario
        with pytest.raises(StopUser):
            user.request(request)

        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', side_effect=[RuntimeError('failed to upload blob')])

        request.method = RequestMethod.SEND

        with pytest.raises(RestartScenario):
            user.request(request)
