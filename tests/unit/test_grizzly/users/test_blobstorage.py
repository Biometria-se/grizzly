import json

from typing import cast

import pytest

from azure.storage.blob._blob_client import BlobClient

from pytest_mock import MockerFixture

from grizzly.types.locust import StopUser
from grizzly.users.blobstorage import BlobStorageUser
from grizzly.users.base.grizzly_user import GrizzlyUser
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.exceptions import RestartScenario
from grizzly.scenarios import GrizzlyScenario

from tests.fixtures import GrizzlyFixture


CONNECTION_STRING = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='


@pytest.fixture
def blob_storage_parent(grizzly_fixture: GrizzlyFixture) -> GrizzlyScenario:
    parent = grizzly_fixture(
        CONNECTION_STRING,
        BlobStorageUser,
    )

    request = grizzly_fixture.request_task.request
    request.method = RequestMethod.SEND

    scenario = GrizzlyContextScenario(99, behave=grizzly_fixture.behave.create_scenario('test scenario'))
    scenario.user.class_name = 'BlobStorageUser'
    scenario.context['host'] = 'test'
    scenario.tasks.clear()
    scenario.tasks.add(request)

    grizzly_fixture.grizzly.scenarios.clear()
    grizzly_fixture.grizzly.scenarios.append(scenario)

    return parent


class TestBlobStorageUser:
    @pytest.mark.usefixtures('blob_storage_parent')
    def test_on_start(self, blob_storage_parent: GrizzlyScenario) -> None:
        assert not hasattr(blob_storage_parent.user, 'blob_client')

        blob_storage_parent.user.on_start()

        assert hasattr(blob_storage_parent.user, 'blob_client')
        assert blob_storage_parent.user.blob_client.account_name == 'my-storage'

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_on_stop(self, mocker: MockerFixture, blob_storage_parent: GrizzlyScenario) -> None:
        assert isinstance(blob_storage_parent.user, BlobStorageUser)
        blob_storage_parent.user.on_start()

        on_stop_spy = mocker.spy(blob_storage_parent.user.blob_client, 'close')

        blob_storage_parent.user.on_stop()

        assert on_stop_spy.call_count == 1

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_create(self, blob_storage_parent: GrizzlyScenario) -> None:
        assert isinstance(blob_storage_parent.user, BlobStorageUser)
        assert issubclass(blob_storage_parent.user.__class__, GrizzlyUser)
        blob_storage_parent.user.on_start()

        BlobStorageUser.host = 'DefaultEndpointsProtocol=http;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError) as e:
            BlobStorageUser(blob_storage_parent.user.environment)
        assert 'is not supported for BlobStorageUser' in str(e)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net'
        with pytest.raises(ValueError) as e:
            BlobStorageUser(blob_storage_parent.user.environment)
        assert 'needs AccountName and AccountKey in the query string' in str(e)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError) as e:
            BlobStorageUser(blob_storage_parent.user.environment)
        assert 'needs AccountName in the query string' in str(e)

        BlobStorageUser.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage'
        with pytest.raises(ValueError) as e:
            BlobStorageUser(blob_storage_parent.user.environment)
        assert 'needs AccountKey in the query string' in str(e)

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_send(self, blob_storage_parent: GrizzlyScenario, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        blob_storage_parent.user.on_start()

        grizzly = grizzly_fixture.grizzly

        remote_variables = {
            'variables': transform(grizzly, {
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }
        blob_storage_parent.user.add_context(remote_variables)
        request = cast(RequestTask, blob_storage_parent.user._scenario.tasks()[-1])
        request.endpoint = 'some_container_name/file.txt'

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

        metadata, payload = blob_storage_parent.user.request(request)

        assert payload is not None
        assert upload_blob.call_count == 1
        args, _ = upload_blob.call_args_list[-1]
        assert len(args) == 2
        assert args[1] == json.dumps(expected_payload, indent=4)

        blob_client = cast(BlobClient, args[0])

        assert metadata == {}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'
        assert blob_client.container_name == 'some_container_name'
        assert blob_client.blob_name == 'file.txt'

        request = cast(RequestTask, blob_storage_parent.user._scenario.tasks()[-1])

        blob_storage_parent.user.request(request)

        request_event = mocker.spy(blob_storage_parent.user.environment.events.request, 'fire')

        request.method = RequestMethod.RECEIVE
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        assert request_event.call_count == 1
        _, kwargs = request_event.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'RECV'
        assert kwargs.get('name', None) == f'{blob_storage_parent.user._scenario.identifier} {request.name}'
        assert kwargs.get('response_time', -1) > -1
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is blob_storage_parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert 'has not implemented RECEIVE' in str(exception)

        blob_storage_parent.user._scenario.failure_exception = None
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_storage_parent.user._scenario.failure_exception = StopUser
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_storage_parent.user._scenario.failure_exception = RestartScenario
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', side_effect=[RuntimeError('failed to upload blob')])

        request.method = RequestMethod.SEND

        with pytest.raises(RestartScenario):
            blob_storage_parent.user.request(request)
