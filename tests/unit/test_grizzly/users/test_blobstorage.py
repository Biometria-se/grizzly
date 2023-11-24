"""Unit tests for grizzly.users.blobstorage."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast
from unittest.mock import ANY

import pytest
from azure.storage.blob._blob_client import BlobClient

from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from grizzly.users import BlobStorageUser, GrizzlyUser

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from grizzly.scenarios import GrizzlyScenario
    from tests.fixtures import GrizzlyFixture

CONNECTION_STRING = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='


@pytest.fixture()
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

        blob_storage_parent.user.__class__.host = 'DefaultEndpointsProtocol=http;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError, match='is not supported for BlobStorageUser'):
            blob_storage_parent.user.__class__(blob_storage_parent.user.environment)

        blob_storage_parent.user.__class__.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net'
        with pytest.raises(ValueError, match='needs AccountName in the query string'):
            blob_storage_parent.user.__class__(blob_storage_parent.user.environment)

        blob_storage_parent.user.__class__.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError, match='needs AccountName in the query string'):
            blob_storage_parent.user.__class__(blob_storage_parent.user.environment)

        blob_storage_parent.user.__class__.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage'
        with pytest.raises(ValueError, match='needs AccountKey in the query string'):
            blob_storage_parent.user.__class__(blob_storage_parent.user.environment)

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
        blob_properties = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.get_blob_properties', return_value={'size': 1337, 'etag': '0xdeadbeef'})

        expected_payload = {
            'result': {
                'id': 'ID-31337',
                'date': '',
                'variable': '137',
                'item': {
                    'description': 'this is just a description',
                },
            },
        }

        assert isinstance(blob_storage_parent.user, BlobStorageUser)

        metadata, payload = blob_storage_parent.user.request(request)

        assert payload is not None
        upload_blob.assert_called_once_with(ANY, json.dumps(expected_payload, indent=4), overwrite=True)
        args, _ = upload_blob.call_args_list[-1]

        blob_client = cast(BlobClient, args[0])

        assert metadata == {'size': 1337, 'etag': '0xdeadbeef'}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'
        assert blob_client.container_name == 'some_container_name'
        assert blob_client.blob_name == 'file.txt'

        blob_properties.assert_called_once_with()
        blob_properties.reset_mock()

        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', side_effect=[RuntimeError('failed to upload blob')])

        request.method = RequestMethod.SEND
        blob_storage_parent.user._scenario.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            blob_storage_parent.user.request(request)

        blob_properties.assert_not_called()

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_receive(self, blob_storage_parent: GrizzlyScenario, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
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
        request.method = RequestMethod.RECEIVE
        request.endpoint = 'some_container_name/file.txt'

        download_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.download_blob', autospec=True)
        blob_properties = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.get_blob_properties', return_value={'size': 7331, 'etag': '0xb000b000'})

        expected_payload = {
            'result': {
                'id': 'ID-31337',
                'date': '',
                'variable': '137',
                'item': {
                    'description': 'this is just a description',
                },
            },
        }

        download_blob.return_value.readall.return_value = json.dumps(expected_payload, indent=4).encode('utf-8')

        metadata, payload = blob_storage_parent.user.request(request)

        assert payload == json.dumps(expected_payload, indent=4)
        args, _ = download_blob.call_args_list[-1]

        blob_client = cast(BlobClient, args[0])
        download_blob.assert_called_once_with(blob_client)
        download_blob.return_value.readall.assert_called_once_with()

        assert metadata == {'size': 7331, 'etag': '0xb000b000'}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'
        assert blob_client.container_name == 'some_container_name'
        assert blob_client.blob_name == 'file.txt'

        blob_properties.assert_called_once_with()
        blob_properties.reset_mock()

        download_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.download_blob', side_effect=[RuntimeError('failed to download blob')])

        blob_storage_parent.user._scenario.failure_exception = StopUser

        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_properties.assert_not_called()

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_not_implemented(self, blob_storage_parent: GrizzlyScenario, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
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

        download_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.download_blob', autospec=True)
        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', autospec=True)
        request_event = mocker.spy(blob_storage_parent.user.environment.events.request, 'fire')

        request.method = RequestMethod.POST
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        assert request_event.call_count == 1
        _, kwargs = request_event.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'POST'
        assert kwargs.get('name', None) == f'{blob_storage_parent.user._scenario.identifier} {request.name}'
        assert kwargs.get('response_time', -1) > -1
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is blob_storage_parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert 'has not implemented POST' in str(exception)

        blob_storage_parent.user._scenario.failure_exception = None
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_storage_parent.user._scenario.failure_exception = StopUser
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_storage_parent.user._scenario.failure_exception = RestartScenario
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        download_blob.assert_not_called()
        upload_blob.assert_not_called()
