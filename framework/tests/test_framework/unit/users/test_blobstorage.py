"""Unit tests for grizzly.users.blobstorage."""

from __future__ import annotations

import json
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, cast

import pytest
from azure.storage.blob._blob_client import BlobClient
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from grizzly.users import BlobStorageUser
from grizzly_common.azure.aad import AuthMethod, AzureAadCredential

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from grizzly.scenarios import GrizzlyScenario

    from test_framework.fixtures import GrizzlyFixture, MockerFixture

CONNECTION_STRING = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='


@pytest.fixture
def blob_storage_parent(grizzly_fixture: GrizzlyFixture) -> GrizzlyScenario:
    parent = grizzly_fixture(
        CONNECTION_STRING,
        BlobStorageUser,
    )

    request = grizzly_fixture.request_task.request
    request.method = RequestMethod.SEND

    scenario = GrizzlyContextScenario(99, behave=grizzly_fixture.behave.create_scenario('test scenario'), grizzly=grizzly_fixture.grizzly)
    scenario.user.class_name = 'BlobStorageUser'
    scenario.context['host'] = 'test'
    scenario.tasks.clear()
    scenario.tasks.add(request)

    grizzly_fixture.grizzly.scenarios.clear()
    grizzly_fixture.grizzly.scenarios.append(scenario)

    return parent


class TestBlobStorageUser:
    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        blob_service_client_mock = mocker.patch('grizzly.users.blobstorage.BlobServiceClient')

        # connection string
        cls_blob_storage_user = type(
            'BlobStorageUserTest',
            (BlobStorageUser,),
            {
                '__scenario__': grizzly.scenario,
                'host': CONNECTION_STRING,
            },
        )

        user = cls_blob_storage_user(grizzly.state.locust.environment)

        user.on_start()

        blob_service_client_mock.from_connection_string.assert_called_once_with(conn_str=CONNECTION_STRING)
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.reset_mock()

        # token credentials
        cls_blob_storage_user = type(
            'BlobStorageUserTest',
            (BlobStorageUser,),
            {
                '__scenario__': grizzly.scenario,
                'host': 'bs://my-storage-account',
                '__context__': {
                    'auth': {
                        'tenant': 'example.com',
                        'user': {
                            'username': 'bob@example.com',
                            'password': 'secret',
                        },
                    },
                },
            },
        )

        user = cls_blob_storage_user(grizzly.state.locust.environment)

        user.on_start()

        blob_service_client_mock.from_connection_string.assert_not_called()
        blob_service_client_mock.assert_called_once_with(
            account_url='https://my-storage-account.blob.core.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='bob@example.com',
                password='secret',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='https://my-storage-account.blob.core.windows.net',
            ),
        )

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_on_stop(self, mocker: MockerFixture, blob_storage_parent: GrizzlyScenario) -> None:
        assert isinstance(blob_storage_parent.user, BlobStorageUser)
        blob_storage_parent.user.on_start()

        on_stop_spy = mocker.spy(blob_storage_parent.user.blob_client, 'close')

        blob_storage_parent.user.on_stop()

        on_stop_spy.assert_called_once_with()

    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        # connection string
        cls_user = type(
            'BlobStorageUserTest',
            (BlobStorageUser,),
            {
                '__scenario__': grizzly.scenario,
                'host': CONNECTION_STRING,
            },
        )

        assert issubclass(cls_user, BlobStorageUser)

        cls_user.host = 'DefaultEndpointsProtocol=http;EndpointSuffix=core.windows.net;AccountName=my-storage;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError, match='is not supported for BlobStorageUser'):
            cls_user(grizzly.state.locust.environment)

        cls_user.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net'
        with pytest.raises(ValueError, match='needs AccountName in the query string'):
            cls_user(grizzly.state.locust.environment)

        cls_user.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountKey=xxxyyyyzzz=='
        with pytest.raises(ValueError, match='needs AccountName in the query string'):
            cls_user(grizzly.state.locust.environment)

        cls_user.host = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=my-storage'
        with pytest.raises(ValueError, match='needs AccountKey in the query string'):
            cls_user(grizzly.state.locust.environment)

        cls_user.host = CONNECTION_STRING
        user = cls_user(grizzly.state.locust.environment)
        assert user.credential is None

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_send(self, blob_storage_parent: GrizzlyScenario, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        blob_storage_parent.user.on_start()

        grizzly = grizzly_fixture.grizzly

        remote_variables = {
            'variables': transform(
                grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 31337,
                    'AtomicDate.now': '',
                    'messageID': 137,
                },
            ),
        }
        blob_storage_parent.user.add_context(remote_variables)
        request = cast('RequestTask', blob_storage_parent.user._scenario.tasks()[-1])
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
        upload_blob.assert_called_once_with(ANY(BlobClient), json.dumps(expected_payload, indent=4), overwrite=True)
        args, _ = upload_blob.call_args_list[-1]

        blob_client = cast('BlobClient', args[0])

        assert metadata == {'size': 1337, 'etag': '0xdeadbeef'}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'
        assert blob_client.container_name == 'some_container_name'
        assert blob_client.blob_name == 'file.txt'

        blob_properties.assert_called_once_with()
        blob_properties.reset_mock()

        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', side_effect=[RuntimeError('failed to upload blob')])

        request.method = RequestMethod.SEND
        blob_storage_parent.user._scenario.failure_handling.update({None: RestartScenario})

        with pytest.raises(RestartScenario):
            blob_storage_parent.user.request(request)

        blob_properties.assert_not_called()

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_receive(self, blob_storage_parent: GrizzlyScenario, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        blob_storage_parent.user.on_start()

        grizzly = grizzly_fixture.grizzly

        remote_variables = {
            'variables': transform(
                grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 31337,
                    'AtomicDate.now': '',
                    'messageID': 137,
                },
            ),
        }
        blob_storage_parent.user.add_context(remote_variables)
        request = cast('RequestTask', blob_storage_parent.user._scenario.tasks()[-1])
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

        blob_client = cast('BlobClient', args[0])
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

        blob_storage_parent.user._scenario.failure_handling.update({None: StopUser})

        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_properties.assert_not_called()

    @pytest.mark.usefixtures('blob_storage_parent')
    def test_not_implemented(self, blob_storage_parent: GrizzlyScenario, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        blob_storage_parent.user.on_start()

        grizzly = grizzly_fixture.grizzly

        remote_variables = {
            'variables': transform(
                grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 31337,
                    'AtomicDate.now': '',
                    'messageID': 137,
                },
            ),
        }
        blob_storage_parent.user.add_context(remote_variables)

        request = cast('RequestTask', blob_storage_parent.user._scenario.tasks()[-1])
        request.endpoint = 'some_container_name/file.txt'

        download_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.download_blob', autospec=True)
        upload_blob = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', autospec=True)
        request_event = mocker.spy(blob_storage_parent.user.environment.events.request, 'fire')

        request.method = RequestMethod.POST
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        request_event.assert_called_once_with(
            request_type='POST',
            name=f'{blob_storage_parent.user._scenario.identifier} {request.name}',
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(blob_storage_parent.user),
                **blob_storage_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=ANY(NotImplementedError, message='BlobStorageUser_001 has not implemented POST'),
        )

        with suppress(KeyError):
            del blob_storage_parent.user._scenario.failure_handling[None]

        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_storage_parent.user._scenario.failure_handling.update({None: StopUser})
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        blob_storage_parent.user._scenario.failure_handling.update({None: RestartScenario})
        with pytest.raises(StopUser):
            blob_storage_parent.user.request(request)

        download_blob.assert_not_called()
        upload_blob.assert_not_called()

    @pytest.mark.skip(reason='requires credentials, should only execute explicitly during development')
    def test_receive_real(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        parent = grizzly_fixture()

        cls_blob_storage_user = type(
            'BlobStorageUserTest',
            (BlobStorageUser,),
            {
                '__scenario__': grizzly.scenario,
                '__context__': {
                    'auth': {
                        'tenant': '<tenant>',
                        'user': {
                            'username': '<username>',
                            'password': '<password>',
                        },
                    },
                },
                'host': 'bs://<storage account>',
            },
        )

        user = cls_blob_storage_user(grizzly.state.locust.environment)
        parent._user = user

        request = RequestTask(RequestMethod.RECEIVE, name='test', endpoint='<container>/<file>/<path>')

        with caplog.at_level(logging.DEBUG):
            user.on_start()

            parent.user.request(request)

        assert 0  # noqa: PT015
