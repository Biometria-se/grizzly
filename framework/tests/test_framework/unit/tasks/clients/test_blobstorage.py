"""Unit tests of grizzly.tasks.clients.blobstorage."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from azure.storage.blob import BlobClient, ContentSettings
from grizzly.tasks.clients import BlobStorageClientTask
from grizzly.testdata import GrizzlyVariables
from grizzly.types import RequestDirection
from grizzly_common.azure.aad import AuthMethod, AzureAadCredential

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture


class TestBlobStorageClientTask:
    def test___init___conn_str(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        BlobStorageClientTask.__scenario__ = grizzly_fixture.grizzly.scenario

        blob_service_client_mock = mocker.patch('grizzly.tasks.clients.blobstorage.BlobServiceClient')

        with pytest.raises(AttributeError, match='BlobStorageClientTask: "" is not supported, must be one of bs, bss'):
            BlobStorageClientTask(
                RequestDirection.TO,
                '',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match='BlobStorageClientTask: source must be set for direction TO'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://?AccountKey=aaaabbb=&Container=my-container',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: could not find storage account name in bs://\?AccountKey=aaaabbb=&Container=my-container'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://?AccountKey=aaaabbb=&Container=my-container',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: container should be the path in the URL, not in the querystring'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: no container name found in URL bs://my-storage/\?AccountKey=aaaabbb='):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage/?AccountKey=aaaabbb=',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match='BlobStorageClientTask: "my/container" is not a valid container name, should be one branch'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage/my/container?AccountKey=aaaabbb=',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match='BlobStorageClientTask: could not find AccountKey in bs://my-storage/my-container'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage/my-container',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bs://my-storage/my-container?AccountKey=aaaabbb=',
            source='',
        )

        for attr, value in [
            ('endpoint', 'bs://my-storage/my-container?AccountKey=aaaabbb='),
            ('name', None),
            ('source', ''),
            ('payload_variable', None),
            ('metadata_variable', None),
            ('destination', None),
            ('_endpoints_protocol', 'http'),
            ('container', 'my-container'),
            ('overwrite', False),
            ('__template_attributes__', {'endpoint', 'destination', 'source', 'name', 'variable_template'}),
        ]:
            assert getattr(task, attr) == value
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_called_once_with(
            conn_str='DefaultEndpointsProtocol=http;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net',
        )
        blob_service_client_mock.reset_mock()

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://my-storage/my-container?AccountKey=aaaabbb=#Overwrite=True',
            'upload-empty-file',
            source='',
        )

        for attr, value in [
            ('endpoint', 'bss://my-storage/my-container?AccountKey=aaaabbb=#Overwrite=True'),
            ('name', 'upload-empty-file'),
            ('source', ''),
            ('payload_variable', None),
            ('metadata_variable', None),
            ('destination', None),
            ('_endpoints_protocol', 'https'),
            ('container', 'my-container'),
            ('overwrite', True),
        ]:
            assert getattr(task, attr) == value

        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_called_once_with(
            conn_str='DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net',
        )
        blob_service_client_mock.reset_mock()

        with pytest.raises(ValueError, match='asdf is not a valid boolean'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://my-storage/my-container?AccountKey=aaaabbb=#Overwrite=asdf',
                'upload-empty-file',
                source='',
            )

        with pytest.raises(NotImplementedError, match='BlobStorageClientTask has not implemented support for step text'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://my-storage/my-container?AccountKey=aaaabbb=#Overwrite=True',
                'upload-empty-file',
                source='',
                text='foobar',
            )

    def test___init___credential(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        BlobStorageClientTask.__scenario__ = grizzly_fixture.grizzly.scenario

        blob_service_client_mock = mocker.patch('grizzly.tasks.clients.blobstorage.BlobServiceClient')

        with pytest.raises(AttributeError, match='BlobStorageClientTask: "" is not supported, must be one of bs, bss'):
            BlobStorageClientTask(
                RequestDirection.TO,
                '',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match='BlobStorageClientTask: source must be set for direction TO'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://username:password@my-storage/my-container',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: could not find storage account name in bs://username:password@/my-container#Tenant=example.com'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://username:password@/my-container#Tenant=example.com',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: container should be the path in the URL, not in the querystring'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://username:password@my-storage?Container=my-container',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: no container name found in URL bs://my-storage/'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage/',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match='BlobStorageClientTask: "my/container" is not a valid container name, should be one branch'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage/my/container',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: no container name found in URL bs://username:password@my-storage#Tenant=example.com'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://username:password@my-storage#Tenant=example.com',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: could not find Tenant in fragments of bs://username:password@my-storage/my-container'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://username:password@my-storage/my-container',
                source='',
            )
        blob_service_client_mock.assert_not_called()
        blob_service_client_mock.from_connection_string.assert_not_called()

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bs://username:password@my-storage/my-container#Tenant=example.com',
            source='',
        )

        for attr, value in [
            ('endpoint', 'bs://username:password@my-storage/my-container#Tenant=example.com'),
            ('name', None),
            ('source', ''),
            ('payload_variable', None),
            ('metadata_variable', None),
            ('destination', None),
            ('_endpoints_protocol', 'http'),
            ('container', 'my-container'),
            ('overwrite', False),
            ('__template_attributes__', {'endpoint', 'destination', 'source', 'name', 'variable_template'}),
        ]:
            assert getattr(task, attr) == value

        blob_service_client_mock.from_connection_string.assert_not_called()
        blob_service_client_mock.assert_called_once_with(
            account_url='http://my-storage.blob.core.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='username',
                password='password',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='http://my-storage.blob.core.windows.net',
            ),
        )
        blob_service_client_mock.reset_mock()

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://username:password@my-storage/my-container#Tenant=example.com&Overwrite=True',
            'upload-empty-file',
            source='',
        )

        for attr, value in [
            ('endpoint', 'bss://username:password@my-storage/my-container#Tenant=example.com&Overwrite=True'),
            ('name', 'upload-empty-file'),
            ('source', ''),
            ('payload_variable', None),
            ('metadata_variable', None),
            ('destination', None),
            ('_endpoints_protocol', 'https'),
            ('container', 'my-container'),
            ('overwrite', True),
        ]:
            actual_value = getattr(task, attr)
            try:
                assert actual_value == value
            except AssertionError:
                print(f'{actual_value} != {value}')

        blob_service_client_mock.from_connection_string.assert_not_called()
        blob_service_client_mock.assert_called_once_with(
            account_url='https://my-storage.blob.core.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='username',
                password='password',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='https://my-storage.blob.core.windows.net',
            ),
        )
        blob_service_client_mock.reset_mock()

        with pytest.raises(ValueError, match='asdf is not a valid boolean'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://username:password@my-storage/my-container#Tenant=example.com&Overwrite=asdf',
                'upload-empty-file',
                source='',
            )

        with pytest.raises(NotImplementedError, match='BlobStorageClientTask has not implemented support for step text'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://username:password@my-storage/my-container#Tenant=example.com&Overwrite=True',
                'upload-empty-file',
                source='',
                text='foobar',
            )

    def test_request_from(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture) -> None:
        behave = behave_fixture.context
        grizzly = cast('GrizzlyContext', behave.grizzly)
        grizzly.scenario.variables['test'] = 'none'

        BlobStorageClientTask.__scenario__ = grizzly.scenario

        task_factory = BlobStorageClientTask(
            RequestDirection.FROM,
            'bs://my-storage/my-container?AccountKey=aaaabbb=',
            payload_variable='test',
        )
        task = task_factory()

        parent = grizzly_fixture()

        with pytest.raises(NotImplementedError, match='BlobStorageClientTask has not implemented GET'):
            task(parent)

    def test_request_to(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        upload_blob_mock = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', autospec=True)

        grizzly = grizzly_fixture.grizzly
        grizzly.scenario.variables.update(
            {
                'test': 'hello world',
                'source': 'source.json',
                'destination': 'destination.json',
            },
        )
        grizzly.state.configuration.update(
            {
                'storage.account': 'my-storage',
                'storage.account_key': 'aaaa+bbb/64=',
                'storage.container': 'my-container',
            },
        )

        BlobStorageClientTask.__scenario__ = grizzly.scenario

        with pytest.raises(AssertionError, match='BlobStorageClientTask: source must be set for direction TO'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://$conf::storage.account$/$conf::storage.container$?AccountKey=$conf::storage.account_key$',
                source=None,
                destination='destination.txt',
            )

        task_factory = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://$conf::storage.account$/$conf::storage.container$?AccountKey=$conf::storage.account_key$',
            source='source.json',
            destination='destination.txt',
        )
        for attr, value in [
            ('container', 'my-container'),
        ]:
            assert getattr(task_factory, attr) == value

        task = task_factory()

        parent = grizzly_fixture()
        # since we haven't received any values
        parent.user.variables = GrizzlyVariables(**grizzly.scenario.variables)

        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        task(parent)

        upload_blob_mock.assert_not_called()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} BlobStorage->my-container',
            response_time=ANY(int),
            response_length=0,
            context=parent.user._context,
            exception=ANY(FileNotFoundError, message='source.json'),
        )
        request_fire_spy.reset_mock()

        test_context = grizzly_fixture.test_context
        source_file = test_context / 'requests' / 'source.json'
        source_file.parent.mkdir(exist_ok=True)
        source_file.write_text('this is my {{ test }} test!')

        task_factory = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://$conf::storage.account$/$conf::storage.container$?AccountKey=$conf::storage.account_key$',
            'test-bss-request',
            source='{{ source }}',
            destination='{{ destination }}',
        )

        task = task_factory()

        task(parent)

        upload_blob_mock.assert_called_once_with(
            SOME(BlobClient, container_name='my-container', blob_name='destination.json'),
            'this is my hello world test!',
            overwrite=False,
            content_settings=SOME(ContentSettings, content_type='application/json'),
        )
        upload_blob_mock.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} test-bss-request',
            response_time=ANY(int),
            response_length=len(b'this is my hello world test!'),
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()

        task_factory.destination = None
        task_factory.overwrite = True

        task(parent)

        upload_blob_mock.assert_called_once_with(
            SOME(BlobClient, container_name='my-container', blob_name='source.json'),
            'this is my hello world test!',
            overwrite=True,
            content_settings=SOME(ContentSettings, content_type='application/json'),
        )
        upload_blob_mock.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='CLTSK',
            name=f'{parent.user._scenario.identifier} test-bss-request',
            response_time=ANY(int),
            response_length=len(b'this is my hello world test!'),
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()
