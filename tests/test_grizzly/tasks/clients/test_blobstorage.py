from typing import cast

import pytest

from pytest_mock import MockerFixture
from _pytest.tmpdir import TempPathFactory
from azure.storage.blob import BlobServiceClient, BlobClient

from grizzly.context import GrizzlyContext
from grizzly.tasks.clients import BlobStorageClientTask
from grizzly.types import RequestDirection

from ...fixtures import BehaveFixture, GrizzlyFixture


class TestBlobStorageClientTask:
    def test___init__(self, mocker: MockerFixture) -> None:
        with pytest.raises(AttributeError) as ae:
            BlobStorageClientTask(
                RequestDirection.TO,
                '',
            )
        assert 'BlobStorageClientTask: "" is not supported, must be one of bs, bss' == str(ae.value)

        with pytest.raises(ValueError) as ve:
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://?AccountKey=aaaabbb=&Container=my-container',
            )
        assert 'BlobStorageClientTask: could not find account name in bs://?AccountKey=aaaabbb=&Container=my-container' == str(ve.value)

        with pytest.raises(ValueError) as ve:
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage',
            )
        assert 'BlobStorageClientTask: could not find AccountKey in bs://my-storage' == str(ve.value)

        with pytest.raises(ValueError) as ve:
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage?AccountKey=aaaabbb=',
            )
        assert 'BlobStorageClientTask: could not find Container in bs://my-storage?AccountKey=aaaabbb=' == str(ve.value)

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
        )

        assert isinstance(task.service_client, BlobServiceClient)
        assert task.endpoint == 'bs://my-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.source is None
        assert task.variable is None
        assert task.destination is None
        assert task._endpoints_protocol == 'http'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=http;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://my-storage?AccountKey=aaaabbb=&Container=my-container',
        )

        assert isinstance(task.service_client, BlobServiceClient)
        assert task.endpoint == 'bss://my-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.source is None
        assert task.variable is None
        assert task.destination is None
        assert task._endpoints_protocol == 'https'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

    def test_get(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables['test'] = 'none'

        task = BlobStorageClientTask(
            RequestDirection.FROM,
            'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
            variable='test',
        )
        implementation = task.implementation()

        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        with pytest.raises(NotImplementedError) as nie:
            implementation(scenario)
        assert 'BlobStorageClientTask has not implemented GET' in str(nie.value)

    def test_put(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
        upload_blob_mock = mocker.patch('azure.storage.blob._blob_service_client.BlobClient.upload_blob', autospec=True)

        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables.update({
            'test': 'hello world',
            'source': 'source.json',
            'destination': 'destination.json',
        })
        grizzly.state.configuration.update({
            'storage.account': 'my-storage',
            'storage.account_key': 'aaaabbb=',
            'storage.container': 'my-container',
        })

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://$conf::storage.account?AccountKey=$conf::storage.account_key&Container=$conf::storage.container',
            source='source.json',
            destination='destination.json',
        )
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'

        implementation = task.implementation()

        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        scenario.user._context['variables'].update(grizzly.state.variables)

        implementation(scenario)

        assert upload_blob_mock.call_count == 1
        args, _ = upload_blob_mock.call_args_list[-1]
        assert len(args) == 2
        assert isinstance(args[0], BlobClient)
        assert args[1] == 'source.json'
        assert args[0].container_name == 'my-container'
        assert args[0].blob_name == 'destination.json'

        test_context = tmp_path_factory.mktemp('test_context')
        (test_context / 'requests').mkdir()
        (test_context / 'requests' / 'source.json').write_text('this is my {{ test }} test!')

        scenario.user._context_root = str(test_context)

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://$conf::storage.account?AccountKey=$conf::storage.account_key&Container=$conf::storage.container',
            source='{{ source }}',
            destination='{{ destination }}',
        )

        implementation = task.implementation()

        implementation(scenario)

        assert upload_blob_mock.call_count == 2

        args, _ = upload_blob_mock.call_args_list[-1]
        assert len(args) == 2
        assert isinstance(args[0], BlobClient)
        assert args[1] == 'this is my hello world test!'
        assert args[0].container_name == 'my-container'
        assert args[0].blob_name == 'destination.json'

        task.destination = None

        implementation(scenario)

        assert upload_blob_mock.call_count == 3

        args, _ = upload_blob_mock.call_args_list[-1]
        assert len(args) == 2
        assert isinstance(args[0], BlobClient)
        assert args[1] == 'this is my hello world test!'
        assert args[0].container_name == 'my-container'
        assert args[0].blob_name == 'source.json'
