from typing import cast
from os import environ
from pathlib import Path

import pytest

from pytest_mock import MockerFixture
from _pytest.tmpdir import TempPathFactory
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings

from grizzly.context import GrizzlyContext
from grizzly.tasks import GrizzlyTask
from grizzly.tasks.clients import BlobStorageClientTask
from grizzly.types import RequestDirection

from ....fixtures import BehaveFixture, GrizzlyFixture


class TestBlobStorageClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
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
        assert 'BlobStorageClientTask: source must be set for direction TO' == str(ve.value)

        with pytest.raises(ValueError) as ve:
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://?AccountKey=aaaabbb=&Container=my-container',
                source='',
            )
        assert 'BlobStorageClientTask: could not find account name in bs://?AccountKey=aaaabbb=&Container=my-container' == str(ve.value)

        with pytest.raises(ValueError) as ve:
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage',
                source='',
            )
        assert 'BlobStorageClientTask: could not find AccountKey in bs://my-storage' == str(ve.value)

        with pytest.raises(ValueError) as ve:
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage?AccountKey=aaaabbb=',
                source='',
            )
        assert 'BlobStorageClientTask: could not find Container in bs://my-storage?AccountKey=aaaabbb=' == str(ve.value)

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
            source='',
        )

        assert isinstance(task.service_client, BlobServiceClient)
        assert task.endpoint == 'bs://my-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name is None
        assert task.source == ''
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
            'upload-empty-file',
            source='',
        )

        assert isinstance(task.service_client, BlobServiceClient)
        assert task.endpoint == 'bss://my-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'upload-empty-file'
        assert task.source == ''
        assert task.variable is None
        assert task.destination is None
        assert task._endpoints_protocol == 'https'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

    def test_get(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture) -> None:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables['test'] = 'none'

        task_factory = BlobStorageClientTask(
            RequestDirection.FROM,
            'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
            variable='test',
        )
        task = task_factory()

        _, _, scenario = grizzly_fixture()
        assert scenario is not None

        with pytest.raises(NotImplementedError) as nie:
            task(scenario)
        assert 'BlobStorageClientTask has not implemented GET' in str(nie.value)

    def test_put(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
        try:
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
                'storage.account_key': 'aaaa+bbb/64=',
                'storage.container': 'my-container',
            })

            with pytest.raises(ValueError) as ve:
                BlobStorageClientTask(
                    RequestDirection.TO,
                    'bss://$conf::storage.account$?AccountKey=$conf::storage.account_key$&Container=$conf::storage.container$',
                    source=None,
                    destination='destination.txt',
                )
            assert 'BlobStorageClientTask: source must be set for direction TO' == str(ve.value)

            task_factory = BlobStorageClientTask(
                RequestDirection.TO,
                'bss://$conf::storage.account$?AccountKey=$conf::storage.account_key$&Container=$conf::storage.container$',
                source='source.json',
                destination='destination.txt',
            )
            assert task_factory.account_name == 'my-storage'
            assert task_factory.account_key == 'aaaa+bbb/64='
            assert task_factory.container == 'my-container'
            assert task_factory.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaa+bbb/64=;EndpointSuffix=core.windows.net'

            task = task_factory()

            _, _, scenario = grizzly_fixture()
            assert scenario is not None

            request_fire_spy = mocker.spy(scenario.user.environment.events.request, 'fire')

            scenario.user._context['variables'].update(grizzly.state.variables)

            task(scenario)

            assert upload_blob_mock.call_count == 0

            assert request_fire_spy.call_count == 1
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} BlobStorage->my-container'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == 0
            assert kwargs.get('context', None) is scenario.user._context
            exception = kwargs.get('exception', '')
            assert isinstance(exception, FileNotFoundError)
            assert str(exception) == 'source.json'

            test_context = Path(task_factory._context_root)
            (test_context / 'requests').mkdir(exist_ok=True)
            (test_context / 'requests' / 'source.json').write_text('this is my {{ test }} test!')

            environ['GRIZZLY_CONTEXT_ROOT'] = str(test_context)
            GrizzlyTask._context_root = str(test_context)

            task_factory = BlobStorageClientTask(
                RequestDirection.TO,
                'bss://$conf::storage.account$?AccountKey=$conf::storage.account_key$&Container=$conf::storage.container$',
                'test-bss-request',
                source='{{ source }}',
                destination='{{ destination }}',
            )

            task = task_factory()

            task(scenario)

            assert upload_blob_mock.call_count == 1

            args, kwargs = upload_blob_mock.call_args_list[-1]
            assert len(args) == 2
            assert len(kwargs.keys()) == 1
            assert isinstance(args[0], BlobClient)
            assert args[1] == 'this is my hello world test!'
            assert args[0].container_name == 'my-container'
            assert args[0].blob_name == 'destination.json'
            content_settings = kwargs.get('content_settings', None)
            assert isinstance(content_settings, ContentSettings)
            assert content_settings.content_type == 'application/json'

            assert request_fire_spy.call_count == 2
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} test-bss-request'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == len('this is my hello world test!')
            assert kwargs.get('context', None) is scenario.user._context
            assert kwargs.get('exception', '') is None

            task_factory.destination = None

            task(scenario)

            assert upload_blob_mock.call_count == 2

            args, _ = upload_blob_mock.call_args_list[-1]
            assert len(args) == 2
            assert isinstance(args[0], BlobClient)
            assert args[1] == 'this is my hello world test!'
            assert args[0].container_name == 'my-container'
            assert args[0].blob_name == 'source.json'

            assert request_fire_spy.call_count == 3
            _, kwargs = request_fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} test-bss-request'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length') == len('this is my hello world test!')
            assert kwargs.get('context', None) is scenario.user._context
            assert kwargs.get('exception', '') is None
        finally:
            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except KeyError:
                pass
