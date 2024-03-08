"""Unit tests of grizzly.tasks.clients.blobstorage."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from azure.storage.blob import BlobClient, BlobServiceClient, ContentSettings

from grizzly.context import GrizzlyContext
from grizzly.tasks import GrizzlyTask
from grizzly.tasks.clients import BlobStorageClientTask
from grizzly.types import RequestDirection
from tests.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from tests.fixtures import BehaveFixture, GrizzlyFixture


class TestBlobStorageClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        BlobStorageClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
        with pytest.raises(AttributeError, match='BlobStorageClientTask: "" is not supported, must be one of bs, bss'):
            BlobStorageClientTask(
                RequestDirection.TO,
                '',
            )

        with pytest.raises(AssertionError, match='BlobStorageClientTask: source must be set for direction TO'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://?AccountKey=aaaabbb=&Container=my-container',
            )

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: could not find account name in bs://\?AccountKey=aaaabbb=&Container=my-container'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://?AccountKey=aaaabbb=&Container=my-container',
                source='',
            )

        with pytest.raises(AssertionError, match='BlobStorageClientTask: could not find AccountKey in bs://my-storage'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage',
                source='',
            )

        with pytest.raises(AssertionError, match=r'BlobStorageClientTask: could not find Container in bs://my-storage\?AccountKey=aaaabbb='):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bs://my-storage?AccountKey=aaaabbb=',
                source='',
            )

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
            source='',
        )

        assert isinstance(task.service_client, BlobServiceClient)
        for attr, value in [
            ('endpoint', 'bs://my-storage?AccountKey=aaaabbb=&Container=my-container'),
            ('name', None),
            ('source', ''),
            ('payload_variable', None),
            ('metadata_variable', None),
            ('destination', None),
            ('_endpoints_protocol', 'http'),
            ('account_name', 'my-storage'),
            ('account_key', 'aaaabbb='),
            ('container', 'my-container'),
            ('connection_string', 'DefaultEndpointsProtocol=http;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'),
            ('overwrite', False),
            ('__template_attributes__', {'endpoint', 'destination', 'source', 'name', 'variable_template'}),
        ]:
            assert getattr(task, attr) == value

        task = BlobStorageClientTask(
            RequestDirection.TO,
            'bss://my-storage?AccountKey=aaaabbb=&Container=my-container&Overwrite=True',
            'upload-empty-file',
            source='',
        )

        assert isinstance(task.service_client, BlobServiceClient)
        for attr, value in [
            ('endpoint', 'bss://my-storage?AccountKey=aaaabbb=&Container=my-container&Overwrite=True'),
            ('name', 'upload-empty-file'),
            ('source', ''),
            ('payload_variable', None),
            ('metadata_variable', None),
            ('destination', None),
            ('_endpoints_protocol', 'https'),
            ('account_name', 'my-storage'),
            ('account_key', 'aaaabbb='),
            ('container', 'my-container'),
            ('connection_string', 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'),
            ('overwrite', True),
        ]:
            assert getattr(task, attr) == value

        with pytest.raises(ValueError, match='asdf is not a valid boolean'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://my-storage?AccountKey=aaaabbb=&Container=my-container&Overwrite=asdf',
                'upload-empty-file',
                source='',
            )

        with pytest.raises(NotImplementedError, match='BlobStorageClientTask has not implemented support for step text'):
            BlobStorageClientTask(
                RequestDirection.TO,
                'bss://my-storage?AccountKey=aaaabbb=&Container=my-container&Overwrite=True',
                'upload-empty-file',
                source='',
                text='foobar',
            )

    def test_get(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture) -> None:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables['test'] = 'none'

        BlobStorageClientTask.__scenario__ = grizzly.scenario

        task_factory = BlobStorageClientTask(
            RequestDirection.FROM,
            'bs://my-storage?AccountKey=aaaabbb=&Container=my-container',
            payload_variable='test',
        )
        task = task_factory()

        parent = grizzly_fixture()

        with pytest.raises(NotImplementedError, match='BlobStorageClientTask has not implemented GET'):
            task(parent)

    def test_put(self, behave_fixture: BehaveFixture, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
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

            BlobStorageClientTask.__scenario__ = grizzly.scenario

            with pytest.raises(AssertionError, match='BlobStorageClientTask: source must be set for direction TO'):
                BlobStorageClientTask(
                    RequestDirection.TO,
                    'bss://$conf::storage.account$?AccountKey=$conf::storage.account_key$&Container=$conf::storage.container$',
                    source=None,
                    destination='destination.txt',
                )

            task_factory = BlobStorageClientTask(
                RequestDirection.TO,
                'bss://$conf::storage.account$?AccountKey=$conf::storage.account_key$&Container=$conf::storage.container$',
                source='source.json',
                destination='destination.txt',
            )
            for attr, value in [
                ('account_name', 'my-storage'),
                ('account_key', 'aaaa+bbb/64='),
                ('container', 'my-container'),
                ('connection_string', 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaa+bbb/64=;EndpointSuffix=core.windows.net'),
            ]:
                assert getattr(task_factory, attr) == value

            task = task_factory()

            parent = grizzly_fixture()

            request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

            parent.user._context['variables'].update(grizzly.state.variables)

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
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']
