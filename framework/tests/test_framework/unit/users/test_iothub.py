"""Unit tests for grizzly.users.iothub."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from azure.iot.device import IoTHubDeviceClient, Message
from azure.storage.blob._blob_client import BlobClient
from azure.storage.blob._models import ContentSettings
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.communication import TestdataConsumer
from grizzly.testdata.utils import transform
from grizzly.types import RequestMethod
from grizzly.users.iothub import IotHubUser, IotMessageDecoder, MessageJsonSerializer
from grizzly_common.transformer import TransformerContentType

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    from grizzly.scenarios import GrizzlyScenario

    from test_framework.fixtures import GrizzlyFixture, MockerFixture

CONNECTION_STRING = 'HostName=some.hostname.nu;DeviceId=my_device;SharedAccessKey=xxxyyyzzz='


@dataclass
class ParentFixture:
    parent: GrizzlyScenario
    user: IotHubUser
    consumer_mock: MagicMock
    iot_device_mock: MagicMock
    blob_client_factory_mock: MagicMock
    blob_client_mock: MagicMock


@pytest.fixture
def parent_fixture(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> ParentFixture:
    iot_hub_device_client_mock = mocker.MagicMock(spec=IoTHubDeviceClient)
    mocker.patch('grizzly.users.iothub.IoTHubDeviceClient.create_from_connection_string', return_value=iot_hub_device_client_mock)
    iot_hub_device_client_mock.get_storage_info_for_blob.return_value = {
        'hostName': 'some_host',
        'containerName': 'some_container',
        'blobName': 'some_blob',
        'sasToken': 'some_sas_token',
        'correlationId': 'correlation_id',
    }

    parent = grizzly_fixture(
        CONNECTION_STRING,
        IotHubUser,
    )

    assert isinstance(parent.user, IotHubUser)

    request = grizzly_fixture.request_task.request
    consumer_mock = mocker.MagicMock(spec=TestdataConsumer)
    parent.user.consumer = consumer_mock

    scenario = GrizzlyContextScenario(99, behave=grizzly_fixture.behave.create_scenario(parent.__class__.__name__), grizzly=grizzly_fixture.grizzly)
    scenario.user.class_name = 'IotHubUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND

    scenario.tasks.add(request)

    parent.user._scenario = scenario

    blob_client_mock = mocker.MagicMock(spec=BlobClient)
    blob_client_factory_mock = mocker.patch('azure.storage.blob._blob_client.BlobClient.from_blob_url', return_value=blob_client_mock)
    blob_client_mock.__enter__.return_value.upload_blob.return_value = {}

    return ParentFixture(
        parent=parent,
        user=parent.user,
        consumer_mock=consumer_mock,
        iot_device_mock=iot_hub_device_client_mock,
        blob_client_factory_mock=blob_client_factory_mock,
        blob_client_mock=blob_client_mock.__enter__.return_value,
    )


def test_message_json_serializer() -> None:
    uuid = UUID(bytes=b'foobar31337fooba', version=4)
    decoder = MessageJsonSerializer()

    assert decoder.default(uuid) == '666f6f62-6172-4331-b333-37666f6f6261'
    assert decoder.default(b'foobar') == 'foobar'

    with pytest.raises(TypeError):
        assert decoder.default(10) == 10


def test_iot_message_decoder(mocker: MockerFixture) -> None:
    decoder = IotMessageDecoder(arg='message')

    instance_mock = mocker.MagicMock()
    instance_mock.device_id = 'foobar'

    message = Message(None, message_id='foobar', content_encoding='utf-8', content_type='application/json')
    message.custom_properties = {'bar': 'foo'}

    actual_metrics, actual_tags = decoder(instance_mock, tags={'foo': 'bar'}, return_value=None, message=message, exception=None)

    assert actual_metrics == {'error': None, 'size': ANY(int), 'message_id': 'foobar'}
    assert actual_tags == {'identifier': instance_mock.device_id, 'foo': 'bar', 'bar': 'foo'}

    exception = RuntimeError('error')

    actual_metrics, actual_tags = decoder(instance_mock, tags={'foo': 'bar'}, return_value=None, message=message, exception=exception)

    assert actual_metrics == {'error': str(exception), 'size': ANY(int), 'message_id': 'foobar'}
    assert actual_tags == {'identifier': instance_mock.device_id, 'foo': 'bar', 'bar': 'foo'}

    actual_metrics, actual_tags = decoder(instance_mock, tags=None, return_value=None, message=message, exception=exception)

    assert actual_metrics == {'error': str(exception), 'size': ANY(int), 'message_id': 'foobar'}
    assert actual_tags == {'identifier': instance_mock.device_id, 'bar': 'foo'}


class TestIotHubUser:
    def test_on_start(self, parent_fixture: ParentFixture) -> None:
        assert not hasattr(parent_fixture.user, 'iot_client')

        parent_fixture.user.on_start()

        assert hasattr(parent_fixture.user, 'iot_client')
        parent_fixture.consumer_mock.keystore_inc.assert_not_called()

    def test_on_stop(self, mocker: MockerFixture, parent_fixture: ParentFixture) -> None:
        user = parent_fixture.user
        assert isinstance(user, IotHubUser)

        user.on_start()  # create `iot_device`
        on_stop_spy = mocker.patch.object(user.iot_client, 'shutdown', return_value=None)

        user.on_stop()

        on_stop_spy.assert_called_once_with()

    def test__init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        test_cls = type('IotHubTestUser', (IotHubUser,), {'__scenario__': grizzly_fixture.grizzly.scenario})
        environment = grizzly_fixture.grizzly.state.locust.environment

        assert issubclass(test_cls, IotHubUser)

        test_cls.host = 'PostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=='
        with pytest.raises(ValueError, match='host needs to start with "HostName="'):
            test_cls(environment)

        test_cls.host = 'HostName=my_iot_host_name'
        with pytest.raises(ValueError, match='needs DeviceId and SharedAccessKey in the query string'):
            test_cls(environment)

        test_cls.host = 'HostName=my_iot_host_name;SharedAccessKey=xxxyyyyzzz=='
        with pytest.raises(ValueError, match='needs DeviceId in the query string'):
            test_cls(environment)

        test_cls.host = 'HostName=my_iot_host_name;DeviceId=my_device'
        with pytest.raises(ValueError, match='needs SharedAccessKey in the query string'):
            test_cls(environment)

        test_cls.host = CONNECTION_STRING
        test = test_cls(environment)

        assert test.host == CONNECTION_STRING
        assert test.device_id == 'my_device'

    def test_send(self, parent_fixture: ParentFixture) -> None:
        user = parent_fixture.user
        assert isinstance(user, IotHubUser)
        user.on_start()

        grizzly = user._scenario.grizzly

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
        old_request = cast('RequestTask', user._scenario.tasks()[-1])
        request = RequestTask(RequestMethod.SEND, 'test-send', 'not_relevant | allow_aready_exists=True', old_request.source)
        user._scenario.tasks().clear()
        user._scenario.tasks.add(request)
        user.add_context(remote_variables)

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

        metadata, payload = user.request(request)

        assert payload is not None
        parent_fixture.blob_client_mock.upload_blob.assert_called_once_with(json.dumps(expected_payload, indent=4))
        parent_fixture.blob_client_factory_mock.assert_called_once_with('https://some_host/some_container/some_blobsome_sas_token')
        parent_fixture.blob_client_mock.reset_mock()
        parent_fixture.blob_client_factory_mock.reset_mock()

        assert metadata == {'sasUrl': 'https://some_host/some_container/some_blobsome_sas_token'}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'

        parent_fixture.iot_device_mock.notify_blob_upload_status.assert_called_once_with(
            correlation_id='correlation_id',
            is_success=True,
            status_code=200,
            status_description='OK: not_relevant',
        )
        parent_fixture.iot_device_mock.notify_blob_upload_status.reset_mock()

        request = cast('RequestTask', user._scenario.tasks()[-1])

        user.request(request)

        user._scenario.failure_handling.update({None: RestartScenario})
        parent_fixture.blob_client_mock.upload_blob.side_effect = [RuntimeError('failed to upload blob')]
        parent_fixture.iot_device_mock.notify_blob_upload_status.reset_mock()

        request.method = RequestMethod.SEND

        with pytest.raises(RestartScenario):
            user.request(request)

        parent_fixture.iot_device_mock.notify_blob_upload_status.assert_called_once_with(
            correlation_id='correlation_id',
            is_success=False,
            status_code=500,
            status_description='Failed: not_relevant',
        )
        parent_fixture.iot_device_mock.notify_blob_upload_status.reset_mock()

    def test_send_gzip(self, parent_fixture: ParentFixture, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        user = parent_fixture.user
        assert isinstance(parent_fixture.user, IotHubUser)
        user.on_start()
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
        user.add_context(remote_variables)
        request = cast('RequestTask', user._scenario.tasks()[-1])
        request.endpoint = 'not_relevant'
        request.response.content_type = TransformerContentType.OCTET_STREAM_UTF8

        request = cast('RequestTask', user._scenario.tasks()[-1])

        gzip_compress = mocker.patch('gzip.compress', autospec=True, return_value='this_is_compressed')
        request.metadata = {}
        request.metadata['content_encoding'] = 'gzip'

        user.request(request)

        gzip_compress.assert_called_once()
        parent_fixture.blob_client_mock.upload_blob.assert_called_once_with(
            'this_is_compressed',
            content_settings=SOME(
                ContentSettings,
                content_type='application/octet-stream; charset=utf-8',
                content_encoding='gzip',
            ),
        )

        parent_fixture.iot_device_mock.notify_blob_upload_status.assert_called_once_with(
            correlation_id='correlation_id',
            is_success=True,
            status_code=200,
            status_description='OK: not_relevant',
        )

    def test_send_unhandled_encoding(self, parent_fixture: ParentFixture, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        user = parent_fixture.user

        assert isinstance(user, IotHubUser)
        user.on_start()
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
        user.add_context(remote_variables)
        request = cast('RequestTask', user._scenario.tasks()[-1])
        request.endpoint = 'not_relevant'

        gzip_compress = mocker.patch('gzip.compress', autospec=True, return_value='this_is_compressed')
        request.metadata = {}
        request.metadata['content_type'] = 'application/octet-stream; charset=utf-8'
        request.metadata['content_encoding'] = 'vulcan'

        user._scenario.failure_handling.update({None: RestartScenario})
        with pytest.raises(RestartScenario):
            user.request(request)

        gzip_compress.assert_not_called()
        parent_fixture.blob_client_mock.upload_blob.assert_not_called()
        parent_fixture.iot_device_mock.notify_blob_upload_status.assert_called_once_with(
            correlation_id='correlation_id',
            is_success=False,
            status_code=500,
            status_description='Failed: not_relevant',
        )

    def test_send_empty_payload(self, parent_fixture: ParentFixture, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        assert isinstance(parent_fixture.user, IotHubUser)
        parent_fixture.user.on_start()
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
        parent_fixture.user.add_context(remote_variables)
        request = cast('RequestTask', parent_fixture.user._scenario.tasks()[-1])
        request.endpoint = 'not_relevant'

        gzip_compress = mocker.patch('gzip.compress', autospec=True, return_value='this_is_compressed')
        request.source = None

        parent_fixture.user._scenario.failure_handling.update({None: RestartScenario})
        with pytest.raises(RestartScenario):
            parent_fixture.user.request(request)

        gzip_compress.assert_not_called()
        parent_fixture.blob_client_mock.upload_blob.assert_not_called()
        parent_fixture.iot_device_mock.notify_blob_upload_status.assert_not_called()

    def test__extract(self) -> None:
        assert IotHubUser._extract({'foo': {'bar': 'baz'}}, '$.foo.bar') == ['baz']
        assert IotHubUser._extract('{"foo": {"bar": "baz"}}', '$.foo.bar') == ['baz']
