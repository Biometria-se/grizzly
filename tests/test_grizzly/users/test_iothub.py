import json
from typing import Any, Tuple, cast

import pytest

from locust.env import Environment
from locust.exception import StopUser
from azure.storage.blob._blob_client import BlobClient
from azure.iot.device import IoTHubDeviceClient

from pytest_mock import MockerFixture

from grizzly.users.iothub import IotHubUser
from grizzly.users.base.grizzly_user import GrizzlyUser
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.exceptions import RestartScenario

from ...fixtures import GrizzlyFixture


CONNECTION_STRING = 'HostName=some.hostname.nu;DeviceId=my_device;SharedAccessKey=xxxyyyzzz='

IoTHubScenarioFixture = Tuple[IotHubUser, GrizzlyContextScenario, Environment]


class MockedIotHubDeviceClient(IoTHubDeviceClient):
    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    def get_storage_info_for_blob(self, blob_name: Any) -> Any:
        return {
            'hostName': 'some_host',
            'containerName': 'some_container',
            'blobName': 'some_blob',
            'sasToken': 'some_sas_token',
            'correlationId': 'correlation_id'
        }


@pytest.fixture
def iot_hub_scenario(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> IoTHubScenarioFixture:
    def mocked_create_from_connection_string(connection_string: Any, **kwargs: Any) -> IoTHubDeviceClient:
        return MockedIotHubDeviceClient(connection_string)

    mocker.patch('azure.iot.device.IoTHubDeviceClient.create_from_connection_string', mocked_create_from_connection_string)

    environment, user, task = grizzly_fixture(
        CONNECTION_STRING,
        IotHubUser,
    )

    request = grizzly_fixture.request_task.request

    scenario = GrizzlyContextScenario(99)
    scenario.name = task.__class__.__name__
    scenario.user.class_name = 'IotHubUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND

    scenario.tasks.add(request)

    return cast(IotHubUser, user), scenario, environment


class TestIotHubUser:
    @pytest.mark.usefixtures('iot_hub_scenario')
    def test_create(self, iot_hub_scenario: IoTHubScenarioFixture) -> None:
        user, _, environment = iot_hub_scenario
        assert issubclass(user.__class__, GrizzlyUser)

        IotHubUser.host = 'PostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=='
        with pytest.raises(ValueError) as e:
            user = IotHubUser(environment)
        assert 'host needs to start with "HostName="' in str(e)

        IotHubUser.host = 'HostName=my_iot_host_name'
        with pytest.raises(ValueError) as e:
            user = IotHubUser(environment)
        assert 'needs DeviceId and SharedAccessKey in the query string' in str(e)

        IotHubUser.host = 'HostName=my_iot_host_name;SharedAccessKey=xxxyyyyzzz=='
        with pytest.raises(ValueError) as e:
            user = IotHubUser(environment)
        assert 'needs DeviceId in the query string' in str(e)

        IotHubUser.host = 'HostName=my_iot_host_name;DeviceId=my_device'
        with pytest.raises(ValueError) as e:
            user = IotHubUser(environment)
        assert 'needs SharedAccessKey in the query string' in str(e)

    @pytest.mark.usefixtures('iot_hub_scenario')
    def test_send(self, iot_hub_scenario: IoTHubScenarioFixture, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        [user, scenario, _] = iot_hub_scenario

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
        request.endpoint = 'not_relevant'

        upload_blob = mocker.patch('azure.storage.blob._blob_client.BlobClient.upload_blob', autospec=True)
        notify_blob_upload_status = mocker.patch('azure.iot.device.IoTHubDeviceClient.notify_blob_upload_status', autospec=True)

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
        assert json.loads(args[1]) == expected_payload

        blob_client = cast(BlobClient, args[0])

        assert metadata == {}

        json_payload = json.loads(payload)
        assert json_payload['result']['id'] == 'ID-31337'
        assert blob_client.container_name == 'some_container'
        assert blob_client.blob_name == 'some_blobsome_sas_token'

        assert notify_blob_upload_status.call_count == 1
        args, _ = notify_blob_upload_status.call_args_list[-1]
        assert len(args) == 5
        assert args[1] == 'correlation_id'
        assert args[2] is True
        assert args[3] == 200

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

        upload_blob = mocker.patch('azure.storage.blob._blob_client.BlobClient.upload_blob', side_effect=[RuntimeError('failed to upload blob')])
        notify_blob_upload_status.reset_mock()

        request.method = RequestMethod.SEND

        with pytest.raises(RestartScenario):
            user.request(request)

        assert notify_blob_upload_status.call_count == 1
        args, _ = notify_blob_upload_status.call_args_list[-1]
        assert len(args) == 5
        assert args[1] == 'correlation_id'
        assert args[2] is False
        assert args[3] == 500
