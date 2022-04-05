from typing import Tuple, List
from json import dumps as jsondumps

import pytest

from pytest_mock import MockerFixture

from locust.exception import StopUser
from grizzly.exceptions import RestartScenario
from grizzly.types import RequestMethod
from grizzly.tasks import UntilRequestTask, RequestTask
from grizzly_extras.transformer import TransformerContentType, transformer

from ..fixtures import GrizzlyFixture


class TestUntilRequestTask:
    def test_create(self) -> None:
        request = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')

        with pytest.raises(ValueError) as ve:
            UntilRequestTask(request, '$.`this`[?status="ready"]')
        assert 'content type must be specified for request' in str(ve)

        request.response.content_type = TransformerContentType.JSON

        with pytest.raises(ValueError) as ve:
            UntilRequestTask(request, '$.`this`[?status="ready"] | foo=bar, bar=foo')
        assert 'unsupported arguments foo, bar' in str(ve)

        task = UntilRequestTask(request, '$.`this`[?status="ready"]')

        assert task.condition == '$.`this`[?status="ready"]'
        assert task.wait == 1.0
        assert task.retries == 3

        task = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=100, retries=10')

        assert task.condition == '$.`this`[?status="ready"]'
        assert task.wait == 100
        assert task.retries == 10

        with pytest.raises(ValueError) as ve:
            UntilRequestTask(request, '$.`this`[?status="ready"] | wait=0.0, retries=10')
        assert 'wait argument cannot be less than 0.1 seconds' in str(ve)

        with pytest.raises(ValueError) as ve:
            UntilRequestTask(request, '$.`this`[?status="ready"] | wait=0.1, retries=0')
        assert 'retries argument cannot be less than 1' in str(ve)

    def test_implementation(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None
        request = grizzly_fixture.request_task.request
        request.response.content_type = TransformerContentType.JSON
        request.method = RequestMethod.GET
        request.name = 'test-request'

        def create_response(status: str) -> str:
            return jsondumps({
                'response': {
                    'status': status,
                }
            })

        request_spy = mocker.patch.object(
            scenario.user,
            'request',
            side_effect=[
                (None, create_response('working')),
                (None, create_response('working')),
                (None, create_response('ready')),
                (None, create_response('ready')),
            ],
        )

        time_spy = mocker.patch('grizzly.tasks.until.time', side_effect=[0.0, 153.5, 0.0, 12.25, 0.0, 12.25, 0.0, 1.5, 0.0, 0.8])

        fire_spy = mocker.patch.object(
            scenario.user.environment.events.request,
            'fire',
        )

        gsleep_spy = mocker.patch('grizzly.tasks.until.gsleep', autospec=True)

        task_factory = UntilRequestTask(request, '/status[text()="ready"]')
        task = task_factory()

        with pytest.raises(RuntimeError) as re:
            task(scenario)
        assert '/status[text()="ready"] is not a valid expression for JSON' in str(re)

        jsontransformer_orig = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        task_factory = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=100, retries=10')

        with pytest.raises(TypeError) as te:
            task_factory()
        assert 'could not find a transformer for JSON' in str(te)

        transformer.available[TransformerContentType.JSON] = jsontransformer_orig

        task_factory = UntilRequestTask(request, "$.`this`[?status='ready'] | wait=100, retries=10")
        task = task_factory()

        task(scenario)

        assert time_spy.call_count == 2
        assert request_spy.call_count == 3
        assert gsleep_spy.call_count == 3
        call_args_list: List[Tuple[float]] = []
        for args_list in gsleep_spy.call_args_list:
            args, _ = args_list
            call_args_list.append(args)
        assert call_args_list == [(100.0, ), (100.0, ), (100.0, )]

        assert fire_spy.call_count == 1
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier} test-request, w=100.0s, r=10'
        assert kwargs.get('response_time', None) == 153500
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        assert kwargs.get('exception', '') is None

        # -->
        scenario.grizzly.state.variables['wait'] = 100.0
        scenario.grizzly.state.variables['retries'] = 10
        task_factory = UntilRequestTask(request, "$.`this`[?status='ready'] | wait='{{ wait }}', retries='{{ retries }}'")
        task = task_factory()

        task(scenario)

        assert time_spy.call_count == 4
        assert request_spy.call_count == 4
        assert gsleep_spy.call_count == 4
        call_args_list = []
        for args_list in gsleep_spy.call_args_list:
            args, _ = args_list
            call_args_list.append(args)
        assert call_args_list == [(100.0, ), (100.0, ), (100.0, ), (100.0, )]

        assert fire_spy.call_count == 2
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier} test-request, w=100.0s, r=10'
        assert kwargs.get('response_time', None) == 12250
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        assert kwargs.get('exception', '') is None
        # <--

        request_spy = mocker.patch.object(
            scenario.user,
            'request',
            side_effect=[
                (None, create_response('working')),
                (None, create_response('working')),
            ],
        )

        request.scenario.failure_exception = StopUser
        task_factory = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=10, retries=2')
        task = task_factory()

        with pytest.raises(StopUser):
            task(scenario)

        assert fire_spy.call_count == 3
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier} test-request, w=10.0s, r=2'
        assert kwargs.get('response_time', None) == 12250
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        exception = kwargs.get('exception', None)
        assert exception is not None
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'found 0 matching values for $.`this`[?status="ready"] in payload after 2 retries and 12250 milliseconds'

        request_spy = mocker.patch.object(
            scenario.user,
            'request',
            side_effect=[
                RuntimeError('foo bar'),
                (None, create_response('working')),
                (None, create_response('working')),
            ],
        )

        request.scenario.failure_exception = RestartScenario
        with pytest.raises(RestartScenario):
            task(scenario)

        assert fire_spy.call_count == 4
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier} test-request, w=10.0s, r=2'
        assert kwargs.get('response_time', None) == 1500
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        exception = kwargs.get('exception', None)
        assert exception is not None
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'foo bar'

        request_spy = mocker.patch.object(
            scenario.user,
            'request',
            side_effect=[
                (None, create_response('working')),
                RuntimeError('foo bar'),
                (None, create_response('working')),
                (None, create_response('ready')),
            ],
        )

        task_factory = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=4, retries=4')
        task = task_factory()

        task(scenario)

        assert fire_spy.call_count == 5
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier} test-request, w=4.0s, r=4'
        assert kwargs.get('response_time', None) == 800
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        assert kwargs.get('exception', '') is None
