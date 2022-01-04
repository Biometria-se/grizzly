from typing import Callable, Dict, Tuple, List, cast

import pytest

from pytest_mock import mocker, MockerFixture  # pylint: disable=unused-import

from locust.exception import StopUser
from grizzly.types import RequestMethod
from grizzly.task import UntilRequestTask, RequestTask
from grizzly_extras.transformer import TransformerContentType, transformer
from ..fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestUntilRequestTask:
    @pytest.mark.usefixtures('grizzly_context')
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

    @pytest.mark.usefixtures('grizzly_context')
    def test_implementation(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        _, _, tasks, [_, _, request] = grizzly_context()
        request = cast(RequestTask, request)
        request.response.content_type = TransformerContentType.JSON
        request.method = RequestMethod.GET

        def create_response(status: str) -> Dict[str, Dict[str, str]]:
            return {
                'response': {
                    'status': status,
                }
            }

        request_spy = mocker.patch.object(
            tasks.user,
            'request',
            side_effect=[
                (None, create_response('working')),
                (None, create_response('working')),
                (None, create_response('ready')),
            ],
        )

        time_spy = mocker.patch('grizzly.task.until.time', side_effect=[0.0, 153.5, 0.0, 12.25, 0.0, 1.5])

        fire_spy = mocker.patch.object(
            tasks.user.environment.events.request,
            'fire',
        )

        gsleep_spy = mocker.patch('grizzly.task.until.gsleep', autospec=True)

        task = UntilRequestTask(request, '/status[text()="ready"]')
        implementation = task.implementation()

        with pytest.raises(RuntimeError) as re:
            implementation(tasks)
        assert '/status[text()="ready"] is not a valid expression for JSON' in str(re)

        jsontransformer_orig = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        task = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=100, retries=10')

        with pytest.raises(TypeError) as te:
            task.implementation()
        assert 'could not find a transformer for JSON' in str(te)

        transformer.available[TransformerContentType.JSON] = jsontransformer_orig

        task = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=100, retries=10')
        implementation = task.implementation()

        implementation(tasks)

        assert time_spy.call_count == 2
        assert request_spy.call_count == 3
        assert gsleep_spy.call_count == 3
        call_args_list: List[Tuple[float]] = []
        for args_list in gsleep_spy.call_args_list:
            args, _ = args_list
            call_args_list.append(args)
        assert call_args_list == [(100.0, ), (100.0, ), (100.0, )]

        assert fire_spy.call_count == 1
        _, kwargs = fire_spy.call_args_list[0]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier}, wait=100.0s, retries=10'
        assert kwargs.get('response_time', None) == 153500
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        assert kwargs.get('exception', '') is None

        request_spy = mocker.patch.object(
            tasks.user,
            'request',
            side_effect=[
                (None, create_response('working')),
                (None, create_response('working')),
            ],
        )

        task = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=10, retries=2')
        implementation = task.implementation()

        with pytest.raises(StopUser):
            implementation(tasks)

        assert fire_spy.call_count == 2
        _, kwargs = fire_spy.call_args_list[1]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier}, wait=10.0s, retries=2'
        assert kwargs.get('response_time', None) == 12250
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        exception = kwargs.get('exception', None)
        assert exception is not None
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'found 0 matching values for $.`this`[?status="ready"] in payload'

        request_spy = mocker.patch.object(
            tasks.user,
            'request',
            side_effect=[
                RuntimeError('foo bar'),
            ],
        )

        with pytest.raises(StopUser):
            implementation(tasks)

        assert fire_spy.call_count == 3
        _, kwargs = fire_spy.call_args_list[2]

        assert kwargs.get('request_type', None) == 'UNTL'
        assert kwargs.get('name', None) == f'{request.scenario.identifier}, wait=10.0s, retries=2'
        assert kwargs.get('response_time', None) == 1500
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {}}
        exception = kwargs.get('exception', None)
        assert exception is not None
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'foo bar'
