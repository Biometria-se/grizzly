import shutil
import logging

from os import environ, path
from json import loads as jsonloads

import pytest

from _pytest.tmpdir import TempPathFactory
from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture
from locust.exception import StopUser

from grizzly.users.base import GrizzlyUser, FileRequests
from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask

from ....fixtures import LocustFixture


logging.getLogger().setLevel(logging.CRITICAL)


class DummyGrizzlyUser(GrizzlyUser):
    host: str = 'http://example.com'

    def request(self, request: RequestTask) -> GrizzlyResponse:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented request')


class TestGrizzlyUser:
    def test_render(self, locust_fixture: LocustFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('renderer_test') / 'requests'
        test_context.mkdir()
        test_file = test_context / 'blobfile.txt'
        test_file.touch()
        test_file_context = path.dirname(path.dirname(str(test_file)))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_file_context

        try:

            user = DummyGrizzlyUser(locust_fixture.env)
            request = RequestTask(RequestMethod.POST, name='test', endpoint='/api/test')

            request.source = 'hello {{ name }}'
            scenario = GrizzlyContextScenario(1)
            scenario.name = 'test'
            request.scenario = scenario

            user.add_context({'variables': {'name': 'bob'}})

            assert user.render(request) == ('test', '/api/test', 'hello bob', None, None)

            user.set_context_variable('name', 'alice')

            assert user.render(request) == ('test', '/api/test', 'hello alice', None, None)

            request.endpoint = '/api/test?data={{ querystring }}'
            user.set_context_variable('querystring', 'querystring_data')
            assert user.render(request) == ('test', '/api/test?data=querystring_data', 'hello alice', None, None)

            request.source = None
            assert user.render(request) == ('test', '/api/test?data=querystring_data', None, None, None)

            request.name = '{{ name }}'
            assert user.render(request) == ('alice', '/api/test?data=querystring_data', None, None, None)

            request.name = '{{ name'
            with pytest.raises(StopUser):
                user.render(request)

            test_file.write_text('this is a test {{ name }}')
            request.name = '{{ name }}'
            request.source = '{{ blobfile }}'
            user.set_context_variable('blobfile', str(test_file))
            assert user.render(request) == ('alice', '/api/test?data=querystring_data', 'this is a test alice', None, None)

            user_type = type(
                'ContextVariablesUserFileRequest',
                (GrizzlyUser, FileRequests, ),
                {
                    'host': 'http://example.io',
                },
            )
            user = user_type(locust_fixture)
            assert issubclass(user.__class__, (FileRequests,))

            request.source = f'{str(test_file)}'
            request.endpoint = '/tmp'
            _, endpoint, _, _, _ = user.render(request)
            assert endpoint == '/tmp/blobfile.txt'

            request = RequestTask(RequestMethod.POST, name='test', endpoint='/api/test | my_argument="{{ argument_variable }}"')
            request.scenario = scenario
            user.set_context_variable('argument_variable', 'argument variable value')
            user.set_context_variable('name', 'donovan')
            request.source = 'hello {{ name }}'
            assert user.render(request) == ('test', '/api/test', 'hello donovan', {'my_argument': 'argument variable value'}, None)

        finally:
            shutil.rmtree(test_file_context)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_fixture')
    def test_render_nested(self, locust_fixture: LocustFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('render_nested') / 'requests' / 'test'
        test_context.mkdir(parents=True)
        test_file = test_context / 'payload.j2.json'
        test_file.touch()
        test_file.write_text('''
        {
            "MeasureResult": {
                "ID": {{ messageID }},
                "name": "{{ name }}",
                "value": "{{ value }}"
            }
        }
        ''')

        test_file_context = path.dirname(
            path.dirname(
                path.dirname(
                    str(test_file)
                )
            )
        )
        environ['GRIZZLY_CONTEXT_ROOT'] = test_file_context

        try:
            user = DummyGrizzlyUser(locust_fixture.env)
            request = RequestTask(RequestMethod.POST, name='{{ name }}', endpoint='/api/test/{{ value }}')

            request.source = '{{ file_path }}'
            scenario = GrizzlyContextScenario(999)
            scenario.name = 'test'
            request.scenario = scenario

            user.add_context({
                'variables': {
                    'name': 'test-name',
                    'value': 'test-value',
                    'messageID': 1337,
                    'file_path': 'test/payload.j2.json',
                }
            })

            name, endpoint, payload, arguments, metadata = user.render(request)

            assert name == 'test-name'
            assert endpoint == '/api/test/test-value'
            assert payload is not None
            assert arguments is None
            assert metadata is None

            data = jsonloads(payload)
            assert data['MeasureResult']['ID'] == user.context_variables['messageID']
            assert data['MeasureResult']['name'] == user.context_variables['name']
            assert data['MeasureResult']['value'] == user.context_variables['value']
        finally:
            shutil.rmtree(test_file_context)
            del environ['GRIZZLY_CONTEXT_ROOT']

    def test_request(self, locust_fixture: LocustFixture) -> None:
        user = DummyGrizzlyUser(locust_fixture.env)
        payload = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')

        with pytest.raises(NotImplementedError):
            user.request(payload)

    def test_context(self, locust_fixture: LocustFixture) -> None:
        user = DummyGrizzlyUser(locust_fixture.env)

        context = user.context()

        assert isinstance(context, dict)
        assert context == {'variables': {}}

        user.set_context_variable('test', 'value')
        assert user.context_variables == {'test': 'value'}

    def test_stop(self, locust_fixture: LocustFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
        user = DummyGrizzlyUser(locust_fixture.env)

        mocker.patch('grizzly.users.base.grizzly_user.User.stop', return_value=True)

        with caplog.at_level(logging.DEBUG):
            assert user._state is None
            assert user._scenario_state is None

            assert user.stop(force=True)

            assert len(caplog.messages) == 0
            assert user._state is None
            assert user._scenario_state is None

            assert not user.stop(force=False)

            assert len(caplog.messages) == 2
            assert caplog.messages[0] == 'stop scenarios before stopping user'
            assert caplog.messages[1] == 'scenario state=None -> ScenarioState.STOPPING'
            assert user._state == 'running'
            assert user._scenario_state == ScenarioState.STOPPING
