import shutil
import logging

from os import environ, path
from json import loads as jsonloads

import pytest

from jinja2 import Template
from _pytest.tmpdir import TempPathFactory
from locust.env import Environment
from locust.exception import StopUser

from grizzly.users.base import GrizzlyUser, FileRequests
from grizzly.types import GrizzlyResponse, RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask

from ...fixtures import locust_environment  # pylint: disable=unused-import


logging.getLogger().setLevel(logging.CRITICAL)


class DummyGrizzlyUser(GrizzlyUser):
    def request(self, request: RequestTask) -> GrizzlyResponse:
        return super().request(request)


class TestGrizzlyUser:
    @pytest.mark.usefixtures('locust_environment')
    def test_render(self, locust_environment: Environment, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('renderer_test') / 'requests'
        test_context.mkdir()
        test_file = test_context / 'blobfile.txt'
        test_file.touch()
        test_file_context = path.dirname(path.dirname(str(test_file)))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_file_context

        try:

            user = DummyGrizzlyUser(locust_environment)
            request = RequestTask(RequestMethod.POST, name='test', endpoint='/api/test')

            request.template = Template('hello {{ name }}')
            scenario = GrizzlyContextScenario()
            scenario.name = 'test'
            request.scenario = scenario

            user.add_context({'variables': {'name': 'bob'}})

            assert user.render(request) == ('test', '/api/test', 'hello bob')

            user.set_context_variable('name', 'alice')

            assert user.render(request) == ('test', '/api/test', 'hello alice')

            request.endpoint = '/api/test?data={{ querystring }}'
            user.set_context_variable('querystring', 'querystring_data')
            assert user.render(request) == ('test', '/api/test?data=querystring_data', 'hello alice')

            request.template = None
            assert user.render(request) == ('test', '/api/test?data=querystring_data', None)

            request.name = '{{ name }}'
            assert user.render(request) == ('alice', '/api/test?data=querystring_data', None)

            request.name = '{{ name'
            with pytest.raises(StopUser):
                user.render(request)

            test_file.write_text('this is a test {{ name }}')
            request.name = '{{ name }}'
            request.source = '{{ blobfile }}'
            request.template = Template(request.source)
            user.set_context_variable('blobfile', str(test_file))
            assert user.render(request) == ('alice', '/api/test?data=querystring_data', 'this is a test alice')

            user_type = type(
                'ContextVariablesUserFileRequest',
                (GrizzlyUser, FileRequests, ),
                {},
            )
            user = user_type(locust_environment)
            assert issubclass(user.__class__, (FileRequests,))

            request.source = f'{str(test_file)}'
            request.template = Template(request.source)
            request.endpoint = '/tmp'
            _, endpoint, _ = user.render(request)
            assert endpoint == '/tmp/blobfile.txt'
        finally:
            shutil.rmtree(test_file_context)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_environment')
    def test_render_nested(self, locust_environment: Environment, tmp_path_factory: TempPathFactory) -> None:
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

        print(str(test_file))

        test_file_context = path.dirname(
            path.dirname(
                path.dirname(
                    str(test_file)
                )
            )
        )
        environ['GRIZZLY_CONTEXT_ROOT'] = test_file_context

        try:
            user = DummyGrizzlyUser(locust_environment)
            request = RequestTask(RequestMethod.POST, name='{{ name }}', endpoint='/api/test/{{ value }}')

            request.template = Template('{{ file_path }}')
            scenario = GrizzlyContextScenario()
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

            name, endpoint, payload = user.render(request)

            assert name == 'test-name'
            assert endpoint == '/api/test/test-value'
            assert payload is not None

            data = jsonloads(payload)
            assert data['MeasureResult']['ID'] == user.context_variables['messageID']
            assert data['MeasureResult']['name'] == user.context_variables['name']
            assert data['MeasureResult']['value'] == user.context_variables['value']
        finally:
            shutil.rmtree(test_file_context)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_environment')
    def test_request(self, locust_environment: Environment) -> None:
        user = DummyGrizzlyUser(locust_environment)
        payload = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')

        with pytest.raises(NotImplementedError):
            user.request(payload)

    @pytest.mark.usefixtures('locust_environment')
    def test_context(self, locust_environment: Environment) -> None:
        user = DummyGrizzlyUser(locust_environment)

        context = user.context()

        assert isinstance(context, dict)
        assert context == {'variables': {}}

        user.set_context_variable('test', 'value')
        assert user.context_variables == {'test': 'value'}
