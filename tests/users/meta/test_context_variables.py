from gevent.monkey import patch_all
patch_all()

import shutil
import logging

from os import environ, path
from json import loads as jsonloads

import pytest

from jinja2 import Template
from _pytest.tmpdir import TempdirFactory
from locust.env import Environment
from locust.exception import StopUser

from grizzly.users.meta import ContextVariables, FileRequests
from grizzly.types import RequestMethod
from grizzly.context import LocustContextScenario, RequestContext

from ...fixtures import locust_environment


logging.getLogger().setLevel(logging.CRITICAL)


class TestContextVariable:
    @pytest.mark.usefixtures('locust_environment')
    def test_render(self, locust_environment: Environment, tmpdir_factory: TempdirFactory) -> None:
        test_file = tmpdir_factory.mktemp('renderer_test').mkdir('requests').join('blobfile.txt')
        test_file_context = path.dirname(path.dirname(str(test_file)))
        environ['LOCUST_CONTEXT_ROOT'] = test_file_context

        try:
            user = ContextVariables(locust_environment)
            request = RequestContext(RequestMethod.POST, name='test', endpoint='/api/test')

            request.template = Template('hello {{ name }}')
            scenario = LocustContextScenario()
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

            test_file.write('this is a test {{ name }}')
            request.name = '{{ name }}'
            request.source = '{{ blobfile }}'
            request.template = Template(request.source)
            user.set_context_variable('blobfile', str(test_file))
            assert user.render(request) == ('alice', '/api/test?data=querystring_data', 'this is a test alice')

            user_type = type(
                'ContextVariablesUserFileRequest',
                (ContextVariables, FileRequests, ),
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
            del environ['LOCUST_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_environment')
    def test_render_nested(self, locust_environment: Environment, tmpdir_factory: TempdirFactory) -> None:
        test_file = tmpdir_factory.mktemp('render_nested').mkdir('requests').mkdir('test').join('payload.j2.json')
        test_file.write('''
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
        environ['LOCUST_CONTEXT_ROOT'] = test_file_context

        try:
            user = ContextVariables(locust_environment)
            request = RequestContext(RequestMethod.POST, name='{{ name }}', endpoint='/api/test/{{ value }}')

            request.template = Template('{{ file_path }}')
            scenario = LocustContextScenario()
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
            del environ['LOCUST_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_environment')
    def test_request(self, locust_environment: Environment) -> None:
        user = ContextVariables(locust_environment)
        payload = RequestContext(RequestMethod.GET, name='test', endpoint='/api/test')

        with pytest.raises(NotImplementedError):
            user.request(payload)

    @pytest.mark.usefixtures('locust_environment')
    def test_context(self, locust_environment: Environment) -> None:
        user = ContextVariables(locust_environment)

        context = user.context()

        assert isinstance(context, dict)
        assert context == {'variables': {}}

        user.set_context_variable('test', 'value')
        assert user.context_variables == {'test': 'value'}
