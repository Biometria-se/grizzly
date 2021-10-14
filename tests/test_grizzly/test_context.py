import shutil

from typing import Tuple, Dict, Any, Optional
from os import path, environ

import pytest

from _pytest.tmpdir import TempdirFactory
from jinja2.environment import Template
from behave.model import Scenario
from locust.clients import ResponseContextManager
from locust.user.users import User

from grizzly.context import (
    LocustContext,
    LocustContextSetup,
    LocustContextScenario,
    LocustContextScenarioValidation,
    LocustContextScenarioWait,
    LocustContextState,
    RequestContext,
    RequestContextHandlers,
    RequestContextResponse,
    RequestMethod,
    generate_identifier,
    load_configuration_file,
    ResponseContentType,
)


from .helpers import get_property_decorated_attributes
from .fixtures import request_context, locust_context, behave_locust_context  # pylint: disable=unused-import


def test_load_configuration_file(tmpdir_factory: TempdirFactory) -> None:
    configuration_file = tmpdir_factory.mktemp('configuration_file').join('configuration.yaml')
    try:
        configuration_file.write('''
            configuration:
                sut:
                    host: 'https://backend.example.com'
                    auth:
                        refresh_time: 1337
                        client:
                            id: "client id"
                        user:
                            username: username
                            password: password
                            redirect_uri: "https://www.example.com/authenticated"
        ''')

        assert load_configuration_file() == {}

        environ['GRIZZLY_CONFIGURATION_FILE'] = '/tmp/does-not-exist.yaml'

        with pytest.raises(SystemExit):
            load_configuration_file()

        [name, _] = path.splitext(str(configuration_file))

        environ['GRIZZLY_CONFIGURATION_FILE'] = f'{name}.json'

        with pytest.raises(SystemExit):
            load_configuration_file()

        environ['GRIZZLY_CONFIGURATION_FILE'] = str(configuration_file)

        assert load_configuration_file() == {
            'sut.host': 'https://backend.example.com',
            'sut.auth.refresh_time': 1337,
            'sut.auth.client.id': 'client id',
            'sut.auth.user.username': 'username',
            'sut.auth.user.password': 'password',
            'sut.auth.user.redirect_uri': 'https://www.example.com/authenticated',
        }
    finally:
        shutil.rmtree(path.dirname(str(configuration_file)))
        try:
            del environ['GRIZZLY_CONFIGURATION_FILE']
        except KeyError:
            pass


def test_generate_identifier() -> None:
    actual = generate_identifier('A scenario description')
    expected = '25867809'

    assert actual == expected

    actual = generate_identifier('asdfasdfasdfasdfasdfasdfasdfasdf')
    expected = '445c64a0'

    assert actual == expected


class TestLocustContextSetup:
    @pytest.mark.usefixtures('behave_locust_context')
    def test(self, behave_locust_context: LocustContext) -> None:
        locust_context_setup = behave_locust_context.setup

        expected_properties: Dict[str, Tuple[Any, Any]] = {
            'log_level': ('INFO', 'DEBUG'),
            'user_count': (0, 10),
            'spawn_rate': (None, 2),
            'timespan': (None, '10s'),
            'statistics_url': (None, 'influxdb://influx.example.org/grizzly'),
            'global_context': ({}, {'test': 'hello world'}),
        }

        expected_attributes = list(expected_properties.keys())
        expected_attributes.sort()

        assert locust_context_setup is not None and isinstance(locust_context_setup, LocustContextSetup)

        for test_attribute_name, test_attribute_values in expected_properties.items():
            assert hasattr(locust_context_setup, test_attribute_name), f'attribute {test_attribute_name} does not exist in LocustContextSetup'

            [default_value, test_value] = test_attribute_values

            assert getattr(locust_context_setup, test_attribute_name) == default_value
            setattr(locust_context_setup, test_attribute_name, test_value)
            assert getattr(locust_context_setup, test_attribute_name) == test_value

        actual_attributes = list(locust_context_setup.__dict__.keys())
        actual_attributes.sort()

        assert expected_attributes == actual_attributes

    @pytest.mark.usefixtures('behave_locust_context')
    def test_scenarios(self, behave_locust_context: LocustContext) -> None:
        assert len(behave_locust_context.scenarios()) == 0
        assert behave_locust_context.state.variables == {}

        behave_locust_context.add_scenario('test1')
        behave_locust_context.scenario.user_class_name = 'TestUser'
        first_scenario = behave_locust_context.scenario
        assert len(behave_locust_context.scenarios()) == 1
        assert behave_locust_context.state.variables == {}
        assert behave_locust_context.scenario.name == 'test1'

        behave_locust_context.scenario.context['host'] = 'http://test:8000'
        assert behave_locust_context.scenario.context['host'] == 'http://test:8000'


        behave_locust_context.add_scenario('test2')
        behave_locust_context.scenario.user_class_name = 'TestUser'
        behave_locust_context.scenario.context['host'] = 'http://test:8001'
        assert len(behave_locust_context.scenarios()) == 2
        assert behave_locust_context.scenario.name == 'test2'
        assert behave_locust_context.scenario.context['host'] != 'http://test:8000'

        behave_scenario = Scenario(filename=None, line=None, keyword='', name='test3')
        behave_locust_context.add_scenario(behave_scenario)
        behave_locust_context.scenario.user_class_name = 'TestUser'
        third_scenario = behave_locust_context.scenario
        assert behave_locust_context.scenario.name == 'test3'
        assert behave_locust_context.scenario.behave is behave_scenario

        for index, scenario in enumerate(behave_locust_context.scenarios(), start=1):
            assert scenario.name == f'test{index}'

        assert behave_locust_context.get_scenario('test4') is None
        assert behave_locust_context.get_scenario('test1') is None
        assert behave_locust_context.get_scenario('test3') is None
        assert behave_locust_context.get_scenario(first_scenario.get_name()) is first_scenario
        assert behave_locust_context.get_scenario(third_scenario.get_name()) is third_scenario


class TestLocustContextState:
    def test(self) -> None:
        state = LocustContextState()

        expected_properties = {
            'spawning_complete': (False, True),
            'background_section_done': (False, True),
            'variables': ({}, {'test': 'hello'}),
            'configuration': ({}, {'sut.host': 'http://example.com'}),
            'alias': ({}, {'AtomicIntegerIncrementer.test', 'redovisning.iterations'})
        }
        actual_attributes = list(state.__dict__.keys())
        actual_attributes.sort()
        expected_attributes = list(expected_properties.keys())
        expected_attributes.sort()

        assert actual_attributes == expected_attributes

        for test_attribute_name, [default_value, test_value] in expected_properties.items():
            assert test_attribute_name in actual_attributes
            assert hasattr(state, test_attribute_name)
            assert getattr(state, test_attribute_name) == default_value
            setattr(state, test_attribute_name, test_value)
            assert getattr(state, test_attribute_name) == test_value


    def test_configuration(self, tmpdir_factory: TempdirFactory) -> None:
        configuration_file = tmpdir_factory.mktemp('configuration_file').join('configuration.yaml')
        try:
            configuration_file.write('''
                configuration:
                    sut:
                        host: 'https://backend.example.com'
                        auth:
                            refresh_time: 1337
                            client:
                                id: "client id"
                            user:
                                username: username
                                password: password
                                redirect_uri: "https://www.example.com/authenticated"
            ''')

            state = LocustContextState()

            assert state.configuration == {}

            del state

            environ['GRIZZLY_CONFIGURATION_FILE'] = str(configuration_file)

            state = LocustContextState()

            assert state.configuration == {
                'sut.host': 'https://backend.example.com',
                'sut.auth.refresh_time': 1337,
                'sut.auth.client.id': 'client id',
                'sut.auth.user.username': 'username',
                'sut.auth.user.password': 'password',
                'sut.auth.user.redirect_uri': 'https://www.example.com/authenticated',
            }

        finally:
            shutil.rmtree(path.dirname(str(configuration_file)))
            try:
                del environ['GRIZZLY_CONFIGURATION_FILE']
            except KeyError:
                pass


class TestLocustContext:
    @pytest.mark.usefixtures('behave_locust_context')
    def test(self, behave_locust_context: LocustContext) -> None:
        assert behave_locust_context is not None
        assert isinstance(behave_locust_context, LocustContext)

        second_locust_context = LocustContext()

        assert behave_locust_context is second_locust_context

        expected_attributes = [
            'setup',
            'state',
            'scenario',
        ]
        expected_attributes.sort()

        actual_attributes = list(get_property_decorated_attributes(behave_locust_context.__class__))
        actual_attributes.sort()

        for test_attribute in expected_attributes:
            assert hasattr(behave_locust_context, test_attribute)

        assert isinstance(behave_locust_context.setup, LocustContextSetup)
        assert callable(getattr(behave_locust_context, 'scenarios', None))
        assert expected_attributes == actual_attributes

    @pytest.mark.usefixtures('behave_locust_context')
    def test_destroy(self, behave_locust_context: LocustContext) -> None:
        assert behave_locust_context == LocustContext()

        LocustContext.destroy()

        with pytest.raises(ValueError):
            LocustContext.destroy()

        behave_locust_context = LocustContext()

        assert behave_locust_context == LocustContext()


class TestRequestMethod:
    def test(self) -> None:
        with pytest.raises(ValueError):
            RequestMethod.from_string('ASDF')

        assert RequestMethod.from_string('get') == RequestMethod.GET
        assert RequestMethod.from_string('post') == RequestMethod.POST
        assert RequestMethod.from_string('receive') == RequestMethod.RECEIVE
        assert RequestMethod.from_string('SeNd') == RequestMethod.SEND


class TestRequestContextHandlers:
    def tests(self) -> None:
        handlers = RequestContextHandlers()

        assert hasattr(handlers, 'metadata')
        assert hasattr(handlers, 'payload')

        assert len(handlers.metadata) == 0
        assert len(handlers.payload) == 0

        def handler(input: Tuple[ResponseContentType, Any], user: User, manager: Optional[ResponseContextManager]) -> None:
            pass

        handlers.add_metadata(handler)
        handlers.add_payload(handler)

        assert len(handlers.metadata) == 1
        assert len(handlers.payload) == 1


class TestRequestContextResponse:
    def test(self) -> None:
        response_context = RequestContextResponse()
        assert response_context.content_type == ResponseContentType.GUESS

        assert isinstance(response_context.handlers, RequestContextHandlers)

        assert 200 in response_context.status_codes

        response_context.add_status_code(-200)
        assert 200 not in response_context.status_codes

        response_context.add_status_code(200)
        response_context.add_status_code(302)
        assert [200, 302] == response_context.status_codes

        response_context.add_status_code(200)
        assert [200, 302] == response_context.status_codes

        response_context.add_status_code(-302)
        response_context.add_status_code(400)
        assert [200, 400] == response_context.status_codes


class TestRequestContext:
    def test(self) -> None:
        request_context = RequestContext(RequestMethod.from_string('POST'), 'test-name', '/api/test')

        assert request_context.method == RequestMethod.POST
        assert request_context.name == 'test-name'
        assert request_context.endpoint == '/api/test'

        assert not hasattr(request_context, 'scenario')

        assert request_context.template is None
        assert request_context.source is None


class TestLocustContextScenario:
    def test(self) -> None:
        scenario = LocustContextScenario()

        assert scenario._identifier is None
        assert not hasattr(scenario, 'name')
        assert not hasattr(scenario, 'user_class_name')
        assert scenario.iterations == 1
        assert scenario.context == {}
        assert isinstance(scenario.wait, LocustContextScenarioWait)
        assert scenario.tasks == []
        assert isinstance(scenario.validation, LocustContextScenarioValidation)
        assert not scenario.stop_on_failure

        with pytest.raises(ValueError):
            hash_id = scenario.identifier

        scenario.name = 'Test'
        hash_id = scenario.identifier
        assert hash_id == generate_identifier('Test')

        scenario._identifier = None
        assert scenario.get_name() == f'Test_{hash_id}'

        scenario.name = f'Test_{hash_id}'
        assert scenario.get_name() == f'Test_{hash_id}'

        assert not scenario.should_validate()

    @pytest.mark.usefixtures('request_context')
    def test_tasks(self, request_context: Tuple[str, str, RequestContext]) -> None:
        scenario = LocustContextScenario()
        scenario.name = 'TestScenario'
        scenario.context['host'] = 'test'
        scenario.user_class_name = 'TestUser'
        [_, _, request] = request_context

        scenario.add_task(request)

        assert scenario.tasks == [request]
        assert isinstance(scenario.tasks[-1], RequestContext) and scenario.tasks[-1].scenario is scenario

        second_request = RequestContext(RequestMethod.POST, name='Second Request', endpoint='/api/test/2')
        second_request.source = '{"hello": "world!"}'
        second_template = Template(second_request.source)
        second_request.template = second_template
        assert second_request.template is second_template

        scenario.add_task(second_request)
        assert scenario.tasks == [request, second_request]
        assert isinstance(scenario.tasks[-1], RequestContext) and scenario.tasks[-1].scenario is scenario

        scenario.add_task(1.337)
        assert scenario.tasks == [request, second_request, 1.337]
