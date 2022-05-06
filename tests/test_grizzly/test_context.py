import shutil

from typing import Tuple, Dict, Any
from os import path, environ

import pytest

from _pytest.tmpdir import TempPathFactory
from jinja2.environment import Template
from behave.model import Scenario

from grizzly.context import (
    GrizzlyContext,
    GrizzlyContextSetup,
    GrizzlyContextScenario,
    GrizzlyContextScenarioValidation,
    GrizzlyContextScenarioWait,
    GrizzlyContextState,
    load_configuration_file,
)

from grizzly.types import RequestMethod
from grizzly.tasks import LogMessage, RequestTask, WaitTask


from ..helpers import get_property_decorated_attributes
from ..fixtures import BehaveFixture, RequestTaskFixture


def test_load_configuration_file(tmp_path_factory: TempPathFactory) -> None:
    configuration_file = tmp_path_factory.mktemp('configuration_file') / 'configuration.yaml'
    try:
        configuration_file.write_text('''
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


class TestGrizzlyContextSetup:
    def test(self, behave_fixture: BehaveFixture) -> None:
        behave = behave_fixture.context
        grizzly = getattr(behave, 'grizzly')
        grizzly_setup = grizzly.setup

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

        assert grizzly_setup is not None and isinstance(grizzly_setup, GrizzlyContextSetup)

        for test_attribute_name, test_attribute_values in expected_properties.items():
            assert hasattr(grizzly_setup, test_attribute_name), f'attribute {test_attribute_name} does not exist in GrizzlyContextSetup'

            [default_value, test_value] = test_attribute_values

            assert getattr(grizzly_setup, test_attribute_name) == default_value
            setattr(grizzly_setup, test_attribute_name, test_value)
            assert getattr(grizzly_setup, test_attribute_name) == test_value

        actual_attributes = list(grizzly_setup.__dict__.keys())
        actual_attributes.sort()

        assert expected_attributes == actual_attributes

    def test_scenarios(self, behave_fixture: BehaveFixture) -> None:
        behave = behave_fixture.context
        grizzly = getattr(behave, 'grizzly')
        assert len(grizzly.scenarios()) == 0
        assert grizzly.state.variables == {}

        grizzly.add_scenario('test1')
        grizzly.scenario.user_class_name = 'TestUser'
        first_scenario = grizzly.scenario
        assert len(grizzly.scenarios()) == 1
        assert grizzly.state.variables == {}
        assert grizzly.scenario.name == 'test1'

        grizzly.scenario.context['host'] = 'http://test:8000'
        assert grizzly.scenario.context['host'] == 'http://test:8000'

        grizzly.add_scenario('test2')
        grizzly.scenario.user_class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test:8001'
        assert len(grizzly.scenarios()) == 2
        assert grizzly.scenario.name == 'test2'
        assert grizzly.scenario.context['host'] != 'http://test:8000'

        behave_scenario = Scenario(filename=None, line=None, keyword='', name='test3')
        grizzly.add_scenario(behave_scenario)
        grizzly.scenario.user_class_name = 'TestUser'
        third_scenario = grizzly.scenario
        assert grizzly.scenario.name == 'test3'
        assert grizzly.scenario.behave is behave_scenario

        for index, scenario in enumerate(grizzly.scenarios(), start=1):
            assert scenario.name == f'test{index}'

        assert grizzly.get_scenario('test4') is None
        assert grizzly.get_scenario('test1') is None
        assert grizzly.get_scenario('test3') is None
        assert grizzly.get_scenario(first_scenario.get_name()) is first_scenario
        assert grizzly.get_scenario(third_scenario.get_name()) is third_scenario


class TestGrizzlyContextState:
    def test(self) -> None:
        state = GrizzlyContextState()

        expected_properties = {
            'spawning_complete': (False, True),
            'background_section_done': (False, True),
            'variables': ({}, {'test': 'hello'}),
            'configuration': ({}, {'sut.host': 'http://example.com'}),
            'alias': ({}, {'AtomicIntegerIncrementer.test', 'redovisning.iterations'}),
            'verbose': (False, True),
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

    def test_configuration(self, tmp_path_factory: TempPathFactory) -> None:
        configuration_file = tmp_path_factory.mktemp('configuration_file') / 'configuration.yaml'
        try:
            configuration_file.write_text('''
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

            state = GrizzlyContextState()

            assert state.configuration == {}

            del state

            environ['GRIZZLY_CONFIGURATION_FILE'] = str(configuration_file)

            state = GrizzlyContextState()

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


class TestGrizzlyContext:
    def test(self, behave_fixture: BehaveFixture) -> None:
        behave = behave_fixture.context
        grizzly = getattr(behave, 'grizzly')
        assert grizzly is not None
        assert isinstance(grizzly, GrizzlyContext)

        second_grizzly = GrizzlyContext()

        assert grizzly is second_grizzly

        expected_attributes = [
            'setup',
            'state',
            'scenario',
        ]
        expected_attributes.sort()

        actual_attributes = list(get_property_decorated_attributes(grizzly.__class__))
        actual_attributes.sort()

        for test_attribute in expected_attributes:
            assert hasattr(grizzly, test_attribute)

        assert isinstance(grizzly.setup, GrizzlyContextSetup)
        assert callable(getattr(grizzly, 'scenarios', None))
        assert expected_attributes == actual_attributes

    def test_destroy(self, behave_fixture: BehaveFixture) -> None:
        behave = behave_fixture.context
        grizzly = getattr(behave, 'grizzly')
        assert grizzly is GrizzlyContext()

        GrizzlyContext.destroy()

        with pytest.raises(ValueError):
            GrizzlyContext.destroy()

        grizzly = GrizzlyContext()

        assert grizzly is GrizzlyContext()


class TestGrizzlyContextScenario:
    @pytest.mark.parametrize('index', [
        1,
        12,
        99,
        104,
        999,
        1004,
    ])
    def test(self, index: int) -> None:
        scenario = GrizzlyContextScenario(index)
        identifier = f'{index:03}'

        assert scenario.index == index
        assert scenario.identifier == identifier
        assert not hasattr(scenario, 'name')
        assert not hasattr(scenario, 'user_class_name')
        assert scenario.iterations == 1
        assert scenario.context == {}
        assert isinstance(scenario.wait, GrizzlyContextScenarioWait)
        assert scenario.tasks == []
        assert isinstance(scenario.validation, GrizzlyContextScenarioValidation)
        assert not scenario.failure_exception

        scenario.name = 'Test'
        assert scenario.get_name() == f'Test_{identifier}'

        scenario.name = f'Test_{identifier}'
        assert scenario.get_name() == f'Test_{identifier}'

        assert not scenario.should_validate()

    def test_tasks(self, request_task: RequestTaskFixture) -> None:
        scenario = GrizzlyContextScenario(1)
        scenario.name = 'TestScenario'
        scenario.context['host'] = 'test'
        scenario.user.class_name = 'TestUser'
        request = request_task.request

        scenario.add_task(request)

        assert scenario.tasks == [request]
        assert isinstance(scenario.tasks[-1], RequestTask) and scenario.tasks[-1].scenario is scenario

        second_request = RequestTask(RequestMethod.POST, name='Second Request', endpoint='/api/test/2')
        second_request.source = '{"hello": "world!"}'
        assert isinstance(second_request.template, Template)

        scenario.add_task(second_request)
        assert scenario.tasks == [request, second_request]
        assert isinstance(scenario.tasks[-1], RequestTask) and scenario.tasks[-1].scenario is scenario

        wait_task = WaitTask(time=1.337)
        scenario.add_task(wait_task)
        assert scenario.tasks == [request, second_request, wait_task]

        log_task = LogMessage(message='hello general')
        scenario.add_task(log_task)
        assert scenario.tasks == [request, second_request, wait_task, log_task]
