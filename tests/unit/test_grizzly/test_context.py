import shutil

from typing import Tuple, Dict, Any, Optional, cast
from os import path, environ
from unittest.mock import ANY

import pytest

from _pytest.tmpdir import TempPathFactory
from jinja2.environment import Template

from grizzly.types.behave import Scenario
from grizzly.types.locust import Environment, Message
from grizzly.context import (
    GrizzlyContext,
    GrizzlyContextScenarios,
    GrizzlyContextSetup,
    GrizzlyContextScenario,
    GrizzlyContextScenarioValidation,
    GrizzlyContextSetupLocust,
    GrizzlyContextSetupLocustMessages,
    GrizzlyContextState,
    GrizzlyContextTasks,
    GrizzlyContextTasksTmp,
    load_configuration_file,
)

from grizzly.types import MessageDirection, RequestMethod
from grizzly.tasks import LogMessageTask, RequestTask, WaitTask, AsyncRequestGroupTask, LoopTask, ConditionalTask


from tests.helpers import TestTask, get_property_decorated_attributes
from tests.fixtures import BehaveFixture, GrizzlyFixture, RequestTaskFixture


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

        expected_properties: Dict[str, Optional[Tuple[Any, Any]]] = {
            'log_level': ('INFO', 'DEBUG'),
            'user_count': (0, 10),
            'spawn_rate': (None, 2),
            'timespan': (None, '10s'),
            'statistics_url': (None, 'influxdb://influx.example.org/grizzly'),
            'global_context': ({}, {'test': 'hello world'}),
            'locust': None,
        }

        expected_attributes = list(expected_properties.keys())
        expected_attributes.sort()

        assert grizzly_setup is not None and isinstance(grizzly_setup, GrizzlyContextSetup)

        for test_attribute_name, test_attribute_values in expected_properties.items():
            assert hasattr(grizzly_setup, test_attribute_name), f'attribute {test_attribute_name} does not exist in GrizzlyContextSetup'

            if test_attribute_values is not None:
                default_value, test_value = test_attribute_values

                assert getattr(grizzly_setup, test_attribute_name) == default_value
                setattr(grizzly_setup, test_attribute_name, test_value)
                assert getattr(grizzly_setup, test_attribute_name) == test_value

        actual_attributes = list(grizzly_setup.__dict__.keys())
        actual_attributes.sort()

        assert expected_attributes == actual_attributes

        assert isinstance(grizzly_setup.locust, GrizzlyContextSetupLocust)
        assert isinstance(grizzly_setup.locust.messages, GrizzlyContextSetupLocustMessages)


class TestGrizzlyContextSetupLocustMessages:
    def test_register(self) -> None:
        context = GrizzlyContextSetupLocustMessages()

        assert isinstance(context, dict)
        assert context == {}

        def callback(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        def callback_ack(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
            pass

        context.register(MessageDirection.SERVER_CLIENT, 'test_message', callback)

        assert context.get(MessageDirection.SERVER_CLIENT, {}) == {
            'test_message': callback,
        }

        context.register(MessageDirection.CLIENT_SERVER, 'test_message_ack', callback_ack)

        assert context == {
            MessageDirection.SERVER_CLIENT: {
                'test_message': callback,
            },
            MessageDirection.CLIENT_SERVER: {
                'test_message_ack': callback_ack,
            }
        }


class TestGrizzlyContextState:
    def test(self) -> None:
        state = GrizzlyContextState()

        expected_properties = {
            'spawning_complete': (False, True),
            'background_section_done': (False, True),
            'variables': ({}, {'test': 'hello'}),
            'configuration': ({}, {'sut.host': 'http://example.com'}),
            'alias': ({}, {'AtomicIntegerIncrementer.test': 'auth.randomseed'}),
            'verbose': (False, True),
            'persistent': ({}, {'AtomicIntegerIncrementer.persist': '1 | step=10, persist=True'}),
            '_jinja2': (ANY, ANY),
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

        grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

        expected_attributes = [
            'setup',
            'state',
            'scenario',
            'scenarios',
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


class TestGrizzlyContextScenarios:
    def test(self) -> None:
        scenarios = GrizzlyContextScenarios()

        assert issubclass(scenarios.__class__, list)
        assert len(scenarios) == 0

        behave_scenario = Scenario(filename=None, line=None, keyword='', name='test-1')
        scenarios.create(behave_scenario)
        assert len(scenarios) == 1
        assert scenarios[-1].name == 'test-1'
        assert scenarios[-1].description == 'test-1'
        assert scenarios[-1].class_name == 'test-1_001'
        assert scenarios[-1].behave is behave_scenario

        behave_scenario = Scenario(filename=None, line=None, keyword='', name='test-2')
        scenarios.create(behave_scenario)
        assert len(scenarios) == 2
        assert scenarios[-1].name == 'test-2'
        assert scenarios[-1].description == 'test-2'
        assert scenarios[-1].class_name == 'test-2_002'
        assert scenarios[-1].behave is behave_scenario

        assert len(scenarios()) == 2

        assert scenarios.find_by_class_name('test-2_002') is scenarios[-1]
        assert scenarios.find_by_name('test-1') is scenarios[-2]


class TestGrizzlyContextScenario:
    @pytest.mark.parametrize('index', [
        1,
        12,
        99,
        104,
        999,
        1004,
    ])
    def test(self, index: int, behave_fixture: BehaveFixture) -> None:
        scenario = GrizzlyContextScenario(index, behave=behave_fixture.create_scenario('Test'))
        identifier = f'{index:03}'

        assert scenario.index == index
        assert scenario.identifier == identifier
        assert scenario.name == 'Test'
        assert scenario.description == 'Test'
        assert not hasattr(scenario, 'user_class_name')
        assert scenario.iterations == 1
        assert scenario.context == {}
        assert scenario.tasks() == []
        assert getattr(scenario, 'pace', '') is None
        assert isinstance(scenario.validation, GrizzlyContextScenarioValidation)
        assert not scenario.failure_exception

        assert scenario.class_name == f'Test_{identifier}'

        scenario.name = f'Test_{identifier}'
        assert scenario.class_name == f'Test_{identifier}'

        assert not scenario.should_validate()

    def test_tasks(self, request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> None:
        scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('TestScenario'))
        scenario.context['host'] = 'test'
        scenario.user.class_name = 'TestUser'
        request = request_task.request

        scenario.tasks.add(request)

        assert scenario.tasks() == [request]

        second_request = RequestTask(RequestMethod.POST, name='Second Request', endpoint='/api/test/2')
        second_request.source = '{"hello": "world!"}'
        assert isinstance(second_request.template, Template)

        scenario.tasks.add(second_request)
        assert scenario.tasks() == [request, second_request]

        wait_task = WaitTask(time_expression='1.337')
        scenario.tasks.add(wait_task)
        assert scenario.tasks() == [request, second_request, wait_task]

        log_task = LogMessageTask(message='hello general')
        scenario.tasks.add(log_task)
        assert scenario.tasks() == [request, second_request, wait_task, log_task]

    def test_scenarios(self, behave_fixture: BehaveFixture) -> None:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        assert len(grizzly.scenarios()) == 0
        assert grizzly.state.variables == {}

        grizzly.scenarios.create(behave_fixture.create_scenario('test1'))
        grizzly.scenario.user.class_name = 'TestUser'
        first_scenario = grizzly.scenario
        assert len(grizzly.scenarios()) == 1
        assert grizzly.state.variables == {}
        assert grizzly.scenario.name == 'test1'

        grizzly.scenario.context['host'] = 'http://test:8000'
        assert grizzly.scenario.context['host'] == 'http://test:8000'

        grizzly.scenarios.create(behave_fixture.create_scenario('test2'))
        grizzly.scenario.user.class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test:8001'
        assert len(grizzly.scenarios()) == 2
        assert grizzly.scenario.name == 'test2'
        assert grizzly.scenario.context['host'] != 'http://test:8000'

        behave_scenario = Scenario(filename=None, line=None, keyword='', name='test3')
        grizzly.scenarios.create(behave_scenario)
        grizzly.scenario.user.class_name = 'TestUser'
        third_scenario = grizzly.scenario
        assert grizzly.scenario.name == 'test3'
        assert grizzly.scenario.behave is behave_scenario

        for index, scenario in enumerate(grizzly.scenarios(), start=1):
            assert scenario.name == f'test{index}'

        assert grizzly.scenarios.find_by_name('test4') is None
        assert grizzly.scenarios.find_by_name('test1') is first_scenario
        assert grizzly.scenarios.find_by_name('test3') is third_scenario
        assert grizzly.scenarios.find_by_class_name(first_scenario.class_name) is first_scenario
        assert grizzly.scenarios.find_by_class_name(third_scenario.class_name) is third_scenario


class TestGrizzlyContextTasksTmp:
    def test_async_group(self) -> None:
        tmp = GrizzlyContextTasksTmp()

        assert len(tmp.__stack__) == 0

        async_group = AsyncRequestGroupTask(name='async-group-1')
        tmp.async_group = async_group

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is async_group

        with pytest.raises(AssertionError) as ae:
            tmp.async_group = AsyncRequestGroupTask(name='async-group-2')
        assert str(ae.value) == 'async_group is already in stack'

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is async_group

        tmp.async_group = None

        assert len(tmp.__stack__) == 0

        with pytest.raises(AssertionError) as ae:
            tmp.async_group = None
        assert str(ae.value) == 'async_group is not in stack'

        assert len(tmp.__stack__) == 0

    def test_conditional(self) -> None:
        tmp = GrizzlyContextTasksTmp()

        assert len(tmp.__stack__) == 0

        conditional = ConditionalTask('cond-1', '{{ value | int > 0 }}')
        tmp.conditional = conditional

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is conditional

        with pytest.raises(AssertionError) as ae:
            tmp.conditional = ConditionalTask('cond-2', '{{ value | int > 10 }}')
        assert str(ae.value) == 'conditional is already in stack'

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is conditional

        tmp.conditional = None

        assert len(tmp.__stack__) == 0

        with pytest.raises(AssertionError) as ae:
            tmp.conditional = None
        assert str(ae.value) == 'conditional is not in stack'

        assert len(tmp.__stack__) == 0

    def test_loop(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        tmp = GrizzlyContextTasksTmp()

        assert len(tmp.__stack__) == 0

        grizzly.state.variables['loop_value'] = 'none'

        loop = LoopTask(grizzly, 'loop-1', '["hello", "world"]', "loop_value")
        tmp.loop = loop

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is loop

        with pytest.raises(AssertionError) as ae:
            tmp.loop = LoopTask(grizzly, 'loop-2', '["hello", "world"]', "loop_value")
        assert str(ae.value) == 'loop is already in stack'

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is loop

        tmp.loop = None

        assert len(tmp.__stack__) == 0

        with pytest.raises(AssertionError) as ae:
            tmp.loop = None
        assert str(ae.value) == 'loop is not in stack'

        assert len(tmp.__stack__) == 0

    def test___stack__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.state.variables['loop_value'] = 'none'

        tmp = GrizzlyContextTasksTmp()

        assert len(tmp.__stack__) == 0

        conditional = ConditionalTask('cond-1', '{{ value | int > 0 }}')
        tmp.conditional = conditional

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is conditional

        loop = LoopTask(grizzly, 'loop-1', '["hello", "world"]', "loop_value")
        tmp.loop = loop

        assert len(tmp.__stack__) == 2
        assert tmp.__stack__[-1] is loop

        tmp.loop = None

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is conditional

        tmp.loop = loop

        assert len(tmp.__stack__) == 2
        assert tmp.__stack__[-1] is loop

        async_group = AsyncRequestGroupTask(name='async-group-1')
        tmp.async_group = async_group

        assert len(tmp.__stack__) == 3
        assert tmp.__stack__[-1] is async_group

        with pytest.raises(AssertionError) as ae:
            tmp.loop = None
        assert str(ae.value) == 'loop is not last in stack'

        assert len(tmp.__stack__) == 3
        assert tmp.__stack__[-1] is async_group

        tmp.async_group = None

        assert len(tmp.__stack__) == 2
        assert tmp.__stack__[-1] is loop

        with pytest.raises(AssertionError) as ae:
            tmp.conditional = None
        assert str(ae.value) == 'conditional is not last in stack'

        tmp.loop = None

        assert len(tmp.__stack__) == 1
        assert tmp.__stack__[-1] is conditional

        tmp.conditional = None
        assert len(tmp.__stack__) == 0


class TestGrizzlyContextTasks:
    def test___init__(self) -> None:
        tasks = GrizzlyContextTasks()

        assert isinstance(tasks._tmp, GrizzlyContextTasksTmp)
        assert tasks.tmp is tasks._tmp

    def test___call__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.state.variables['loop_value'] = 'none'

        tasks = GrizzlyContextTasks()

        assert len(tasks()) == 0

        tasks.add(TestTask(name='test-1'))
        tasks.add(TestTask(name='test-2'))

        assert len(tasks()) == 2
        assert len(tasks.tmp.__stack__) == 0

        tasks.tmp.conditional = ConditionalTask('cond-1', '{{ value | int > 0 }}')
        tasks.tmp.conditional.switch(True)

        assert len(tasks()) == 0
        assert len(tasks.tmp.__stack__) == 1
        assert tasks.tmp.__stack__[-1] is tasks.tmp.conditional

        tasks.add(TestTask(name='test-3'))

        assert len(tasks()) == 1

        tasks.tmp.loop = LoopTask(grizzly, 'loop-1', '["hello", "world"]', "loop_value")

        assert len(tasks()) == 0
        assert len(tasks.tmp.__stack__) == 2
        assert tasks.tmp.__stack__[-1] is tasks.tmp.loop
        assert tasks.tmp.__stack__[-2] is tasks.tmp.conditional

        tasks.add(TestTask(name='test-4'))

        assert len(tasks()) == 1

        loop = tasks.tmp.loop
        tasks.tmp.loop = None

        tasks.add(loop)

        assert len(tasks()) == 2
        assert len(tasks.tmp.__stack__) == 1
        assert tasks.tmp.__stack__[-1] is tasks.tmp.conditional

        conditional = tasks.tmp.conditional
        tasks.tmp.conditional = None

        tasks.add(conditional)

        assert len(tasks()) == 3
        assert len(tasks.tmp.__stack__) == 0
