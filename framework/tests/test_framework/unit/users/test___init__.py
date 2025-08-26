"""Unit tests for grizzly.users.base.grizzly_user."""

from __future__ import annotations

import logging
from contextlib import suppress
from json import loads as jsonloads
from typing import TYPE_CHECKING

import pytest
from grizzly.exceptions import RestartIteration, RestartScenario, RetryTask, StopUser
from grizzly.tasks import RequestTask
from grizzly.testdata.filters import templatingfilter
from grizzly.types import FailureAction, GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.users import GrizzlyUser
from jinja2.filters import FILTERS

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture

logging.getLogger().setLevel(logging.CRITICAL)


class DummyGrizzlyUser(GrizzlyUser):
    host: str = 'http://example.com'

    def request_impl(self, _request: RequestTask) -> GrizzlyResponse:
        message = f'{self.__class__.__name__} has not implemented request'
        raise NotImplementedError(message)


class TestGrizzlyUser:
    def test_failure_handler(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        assert parent.user._scenario.failure_handling == {}

        parent.user._scenario.failure_handling.update(
            {
                None: StopUser,
                '504 gateway timeout': RetryTask,
            },
        )

        parent.user.failure_handler(None)

        with pytest.raises(StopUser):
            parent.user.failure_handler(RuntimeError('foobar'))

        with pytest.raises(RetryTask):
            parent.user.failure_handler(RuntimeError('504 gateway timeout'))

        del parent.user._scenario.failure_handling[None]

        parent.user.failure_handler(RuntimeError('foobar'))

        with pytest.raises(RetryTask):
            parent.user.failure_handler(RuntimeError('504 gateway timeout'))

        parent.user._scenario.failure_handling.update({AttributeError: RestartScenario})

        with pytest.raises(StopUser):
            parent.user.failure_handler(AttributeError('foobaz'))

        parent.user._scenario.failure_handling.update({MemoryError: RestartScenario})

        with pytest.raises(RestartScenario):
            parent.user.failure_handler(MemoryError('0% free'))

        for exception in FailureAction.get_failure_exceptions():
            with pytest.raises(exception):
                parent.user.failure_handler(exception())

        task = parent.user._scenario.tasks[-1]
        task.failure_handling.update(
            {
                '504 gateway timeout': RestartIteration,
                MemoryError: StopUser,
            },
        )

        with pytest.raises(RestartIteration):
            parent.user.failure_handler(RuntimeError('504 gateway timeout'), task=task)

        with pytest.raises(StopUser):
            parent.user.failure_handler(MemoryError('0% free'), task=task)

        task.failure_handling.clear()
        # it's failure_handler exception, so the custom should not matter
        for exception in FailureAction.get_failure_exceptions():
            task.failure_handling.update({exception: StopUser})

            with pytest.raises(exception):
                parent.user.failure_handler(exception(), task=task)

    def test_render(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=DummyGrizzlyUser)

        assert isinstance(parent.user, DummyGrizzlyUser)

        @templatingfilter
        def sarcasm(value: str) -> str:
            sarcastic_value: list[str] = []
            for index, c in enumerate(value):
                if index % 2 == 0:
                    sarcastic_value.append(c.upper())
                else:
                    sarcastic_value.append(c.lower())

            return ''.join(sarcastic_value)

        parent.user.set_variable('are', 'foo')
        assert parent.user.render('how {{ are }} we {{ doing | sarcasm }} today', variables={'doing': 'bar'}) == 'how foo we BaR today'

    def test_render_request(self, grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
        grizzly = grizzly_fixture.grizzly
        test_context = grizzly_fixture.test_context / 'requests'
        test_context.mkdir(exist_ok=True)
        test_file = test_context / 'blobfile.txt'
        test_file.touch()

        grizzly.scenarios.clear()
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test scenario'))

        @templatingfilter
        def uppercase(value: str) -> str:
            return value.upper()

        try:
            DummyGrizzlyUser.__scenario__ = grizzly.scenario
            user = DummyGrizzlyUser(grizzly_fixture.behave.locust.environment)
            template = RequestTask(RequestMethod.POST, name='test', endpoint='/api/test')
            template.source = 'hello {{ name | uppercase }}'

            user.set_variable('name', 'bob')
            request = user.render_request(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test'
            assert request.source == 'hello BOB'
            assert request.arguments is None
            assert request.metadata == {}

            user.set_variable('name', 'alice')
            request = user.render_request(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test'
            assert request.source == 'hello ALICE'
            assert request.arguments is None
            assert request.metadata == {}

            template.endpoint = '/api/test?data={{ querystring | uppercase }}'
            user.set_variable('querystring', 'querystring_data')
            request = user.render_request(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source == 'hello ALICE'
            assert request.arguments is None
            assert request.metadata == {}

            template.source = None
            request = user.render_request(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source is None
            assert request.arguments is None
            assert request.metadata == {}

            template.name = '{{ name | uppercase }}'
            request = user.render_request(template)
            assert request.name == '001 ALICE'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source is None
            assert request.arguments is None
            assert request.metadata == {}

            test_file.write_text('this is a test {{ name }}')
            template.name = '{{ name }}'
            template.source = '{{ blobfile }}'
            user.set_variable('blobfile', str(test_file))
            request = user.render_request(template)
            assert request.name == '001 alice'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source == 'this is a test alice'
            assert request.arguments is None
            assert request.metadata == {}
        finally:
            with suppress(KeyError):
                del FILTERS['uppercase']

    def test_render_request_nested(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        test_context = grizzly_fixture.test_context / 'requests' / 'test'
        test_context.mkdir(parents=True)
        test_file = test_context / 'payload.j2.json'
        test_file.write_text("""
        {
            "MeasureResult": {
                "ID": {{ messageID }},
                "name": "{{ name }}",
                "value": "{{ value }}"
            }
        }
        """)

        grizzly.scenarios.clear()
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test'))
        DummyGrizzlyUser.__scenario__ = grizzly.scenario
        user = DummyGrizzlyUser(grizzly_fixture.behave.locust.environment)
        template = RequestTask(RequestMethod.POST, name='{{ name }}', endpoint='/api/test/{{ value }}')

        template.source = '{{ file_path }}'

        user.variables.update(
            {
                'name': 'test-name',
                'value': 'test-value',
                'messageID': 1337,
                'file_path': 'test/payload.j2.json',
            },
        )

        request = user.render_request(template)

        assert request.name == '001 test-name'
        assert request.endpoint == '/api/test/test-value'
        assert request.source is not None
        assert request.arguments is None
        assert request.metadata == {}

        data = jsonloads(request.source)
        assert data['MeasureResult']['ID'] == user.variables['messageID']
        assert data['MeasureResult']['name'] == user.variables['name']
        assert data['MeasureResult']['value'] == user.variables['value']

    def test_request(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=DummyGrizzlyUser)
        payload = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')

        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        with pytest.raises(StopUser):
            parent.user.request(payload)

        request_spy.assert_called_once_with(
            request_type='GET',
            name='001 test',
            response_time=ANY(int),
            response_length=0,
            context={
                'host': '',
                'log_all_requests': False,
                'metadata': None,
                'user': id(parent.user),
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=ANY(NotImplementedError, message='test_framework.unit.users.test___init__.DummyGrizzlyUser_001 has not implemented request'),
        )

    def test_context(self, behave_fixture: BehaveFixture) -> None:
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        DummyGrizzlyUser.__scenario__ = behave_fixture.grizzly.scenario
        user = DummyGrizzlyUser(behave_fixture.locust.environment)

        assert user._scenario is not DummyGrizzlyUser.__scenario__

        context = user.context()

        assert isinstance(context, dict)
        assert context == {'log_all_requests': False, 'metadata': None}

        user.set_variable('test', 'value')
        assert user.variables == SOME(dict, {'test': 'value'})

    def test_stop(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=DummyGrizzlyUser)

        mocker.patch('locust.user.users.User.stop', return_value=True)

        with caplog.at_level(logging.DEBUG):
            assert parent.user._state is None
            assert getattr(parent.user, '_scenario_state', None) == ScenarioState.STOPPED

            assert parent.user.stop(force=True)

            assert len(caplog.messages) == 0
            assert parent.user._state is None
            assert getattr(parent.user, '_scenario_state', None) == ScenarioState.STOPPED

            assert not parent.user.stop(force=False)

            assert len(caplog.messages) == 2
            assert caplog.messages[0] == 'stop scenarios before stopping user'
            assert caplog.messages[1] == 'scenario state=ScenarioState.STOPPED -> ScenarioState.STOPPING'
            assert parent.user._state == 'running'
            assert parent.user._scenario_state == ScenarioState.STOPPING
