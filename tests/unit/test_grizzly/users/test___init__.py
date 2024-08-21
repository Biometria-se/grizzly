"""Unit tests for grizzly.users.base.grizzly_user."""
from __future__ import annotations

import logging
from contextlib import suppress
from json import loads as jsonloads
from os import environ
from typing import TYPE_CHECKING

import pytest
from jinja2.filters import FILTERS

from grizzly.tasks import RequestTask
from grizzly.testdata.utils import templatingfilter
from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.types.locust import StopUser
from grizzly.users import GrizzlyUser
from tests.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture

    from tests.fixtures import BehaveFixture, GrizzlyFixture

logging.getLogger().setLevel(logging.CRITICAL)


class DummyGrizzlyUser(GrizzlyUser):
    host: str = 'http://example.com'

    def request_impl(self, _request: RequestTask) -> GrizzlyResponse:
        message = f'{self.__class__.__name__} has not implemented request'
        raise NotImplementedError(message)


class TestGrizzlyUser:
    def test_render(self, grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
        grizzly = grizzly_fixture.grizzly
        test_context = grizzly_fixture.test_context / 'requests'
        test_context.mkdir(exist_ok=True)
        test_file = test_context / 'blobfile.txt'
        test_file.touch()
        environ['GRIZZLY_CONTEXT_ROOT'] = str(test_context.parent)

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

            user.add_context({'variables': {'name': 'bob'}})
            request = user.render(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test'
            assert request.source == 'hello BOB'
            assert request.arguments is None
            assert request.metadata == {}

            user.set_variable('name', 'alice')
            request = user.render(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test'
            assert request.source == 'hello ALICE'
            assert request.arguments is None
            assert request.metadata == {}

            template.endpoint = '/api/test?data={{ querystring | uppercase }}'
            user.set_variable('querystring', 'querystring_data')
            request = user.render(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source == 'hello ALICE'
            assert request.arguments is None
            assert request.metadata == {}

            template.source = None
            request = user.render(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source is None
            assert request.arguments is None
            assert request.metadata == {}

            template.name = '{{ name | uppercase }}'
            request = user.render(template)
            assert request.name == '001 ALICE'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source is None
            assert request.arguments is None
            assert request.metadata == {}

            template.name = '{{ name'
            with pytest.raises(StopUser):
                user.render(template)

            test_file.write_text('this is a test {{ name }}')
            template.name = '{{ name }}'
            template.source = '{{ blobfile }}'
            user.set_variable('blobfile', str(test_file))
            request = user.render(template)
            assert request.name == '001 alice'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source == 'this is a test alice'
            assert request.arguments is None
            assert request.metadata == {}
        finally:
            with suppress(KeyError):
                del FILTERS['uppercase']

    def test_render_nested(self, grizzly_fixture: GrizzlyFixture) -> None:
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
        template._template = grizzly.scenario.jinja2.from_string(template.source)

        user._scenario.variables.update({
            'name': 'test-name',
            'value': 'test-value',
            'messageID': 1337,
            'file_path': 'test/payload.j2.json',
        })

        request = user.render(template)

        assert request.name == '001 test-name'
        assert request.endpoint == '/api/test/test-value'
        assert request.source is not None
        assert request.arguments is None
        assert request.metadata == {}

        data = jsonloads(request.source)
        assert data['MeasureResult']['ID'] == user._scenario.variables['messageID']
        assert data['MeasureResult']['name'] == user._scenario.variables['name']
        assert data['MeasureResult']['value'] == user._scenario.variables['value']

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
            context={'host': '', 'log_all_requests': False, 'metadata': None},
            exception=ANY(NotImplementedError, message='tests.unit.test_grizzly.users.test___init__.DummyGrizzlyUser_001 has not implemented request'),
        )

    def test_context(self, behave_fixture: BehaveFixture) -> None:
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        DummyGrizzlyUser.__scenario__ = behave_fixture.grizzly.scenario
        original_id = id(DummyGrizzlyUser.__scenario__._jinja2)
        user = DummyGrizzlyUser(behave_fixture.locust.environment)

        assert user._scenario is not DummyGrizzlyUser.__scenario__
        assert id(user._scenario._jinja2) != original_id
        assert id(user._scenario._jinja2.globals) != id(DummyGrizzlyUser.__scenario__._jinja2.globals)
        assert user._scenario._jinja2.globals.keys() == DummyGrizzlyUser.__scenario__._jinja2.globals.keys()

        context = user.context()

        assert isinstance(context, dict)
        assert context == {'log_all_requests': False, 'metadata': None}

        user.set_variable('test', 'value')
        assert user._scenario.variables == SOME(dict, {'test': 'value'})

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
