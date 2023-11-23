"""Unit tests for grizzly.users.base.grizzly_user."""
from __future__ import annotations

import logging
from contextlib import suppress
from json import loads as jsonloads
from os import environ
from shutil import rmtree
from typing import TYPE_CHECKING, cast

import pytest
from jinja2.filters import FILTERS

from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import templatingfilter
from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.types.locust import StopUser
from grizzly.users import GrizzlyUser
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture

    from tests.fixtures import BehaveFixture, GrizzlyFixture

logging.getLogger().setLevel(logging.CRITICAL)


class DummyGrizzlyUser(GrizzlyUser):
    host: str = 'http://example.com'

    def request_impl(self, _request: RequestTask) -> GrizzlyResponse:
        message = f'{self.__class__.__name__} has not implemented request'
        raise NotImplementedError(message)


class TestGrizzlyUser:
    def test_render(self, behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
        test_context = tmp_path_factory.mktemp('renderer_test') / 'requests'
        test_context.mkdir()
        test_file = test_context / 'blobfile.txt'
        test_file.touch()
        environ['GRIZZLY_CONTEXT_ROOT'] = str(test_context.parent)

        grizzly = cast(GrizzlyContext, behave_fixture.context.grizzly)
        grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

        @templatingfilter
        def uppercase(value: str) -> str:
            return value.upper()

        try:
            DummyGrizzlyUser.__scenario__ = grizzly.scenario
            user = DummyGrizzlyUser(behave_fixture.locust.environment)
            template = RequestTask(RequestMethod.POST, name='test', endpoint='/api/test')
            template.source = 'hello {{ name | uppercase }}'

            user.add_context({'variables': {'name': 'bob'}})
            request = user.render(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test'
            assert request.source == 'hello BOB'
            assert request.arguments is None
            assert request.metadata == {}

            user.set_context_variable('name', 'alice')
            request = user.render(template)
            assert request.name == '001 test'
            assert request.endpoint == '/api/test'
            assert request.source == 'hello ALICE'
            assert request.arguments is None
            assert request.metadata == {}

            template.endpoint = '/api/test?data={{ querystring | uppercase }}'
            user.set_context_variable('querystring', 'querystring_data')
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
            user.set_context_variable('blobfile', str(test_file))
            request = user.render(template)
            assert request.name == '001 alice'
            assert request.endpoint == '/api/test?data=QUERYSTRING_DATA'
            assert request.source == 'this is a test alice'
            assert request.arguments is None
            assert request.metadata == {}
        finally:
            with suppress(KeyError):
                del FILTERS['uppercase']

            rmtree(test_context)

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_fixture')
    def test_render_nested(self, behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('render_nested') / 'requests' / 'test'
        test_context.mkdir(parents=True)
        test_file = test_context / 'payload.j2.json'
        test_file.touch()
        test_file.write_text("""
        {
            "MeasureResult": {
                "ID": {{ messageID }},
                "name": "{{ name }}",
                "value": "{{ value }}"
            }
        }
        """)

        environ['GRIZZLY_CONTEXT_ROOT'] = str(test_context.parent.parent)

        grizzly = cast(GrizzlyContext, behave_fixture.context.grizzly)

        try:
            grizzly.scenarios.clear()
            grizzly._scenarios.append(GrizzlyContextScenario(999, behave=behave_fixture.create_scenario('test')))
            DummyGrizzlyUser.__scenario__ = grizzly.scenario
            user = DummyGrizzlyUser(behave_fixture.locust.environment)
            template = RequestTask(RequestMethod.POST, name='{{ name }}', endpoint='/api/test/{{ value }}')

            template.source = '{{ file_path }}'

            user.add_context({
                'variables': {
                    'name': 'test-name',
                    'value': 'test-value',
                    'messageID': 1337,
                    'file_path': 'test/payload.j2.json',
                },
            })

            request = user.render(template)

            assert request.name == '999 test-name'
            assert request.endpoint == '/api/test/test-value'
            assert request.source is not None
            assert request.arguments is None
            assert request.metadata == {}

            data = jsonloads(request.source)
            assert data['MeasureResult']['ID'] == user.context_variables['messageID']
            assert data['MeasureResult']['name'] == user.context_variables['name']
            assert data['MeasureResult']['value'] == user.context_variables['value']
        finally:
            rmtree(test_context.parent.parent)
            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

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
            context={'host': '', 'variables': {}, 'log_all_requests': False, 'metadata': None},
            exception=ANY(NotImplementedError, message='tests.unit.test_grizzly.users.test___init__.DummyGrizzlyUser_001 has not implemented request'),
        )

    def test_context(self, behave_fixture: BehaveFixture) -> None:
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        DummyGrizzlyUser.__scenario__ = behave_fixture.grizzly.scenario
        user = DummyGrizzlyUser(behave_fixture.locust.environment)

        context = user.context()

        assert isinstance(context, dict)
        assert context == {'variables': {}, 'log_all_requests': False, 'metadata': None}

        user.set_context_variable('test', 'value')
        assert user.context_variables == {'test': 'value'}

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
