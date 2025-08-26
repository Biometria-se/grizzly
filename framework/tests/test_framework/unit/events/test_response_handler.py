"""Unit tests for grizzly.users.base.response_handler."""

from __future__ import annotations

from abc import ABC
from contextlib import suppress
from json import dumps as jsondumps
from json import loads as jsonloads
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from grizzly.events import GrizzlyEventHandlerClass
from grizzly.events.response_handler import ResponseHandler, ResponseHandlerAction, SaveHandlerAction, ValidationHandlerAction
from grizzly.exceptions import ResponseHandlerError, RestartScenario
from grizzly.tasks import RequestTask
from grizzly.testdata.filters import templatingfilter
from grizzly.types import HandlerContextType, RequestMethod
from grizzly.types.locust import StopUser
from grizzly.users import GrizzlyUser, RestApiUser
from grizzly_common.transformer import TransformerContentType
from jinja2.filters import FILTERS
from lxml import etree as XML  # noqa: N812

from test_framework.helpers import ANY, JSON_EXAMPLE, TestUser

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from pytest_mock import MockerFixture

    from test_framework.fixtures import GrizzlyFixture


@pytest.fixture
def get_log_files() -> Callable[[], list[Path]]:
    def wrapped() -> list[Path]:
        logs_root = Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs'
        log_dir = environ.get('GRIZZLY_LOG_DIR', None)
        if log_dir is not None:
            logs_root = logs_root / log_dir

        return list(logs_root.glob('*.log'))

    return wrapped


class TestResponseHandlerAction:
    class Dummy(ResponseHandlerAction):
        """Dummy response handler action."""

        def __call__(
            self,
            input_context: tuple[TransformerContentType, HandlerContextType],
            user: GrizzlyUser,
        ) -> None:
            """Use super-class implementation."""
            super().__call__(input_context, user)

    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        assert issubclass(ResponseHandlerAction, ABC)
        TestUser.__scenario__ = grizzly_fixture.grizzly.scenario
        TestUser.host = 'http://example.net'
        user = TestUser(grizzly_fixture.behave.locust.environment)
        handler = TestResponseHandlerAction.Dummy('$.', '.*')
        assert handler.expression == '$.'
        assert handler.match_with == '.*'
        assert handler.expected_matches == '1'

        with pytest.raises(NotImplementedError, match='Dummy has not implemented __call__'):
            handler((TransformerContentType.JSON, None), user)

    def test_get_matches(self, grizzly_fixture: GrizzlyFixture) -> None:
        TestUser.__scenario__ = grizzly_fixture.grizzly.scenario
        TestUser.host = 'http://example.net'
        user = TestUser(grizzly_fixture.behave.locust.environment)
        handler = TestResponseHandlerAction.Dummy('//hello/world', '.*')

        with pytest.raises(TypeError, match='could not find a transformer for UNDEFINED'):
            handler.get_match((TransformerContentType.UNDEFINED, None), user)

        with pytest.raises(TypeError, match=r'".*" is not a valid expression for JSON'):
            handler.get_match((TransformerContentType.JSON, None), user)

        response = {
            'hello': [
                {
                    'world': 'bar',
                    'foo': 1,
                },
                {
                    'world': 'hello',
                    'foo': 999,
                },
                {
                    'world': 'bar',
                    'foo': 2,
                },
            ],
        }

        user.set_variable('count', '2')
        handler = TestResponseHandlerAction.Dummy('$.hello[?world="bar"].foo', '.*', '{{ count }}', as_json=True)
        match, _, _ = handler.get_match((TransformerContentType.JSON, response), user)
        assert match == '["1", "2"]'

        handler = TestResponseHandlerAction.Dummy('$.hello[?world="bar"].foo', '.*', '2', as_json=False)
        match, _, _ = handler.get_match((TransformerContentType.JSON, response), user)
        assert match == '1\n2'


class TestValidationHandlerAction:
    def test___init__(self) -> None:
        handler = ValidationHandlerAction(condition=False, expression='$.hello.world', match_with='foo')

        assert issubclass(handler.__class__, ResponseHandlerAction)
        assert not handler.condition
        assert handler.expression == '$.hello.world'
        assert handler.match_with == 'foo'
        assert handler.expected_matches == '1'

    def test___call___true(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        try:
            handler = ValidationHandlerAction(
                condition=True,
                expression='$.test.value',
                match_with='test',
            )

            # match fixed string expression
            with pytest.raises(ResponseHandlerError, match='"test" was test'):
                handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)

            # no match fixed string expression
            handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), parent.user)

            # regexp match expression value
            handler = ValidationHandlerAction(
                condition=True,
                expression='$.test.value',
                match_with='.*(test)$',
            )
            with pytest.raises(ResponseHandlerError, match=r'"\.\*\(test\)\$" was test'):
                handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), parent.user)

            # ony allows 1 match per expression
            handler = ValidationHandlerAction(
                condition=True,
                expression='$.test[*].value',
                match_with='.*(test)$',
            )
            handler(
                (TransformerContentType.JSON, {'test': [{'value': 'nottest'}, {'value': 'reallynottest'}, {'value': 'test'}]}),
                parent.user,
            )

            # 1 match expression
            with pytest.raises(ResponseHandlerError, match=r'"\.\*\(test\)\$" was test'):
                handler(
                    (TransformerContentType.JSON, {'test': [{'value': 'not'}, {'value': 'reallynot'}, {'value': 'test'}]}),
                    parent.user,
                )

            handler = ValidationHandlerAction(
                condition=True,
                expression='$.[*]',
                match_with='STTO_31337',
            )

            # 1 match expression
            with pytest.raises(ResponseHandlerError, match=r'"STTO_31337" was STTO_31337'):
                handler((TransformerContentType.JSON, ['STTO_1337', 'STTO_31337', 'STTO_73313']), parent.user)

            # 1 match in multiple values (list)
            parent.user.set_variable('format', 'XML')
            handler = ValidationHandlerAction(
                condition=True,
                expression='$.*..GlossSeeAlso[*]',
                match_with='{{ format }}',
            )

            with pytest.raises(ResponseHandlerError, match='"XML" was XML'):
                handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

            @templatingfilter
            def uppercase(value: str) -> str:
                return value.upper()

            @templatingfilter
            def lowercase(value: str) -> str:
                return value.lower()

            # no match in multiple values (list)
            parent.user.set_variable('format', 'yaml')
            handler = ValidationHandlerAction(
                condition=True,
                expression='$.*..GlossSeeAlso[*]',
                match_with='{{ format | uppercase }}',
            )
            handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

            parent.user.set_variable('property', 'TITLE')
            parent.user.set_variable('regexp', '.*ary$')
            handler = ValidationHandlerAction(
                condition=True,
                expression='$.glossary.{{ property | lowercase }}',
                match_with='{{ regexp }}',
            )
            with pytest.raises(ResponseHandlerError, match='was example glossary'):
                handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

            handler = ValidationHandlerAction(
                condition=True,
                expression='$..Additional[?addtitle="test1"].addvalue',
                match_with='.*world$',
            )
            with pytest.raises(ResponseHandlerError, match='was hello world'):
                handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

            handler = ValidationHandlerAction(
                condition=True,
                expression='$.`this`',
                match_with='False',
            )
            handler((TransformerContentType.JSON, True), parent.user)

            with pytest.raises(ResponseHandlerError, match='"False" was False'):
                handler((TransformerContentType.JSON, False), parent.user)
        finally:
            for filter_name in ['uppercase', 'lowercase']:
                with suppress(KeyError):
                    del FILTERS[filter_name]

            assert parent.user._context is not parent.user.__class__(grizzly_fixture.behave.locust.environment)._context

    def test___call___false(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        handler = ValidationHandlerAction(condition=False, expression='$.test.value', match_with='test')

        # match fixed string expression
        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)

        # no match fixed string expression
        with pytest.raises(ResponseHandlerError, match='"test" was None'):
            handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), parent.user)

        # regexp match expression value
        parent.user.set_variable('expression', '$.test.value')
        parent.user.set_variable('value', 'test')
        handler = ValidationHandlerAction(
            condition=False,
            expression='{{ expression }}',
            match_with='.*({{ value }})$',
        )
        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), parent.user)

        # ony allows 1 match per expression
        handler = ValidationHandlerAction(
            condition=False,
            expression='$.test[*].value',
            match_with='.*(test)$',
        )

        with pytest.raises(ResponseHandlerError, match=r'"\.\*\(test\)\$" was None'):
            handler(
                (TransformerContentType.JSON, {'test': [{'value': 'nottest'}, {'value': 'reallynottest'}, {'value': 'test'}]}),
                parent.user,
            )

        # 1 match expression
        handler(
            (TransformerContentType.JSON, {'test': [{'value': 'not'}, {'value': 'reallynot'}, {'value': 'test'}]}),
            parent.user,
        )

        handler = ValidationHandlerAction(
            condition=False,
            expression='$.[*]',
            match_with='ID_31337',
        )

        # 1 match expression
        handler((TransformerContentType.JSON, ['ID_1337', 'ID_31337', 'ID_73313']), parent.user)

        # 1 match in multiple values (list)
        handler = ValidationHandlerAction(
            condition=False,
            expression='$.*..GlossSeeAlso[*]',
            match_with='XML',
        )
        handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

        # no match in multiple values (list)
        handler = ValidationHandlerAction(
            condition=False,
            expression='$.*..GlossSeeAlso[*]',
            match_with='YAML',
        )

        with pytest.raises(ResponseHandlerError, match='"YAML" was None'):
            handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

        handler = ValidationHandlerAction(
            condition=False,
            expression='$.glossary.title',
            match_with='.*ary$',
        )
        handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

        handler = ValidationHandlerAction(
            condition=False,
            expression='$..Additional[?addtitle="test2"].addvalue',
            match_with='.*stuff$',
        )
        handler((TransformerContentType.JSON, JSON_EXAMPLE), parent.user)

        handler = ValidationHandlerAction(
            condition=False,
            expression='$.`this`',
            match_with='False',
        )
        with pytest.raises(ResponseHandlerError, match='"False" was None'):
            handler((TransformerContentType.JSON, True), parent.user)

        for failure_exception in [None, StopUser, RestartScenario]:
            if failure_exception is not None:
                parent.user._scenario.failure_handling.update({None: failure_exception})

            with pytest.raises(ResponseHandlerError, match=r'".*?": "False" was None'):
                handler((TransformerContentType.JSON, True), parent.user)

            with suppress(KeyError):
                del parent.user._scenario.failure_handling[None]

        handler((TransformerContentType.JSON, False), parent.user)


class TestSaveHandlerAction:
    def test___init__(self) -> None:
        handler = SaveHandlerAction('foobar', expression='$.hello.world', match_with='foo')

        assert issubclass(handler.__class__, ResponseHandlerAction)
        assert handler.variable == 'foobar'
        assert handler.expression == '$.hello.world'
        assert handler.match_with == 'foo'
        assert handler.expected_matches == '1'

    def test___call__(self, grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture()

        assert 'test' not in parent.user.variables

        handler = SaveHandlerAction('test', expression='.*', match_with='.*')
        with pytest.raises(TypeError, match='could not find a transformer for UNDEFINED'):
            handler((TransformerContentType.UNDEFINED, {'test': {'value': 'test'}}), parent.user)

        with pytest.raises(TypeError, match='is not a valid expression'):
            handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)

        handler = SaveHandlerAction('test', expression='$.test.value', match_with='.*')

        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)
        assert parent.user.variables.get('test', None) == 'test'
        del parent.user.variables['test']

        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), parent.user)
        assert parent.user.variables.get('test', None) == 'nottest'
        del parent.user.variables['test']

        parent.user.set_variable('value', 'test')
        handler = SaveHandlerAction('test', expression='$.test.value', match_with='.*({{ value }})$')

        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)
        assert parent.user.variables.get('test', None) == 'test'
        del parent.user.variables['test']

        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), parent.user)
        assert parent.user.variables.get('test', None) == 'test'
        del parent.user.variables['test']

        # failed
        with pytest.raises(ResponseHandlerError, match='did not match value'):
            handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), parent.user)
        assert parent.user.variables.get('test', 'test') is None

        for failure_exception in [None, StopUser, RestartScenario]:
            if failure_exception is not None:
                parent.user._scenario.failure_handling.update({None: failure_exception})

            with pytest.raises(ResponseHandlerError, match='did not match value'):
                handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), parent.user)

        # multiple matches
        handler = SaveHandlerAction('test', expression='$.test[*].value', match_with='.*t.*')
        with pytest.raises(ResponseHandlerError, match='did not match value'):
            handler((TransformerContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), parent.user)
        assert parent.user.variables.get('test', None) is None

        # save object dict
        handler = SaveHandlerAction(
            'test_object',
            expression='$.test.prop2',
            match_with='.*',
        )

        handler(
            (
                TransformerContentType.JSON,
                {
                    'test': {
                        'prop1': 'value1',
                        'prop2': {
                            'prop21': False,
                            'prop22': 100,
                            'prop23': {
                                'prop231': True,
                                'prop232': 'hello',
                                'prop233': 'world!',
                                'prop234': 200,
                            },
                        },
                        'prop3': 'value3',
                        'prop4': [
                            'prop41',
                            True,
                            'prop42',
                            300,
                        ],
                    },
                },
            ),
            parent.user,
        )

        test_object: str | None = parent.user.variables.get('test_object', None)
        assert test_object is not None
        assert jsonloads(test_object) == {
            'prop21': False,
            'prop22': 100,
            'prop23': {
                'prop231': True,
                'prop232': 'hello',
                'prop233': 'world!',
                'prop234': 200,
            },
        }

        # save object list
        handler = SaveHandlerAction(
            'test_list',
            expression='$.test.prop4',
            match_with='.*',
        )

        handler(
            (
                TransformerContentType.JSON,
                {
                    'test': {
                        'prop1': 'value1',
                        'prop2': {
                            'prop21': False,
                            'prop22': 100,
                            'prop23': {
                                'prop231': True,
                                'prop232': 'hello',
                                'prop233': 'world!',
                                'prop234': 200,
                            },
                        },
                        'prop3': 'value3',
                        'prop4': [
                            'prop41',
                            True,
                            'prop42',
                            300,
                        ],
                    },
                },
            ),
            parent.user,
        )

        test_list: str | None = parent.user.variables.get('test_list')
        assert test_list is not None
        assert jsonloads(test_list) == [
            'prop41',
            True,
            'prop42',
            300,
        ]

        parent.user.set_variable('count', '-1')

        handler = SaveHandlerAction(
            'test_list',
            expression='$.test[?hello="world"].value',
            match_with='.*',
            expected_matches='{{ count }}',
            as_json=True,
        )

        handler(
            (
                TransformerContentType.JSON,
                {
                    'test': [
                        {
                            'hello': 'world',
                            'value': 'prop41',
                        },
                    ],
                },
            ),
            parent.user,
        )

        test_list = parent.user.variables.get('test_list', None)
        assert test_list is not None
        assert jsonloads(test_list) == [
            'prop41',
        ]

        handler(
            (
                TransformerContentType.JSON,
                {
                    'test': [
                        {
                            'hello': 'world',
                            'value': 'prop41',
                        },
                        {
                            'hello': 'world',
                            'value': 'prop42',
                        },
                    ],
                },
            ),
            parent.user,
        )

        test_list = parent.user.variables.get('test_list', None)
        assert test_list is not None
        assert jsonloads(test_list) == [
            'prop41',
            'prop42',
        ]


class TestResponseHandler:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(host='http://example.com')
        assert len(parent.user.events.request._handlers) == 2
        assert any(
            h.__class__ is ResponseHandler and isinstance(h, GrizzlyEventHandlerClass) and isinstance(h.user, GrizzlyUser) and h.user is parent.user
            for h in parent.user.events.request._handlers
        )

    def test___call__(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture()

        event = ResponseHandler(parent.user)

        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/v2/test')

        request_logger_mock = mocker.patch('grizzly.events.request_logger.RequestLogger.__call__', return_value=None)

        payload_handler = mocker.MagicMock()
        metadata_handler = mocker.MagicMock()

        # no handler called
        event('test', (None, ''), request)
        request_logger_mock.assert_not_called()

        # payload handler called
        request.response.content_type = TransformerContentType.JSON
        request.response.handlers.add_payload(payload_handler)
        event('test', (None, '{}'), request)

        request_logger_mock.assert_not_called()
        payload_handler.assert_called_once_with((TransformerContentType.JSON, {}), parent.user)
        payload_handler.reset_mock()
        request.response.handlers.payload.clear()

        # metadata handler called
        request.response.handlers.add_metadata(metadata_handler)
        event('test', ({}, None), request)

        request_logger_mock.assert_not_called()
        metadata_handler.assert_called_once_with((TransformerContentType.JSON, {}), parent.user)
        metadata_handler.reset_mock()
        request.response.handlers.metadata.clear()

        # invalid json content in payload
        request.response.handlers.add_payload(payload_handler)

        with pytest.raises(ResponseHandlerError, match='failed to transform input as JSON'):
            event('test', (None, '{"test: "value"'), request)

        payload_handler.assert_not_called()
        request_logger_mock.assert_called_once_with(
            name='test',
            context=(None, '{"test: "value"'),
            request=request,
            exception=ANY(ResponseHandlerError, message='failed to transform input as JSON'),
        )
        request_logger_mock.reset_mock()

        request.response.content_type = TransformerContentType.JSON
        with pytest.raises(ResponseHandlerError, match='failed to transform input as JSON'):
            event('test', (None, '{"test: "value"'), request)

        request_logger_mock.assert_called_once_with(
            name='test',
            context=(None, '{"test: "value"'),
            request=request,
            exception=ANY(ResponseHandlerError, message='failed to transform input as JSON'),
        )
        request_logger_mock.reset_mock()

        request.response.content_type = TransformerContentType.XML
        with pytest.raises(ResponseHandlerError, match='failed to transform input as XML'):
            event('test', ({}, '{"test": "value"}'), request)

        request_logger_mock.assert_called_once_with(
            name='test',
            context=({}, '{"test": "value"}'),
            request=request,
            exception=ANY(ResponseHandlerError, message='failed to transform input as XML'),
        )
        request_logger_mock.reset_mock()

        request.response.content_type = TransformerContentType.PLAIN
        event('test', ({}, '{"test": "value"}'), request)
        payload_handler.assert_called_once_with((TransformerContentType.PLAIN, '{"test": "value"}'), parent.user)
        payload_handler.reset_mock()

        # XML input
        request.response.content_type = TransformerContentType.XML
        event(
            'test',
            (
                None,
                """<?xml version="1.0" encoding="UTF-8"?>
                <test>
                    value
                </test>""",
            ),
            request,
        )

        payload_handler.assert_called_once_with(
            (TransformerContentType.XML, ANY(XML._Element)),
            parent.user,
        )
        payload_handler.reset_mock()
        actual_payload = """<?xml encoding="UTF-8"?>
        <test>
            value
        </test>"""

        with pytest.raises(ResponseHandlerError, match='failed to transform input as XML'):
            event(
                'test',
                (
                    None,
                    actual_payload,
                ),
                request,
            )

        payload_handler.assert_not_called()
        request_logger_mock.assert_called_once_with(
            name='test',
            context=(None, actual_payload),
            request=request,
            exception=ANY(ResponseHandlerError, message='failed to transform input as XML'),
        )
        request_logger_mock.reset_mock()

        request.response.content_type = TransformerContentType.UNDEFINED

        with pytest.raises(ResponseHandlerError, match='failed to transform'):
            event(
                'test',
                (
                    None,
                    actual_payload,
                ),
                request,
            )

        payload_handler.assert_not_called()
        request_logger_mock.assert_called_once_with(
            name='test',
            context=(None, actual_payload),
            request=request,
            exception=ANY(ResponseHandlerError, message='failed to transform'),
        )
        request_logger_mock.reset_mock()

    def test_response_handler_failure(self, grizzly_fixture: GrizzlyFixture, get_log_files: Callable[[], list[Path]]) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)

        event = ResponseHandler(parent.user)

        request = RequestTask(RequestMethod.GET, name='test-request', endpoint='/api/v2/test | content_type=json')
        request.response.handlers.add_payload(SaveHandlerAction('foobar', expression='$.hello.world[?value="foobar"]', match_with='.*'))

        parent.user._scenario.failure_handling.update({None: StopUser})

        assert get_log_files() == []

        response_payload = jsondumps(
            {
                'hello': {
                    'world': [
                        {'value': 'foo'},
                        {'value': 'bar'},
                    ],
                },
            },
        )

        with pytest.raises(ResponseHandlerError, match='did not match value'):
            event(
                request.name,
                (
                    None,
                    response_payload,
                ),
                request,
            )

        log_files = get_log_files()

        assert len(log_files) == 1

        log_file = next(iter(log_files))
        log_file_contents = log_file.read_text()

        assert '"$.hello.world[?value="foobar"]" did not match value' in log_file_contents
        assert response_payload in log_file_contents
