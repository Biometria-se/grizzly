from abc import ABC
from json import dumps as jsondumps, loads as jsonloads
from typing import TYPE_CHECKING, Tuple, Any, Optional

import pytest

from lxml import etree as XML
from pytest_mock import MockerFixture

from locust.event import EventHook
from locust.clients import ResponseContextManager
from locust.exception import LocustError, CatchResponseError, StopUser
from requests.models import Response

from grizzly.clients import ResponseEventSession
from grizzly.context import GrizzlyContextScenario
from grizzly.users.base import HttpRequests, ResponseEvent
from grizzly.users.base.response_handler import ResponseHandler, ValidationHandlerAction, SaveHandlerAction, ResponseHandlerAction
from grizzly.exceptions import ResponseHandlerError, RestartScenario
from grizzly.types import RequestMethod
from grizzly.tasks import RequestTask
from grizzly_extras.transformer import TransformerContentType

from ....fixtures import LocustFixture
from ....helpers import TestUser

if TYPE_CHECKING:
    from grizzly.users.base import GrizzlyUser


class TestResponseHandlerAction:
    class Dummy(ResponseHandlerAction):
        def __call__(
            self,
            input_context: Tuple[TransformerContentType, Any],
            user: 'GrizzlyUser',
            response: Optional[ResponseContextManager] = None,
        ) -> None:
            super().__call__(input_context, user, response)

    def test___(self, locust_fixture: LocustFixture) -> None:
        assert issubclass(ResponseHandlerAction, ABC)
        user = TestUser(locust_fixture.env)
        handler = TestResponseHandlerAction.Dummy('$.', '.*')
        assert handler.expression == '$.'
        assert handler.match_with == '.*'
        assert handler.expected_matches == 1

        with pytest.raises(NotImplementedError) as nie:
            handler((TransformerContentType.JSON, None,), user)
        assert str(nie.value) == 'Dummy has not implemented __call__'

    def test_get_matches(self, locust_fixture: LocustFixture) -> None:
        user = TestUser(locust_fixture.env)
        handler = TestResponseHandlerAction.Dummy('//hello/world', '.*')

        with pytest.raises(TypeError) as te:
            handler.get_match((TransformerContentType.UNDEFINED, None, ), user)
        assert str(te.value) == 'could not find a transformer for UNDEFINED'

        with pytest.raises(TypeError) as te:
            handler.get_match((TransformerContentType.JSON, None, ), user)
        assert str(te.value) == '"//hello/world" is not a valid expression for JSON'

        handler = TestResponseHandlerAction.Dummy('$.hello[?world="bar"].foo', '.*', 2)

        response = {
            'hello': [{
                'world': 'bar',
                'foo': 1,
            }, {
                'world': 'hello',
                'foo': 999,
            }, {
                'world': 'bar',
                'foo': 2,
            }]
        }
        print(response)
        match, _, _ = handler.get_match((TransformerContentType.JSON, response, ), user)
        assert match == '["1", "2"]'


class TestValidationHandlerAction:
    def test___init__(self) -> None:
        handler = ValidationHandlerAction(False, expression='$.hello.world', match_with='foo')

        assert issubclass(handler.__class__, ResponseHandlerAction)
        assert not handler.condition
        assert handler.expression == '$.hello.world'
        assert handler.match_with == 'foo'
        assert handler.expected_matches == 1

    def test___call___true(self, locust_fixture: LocustFixture) -> None:
        scenario = GrizzlyContextScenario(index=1)
        scenario.name = 'test-scenario'
        user = TestUser(locust_fixture.env)
        user._scenario = scenario

        try:
            response = Response()
            response._content = '{}'.encode('utf-8')
            response.status_code = 200
            response_context_manager = ResponseContextManager(response, locust_fixture.env.events.request, {})
            response_context_manager._entered = True

            handler = ValidationHandlerAction(
                True,
                expression='$.test.value',
                match_with='test',
            )

            # match fixed string expression
            handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            # no match fixed string expression
            handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
            assert response_context_manager._manual_result is None

            # regexp match expression value
            handler = ValidationHandlerAction(
                True,
                expression='$.test.value',
                match_with='.*(test)$',
            )
            handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            # ony allows 1 match per expression
            handler = ValidationHandlerAction(
                True,
                expression='$.test[*].value',
                match_with='.*(test)$',
            )
            handler(
                (TransformerContentType.JSON, {'test': [{'value': 'nottest'}, {'value': 'reallynottest'}, {'value': 'test'}]}),
                user,
                response_context_manager,
            )
            assert response_context_manager._manual_result is None

            # 1 match expression
            handler(
                (TransformerContentType.JSON, {'test': [{'value': 'not'}, {'value': 'reallynot'}, {'value': 'test'}]}),
                user,
                response_context_manager,
            )
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            handler = ValidationHandlerAction(
                True,
                expression='$.[*]',
                match_with='STTO_31337',
            )

            # 1 match expression
            handler((TransformerContentType.JSON, ['STTO_1337', 'STTO_31337', 'STTO_73313']), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            example = {
                'glossary': {
                    'title': 'example glossary',
                    'GlossDiv': {
                        'title': 'S',
                        'GlossList': {
                            'GlossEntry': {
                                'ID': 'SGML',
                                'SortAs': 'SGML',
                                'GlossTerm': 'Standard Generalized Markup Language',
                                'Acronym': 'SGML',
                                'Abbrev': 'ISO 8879:1986',
                                'GlossDef': {
                                    'para': 'A meta-markup language, used to create markup languages such as DocBook.',
                                    'GlossSeeAlso': ['GML', 'XML']
                                },
                                'GlossSee': 'markup',
                                'Additional': [
                                    {
                                        'addtitle': 'test1',
                                        'addvalue': 'hello world',
                                    },
                                    {
                                        'addtitle': 'test2',
                                        'addvalue': 'good stuff',
                                    },
                                ]
                            }
                        }
                    }
                }
            }

            # 1 match in multiple values (list)
            user.set_context_variable('format', 'XML')
            handler = ValidationHandlerAction(
                True,
                expression='$.*..GlossSeeAlso[*]',
                match_with='{{ format }}',
            )
            handler((TransformerContentType.JSON, example), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            with pytest.raises(ResponseHandlerError):
                handler((TransformerContentType.JSON, example), user, None)

            # no match in multiple values (list)
            user.set_context_variable('format', 'YAML')
            handler = ValidationHandlerAction(
                True,
                expression='$.*..GlossSeeAlso[*]',
                match_with='{{ format }}',
            )
            handler((TransformerContentType.JSON, example), user, response_context_manager)
            assert response_context_manager._manual_result is None

            user.set_context_variable('property', 'title')
            user.set_context_variable('regexp', '.*ary$')
            handler = ValidationHandlerAction(
                True,
                expression='$.glossary.{{ property }}',
                match_with='{{ regexp }}',
            )
            handler((TransformerContentType.JSON, example), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            handler = ValidationHandlerAction(
                True,
                expression='$..Additional[?addtitle="test1"].addvalue',
                match_with='.*world$',
            )
            handler((TransformerContentType.JSON, example), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None

            handler = ValidationHandlerAction(
                True,
                expression='$.`this`',
                match_with='False',
            )
            handler((TransformerContentType.JSON, True), user, response_context_manager)
            assert response_context_manager._manual_result is None

            handler((TransformerContentType.JSON, False), user, response_context_manager)
            assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
            response_context_manager._manual_result = None
        finally:
            assert user._context['variables'] is not TestUser(locust_fixture.env)._context['variables']

    def test___call___false(self, locust_fixture: LocustFixture) -> None:
        scenario = GrizzlyContextScenario(index=1)
        scenario.name = 'test-scenario'
        user = TestUser(locust_fixture.env)
        user._scenario = scenario
        response = Response()
        response._content = '{}'.encode('utf-8')
        response.status_code = 200
        response_context_manager = ResponseContextManager(response, locust_fixture.env.events.request, {})
        response_context_manager._entered = True

        handler = ValidationHandlerAction(False, expression='$.test.value', match_with='test')

        # match fixed string expression
        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

        # no match fixed string expression
        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert getattr(response_context_manager, '_manual_result', None) is not None
        response_context_manager._manual_result = None

        # regexp match expression value
        user.set_context_variable('expression', '$.test.value')
        user.set_context_variable('value', 'test')
        handler = ValidationHandlerAction(
            False,
            expression='{{ expression }}',
            match_with='.*({{ value }})$',
        )
        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

        # ony allows 1 match per expression
        handler = ValidationHandlerAction(
            False,
            expression='$.test[*].value',
            match_with='.*(test)$',
        )
        handler(
            (TransformerContentType.JSON, {'test': [{'value': 'nottest'}, {'value': 'reallynottest'}, {'value': 'test'}]}),
            user,
            response_context_manager,
        )
        assert getattr(response_context_manager, '_manual_result', None) is not None
        response_context_manager._manual_result = None

        # 1 match expression
        handler(
            (TransformerContentType.JSON, {'test': [{'value': 'not'}, {'value': 'reallynot'}, {'value': 'test'}]}),
            user,
            response_context_manager,
        )
        assert response_context_manager._manual_result is None

        handler = ValidationHandlerAction(
            False,
            expression='$.[*]',
            match_with='ID_31337',
        )

        # 1 match expression
        handler((TransformerContentType.JSON, ['ID_1337', 'ID_31337', 'ID_73313']), user, response_context_manager)
        assert response_context_manager._manual_result is None

        example = {
            'glossary': {
                'title': 'example glossary',
                'GlossDiv': {
                    'title': 'S',
                    'GlossList': {
                        'GlossEntry': {
                            'ID': 'SGML',
                            'SortAs': 'SGML',
                            'GlossTerm': 'Standard Generalized Markup Language',
                            'Acronym': 'SGML',
                            'Abbrev': 'ISO 8879:1986',
                            'GlossDef': {
                                'para': 'A meta-markup language, used to create markup languages such as DocBook.',
                                'GlossSeeAlso': ['GML', 'XML']
                            },
                            'GlossSee': 'markup',
                            'Additional': [
                                {
                                    'addtitle': 'test1',
                                    'addvalue': 'hello world',
                                },
                                {
                                    'addtitle': 'test2',
                                    'addvalue': 'good stuff',
                                },
                            ]
                        }
                    }
                }
            }
        }

        # 1 match in multiple values (list)
        handler = ValidationHandlerAction(
            False,
            expression='$.*..GlossSeeAlso[*]',
            match_with='XML',
        )
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert response_context_manager._manual_result is None

        # no match in multiple values (list)
        handler = ValidationHandlerAction(
            False,
            expression='$.*..GlossSeeAlso[*]',
            match_with='YAML',
        )
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert getattr(response_context_manager, '_manual_result', None) is not None
        response_context_manager._manual_result = None

        handler = ValidationHandlerAction(
            False,
            expression='$.glossary.title',
            match_with='.*ary$',
        )
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert response_context_manager._manual_result is None

        handler = ValidationHandlerAction(
            False,
            expression='$..Additional[?addtitle="test2"].addvalue',
            match_with='.*stuff$',
        )
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert response_context_manager._manual_result is None

        handler = ValidationHandlerAction(
            False,
            expression='$.`this`',
            match_with='False',
        )
        handler((TransformerContentType.JSON, True), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
        response_context_manager._manual_result = None

        scenario.failure_exception = None

        with pytest.raises(ResponseHandlerError):
            handler((TransformerContentType.JSON, True), user, None)

        scenario.failure_exception = StopUser

        with pytest.raises(StopUser):
            handler((TransformerContentType.JSON, True), user, None)

        scenario.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            handler((TransformerContentType.JSON, True), user, None)

        scenario.failure_exception = None

        handler((TransformerContentType.JSON, False), user, response_context_manager)
        assert response_context_manager._manual_result is None


class TestSaveHandlerAction:
    def test___init__(self) -> None:
        handler = SaveHandlerAction('foobar', expression='$.hello.world', match_with='foo')

        assert issubclass(handler.__class__, ResponseHandlerAction)
        assert handler.variable == 'foobar'
        assert handler.expression == '$.hello.world'
        assert handler.match_with == 'foo'
        assert handler.expected_matches == 1

    def test___call__(self, locust_fixture: LocustFixture) -> None:
        user = TestUser(locust_fixture.env)
        response = Response()
        response._content = '{}'.encode('utf-8')
        response.status_code = 200
        response_context_manager = ResponseContextManager(response, locust_fixture.env.events.request, {})
        response_context_manager._entered = True
        scenario = GrizzlyContextScenario(index=1)
        scenario.name = 'test-scenario'
        user._scenario = scenario

        assert 'test' not in user.context_variables

        handler = SaveHandlerAction('test', expression='.*', match_with='.*')
        with pytest.raises(TypeError) as te:
            handler((TransformerContentType.UNDEFINED, {'test': {'value': 'test'}}), user, response_context_manager)
        assert 'could not find a transformer for UNDEFINED' in str(te)

        with pytest.raises(TypeError) as te:
            handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
        assert 'is not a valid expression' in str(te)

        handler = SaveHandlerAction('test', expression='$.test.value', match_with='.*')

        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None
        assert user.context_variables.get('test', None) == 'test'
        del user.context_variables['test']

        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None
        assert user.context_variables.get('test', None) == 'nottest'
        del user.context_variables['test']

        user.set_context_variable('value', 'test')
        handler = SaveHandlerAction('test', expression='$.test.value', match_with='.*({{ value }})$')

        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None
        assert user.context_variables.get('test', None) == 'test'
        del user.context_variables['test']

        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None
        assert user.context_variables.get('test', None) == 'test'
        del user.context_variables['test']

        # failed
        handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
        assert user.context_variables.get('test', 'test') is None

        scenario.failure_exception = None

        with pytest.raises(StopUser):
            handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), user, None)

        scenario.failure_exception = StopUser

        with pytest.raises(StopUser):
            handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), user, None)

        scenario.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), user, None)

        # multiple matches
        handler = SaveHandlerAction('test', expression='$.test[*].value', match_with='.*t.*')
        handler((TransformerContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), RestartScenario)
        assert user._context['variables']['test'] is None

        with pytest.raises(RestartScenario):
            handler((TransformerContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), user, None)

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
                    }
                }
            ),
            user,
            response_context_manager,
        )

        test_object = user.context_variables.get('test_object', None)
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
                    }
                }
            ),
            user,
            response_context_manager,
        )

        test_list = user.context_variables.get('test_list', None)
        assert jsonloads(test_list) == [
            'prop41',
            True,
            'prop42',
            300,
        ]

        handler = SaveHandlerAction(
            'test_list',
            expression='$.test[?hello="world"].value',
            match_with='.*',
            expected_matches=-1,
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
                        }
                    ],
                }
            ),
            user,
            response_context_manager,
        )

        test_list = user.context_variables.get('test_list', None)
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
                }
            ),
            user,
            response_context_manager,
        )

        test_list = user.context_variables.get('test_list', None)
        assert jsonloads(test_list) == [
            'prop41',
            'prop42',
        ]


class TestResponseHandler:
    def test___init__(self, locust_fixture: LocustFixture) -> None:
        ResponseHandler.host = None

        with pytest.raises(LocustError):
            ResponseHandler(locust_fixture.env)

        fake_user_type = type('FakeResponseHandlerUser', (ResponseHandler, HttpRequests, ), {
            'host': '',
        })

        user = fake_user_type(locust_fixture.env)

        assert issubclass(user.__class__, ResponseEvent)
        assert isinstance(user.client, ResponseEventSession)
        assert isinstance(user.response_event, EventHook)
        assert len(user.response_event._handlers) == 1

        ResponseHandler.host = ''
        user = ResponseHandler(locust_fixture.env)
        assert user.client is None
        assert isinstance(user.response_event, EventHook)
        assert len(user.response_event._handlers) == 1

    def test_response_handler_response_context(self, mocker: MockerFixture, locust_fixture: LocustFixture) -> None:
        ResponseHandler.host = 'http://example.com'
        user = ResponseHandler(locust_fixture.env)
        test_user = TestUser(locust_fixture.env)

        response = Response()
        response._content = jsondumps({}).encode('utf-8')
        response.status_code = 200
        response_context_manager = ResponseContextManager(response, None, {})

        request_event = mocker.patch.object(response_context_manager, '_request_event', autospec=True)

        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/v2/test')

        # edge scenario -- from RestApiUser and *_token calls, they don't have a RequestTask
        original_response = request.response
        setattr(request, 'response', None)

        user.response_handler('test', response_context_manager, request, test_user)

        request.response = original_response

        payload_handler = mocker.MagicMock()
        metadata_handler = mocker.MagicMock()

        # no handler called
        user.response_handler('test', response_context_manager, request, test_user)

        assert request_event.call_count == 0

        # payload handler called
        request.response.handlers.add_payload(payload_handler)
        request.response.content_type = TransformerContentType.JSON
        user.response_handler('test', response_context_manager, request, test_user)

        assert metadata_handler.call_count == 0
        payload_handler.assert_called_once_with((TransformerContentType.JSON, {}), test_user, response_context_manager)
        request.response.handlers.payload.clear()
        payload_handler.reset_mock()

        # metadata handler called
        request.response.handlers.add_metadata(metadata_handler)
        user.response_handler('test', response_context_manager, request, test_user)

        assert payload_handler.call_count == 0
        metadata_handler.assert_called_once_with((TransformerContentType.JSON, {}), test_user, response_context_manager)
        metadata_handler.reset_mock()
        request.response.handlers.metadata.clear()

        # invalid json content in payload
        response._content = '{"test: "value"}'.encode('utf-8')
        response_context_manager = ResponseContextManager(response, None, {})
        response_context_manager._entered = True
        mocker.patch.object(response_context_manager, '_request_event', autospec=True)
        request.response.handlers.add_payload(payload_handler)

        assert response_context_manager._manual_result is None

        user.response_handler('test', response_context_manager, request, test_user)

        assert payload_handler.call_count == 0
        assert metadata_handler.call_count == 0
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        assert 'failed to transform' in str(response_context_manager._manual_result)
        request.response.handlers.payload.clear()

        # XML in response
        response._content = '''<?xml version="1.0" encoding="UTF-8"?>
        <test>
            value
        </test>'''.encode('utf-8')

        request.response.content_type = TransformerContentType.XML
        request.response.handlers.add_payload(payload_handler)
        user.response_handler('test', response_context_manager, request, test_user)

        assert payload_handler.call_count == 1
        ((call_content_type, call_payload), call_user, call_context, ), _ = payload_handler.call_args_list[0]
        assert call_content_type == TransformerContentType.XML
        assert isinstance(call_payload, XML._Element)
        assert call_user is test_user
        assert call_context is response_context_manager

        request.response.content_type = TransformerContentType.XML
        response._content = '''<?xml encoding="UTF-8"?>
        <test>
            value
        </test>'''.encode('utf-8')

        user.response_handler('test', response_context_manager, request, test_user)
        assert payload_handler.call_count == 1
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        assert 'failed to transform' in str(response_context_manager._manual_result)

    def test_response_handler_custom_response(self, mocker: MockerFixture, locust_fixture: LocustFixture) -> None:
        ResponseHandler.host = 'http://example.com'
        user = ResponseHandler(locust_fixture.env)
        test_user = TestUser(locust_fixture.env)

        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/v2/test')

        payload_handler = mocker.MagicMock()
        metadata_handler = mocker.MagicMock()

        # no handler called
        user.response_handler('test', (None, ''), request, test_user)

        # payload handler called
        request.response.content_type = TransformerContentType.JSON
        request.response.handlers.add_payload(payload_handler)
        user.response_handler('test', (None, '{}'), request, test_user)

        payload_handler.assert_called_once_with((TransformerContentType.JSON, {}), test_user, None)
        payload_handler.reset_mock()
        request.response.handlers.payload.clear()

        # metadata handler called
        request.response.handlers.add_metadata(metadata_handler)
        user.response_handler('test', ({}, None), request, test_user)

        metadata_handler.assert_called_once_with((TransformerContentType.JSON, {}), test_user, None)
        metadata_handler.reset_mock()
        request.response.handlers.metadata.clear()

        # invalid json content in payload
        request.response.handlers.add_payload(payload_handler)

        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', (None, '{"test: "value"'), request, test_user)
        assert 'failed to transform' in str(e)
        assert payload_handler.call_count == 0

        request.response.content_type = TransformerContentType.JSON
        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', (None, '{"test: "value"'), request, test_user)
        assert 'failed to transform input as JSON' in str(e)

        request.response.content_type = TransformerContentType.XML
        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', ({}, '{"test": "value"}'), request, test_user)
        assert 'failed to transform input as XML' in str(e)

        request.response.content_type = TransformerContentType.PLAIN
        user.response_handler('test', ({}, '{"test": "value"}'), request, test_user)
        assert payload_handler.call_count == 1
        payload_handler.reset_mock()

        # XML input
        request.response.content_type = TransformerContentType.XML
        user.response_handler(
            'test',
            (
                None,
                '''<?xml version="1.0" encoding="UTF-8"?>
                <test>
                    value
                </test>''',
            ),
            request,
            test_user,
        )

        assert payload_handler.call_count == 1
        ((call_content_type, call_payload), call_user, call_context, ), _ = payload_handler.call_args_list[0]
        assert call_content_type == TransformerContentType.XML
        assert isinstance(call_payload, XML._Element)
        assert call_user is test_user
        assert call_context is None

        with pytest.raises(ResponseHandlerError):
            user.response_handler(
                'test',
                (
                    None,
                    '''<?xml encoding="UTF-8"?>
                    <test>
                        value
                    </test>''',
                ),
                request,
                test_user,
            )

        request.response.content_type = TransformerContentType.UNDEFINED

        with pytest.raises(ResponseHandlerError) as rhe:
            user.response_handler(
                'test',
                (
                    None,
                    '''<?xml version="1.0" encoding="UTF-8"?>
                    <test>
                        value
                    </test>''',
                ),
                request,
                test_user,
            )
        assert rhe.value.message is not None
        assert rhe.value.message.startswith('failed to transform: <?xml version=')
        assert rhe.value.message.endswith('</test> with content type UNDEFINED')
