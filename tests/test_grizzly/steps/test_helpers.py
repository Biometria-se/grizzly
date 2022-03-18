import os
import json
import shutil

from typing import List, Any, cast

import pytest

from behave.model import Table, Row
from _pytest.tmpdir import TempPathFactory
from locust.exception import CatchResponseError
from locust.clients import ResponseContextManager
from requests.models import Response

from grizzly.context import GrizzlyContext
from grizzly.types import RequestMethod, ResponseTarget, ResponseAction
from grizzly.tasks import RequestTask, WaitTask
from grizzly.exceptions import ResponseHandlerError
from grizzly.steps.helpers import (
    add_validation_handler,
    add_save_handler,
    add_request_task,
    add_request_task_response_status_codes,
    normalize_step_name,
    generate_save_handler,
    generate_validation_handler,
    _add_response_handler,
    get_matches,
)

from grizzly_extras.transformer import TransformerContentType

from ..helpers import TestUser
from ..fixtures import BehaveFixture, GrizzlyFixture, LocustFixture


def test_add_request_task_response_status_codes() -> None:
    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')

    assert request.response.status_codes == [200]

    add_request_task_response_status_codes(request, '-200')
    assert request.response.status_codes == []

    add_request_task_response_status_codes(request, '200,302, 400')
    assert request.response.status_codes == [200, 302, 400]


def test_add_request_task(grizzly_fixture: GrizzlyFixture, tmp_path_factory: TempPathFactory) -> None:
    behave = grizzly_fixture.behave
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenario.context['host'] = 'http://test'

    grizzly.scenario.tasks = []

    assert len(grizzly.scenario.tasks) == 0

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.POST, source='{}')

    assert len(grizzly.scenario.tasks) == 0

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.POST, source='{}', endpoint='http://test/api/v1/test')

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', endpoint='/api/v1/test')

    add_request_task(behave, method=RequestMethod.POST, source='{}', endpoint='/api/v1/test')

    assert len(grizzly.scenario.tasks) == 1
    assert isinstance(grizzly.scenario.tasks[0], RequestTask)
    assert grizzly.scenario.tasks[0].name == '<unknown>'

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', name='test')

    add_request_task(behave, method=RequestMethod.from_string('POST'), source='{}', name='test')

    assert len(grizzly.scenario.tasks) == 2
    assert isinstance(grizzly.scenario.tasks[1], RequestTask)
    assert grizzly.scenario.tasks[0].endpoint == grizzly.scenario.tasks[1].endpoint
    assert grizzly.scenario.tasks[1].name == 'test'

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', name='test', endpoint='/api/v2/test')

    add_request_task(behave, method=RequestMethod.POST, source='{}', name='test', endpoint='/api/v2/test')

    assert len(grizzly.scenario.tasks) == 3
    assert isinstance(grizzly.scenario.tasks[2], RequestTask)
    assert grizzly.scenario.tasks[1].endpoint != grizzly.scenario.tasks[2].endpoint
    assert grizzly.scenario.tasks[2].name == 'test'

    template_path = grizzly_fixture.request_task.context_root
    template_name = grizzly_fixture.request_task.relative_path
    template_full_path = os.path.join(template_path, template_name)

    add_request_task(behave, method=RequestMethod.SEND, source=template_full_path, name='my_blob', endpoint='my_container')

    with open(template_full_path, 'r') as fd:
        template_source = json.dumps(json.load(fd))

    assert len(grizzly.scenario.tasks) == 4
    assert isinstance(grizzly.scenario.tasks[-1], RequestTask)
    task = grizzly.scenario.tasks[-1]
    assert task.source == template_source
    assert task.endpoint == 'my_container'
    assert task.name == 'my_blob'
    assert task.response.content_type == TransformerContentType.UNDEFINED

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.POST, source='{}', name='test')

    add_request_task(behave, method=RequestMethod.SEND, source=template_full_path, name='my_blob2')
    assert len(grizzly.scenario.tasks) == 5
    assert isinstance(grizzly.scenario.tasks[-1], RequestTask)
    assert isinstance(grizzly.scenario.tasks[-2], RequestTask)
    assert grizzly.scenario.tasks[-1].source == template_source
    assert grizzly.scenario.tasks[-1].endpoint == grizzly.scenario.tasks[-2].endpoint
    assert grizzly.scenario.tasks[-1].name == 'my_blob2'

    try:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = os.path.dirname(test_context)
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        behave.config.base_dir = test_context_root
        test_template = test_context / 'template.j2.json'
        test_template.touch()
        test_template.write_text('{{ hello_world }}')

        rows: List[Row] = []
        rows.append(Row(['test'], ['-200,400']))
        rows.append(Row(['test'], ['302']))
        behave.table = Table(['test'], rows=rows)

        grizzly.scenario.tasks = [WaitTask(time=1.0)]

        with pytest.raises(ValueError) as e:
            add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json')
        assert 'previous task was not a request' in str(e)

        add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json', name='test', endpoint='/api/test')

        add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json', endpoint='/api/test')
        assert cast(RequestTask, grizzly.scenario.tasks[-1]).name == 'template'

        grizzly.scenario.tasks.clear()

        test_datatable_template = test_context / 'datatable_template.j2.json'
        test_datatable_template.write_text('Hello {{ name }} and good {{ time_of_day }}!')

        values = [
            ['bob', 'morning', '{{ AtomicRandomString.object }} is garbage'],
            ['alice', 'noon', 'i like {{ fruit }}'],
            ['chad', 'evening', 'have you tried {{ AtomicDate.action }} it off and on again?'],
            ['dave', 'night', 'yabba {{ AtomicCsvRow.response.word }} doo'],
        ]

        rows = []
        for value in values:
            rows.append(Row(['name', 'time_of_day', 'quote'], value))
        behave.table = Table(['name', 'time_of_day', 'quote'], rows=rows)

        add_request_task(behave, method=RequestMethod.SEND, source='datatable_template.j2.json', name='quote: {{ quote }}', endpoint='/api/test/{{ time_of_day }}')

        assert len(grizzly.scenario.tasks) == 4

        for i, t in enumerate(grizzly.scenario.tasks):
            request = cast(RequestTask, t)
            name, time_of_day, quote = values[i]
            assert request.name == f'quote: {quote}'
            assert request.endpoint == f'/api/test/{time_of_day}'
            assert request.source == f'Hello {name} and good {time_of_day}!'
    finally:
        del os.environ['GRIZZLY_CONTEXT_ROOT']
        shutil.rmtree(test_context_root)

    with pytest.raises(ValueError) as ve:
        add_request_task(behave, method=RequestMethod.GET, endpoint='hello | world:False', source=None, name='hello-world')
    assert 'incorrect format in arguments: "world:False"' in str(ve)

    with pytest.raises(ValueError) as ve:
        add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | content_type=asdf', name='hello-world')
    assert '"asdf" is an unknown response content type' in str(ve)

    add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | content_type=json', name='hello-world')

    task = cast(RequestTask, grizzly.scenario.tasks[-1])
    assert task.endpoint == 'hello world'
    assert task.response.content_type == TransformerContentType.JSON

    add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | expression=$.test.value, content_type=json', name='hello-world')

    task = cast(RequestTask, grizzly.scenario.tasks[-1])
    assert task.endpoint == 'hello world | expression=$.test.value'
    assert task.response.content_type == TransformerContentType.JSON

    add_request_task(behave, method=RequestMethod.GET, source=None, endpoint=None, name='world-hello')

    task = cast(RequestTask, grizzly.scenario.tasks[-1])
    assert task.endpoint == 'hello world | expression=$.test.value'
    assert task.response.content_type == TransformerContentType.JSON


def test_generate_save_handler(locust_fixture: LocustFixture) -> None:
    user = TestUser(locust_fixture.env)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, locust_fixture.env.events.request, {})
    response_context_manager._entered = True

    assert 'test' not in user.context_variables

    handler = generate_save_handler('$.', '.*', 'test')
    with pytest.raises(TypeError) as te:
        handler((TransformerContentType.UNDEFINED, {'test': {'value': 'test'}}), user, response_context_manager)
    assert 'could not find a transformer for UNDEFINED' in str(te)

    with pytest.raises(TypeError) as te:
        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert 'is not a valid expression' in str(te)

    handler = generate_save_handler('$.test.value', '.*', 'test')

    handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test', None) == 'test'
    del user.context_variables['test']

    handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test', None) == 'nottest'
    del user.context_variables['test']

    user.set_context_variable('value', 'test')
    handler = generate_save_handler('$.test.value', '.*({{ value }})$', 'test')

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
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    assert user.context_variables.get('test', 'test') is None

    with pytest.raises(ResponseHandlerError):
        handler((TransformerContentType.JSON, {'test': {'name': 'test'}}), user, None)

    # multiple matches
    handler = generate_save_handler('$.test[*].value', '.*t.*', 'test')
    handler((TransformerContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    assert user._context['variables']['test'] is None

    with pytest.raises(ResponseHandlerError):
        handler((TransformerContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), user, None)

    # save object dict
    handler = generate_save_handler(
        '$.test.prop2',
        '.*',
        'test_object',
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
    assert json.loads(test_object) == {
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
    handler = generate_save_handler(
        '$.test.prop4',
        '.*',
        'test_list',
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
    assert json.loads(test_list) == [
        'prop41',
        True,
        'prop42',
        300,
    ]


def test_generate_validation_handler_negative(locust_fixture: LocustFixture) -> None:
    user = TestUser(locust_fixture.env)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, locust_fixture.env.events.request, {})
    response_context_manager._entered = True

    handler = generate_validation_handler('$.test.value', 'test', False)

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
    handler = generate_validation_handler('{{ expression }}', '.*({{ value }})$', False)
    handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # ony allows 1 match per expression
    handler = generate_validation_handler('$.test[*].value', '.*(test)$', False)
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

    handler = generate_validation_handler('$.[*]', 'ID_31337', False)

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
    handler = generate_validation_handler('$.*..GlossSeeAlso[*]', 'XML', False)
    handler((TransformerContentType.JSON, example), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # no match in multiple values (list)
    handler = generate_validation_handler('$.*..GlossSeeAlso[*]', 'YAML', False)
    handler((TransformerContentType.JSON, example), user, response_context_manager)
    assert getattr(response_context_manager, '_manual_result', None) is not None
    response_context_manager._manual_result = None

    handler = generate_validation_handler('$.glossary.title', '.*ary$', False)
    handler((TransformerContentType.JSON, example), user, response_context_manager)
    assert response_context_manager._manual_result is None

    handler = generate_validation_handler('$..Additional[?addtitle="test2"].addvalue', '.*stuff$', False)
    handler((TransformerContentType.JSON, example), user, response_context_manager)
    assert response_context_manager._manual_result is None

    handler = generate_validation_handler('$.`this`', 'False', False)
    handler((TransformerContentType.JSON, True), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    response_context_manager._manual_result = None

    with pytest.raises(ResponseHandlerError):
        handler((TransformerContentType.JSON, True), user, None)

    handler((TransformerContentType.JSON, False), user, response_context_manager)
    assert response_context_manager._manual_result is None


def test_generate_validation_handler_positive(locust_fixture: LocustFixture) -> None:
    user = TestUser(locust_fixture.env)
    try:
        response = Response()
        response._content = '{}'.encode('utf-8')
        response.status_code = 200
        response_context_manager = ResponseContextManager(response, locust_fixture.env.events.request, {})
        response_context_manager._entered = True

        handler = generate_validation_handler('$.test.value', 'test', True)

        # match fixed string expression
        handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None

        # no match fixed string expression
        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

        # regexp match expression value
        handler = generate_validation_handler('$.test.value', '.*(test)$', True)
        handler((TransformerContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None

        # ony allows 1 match per expression
        handler = generate_validation_handler('$.test[*].value', '.*(test)$', True)
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
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None

        handler = generate_validation_handler('$.[*]', 'STTO_31337', True)

        # 1 match expression
        handler((TransformerContentType.JSON, ['STTO_1337', 'STTO_31337', 'STTO_73313']), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
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
        handler = generate_validation_handler('$.*..GlossSeeAlso[*]', '{{ format }}', True)
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None

        with pytest.raises(ResponseHandlerError):
            handler((TransformerContentType.JSON, example), user, None)

        # no match in multiple values (list)
        user.set_context_variable('format', 'YAML')
        handler = generate_validation_handler('$.*..GlossSeeAlso[*]', '{{ format }}', True)
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert response_context_manager._manual_result is None

        user.set_context_variable('property', 'title')
        user.set_context_variable('regexp', '.*ary$')
        handler = generate_validation_handler('$.glossary.{{ property }}', '{{ regexp }}', True)
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None

        handler = generate_validation_handler('$..Additional[?addtitle="test1"].addvalue', '.*world$', True)
        handler((TransformerContentType.JSON, example), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None

        handler = generate_validation_handler('$.`this`', 'False', True)
        handler((TransformerContentType.JSON, True), user, response_context_manager)
        assert response_context_manager._manual_result is None

        handler((TransformerContentType.JSON, False), user, response_context_manager)
        assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
        response_context_manager._manual_result = None
    finally:
        assert user._context['variables'] is not TestUser(locust_fixture.env)._context['variables']


def test_add_save_handler(behave_fixture: BehaveFixture, locust_fixture: LocustFixture) -> None:
    user = TestUser(locust_fixture.env)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)
    response_context_manager._entered = True

    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    tasks = grizzly.scenario.tasks

    assert len(tasks) == 0
    assert len(user.context_variables) == 0

    # not preceeded by a request source
    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', 'test-variable')

    assert len(user.context_variables) == 0

    # add request source
    add_request_task(behave, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    task = cast(RequestTask, tasks[0])

    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.METADATA, '', 'test', 'test-variable')

    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata')

    try:
        grizzly.state.variables['test-variable-metadata'] = 'none'
        task.response.content_type = TransformerContentType.JSON
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 0
    finally:
        del grizzly.state.variables['test-variable-metadata']

    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload')

    try:
        grizzly.state.variables['test-variable-payload'] = 'none'

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 1
    finally:
        del grizzly.state.variables['test-variable-payload']

    metadata_handler = list(task.response.handlers.metadata)[0]
    payload_handler = list(task.response.handlers.payload)[0]

    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'metadata'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test-variable-metadata', None) == 'metadata'

    payload_handler((TransformerContentType.JSON, {'test': {'value': 'payload'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test-variable-metadata', None) == 'metadata'
    assert user.context_variables.get('test-variable-payload', None) == 'payload'

    metadata_handler((TransformerContentType.JSON, {'test': {'name': 'metadata'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    response_context_manager._manual_result = None
    assert user.context_variables.get('test-variable-metadata', 'metadata') is None

    payload_handler((TransformerContentType.JSON, {'test': {'name': 'payload'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    response_context_manager._manual_result = None
    assert user.context_variables.get('test-variable-payload', 'payload') is None

    # previous non RequestTask task
    grizzly.scenario.tasks.append(WaitTask(time=1.0))

    grizzly.state.variables['test'] = 'none'
    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test')

    # remove non RequestTask task
    grizzly.scenario.tasks.pop()

    # add_save_handler calling _add_response_handler incorrectly
    with pytest.raises(ValueError) as e:
        _add_response_handler(grizzly, ResponseTarget.PAYLOAD, ResponseAction.SAVE, '$test.value', '.*', variable=None)
    assert 'variable is not set' in str(e)


def test_add_validation_handler(behave_fixture: BehaveFixture, locust_fixture: LocustFixture) -> None:
    user = TestUser(locust_fixture.env)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)
    response_context_manager._entered = True

    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    tasks = grizzly.scenario.tasks
    assert len(tasks) == 0

    # not preceeded by a request source
    with pytest.raises(ValueError):
        add_validation_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', False)

    # add request source
    add_request_task(behave, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    # empty expression, fail
    with pytest.raises(ValueError):
        add_validation_handler(grizzly, ResponseTarget.METADATA, '', 'test', False)

    # add metadata response handler
    task = cast(RequestTask, tasks[0])
    task.response.content_type = TransformerContentType.JSON
    add_validation_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 0

    # add payload response handler
    add_validation_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', 'test', False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 1

    metadata_handler = list(task.response.handlers.metadata)[0]
    payload_handler = list(task.response.handlers.payload)[0]

    # test that they validates
    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    payload_handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # test that they validates, negative
    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'no-test'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    response_context_manager._manual_result = None

    payload_handler((TransformerContentType.JSON, {'test': {'value': 'no-test'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), CatchResponseError)
    response_context_manager._manual_result = None

    # add a second payload response handler
    user.add_context({'variables': {'property': 'name', 'name': 'bob'}})
    add_validation_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.{{ property }}', '{{ name }}', False)
    assert len(task.response.handlers.payload) == 2

    # test that they validates
    for handler in task.response.handlers.payload:
        handler((TransformerContentType.JSON, {'test': {'value': 'test', 'name': 'bob'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

    # add_validation_handler calling _add_response_handler incorrectly
    with pytest.raises(ValueError) as e:
        _add_response_handler(grizzly, ResponseTarget.PAYLOAD, ResponseAction.VALIDATE, '$.test', 'value', condition=None)
    assert 'condition is not set' in str(e)


def test_normalize_step_name() -> None:
    expected = 'this is just a "" of text with quoted ""'
    actual = normalize_step_name('this is just a "string" of text with quoted "words"')

    assert expected == actual


def test_get_matches() -> None:
    def match_get_values(input_payload: Any) -> List[str]:
        if str(input_payload) == 'world':
            return ['world']
        elif str(input_payload) == 'hello':
            return ['']
        else:
            return []

    def input_get_values(input_payload: Any) -> List[str]:
        return cast(List[str], input_payload)

    matches = get_matches(input_get_values, match_get_values, ['hello', 'world', 'foo', 'bar'])

    assert matches == (['hello', 'world', 'foo', 'bar'], ['world'],)
