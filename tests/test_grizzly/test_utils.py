import os
import json
import shutil

from typing import Any, Callable, Optional, Type, List, cast
from types import FunctionType

from gevent.monkey import patch_all
patch_all()

import pytest

from behave.model import Table, Row
from _pytest.tmpdir import TempdirFactory
from locust.exception import CatchResponseError
from requests.models import Response
from locust.user.users import User
from locust.env import Environment
from locust.clients import ResponseContextManager
from locust import TaskSet
from behave.runner import Context
from behave.model import Scenario
from behave.model_core import Status

from .helpers import TestUser
# pylint: disable=unused-import
from .fixtures import (
    locust_context,
    locust_environment,
    request_context,
    behave_context,
    behave_runner,
    behave_scenario,
)

from grizzly.users.meta.response_handler import ResponseHandlerError
from grizzly.utils import ModuleLoader, in_correct_section
from grizzly.utils import (
    add_validation_handler,
    add_save_handler,
    add_request_context,
    add_request_context_response_status_codes,
    catch,
    create_task_class_type,
    create_user_class_type,
    fail_direct,
    normalize_step_name,
    create_context_variable,
    generate_save_handler,
    generate_validation_handler,
    resolve_variable,
    _add_response_handler,
    get_matches,
)
from grizzly.types import RequestMethod, ResponseContentType
from grizzly.context import LocustContext, LocustContextScenario, RequestContext, ResponseTarget, ResponseAction
from grizzly.users import RestApiUser
from grizzly.tasks import TrafficIteratorTasks


class TestModuleLoader:
    def test_load_class_non_existent(self) -> None:
        class_name = 'ANonExistingModule'

        with pytest.raises(ModuleNotFoundError):
            ModuleLoader[User].load('a.non.existing.package', class_name)

        with pytest.raises(AttributeError):
            ModuleLoader[User].load('grizzly.users', class_name)


    @pytest.mark.usefixtures('locust_environment')
    def test_load_user_class(self, locust_environment: Environment) -> None:
        try:
            test_context = LocustContext()
            test_context.scenario.context['host'] = 'test'
            user_class_name = 'RestApiUser'
            for user_package in ['', 'grizzly.users.', 'grizzly.users.restapi.']:
                user_class_name_value = f'{user_package}{user_class_name}'
                user_class = cast(Type[User], ModuleLoader[User].load('grizzly.users', user_class_name_value))
                user_class.host = test_context.scenario.context['host']
                assert user_class.__module__ == 'grizzly.users.restapi'
                assert user_class.host == 'test'
                assert hasattr(user_class, 'tasks')

                # try to initialize it, without any token information
                user_class_instance = user_class(locust_environment)

                # with token context
                test_context.scenario.context['token'] = {
                    'client_secret': 'asdf',
                    'client_id': 'asdf',
                    'url': 'http://test',
                    'resource': None,
                }
                user_class_instance = user_class(locust_environment)

                # without token context
                test_context.scenario.context['token'] = {
                    'client_secret': None,
                    'client_id': None,
                    'url': None,
                    'resource': None,
                }

                assert type(user_class_instance).__name__ == 'RestApiUser'
                assert user_class_instance.host == 'test'
                assert hasattr(user_class_instance, 'tasks')
        finally:
            LocustContext.destroy()


@pytest.mark.usefixtures('behave_context', 'behave_scenario')
def test_catch(behave_context: Context, behave_scenario: Scenario) -> None:
    @catch(KeyboardInterrupt)
    def raises_KeyboardInterrupt(context: Context, scenario: Scenario) -> None:
        raise KeyboardInterrupt()

    try:
        raises_KeyboardInterrupt(behave_context, behave_scenario)
    except KeyboardInterrupt:
        pytest.fail(f'function raised KeyboardInterrupt, when it should not have')

    assert behave_context.failed
    assert behave_scenario.status == Status.failed
    behave_context._set_root_attribute('failed', False)
    behave_scenario.set_status(Status.undefined)

    @catch(ValueError)
    def raises_ValueError_not(context: Context, scenario: Scenario) -> None:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        raises_ValueError_not(behave_context, behave_scenario)

    assert not behave_context.failed
    assert not behave_scenario.status == Status.failed
    behave_context._set_root_attribute('failed', False)
    behave_scenario.set_status(Status.undefined)

    @catch(ValueError)
    def raises_ValueError(context: Context, scenario: Optional[Scenario] = None) -> None:
        raise ValueError()

    with pytest.raises(ValueError):
        raises_ValueError(behave_context)

    @catch(NotImplementedError)
    def no_scenario_argument(context: Context, other: str) -> None:
        raise NotImplementedError()

    with pytest.raises(NotImplementedError):
        no_scenario_argument(behave_context, 'not a scenario')

    try:
        raises_ValueError(behave_context, behave_scenario)
    except ValueError:
        pytest.fail(f'function raised ValueError, when it should not have')


def test_add_request_context_response_status_codes() -> None:
    request = RequestContext(RequestMethod.SEND, name='test', endpoint='/api/test')

    assert request.response.status_codes == [200]

    add_request_context_response_status_codes(request, '-200')
    assert request.response.status_codes == []

    add_request_context_response_status_codes(request, '200,302, 400')
    assert request.response.status_codes == [200, 302, 400]


@pytest.mark.usefixtures('behave_context', 'locust_context')
def test_add_request_context(behave_context: Context, locust_context: Callable, tmpdir_factory: TempdirFactory) -> None:
    context_locust = cast(LocustContext, behave_context.locust)
    context_locust.scenario.context['host'] = 'http://test'

    assert len(context_locust.scenario.tasks) == 0

    with pytest.raises(ValueError):
        add_request_context(behave_context, method=RequestMethod.POST, source='{}')

    assert len(context_locust.scenario.tasks) == 0

    with pytest.raises(ValueError):
        add_request_context(behave_context, method=RequestMethod.POST, source='{}', endpoint='http://test/api/v1/test')

    with pytest.raises(ValueError):
        add_request_context(behave_context, method=RequestMethod.from_string('TEST'), source='{}', endpoint='/api/v1/test')

    add_request_context(behave_context, method=RequestMethod.POST, source='{}', endpoint='/api/v1/test')

    assert len(context_locust.scenario.tasks) == 1
    assert isinstance(context_locust.scenario.tasks[0], RequestContext)
    assert context_locust.scenario.tasks[0].name == '<unknown>'

    with pytest.raises(ValueError):
        add_request_context(behave_context, method=RequestMethod.from_string('TEST'), source='{}', name='test')

    add_request_context(behave_context, method=RequestMethod.from_string('POST'), source='{}', name='test')

    assert len(context_locust.scenario.tasks) == 2
    assert isinstance(context_locust.scenario.tasks[1], RequestContext)
    assert context_locust.scenario.tasks[0].endpoint == context_locust.scenario.tasks[1].endpoint
    assert context_locust.scenario.tasks[1].name == 'test'

    with pytest.raises(ValueError):
        add_request_context(behave_context, method=RequestMethod.from_string('TEST'), source='{}', name='test', endpoint='/api/v2/test')

    add_request_context(behave_context, method=RequestMethod.POST, source='{}', name='test', endpoint='/api/v2/test')

    assert len(context_locust.scenario.tasks) == 3
    assert isinstance(context_locust.scenario.tasks[2], RequestContext)
    assert context_locust.scenario.tasks[1].endpoint != context_locust.scenario.tasks[2].endpoint
    assert context_locust.scenario.tasks[2].name == 'test'

    _, _, _, (template_path, template_name, _) = locust_context()
    template_full_path = os.path.join(template_path, template_name)
    add_request_context(behave_context, method=RequestMethod.SEND, source=template_full_path, name='my_blob', endpoint='my_container')

    with open(template_full_path, 'r') as fd:
        template_source = json.dumps(json.load(fd))

    assert len(context_locust.scenario.tasks) == 4
    assert isinstance(context_locust.scenario.tasks[-1], RequestContext)
    assert context_locust.scenario.tasks[-1].source == template_source
    assert context_locust.scenario.tasks[-1].endpoint == 'my_container'
    assert context_locust.scenario.tasks[-1].name == 'my_blob'

    with pytest.raises(ValueError):
        add_request_context(behave_context, method=RequestMethod.POST, source='{}', name='test')

    add_request_context(behave_context, method=RequestMethod.SEND, source=template_full_path, name='my_blob2')
    assert len(context_locust.scenario.tasks) == 5
    assert isinstance(context_locust.scenario.tasks[-1], RequestContext)
    assert isinstance(context_locust.scenario.tasks[-2], RequestContext)
    assert context_locust.scenario.tasks[-1].source == template_source
    assert context_locust.scenario.tasks[-1].endpoint == context_locust.scenario.tasks[-2].endpoint
    assert context_locust.scenario.tasks[-1].name == 'my_blob2'

    try:
        test_context = tmpdir_factory.mktemp('test_context').mkdir('requests')
        test_context_root = os.path.dirname(str(test_context))
        os.environ['LOCUST_CONTEXT_ROOT'] = test_context_root
        behave_context.config.base_dir = test_context_root
        test_template = test_context.join('template.j2.json')
        test_template.write('{{ hello_world }}')

        rows: List[Row] = []
        rows.append(Row(['test'], ['-200,400']))
        rows.append(Row(['test'], ['302']))
        behave_context.table = Table(['test'], rows=rows)

        context_locust.scenario.tasks = [1.0]

        with pytest.raises(ValueError) as e:
            add_request_context(behave_context, method=RequestMethod.PUT, source='template.j2.json')
        assert 'previous task was not a request' in str(e)

        add_request_context(behave_context, method=RequestMethod.PUT, source='template.j2.json', name='test', endpoint='/api/test')

        add_request_context(behave_context, method=RequestMethod.PUT, source='template.j2.json', endpoint='/api/test')
        assert cast(RequestContext, context_locust.scenario.tasks[-1]).name == 'template'
    finally:
        del os.environ['LOCUST_CONTEXT_ROOT']
        shutil.rmtree(test_context_root)


@pytest.mark.usefixtures('locust_environment')
def test_generate_save_handler(locust_environment: Environment) -> None:
    user = TestUser(locust_environment)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)

    assert 'test' not in user.context_variables

    handler = generate_save_handler('$.', '.*', 'test')
    with pytest.raises(TypeError) as te:
        handler((ResponseContentType.GUESS, {'test': {'value': 'test'}}), user, response_context_manager)
    assert 'could not find a transformer for GUESS' in str(te)

    with pytest.raises(TypeError) as te:
        handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert 'is not a valid expression' in str(te)

    handler = generate_save_handler('$.test.value', '.*', 'test')

    handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test', None) == 'test'
    del user.context_variables['test']

    handler((ResponseContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test', None) == 'nottest'
    del user.context_variables['test']

    user.set_context_variable('value', 'test')
    handler = generate_save_handler('$.test.value', '.*({{ value }})$', 'test')

    handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test', None) == 'test'
    del user.context_variables['test']

    handler((ResponseContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test', None) == 'test'
    del user.context_variables['test']

    # failed
    handler((ResponseContentType.JSON, {'test': {'name': 'test'}}), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    assert user.context_variables.get('test', 'test') is None

    with pytest.raises(ResponseHandlerError):
        handler((ResponseContentType.JSON, {'test': {'name': 'test'}}), user, None)

    # multiple matches
    handler = generate_save_handler('$.test[*].value', '.*t.*', 'test')
    handler((ResponseContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    assert user._context['variables']['test'] is None

    with pytest.raises(ResponseHandlerError):
        handler((ResponseContentType.JSON, {'test': [{'value': 'test'}, {'value': 'test'}]}), user, None)



@pytest.mark.usefixtures('locust_environment')
def test_generate_validation_handler_negative(locust_environment: Environment) -> None:
    user = TestUser(locust_environment)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)

    handler = generate_validation_handler('$.test.value', 'test', False)

    # match fixed string expression
    handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # no match fixed string expression
    handler((ResponseContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
    assert not response_context_manager._manual_result == None
    response_context_manager._manual_result = None

    # regexp match expression value
    user.set_context_variable('expression', '$.test.value')
    user.set_context_variable('value', 'test')
    handler = generate_validation_handler('{{ expression }}', '.*({{ value }})$', False)
    handler((ResponseContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # ony allows 1 match per expression
    handler = generate_validation_handler('$.test[*].value', '.*(test)$', False)
    handler(
        (ResponseContentType.JSON, {'test': [{'value': 'nottest'}, {'value': 'reallynottest'}, {'value': 'test'}]}),
        user,
        response_context_manager,
    )
    assert not response_context_manager._manual_result == None
    response_context_manager._manual_result = None

    # 1 match expression
    handler(
        (ResponseContentType.JSON, {'test': [{'value': 'not'}, {'value': 'reallynot'}, {'value': 'test'}]}),
        user,
        response_context_manager,
    )
    assert response_context_manager._manual_result is None

    handler = generate_validation_handler('$.[*]', 'ID_31337', False)

    # 1 match expression
    handler((ResponseContentType.JSON, ['ID_1337', 'ID_31337', 'ID_73313']), user, response_context_manager)
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
    handler((ResponseContentType.JSON, example), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # no match in multiple values (list)
    handler = generate_validation_handler('$.*..GlossSeeAlso[*]', 'YAML', False)
    handler((ResponseContentType.JSON, example), user, response_context_manager)
    assert not response_context_manager._manual_result == None
    response_context_manager._manual_result = None

    handler = generate_validation_handler('$.glossary.title', '.*ary$', False)
    handler((ResponseContentType.JSON, example), user, response_context_manager)
    assert response_context_manager._manual_result is None

    handler = generate_validation_handler('$..Additional[?addtitle="test2"].addvalue', '.*stuff$', False)
    handler((ResponseContentType.JSON, example), user, response_context_manager)
    assert response_context_manager._manual_result is None

    handler = generate_validation_handler('$.`this`', 'False', False)
    handler((ResponseContentType.JSON, True), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    response_context_manager._manual_result = None

    with pytest.raises(ResponseHandlerError):
        handler((ResponseContentType.JSON, True), user, None)

    handler((ResponseContentType.JSON, False), user, response_context_manager)
    assert response_context_manager._manual_result is None


@pytest.mark.usefixtures('locust_environment')
def test_generate_validation_handler_positive(locust_environment: Environment) -> None:
    user = TestUser(locust_environment)
    try:
        response = Response()
        response._content = '{}'.encode('utf-8')
        response.status_code = 200
        response_context_manager = ResponseContextManager(response, None, None)

        handler = generate_validation_handler('$.test.value', 'test', True)

        # match fixed string expression
        handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None

        # no match fixed string expression
        handler((ResponseContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

        # regexp match expression value
        handler = generate_validation_handler('$.test.value', '.*(test)$', True)
        handler((ResponseContentType.JSON, {'test': {'value': 'nottest'}}), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None

        # ony allows 1 match per expression
        handler = generate_validation_handler('$.test[*].value', '.*(test)$', True)
        handler(
            (ResponseContentType.JSON, {'test': [{'value': 'nottest'}, {'value': 'reallynottest'}, {'value': 'test'}]}),
            user,
            response_context_manager,
        )
        assert response_context_manager._manual_result is None

        # 1 match expression
        handler(
            (ResponseContentType.JSON, {'test': [{'value': 'not'}, {'value': 'reallynot'}, {'value': 'test'}]}),
            user,
            response_context_manager,
        )
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None

        handler = generate_validation_handler('$.[*]', 'STTO_31337', True)

        # 1 match expression
        handler((ResponseContentType.JSON, ['STTO_1337', 'STTO_31337', 'STTO_73313']), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
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
        handler((ResponseContentType.JSON, example), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None

        with pytest.raises(ResponseHandlerError):
            handler((ResponseContentType.JSON, example), user, None)

        # no match in multiple values (list)
        user.set_context_variable('format', 'YAML')
        handler = generate_validation_handler('$.*..GlossSeeAlso[*]', '{{ format }}', True)
        handler((ResponseContentType.JSON, example), user, response_context_manager)
        assert response_context_manager._manual_result is None

        user.set_context_variable('property', 'title')
        user.set_context_variable('regexp', '.*ary$')
        handler = generate_validation_handler('$.glossary.{{ property }}', '{{ regexp }}', True)
        handler((ResponseContentType.JSON, example), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None

        handler = generate_validation_handler('$..Additional[?addtitle="test1"].addvalue', '.*world$', True)
        handler((ResponseContentType.JSON, example), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None

        handler = generate_validation_handler('$.`this`', 'False', True)
        handler((ResponseContentType.JSON, True), user, response_context_manager)
        assert response_context_manager._manual_result is None

        handler((ResponseContentType.JSON, False), user, response_context_manager)
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        response_context_manager._manual_result = None
    finally:
        assert user._context['variables'] != TestUser(locust_environment)._context['variables']


@pytest.mark.usefixtures('behave_context', 'locust_environment')
def test_add_save_handler(behave_context: Context, locust_environment: Environment) -> None:
    user = TestUser(locust_environment)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)
    context_locust = cast(LocustContext, behave_context.locust)
    tasks = context_locust.scenario.tasks

    assert len(tasks) == 0
    assert len(user.context_variables) == 0

    # not preceeded by a request source
    with pytest.raises(ValueError):
        add_save_handler(context_locust, ResponseTarget.METADATA, '$.test.value', 'test', 'test-variable')

    assert len(user.context_variables) == 0

    # add request source
    add_request_context(behave_context, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    task = cast(RequestContext, tasks[0])

    with pytest.raises(ValueError):
        add_save_handler(context_locust, ResponseTarget.METADATA, '', 'test', 'test-variable')

    with pytest.raises(ValueError):
        add_save_handler(context_locust, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata')

    try:
        context_locust.state.variables['test-variable-metadata'] = 'none'
        add_save_handler(context_locust, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 0
    finally:
        del context_locust.state.variables['test-variable-metadata']

    with pytest.raises(ValueError):
        add_save_handler(context_locust, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload')

    try:
        context_locust.state.variables['test-variable-payload'] = 'none'

        add_save_handler(context_locust, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 1
    finally:
        del context_locust.state.variables['test-variable-payload']

    metadata_handler = list(task.response.handlers.metadata)[0]
    payload_handler = list(task.response.handlers.payload)[0]

    metadata_handler((ResponseContentType.JSON, {'test': {'value': 'metadata'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test-variable-metadata', None) == 'metadata'

    payload_handler((ResponseContentType.JSON, {'test': {'value': 'payload'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test-variable-metadata', None) == 'metadata'
    assert user.context_variables.get('test-variable-payload', None) == 'payload'

    metadata_handler((ResponseContentType.JSON, {'test': {'name': 'metadata'}}), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    response_context_manager._manual_result = None
    assert user.context_variables.get('test-variable-metadata', 'metadata') is None

    payload_handler((ResponseContentType.JSON, {'test': {'name': 'payload'}}), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    response_context_manager._manual_result = None
    assert user.context_variables.get('test-variable-payload', 'payload') is None

    # previous non RequestContext task
    context_locust.scenario.tasks.append(1.0)

    context_locust.state.variables['test'] = 'none'
    with pytest.raises(ValueError):
        add_save_handler(context_locust, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test')

    # remove non RequestContext task
    context_locust.scenario.tasks.pop()

    # add_save_handler calling _add_response_handler incorrectly
    with pytest.raises(ValueError) as e:
        _add_response_handler(context_locust, ResponseTarget.PAYLOAD, ResponseAction.SAVE, '$test.value', '.*', variable=None)
    assert 'variable is not set' in str(e)



@pytest.mark.usefixtures('behave_context', 'locust_environment')
def test_add_validation_handler(behave_context: Context, locust_environment: Environment) -> None:
    user = TestUser(locust_environment)
    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)
    context_locust = cast(LocustContext, behave_context.locust)
    tasks = context_locust.scenario.tasks
    assert len(tasks) == 0

    # not preceeded by a request source
    with pytest.raises(ValueError):
        add_validation_handler(context_locust, ResponseTarget.METADATA, '$.test.value', 'test', False)

    # add request source
    add_request_context(behave_context, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    # empty expression, fail
    with pytest.raises(ValueError):
        add_validation_handler(context_locust, ResponseTarget.METADATA, '', 'test', False)

    # add metadata response handler
    add_validation_handler(context_locust, ResponseTarget.METADATA, '$.test.value', 'test', False)
    task = cast(RequestContext, tasks[0])
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 0

    # add payload response handler
    add_validation_handler(context_locust, ResponseTarget.PAYLOAD, '$.test.value', 'test', False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 1

    metadata_handler = list(task.response.handlers.metadata)[0]
    payload_handler = list(task.response.handlers.payload)[0]

    # test that they validates
    metadata_handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    payload_handler((ResponseContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # test that they validates, negative
    metadata_handler((ResponseContentType.JSON, {'test': {'value': 'no-test'}}), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    response_context_manager._manual_result = None

    payload_handler((ResponseContentType.JSON, {'test': {'value': 'no-test'}}), user, response_context_manager)
    assert isinstance(response_context_manager._manual_result, CatchResponseError)
    response_context_manager._manual_result = None

    # add a second payload response handler
    user.add_context({'variables': {'property': 'name', 'name': 'bob'}})
    add_validation_handler(context_locust, ResponseTarget.PAYLOAD, '$.test.{{ property }}', '{{ name }}', False)
    assert len(task.response.handlers.payload) == 2

    # test that they validates
    for handler in task.response.handlers.payload:
        handler((ResponseContentType.JSON, {'test': {'value': 'test', 'name': 'bob'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

    # add_validation_handler calling _add_response_handler incorrectly
    with pytest.raises(ValueError) as e:
        _add_response_handler(context_locust, ResponseTarget.PAYLOAD, ResponseAction.VALIDATE, '$.test', 'value', condition=None)
    assert 'condition is not set' in str(e)


def test_normalize_step_name() -> None:
    expected = 'this is just a "" of text with quoted ""'
    actual = normalize_step_name('this is just a "string" of text with quoted "words"')

    assert expected == actual


def test_in_correct_section() -> None:
    from grizzly.steps import step_setup_iterations
    assert in_correct_section(step_setup_iterations, ['grizzly.steps.scenario'])
    assert not in_correct_section(step_setup_iterations, ['grizzly.steps.background'])

    def step_custom(context: Context) -> None:
        pass

    # force AttributeError, for when a step function isn't part of a module
    setattr(step_custom, '__module__', None)

    assert in_correct_section(cast(FunctionType, step_custom), ['grizzly.steps.scenario'])


@pytest.mark.usefixtures('behave_context')
def test_fail_directly(behave_context: Context) -> None:
    behave_context.config.stop = False
    behave_context.config.verbose = True

    with fail_direct(behave_context):
        assert behave_context.config.stop == True
        assert behave_context.config.verbose == False

    assert behave_context.config.stop == False
    assert behave_context.config.verbose == True


@pytest.mark.usefixtures('locust_environment')
def test_create_user_class_type(locust_environment: Environment) -> None:
    scenario = LocustContextScenario()
    scenario.name = 'A scenario description'

    with pytest.raises(ValueError):
        create_user_class_type(scenario)

    scenario.user_class_name = 'RestApiUser'
    user_class_type_1 = create_user_class_type(scenario)
    user_class_type_1.host = 'http://localhost:8000'

    assert issubclass(user_class_type_1, (RestApiUser, User))
    assert user_class_type_1.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_1.host == 'http://localhost:8000'
    assert user_class_type_1.__module__ == 'locust.user.users'
    assert user_class_type_1._context == {
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            }
        }
    }
    user_type_1 = user_class_type_1(locust_environment)

    assert user_type_1.context() == {
        'log_all_requests': False,
        'variables': {},
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    scenario = LocustContextScenario()
    scenario.name = 'TestTestTest'
    scenario.user_class_name = 'RestApiUser'
    user_class_type_2 = create_user_class_type(
        scenario,
        {
            'test': {
                'value': 1,
            },
            'log_all_requests': True,
            'auth': {
                'refresh_time': 1337,
                'url': 'https://auth.example.com',
                'user': {
                    'username': 'grizzly-user',
                }
            },
        }
    )
    user_class_type_2.host = 'http://localhost:8001'

    assert issubclass(user_class_type_2, (RestApiUser, User))
    assert user_class_type_2.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_2.host == 'http://localhost:8001'
    assert user_class_type_2.__module__ == 'locust.user.users'
    assert user_class_type_2._context == {
        'log_all_requests': True,
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'url': 'https://auth.example.com',
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': 'grizzly-user',
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    user_type_2 = user_class_type_2(locust_environment)
    assert user_type_2.context() == {
        'log_all_requests': True,
        'variables': {},
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'url': 'https://auth.example.com',
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': 'grizzly-user',
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    scenario = LocustContextScenario()
    scenario.name = 'TestTestTest2'
    scenario.user_class_name = 'RestApiUser'
    scenario.context = {'test': {'value': 'hello world', 'description': 'simple text'}}
    user_class_type_3 = create_user_class_type(scenario, {'test': {'value': 1}})
    user_class_type_3.host = 'http://localhost:8002'

    assert issubclass(user_class_type_3, (RestApiUser, User))
    assert user_class_type_3.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_3.host == 'http://localhost:8002'
    assert user_class_type_3.__module__ == 'locust.user.users'
    assert user_class_type_3._context == {
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    assert user_class_type_1.host is not user_class_type_2.host
    assert user_class_type_2.host is not user_class_type_3.host

    with pytest.raises(AttributeError):
        scenario = LocustContextScenario()
        scenario.name = 'A scenario description'
        scenario.user_class_name = 'DoNotExistInGrizzlyUsersUser'
        create_user_class_type(scenario)


def test_create_task_class_type() -> None:
    scenario = LocustContextScenario()
    scenario.name = 'A scenario description'
    task_class_type_1 = create_task_class_type('TrafficIteratorTasks', scenario)

    assert issubclass(task_class_type_1, (TrafficIteratorTasks, TaskSet))
    assert task_class_type_1.__name__ == 'TrafficIteratorTasks_25867809'
    assert task_class_type_1.__module__ == 'locust.user.sequential_taskset'
    task_class_type_1.add_scenario_task(RequestContext(RequestMethod.POST, name='test-request', endpoint='/api/test'))

    scenario = LocustContextScenario()
    scenario.name = 'TestTestTest'
    task_class_type_2 = create_task_class_type('TrafficIteratorTasks', scenario)
    assert issubclass(task_class_type_2, (TrafficIteratorTasks, TaskSet))
    assert task_class_type_2.__name__ == 'TrafficIteratorTasks_cf4fa8aa'
    assert task_class_type_2.__module__ == 'locust.user.sequential_taskset'

    assert task_class_type_1.tasks != task_class_type_2.tasks

    with pytest.raises(AttributeError):
        scenario = LocustContextScenario()
        scenario.name = 'A scenario description'
        create_task_class_type('DoesNotExistInGrizzlyScenariosModel', scenario)


def test_create_context_variable() -> None:
    context_locust = LocustContext()

    try:
        assert create_context_variable(context_locust, 'test.value', '1') == {
            'test': {
                'value': 1,
            }
        }

        assert create_context_variable(context_locust, 'test.value', 'trUe') == {
            'test': {
                'value': True,
            }
        }

        assert create_context_variable(context_locust, 'test.value', 'AZURE') == {
            'test': {
                'value': 'AZURE',
            }
        }

        assert create_context_variable(context_locust, 'test.value', 'HOST') == {
            'test': {
                'value': 'HOST'
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(context_locust, 'test.value', '$env::HELLO_WORLD')

        os.environ['HELLO_WORLD'] = 'environment variable value'
        assert create_context_variable(context_locust, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': 'environment variable value',
            }
        }

        os.environ['HELLO_WORLD'] = 'true'
        assert create_context_variable(context_locust, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': True,
            }
        }

        os.environ['HELLO_WORLD'] = '1337'
        assert create_context_variable(context_locust, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': 1337,
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(context_locust, 'test.value', '$conf::test.auth.user.username')

        context_locust.state.configuration['test.auth.user.username'] = 'username'
        assert create_context_variable(context_locust, 'test.value', '$conf::test.auth.user.username') == {
            'test': {
                'value': 'username',
            }
        }

        context_locust.state.configuration['test.auth.refresh_time'] = 3000
        assert create_context_variable(context_locust, 'test.value', '$conf::test.auth.refresh_time') == {
            'test': {
                'value': 3000,
            }
        }
    finally:
        LocustContext.destroy()
        del os.environ['HELLO_WORLD']


def test_resolve_variable() -> None:
    context_locust = LocustContext()

    try:
        assert 'test' not in context_locust.state.variables
        with pytest.raises(AssertionError):
            resolve_variable(context_locust, '{{ test }}')

        context_locust.state.variables['test'] = 'some value'
        assert resolve_variable(context_locust, '{{ test }}') == 'some value'

        assert resolve_variable(context_locust, "now | format='%Y-%m-%d %H'") == "now | format='%Y-%m-%d %H'"

        assert resolve_variable(context_locust, "{{ test }} | format='%Y-%m-%d %H'") == "some value | format='%Y-%m-%d %H'"

        assert resolve_variable(context_locust, 'static value') == 'static value'
        assert resolve_variable(context_locust, '"static value"') == 'static value'
        assert resolve_variable(context_locust, "'static value'") == 'static value'
        assert resolve_variable(context_locust, "'static' value") == "'static' value"
        assert resolve_variable(context_locust, "static 'value'") == "static 'value'"

        with pytest.raises(ValueError):
            resolve_variable(context_locust, "'static value\"")

        with pytest.raises(ValueError):
            resolve_variable(context_locust, "static 'value\"")

        with pytest.raises(ValueError):
            resolve_variable(context_locust, "'static\" value")

        context_locust.state.variables['number'] = 100
        assert resolve_variable(context_locust, '{{ (number * 0.25) | int }}') == 25

        assert resolve_variable(context_locust, '{{ (number * 0.25 * 0.2) | int }}') == 5

        try:
            with pytest.raises(AssertionError):
                resolve_variable(context_locust, '$env::HELLO_WORLD')

            os.environ['HELLO_WORLD'] = 'first environment variable!'

            assert resolve_variable(context_locust, '$env::HELLO_WORLD') == 'first environment variable!'

            os.environ['HELLO_WORLD'] = 'first "environment" variable!'
            assert resolve_variable(context_locust, '$env::HELLO_WORLD') == 'first "environment" variable!'
        finally:
            del os.environ['HELLO_WORLD']

        with pytest.raises(AssertionError):
            resolve_variable(context_locust, '$conf::sut.host')

        context_locust.state.configuration['sut.host'] = 'http://host.docker.internal:8003'

        assert resolve_variable(context_locust, '$conf::sut.host')

        context_locust.state.configuration['sut.greeting'] = 'hello "{{ test }}"!'
        assert resolve_variable(context_locust, '$conf::sut.greeting') == 'hello "{{ test }}"!'

        with pytest.raises(ValueError):
            resolve_variable(context_locust, '$test::hello')

        assert resolve_variable(context_locust, '') == ''
    finally:
        LocustContext.destroy()


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
