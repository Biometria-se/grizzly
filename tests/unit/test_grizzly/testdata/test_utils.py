from os import path, environ, mkdir, sep
from typing import Dict, Any, cast
from json import dumps as jsondumps, loads as jsonloads

import pytest

from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from jinja2.filters import FILTERS

from grizzly.types.behave import Scenario
from grizzly.context import GrizzlyContext
from grizzly.tasks import LogMessageTask, DateTask, TransformerTask, UntilRequestTask, ConditionalTask
from grizzly.testdata.variables.csv_writer import atomiccsvwriter_message_handler
from grizzly.testdata.utils import (
    initialize_testdata,
    create_context_variable,
    resolve_variable,
    _objectify,
    transform,
    templatingfilter,
)
from grizzly_extras.transformer import TransformerContentType

from tests.fixtures import BehaveFixture, AtomicVariableCleanupFixture, GrizzlyFixture, RequestTaskFixture, NoopZmqFixture


def test_initialize_testdata_no_tasks(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture.grizzly.scenario.tasks.clear()
    testdata, external_dependencies, message_handlers = initialize_testdata(grizzly_fixture.grizzly)
    assert testdata == {}
    assert external_dependencies == set()
    assert message_handlers == {}


def test_initialize_testdata_with_tasks(
    grizzly_fixture: GrizzlyFixture,
    request_task: RequestTaskFixture,
    cleanup: AtomicVariableCleanupFixture,
) -> None:
    try:
        grizzly = grizzly_fixture.grizzly
        grizzly.state.variables.update({
            'AtomicIntegerIncrementer.messageID': 1337,
            'AtomicDate.now': 'now',
            'transformer_task': 'none',
            'AtomicIntegerIncrementer.value': 20,
            'request_name': 'none',
            'messageID': 2022,
            'value': 'none',
            'condition': False,
            'timezone': 'GMT',
            'content': 'none',
            'days': 365,
            'date_task_date': '2022-09-13 15:08:00',
            'endpoint_part': '/api',
            'message': 'hello world!',
            'orphan': 'most likely',
        })
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 1337
        grizzly.state.variables['AtomicDate.now'] = 'now'
        request = request_task.request
        request.name = '{{ request_name }}'
        request.endpoint = '/api/{{ endpoint_part }}/test'
        request.response.content_type = TransformerContentType.JSON
        grizzly.scenario.tasks.clear()
        grizzly.scenario.tasks.add(request)
        grizzly.scenario.tasks.add(LogMessageTask(message='{{ message }}'))
        grizzly.scenario.tasks.add(DateTask(variable='date_task', value='{{ date_task_date }} | timezone="{{ timezone }}", offset="-{{ days }}D"'))
        grizzly.scenario.tasks.add(TransformerTask(
            grizzly,
            expression='$.expression',
            variable='transformer_task',
            content='hello this is the {{ content }}!',
            content_type=TransformerContentType.JSON,
        ))
        request.content_type = TransformerContentType.JSON
        grizzly.scenario.tasks.add(UntilRequestTask(grizzly, request=request, condition='{{ condition }}'))
        grizzly.scenario.tasks.add(ConditionalTask(name='conditional-1', condition='{{ value | int > 5 }}'))
        grizzly.scenario.tasks.add(ConditionalTask(name='conditional-1', condition='{{ AtomicIntegerIncrementer.value | int > 5 }}'))
        grizzly.scenario.tasks.add(LogMessageTask(message='transformer_task={{ transformer_task }}'))
        grizzly.scenario.orphan_templates.append('hello {{ orphan }} template')
        testdata, external_dependencies, message_handlers = initialize_testdata(grizzly)

        scenario_name = grizzly.scenario.class_name

        assert external_dependencies == set()
        assert message_handlers == {}
        assert scenario_name in testdata
        variables = testdata[scenario_name]
        assert len(variables) == 15
        print(sorted(variables.keys()))
        assert sorted(variables.keys()) == sorted([
            'AtomicDate.now',
            'AtomicIntegerIncrementer.messageID',
            'AtomicIntegerIncrementer.value',
            'condition',
            'content',
            'date_task_date',
            'days',
            'endpoint_part',
            'message',
            'messageID',
            'orphan',
            'request_name',
            'transformer_task',
            'timezone',
            'value',
        ])
    finally:
        cleanup()


def test_initialize_testdata_with_payload_context(grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture, noop_zmq: NoopZmqFixture) -> None:
    noop_zmq('grizzly.testdata.communication')

    try:
        grizzly = grizzly_fixture.grizzly
        parent = grizzly_fixture()
        context_root = grizzly_fixture.request_task.context_root
        request = grizzly_fixture.request_task.request
        mkdir(path.join(context_root, 'adirectory'))
        for index in range(1, 3):
            with open(path.join(context_root, 'adirectory', f'file{index}.txt'), 'w') as fd:
                fd.write(f'file{index}.txt\n')
                fd.flush()

        with open(path.join(context_root, 'test.csv'), 'w') as fd:
            fd.write('header1,header2\n')
            fd.write('value1,value2\n')
            fd.write('value3,value4\n')
            fd.flush()

        assert request.source is not None
        source = jsonloads(request.source)
        source['result']['CsvRowValue1'] = '{{ AtomicCsvReader.test.header1 }}'
        source['result']['CsvRowValue2'] = '{{ AtomicCsvReader.test.header2 }}'
        source['result']['File'] = '{{ AtomicDirectoryContents.test }}'

        behave_scenario = Scenario(filename=None, line=None, keyword='', name=parent.__class__.__name__)
        grizzly.scenarios.create(behave_scenario)
        grizzly.state.variables['messageID'] = 123
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
        grizzly.state.variables['AtomicCsvReader.test'] = 'test.csv'
        grizzly.state.variables['AtomicCsvWriter.output'] = 'output.csv | headers="foo,bar"'
        grizzly.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
        grizzly.state.variables['AtomicDate.now'] = 'now'
        grizzly.scenario.user.class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test.nu'
        grizzly.scenario.iterations = 2

        # hackish, to get around them not being used in any template when doing it barebone.
        grizzly.scenario.orphan_templates.append('{{ AtomicCsvWriter.output.foo }}')

        request.source = jsondumps(source)

        grizzly.scenario.tasks.add(request)

        testdata, external_dependencies, message_handlers = initialize_testdata(grizzly)

        scenario_name = grizzly.scenario.class_name

        assert scenario_name in testdata
        assert external_dependencies == set()
        assert message_handlers == {'atomiccsvwriter': atomiccsvwriter_message_handler}

        data = testdata[scenario_name]

        assert data['messageID'] == 123

        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 456
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 457
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 458
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 459

        assert data['AtomicCsvReader.test.header1']['test'] == {'header1': 'value1', 'header2': 'value2'}
        assert data['AtomicCsvReader.test.header2']['test']['header2'] == 'value4'
        with pytest.raises(TypeError):
            assert data['AtomicCsvReader.test.header1']['test']['header1'] is None
        assert data['AtomicCsvReader.test.header2']['test'] is None
        assert data['AtomicCsvReader.test.header1']['test'] is None

        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory{sep}file1.txt'
        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory{sep}file2.txt'
        assert data['AtomicDirectoryContents.test']['test'] is None
    finally:
        cleanup()


def test_create_context_variable() -> None:
    grizzly = GrizzlyContext()

    try:
        assert create_context_variable(grizzly, 'test.value', '1') == {
            'test': {
                'value': 1,
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'trUe') == {
            'test': {
                'value': True,
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'AZURE') == {
            'test': {
                'value': 'AZURE',
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'HOST') == {
            'test': {
                'value': 'HOST'
            }
        }

        with pytest.raises(AssertionError) as ae:
            create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD$')
        assert str(ae.value) == 'environment variable "HELLO_WORLD" is not set'

        with pytest.raises(AssertionError):
            create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD$')

        environ['HELLO_WORLD'] = 'environment variable value'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD$') == {
            'test': {
                'value': 'environment variable value',
            }
        }

        environ['HELLO_WORLD'] = 'true'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD$') == {
            'test': {
                'value': True,
            }
        }

        environ['HELLO_WORLD'] = '1337'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD$') == {
            'test': {
                'value': 1337,
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(grizzly, 'test.value', '$conf::test.auth.user.username$')

        grizzly.state.configuration['test.auth.user.username'] = 'username'
        assert create_context_variable(grizzly, 'test.value', '$conf::test.auth.user.username$') == {
            'test': {
                'value': 'username',
            }
        }

        grizzly.state.configuration['test.auth.refresh_time'] = 3000
        assert create_context_variable(grizzly, 'test.value', '$conf::test.auth.refresh_time$') == {
            'test': {
                'value': 3000,
            }
        }

        assert create_context_variable(grizzly, 'www.example.com/auth.user.username', 'bob') == {
            'www.example.com': {
                'auth': {
                    'user': {
                        'username': 'bob',
                    },
                },
            },
        }

        grizzly.state.configuration.update({'test.host': 'www.example.net'})

        assert create_context_variable(grizzly, '$conf::test.host$/auth.user.username', 'bob') == {
            'www.example.net': {
                'auth': {
                    'user': {
                        'username': 'bob',
                    },
                },
            },
        }
    finally:
        GrizzlyContext.destroy()
        try:
            del environ['HELLO_WORLD']
        except KeyError:
            pass


def test_resolve_variable(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly = grizzly_fixture.grizzly

    try:
        assert 'test' not in grizzly.state.variables
        with pytest.raises(AssertionError):
            resolve_variable(grizzly, '{{ test }}')

        grizzly.state.variables['test'] = 'some value'
        assert resolve_variable(grizzly, '{{ test }}') == 'some value'

        assert resolve_variable(grizzly, "now | format='%Y-%m-%d %H'") == "now | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly, "{{ test }} | format='%Y-%m-%d %H'") == "some value | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly, 'static value') == 'static value'
        assert resolve_variable(grizzly, '"static value"') == 'static value'
        assert resolve_variable(grizzly, "'static value'") == 'static value'
        assert resolve_variable(grizzly, "'static' value") == "'static' value"
        assert resolve_variable(grizzly, "static 'value'") == "static 'value'"

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "'static value\"")

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "static 'value\"")

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "'static\" value")

        grizzly.state.variables['number'] = 100
        assert resolve_variable(grizzly, '{{ (number * 0.25) | int }}') == 25

        assert resolve_variable(grizzly, '{{ (number * 0.25 * 0.2) | int }}') == 5

        with pytest.raises(ValueError) as ve:
            resolve_variable(grizzly, '$env::HELLO_WORLD')
        assert str(ve.value) == '"$env::HELLO_WORLD" is not a correctly specified templating variable, variables must match "$(conf|env)::<variable name>$"'

        with pytest.raises(AssertionError) as ae:
            resolve_variable(grizzly, '$env::HELLO_WORLD$')
        assert str(ae.value) == 'environment variable "HELLO_WORLD" is not set'

        environ['HELLO_WORLD'] = 'first environment variable!'

        assert resolve_variable(grizzly, '$env::HELLO_WORLD$') == 'first environment variable!'

        environ['HELLO_WORLD'] = 'first "environment" variable!'
        assert resolve_variable(grizzly, '$env::HELLO_WORLD$') == 'first "environment" variable!'

        with pytest.raises(ValueError) as ve:
            resolve_variable(grizzly, '$conf::sut.host')
        assert str(ve.value) == '"$conf::sut.host" is not a correctly specified templating variable, variables must match "$(conf|env)::<variable name>$"'

        with pytest.raises(AssertionError) as ae:
            resolve_variable(grizzly, '$conf::sut.host$')
        assert str(ae.value) == 'configuration variable "sut.host" is not set'

        grizzly.state.configuration['sut.host'] = 'http://host.docker.internal:8003'
        grizzly.state.configuration['sut.path'] = '/hello/world'

        assert resolve_variable(grizzly, '$conf::sut.host$') == 'http://host.docker.internal:8003'
        assert resolve_variable(grizzly, '$conf::sut.host$$conf::sut.path$') == 'http://host.docker.internal:8003/hello/world'

        grizzly.state.configuration['sut.greeting'] = 'hello "{{ test }}"!'
        assert resolve_variable(grizzly, '$conf::sut.greeting$') == 'hello "{{ test }}"!'

        assert resolve_variable(grizzly, '$test::hello$') == '$test::hello$'

        assert resolve_variable(grizzly, '') == ''

        assert resolve_variable(grizzly, '$conf::sut.host$ blah $env::HELLO_WORLD$ blah') == 'http://host.docker.internal:8003 blah first "environment" variable! blah'

        @templatingfilter
        def testuppercase(value: str) -> str:
            return value.upper()

        grizzly.state.variables['lowercase_value'] = 'foobar'

        assert resolve_variable(grizzly, 'hello {{ lowercase_value | testuppercase }}!') == 'hello FOOBAR!'

    finally:
        try:
            del environ['HELLO_WORLD']
        except KeyError:
            pass


def test__objectify() -> None:
    testdata: Dict[str, Any] = {
        'AtomicIntegerIncrementer': {
            'test': 1337,
        },
        'test': 1338,
        'AtomicCsvReader': {
            'input': {
                'test1': 'hello',
                'test2': 'world!',
            }
        },
        'Test': {
            'test1': {
                'test2': {
                    'test3': 'value',
                }
            }
        },
        'tests': {
            'helpers': {
                'AtomicCustomVariable': {
                    'hello': 'world',
                    'foo': 'bar',
                }
            }
        }
    }

    obj = _objectify(testdata)

    assert (
        obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils'
        and obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
    )
    assert getattr(obj['AtomicIntegerIncrementer'], 'test') == 1337
    assert isinstance(obj['test'], int)
    assert obj['test'] == 1338
    assert (
        obj['AtomicCsvReader'].__module__ == 'grizzly.testdata.utils'
        and obj['AtomicCsvReader'].__class__.__name__ == 'Testdata'
    )
    atomiccsvrow_input = getattr(obj['AtomicCsvReader'], 'input', None)
    assert atomiccsvrow_input is not None
    assert (
        atomiccsvrow_input.__module__ == 'grizzly.testdata.utils'
        and atomiccsvrow_input.__class__.__name__ == 'Testdata'
    )
    assert getattr(atomiccsvrow_input, 'test1', None) == 'hello'
    assert getattr(atomiccsvrow_input, 'test2', None) == 'world!'

    assert (
        obj['Test'].__module__ == 'grizzly.testdata.utils'
        and obj['Test'].__class__.__name__ == 'Testdata'
    )
    test = getattr(obj['Test'], 'test1', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils'
        and test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test2', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils'
        and test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test3', None)
    assert test is not None
    assert isinstance(test, str)
    assert test == 'value'


def test_transform_no_objectify(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly = grizzly_fixture.grizzly

    data = {
        'test.number.value': 1337,
        'test.number.description': 'simple description',
        'test.string.value': 'hello world!',
        'test.bool.value': True,
        'tests.helpers.AtomicCustomVariable.hello': 'world',
    }

    actual = transform(grizzly, data, objectify=False)

    assert actual == {
        'test': {
            'number': {
                'value': 1337,
                'description': 'simple description',
            },
            'string': {
                'value': 'hello world!',
            },
            'bool': {
                'value': True,
            },
        },
        'tests': {
            'helpers': {
                'AtomicCustomVariable': {
                    'hello': 'world',
                },
            },
        },
    }


def test_transform(behave_fixture: BehaveFixture, noop_zmq: NoopZmqFixture, cleanup: AtomicVariableCleanupFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    behave = behave_fixture.context

    try:
        grizzly = cast(GrizzlyContext, behave.grizzly)
        data: Dict[str, Any] = {
            'AtomicIntegerIncrementer.test': 1337,
            'test': 1338,
            'AtomicCsvReader.input.test1': 'hello',
            'AtomicCsvReader.input.test2': 'world!',
            'Test.test1.test2.test3': 'value',
            'Test.test1.test2.test4': 'value',
            'Test.test2.test3': 'value',
            'tests.helpers.AtomicCustomVariable.hello': 'world',
            'tests.helpers.AtomicCustomVariable.foo': 'bar',
        }

        obj = transform(grizzly, data)

        assert (
            obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils'
            and obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicIntegerIncrementer'], 'test', None) == 1337
        assert isinstance(obj['test'], int)
        assert obj['test'] == 1338
        assert (
            obj['AtomicCsvReader'].__module__ == 'grizzly.testdata.utils'
            and obj['AtomicCsvReader'].__class__.__name__ == 'Testdata'
        )
        assert (
            obj['AtomicCsvReader'].input.__module__ == 'grizzly.testdata.utils'
            and obj['AtomicCsvReader'].input.__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicCsvReader'].input, 'test1', None) == 'hello'
        assert getattr(obj['AtomicCsvReader'].input, 'test2', None) == 'world!'
        assert (
            obj['Test'].__module__ == 'grizzly.testdata.utils'
            and obj['Test'].__class__.__name__ == 'Testdata'
        )
        test = getattr(obj['Test'], 'test1', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils'
            and test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test2', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils'
            and test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test3', None)
        assert test is not None
        assert isinstance(test, str)
        assert test == 'value'

        custom_variable = getattr(getattr(obj['tests'], 'helpers', None), 'AtomicCustomVariable', None)
        assert custom_variable is not None
        assert getattr(custom_variable, 'hello', None) == 'world'
        assert getattr(custom_variable, 'foo', None) == 'bar'

        caplog.clear()
    finally:
        cleanup()


def test_templatingfilter(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly = grizzly_fixture.grizzly

    def testuppercase(value: str) -> str:
        return value.upper()

    assert FILTERS.get('testuppercase', None) is None

    templatingfilter(testuppercase)

    assert FILTERS.get('testuppercase', None) is testuppercase

    actual = grizzly.state.jinja2.from_string('{{ variable | testuppercase }}').render(variable='foobar')

    assert actual == 'FOOBAR'

    def _testuppercase(value: str) -> str:
        return value.upper()

    uc = _testuppercase
    setattr(uc, '__name__', 'testuppercase')

    with pytest.raises(AssertionError) as ae:
        templatingfilter(uc)
    assert str(ae.value) == 'testuppercase is already registered as a filter'

    assert FILTERS.get('testuppercase', None) is testuppercase
