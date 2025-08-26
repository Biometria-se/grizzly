"""Unit tests for grizzly.testdata.utils."""

from __future__ import annotations

import json
from contextlib import suppress
from json import dumps as jsondumps
from json import loads as jsonloads
from os import environ, sep
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.tasks import ConditionalTask, DateTask, LogMessageTask, TransformerTask, UntilRequestTask
from grizzly.testdata.filters import templatingfilter
from grizzly.testdata.utils import (
    _objectify,
    create_context_variable,
    initialize_testdata,
    resolve_template,
    resolve_variable,
    transform,
)
from grizzly.testdata.variables import AtomicDate, AtomicIntegerIncrementer
from grizzly.testdata.variables.csv_writer import atomiccsvwriter_message_handler
from grizzly_common.transformer import TransformerContentType

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario
    from grizzly.types import StrDict

    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture, RequestTaskFixture


def test_initialize_testdata_no_tasks(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly_fixture.grizzly.scenario.tasks.clear()
    testdata, dependencies = initialize_testdata(grizzly_fixture.grizzly)
    assert testdata == {}
    assert dependencies == set()


def test_initialize_testdata_with_tasks(
    grizzly_fixture: GrizzlyFixture,
    request_task: RequestTaskFixture,
    cleanup: AtomicVariableCleanupFixture,
) -> None:
    try:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.clear()

        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('scenario1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('scenario2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('scenario3'))

        scenario_map: dict[str, GrizzlyContextScenario] = {}

        for scenario in grizzly.scenarios:
            scenario_map.update({scenario.class_name: scenario})
            grizzly.scenarios.select(scenario.behave)
            grizzly.scenario.variables.update(
                {
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
                    'unused_variable': 'some value',
                },
            )
            request = request_task.request
            request.name = '{{ request_name }}'
            request.endpoint = '/api/{{ endpoint_part }}/test'
            request.response.content_type = TransformerContentType.JSON
            grizzly.scenario.tasks.clear()
            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='{{ message }}'))
            grizzly.scenario.tasks.add(DateTask(variable='date_task', value='{{ date_task_date }} | timezone="{{ timezone }}", offset="-{{ days }}D"'))
            grizzly.scenario.tasks.add(
                TransformerTask(
                    expression='$.expression',
                    variable='transformer_task',
                    content='hello this is the {{ undeclared_variable if undeclared_variable is defined else content }}!',
                    content_type=TransformerContentType.JSON,
                ),
            )
            request.content_type = TransformerContentType.JSON
            grizzly.scenario.tasks.add(LogMessageTask('{{ unused_variable if unused_variable is defined else "hello" }}'))
            grizzly.scenario.tasks.add(UntilRequestTask(request=request, condition='{{ condition }}'))
            grizzly.scenario.tasks.add(ConditionalTask(name='conditional-1', condition='{{ value | int > 5 }}'))
            grizzly.scenario.tasks.add(ConditionalTask(name='conditional-1', condition='{{ AtomicIntegerIncrementer.value | int > 5 }}'))
            grizzly.scenario.tasks.add(LogMessageTask(message='transformer_task={{ transformer_task }}'))
            grizzly.scenario.orphan_templates.append('hello {{ orphan }} template')
            grizzly.scenario.orphan_templates.append('{{ (((max_days * 0.33) + 0.5) | int) if max_days is defined else days }}')

        grizzly.scenarios.deselect()

        testdata, dependencies = initialize_testdata(grizzly)

        assert dependencies == set()

        for index, (scenario_name, variables) in enumerate(testdata.items(), start=1):
            assert scenario_name == f'IteratorScenario_00{index}'
            assert sorted(variables.keys()) == sorted(
                [
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
                    'unused_variable',
                ],
            )

            assert isinstance(variables['AtomicDate.now'], AtomicDate)
            assert isinstance(variables['AtomicIntegerIncrementer.messageID'], AtomicIntegerIncrementer)
            assert isinstance(variables['AtomicIntegerIncrementer.value'], AtomicIntegerIncrementer)

            assert variables['AtomicDate.now']._scenario is scenario_map[scenario_name]
            assert variables['AtomicIntegerIncrementer.messageID']._scenario is scenario_map[scenario_name]
            assert variables['AtomicIntegerIncrementer.value']._scenario is scenario_map[scenario_name]
    finally:
        cleanup()


def test_initialize_testdata_with_payload_context(grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:  # noqa: PLR0915
    try:
        grizzly = grizzly_fixture.grizzly
        parent = grizzly_fixture()
        context_root = Path(grizzly_fixture.request_task.context_root)
        request = grizzly_fixture.request_task.request
        (context_root / 'adirectory').mkdir()
        for index in range(1, 3):
            (context_root / 'adirectory' / f'file{index}.txt').write_text(f'file{index}.txt\n')

        (context_root / 'test.csv').write_text("""header1,header2
value1,value2
value3,value4
""")

        with (context_root / 'test.json').open('w') as fd:
            json.dump([{'header1': 'value1', 'header2': 'value2'}, {'header1': 'value3', 'header2': 'value4'}], fd)

        assert request.source is not None
        source = jsonloads(request.source)
        source['result']['CsvRowValue1'] = '{{ AtomicCsvReader.test.header1 }}'
        source['result']['CsvRowValue2'] = '{{ AtomicCsvReader.test.header2 }}'
        source['result']['JsonRowValue1'] = '{{ AtomicJsonReader.test.header1 }}'
        source['result']['JsonRowValue2'] = '{{ AtomicJsonReader.test.header2 }}'
        source['result']['JsonRowValue'] = '{{ AtomicJsonReader.test2 }}'
        source['result']['File'] = '{{ AtomicDirectoryContents.test }}'
        source['result']['Mod'] = "{{ '%08d' % (messageID | string)[:2] | int }}"

        grizzly.scenarios.clear()
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario(parent.__class__.__name__))
        grizzly.scenario.variables.update(
            {
                'messageID': 123,
                'AtomicIntegerIncrementer.messageID': 456,
                'AtomicCsvReader.test': 'test.csv',
                'AtomicJsonReader.test': 'test.json',
                'AtomicJsonReader.test2': 'test.json',
                'AtomicCsvWriter.output': 'output.csv | headers="foo,bar"',
                'AtomicDirectoryContents.test': 'adirectory',
                'AtomicDate.now': 'now',
            },
        )
        grizzly.scenario.user.class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test.example.com'
        grizzly.scenario.iterations = 2

        # get around them not being used in any template when doing it barebone.
        grizzly.scenario.orphan_templates.append('{{ AtomicCsvWriter.output.foo }}')

        request.source = jsondumps(source)

        grizzly.scenario.tasks.add(request)

        testdata, dependencies = initialize_testdata(grizzly)

        scenario_name = grizzly.scenario.class_name

        assert scenario_name in testdata
        assert dependencies == {('atomiccsvwriter', atomiccsvwriter_message_handler)}

        data = testdata[scenario_name]

        assert data['messageID'] == 123

        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 456
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 457
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 458
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 459

        assert data['AtomicCsvReader.test.header1']['test'] == {'header1': 'value1', 'header2': 'value2'}
        assert data['AtomicCsvReader.test.header2']['test']['header2'] == 'value4'
        with pytest.raises(TypeError, match="'NoneType' object is not subscriptable"):
            assert data['AtomicCsvReader.test.header1']['test']['header1'] is None
        assert data['AtomicCsvReader.test.header2']['test'] is None
        assert data['AtomicCsvReader.test.header1']['test'] is None

        assert data['AtomicJsonReader.test2']['test2'] == {'header1': 'value1', 'header2': 'value2'}
        assert data['AtomicJsonReader.test.header2']['test.header2']['header2'] == 'value2'
        assert data['AtomicJsonReader.test.header2']['test'] is not None
        assert data['AtomicJsonReader.test.header2']['test'] is None
        assert data['AtomicJsonReader.test.header1']['test'] is None

        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory{sep}file1.txt'
        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory{sep}file2.txt'
        assert data['AtomicDirectoryContents.test']['test'] is None
    finally:
        cleanup()


def test_create_context_variable(grizzly_fixture: GrizzlyFixture) -> None:
    grizzly = grizzly_fixture.grizzly

    try:
        grizzly.scenario.variables.update({'foo': 'baz'})
        assert create_context_variable(grizzly.scenario, 'foo.bar', '{{ foo }}') == {
            'foo': {
                'bar': 'baz',
            },
        }

        assert create_context_variable(grizzly.scenario, 'test.value', '1') == {
            'test': {
                'value': 1,
            },
        }

        assert create_context_variable(grizzly.scenario, 'test.value', 'trUe') == {
            'test': {
                'value': True,
            },
        }

        assert create_context_variable(grizzly.scenario, 'test.value', 'AZURE') == {
            'test': {
                'value': 'AZURE',
            },
        }

        assert create_context_variable(grizzly.scenario, 'test.value', 'HOST') == {
            'test': {
                'value': 'HOST',
            },
        }

        with pytest.raises(AssertionError, match='environment variable "HELLO_WORLD" is not set'):
            create_context_variable(grizzly.scenario, 'test.value', '$env::HELLO_WORLD$')

        environ['HELLO_WORLD'] = 'environment variable value'
        assert create_context_variable(grizzly.scenario, 'test.value', '$env::HELLO_WORLD$') == {
            'test': {
                'value': 'environment variable value',
            },
        }

        environ['HELLO_WORLD'] = 'true'
        assert create_context_variable(grizzly.scenario, 'test.value', '$env::HELLO_WORLD$') == {
            'test': {
                'value': True,
            },
        }

        environ['HELLO_WORLD'] = '1337'
        assert create_context_variable(grizzly.scenario, 'test.value', '$env::HELLO_WORLD$') == {
            'test': {
                'value': 1337,
            },
        }

        with pytest.raises(AssertionError, match='configuration variable "test.auth.user.username" is not set'):
            create_context_variable(grizzly.scenario, 'test.value', '$conf::test.auth.user.username$')

        grizzly.state.configuration['test.auth.user.username'] = 'username'
        assert create_context_variable(grizzly.scenario, 'test.value', '$conf::test.auth.user.username$') == {
            'test': {
                'value': 'username',
            },
        }

        grizzly.state.configuration['test.auth.refresh_time'] = 3000
        assert create_context_variable(grizzly.scenario, 'test.value', '$conf::test.auth.refresh_time$') == {
            'test': {
                'value': 3000,
            },
        }

        assert create_context_variable(grizzly.scenario, 'www.example.com/auth.user.username', 'bob') == {
            'www.example.com': {
                'auth': {
                    'user': {
                        'username': 'bob',
                    },
                },
            },
        }

        grizzly.state.configuration.update({'test.host': 'www.example.net'})

        assert create_context_variable(grizzly.scenario, '$conf::test.host$/auth.user.username', 'bob') == {
            'www.example.net': {
                'auth': {
                    'user': {
                        'username': 'bob',
                    },
                },
            },
        }
    finally:
        with suppress(KeyError):
            del environ['HELLO_WORLD']


def test_resolve_variable(grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
    grizzly = grizzly_fixture.grizzly

    try:
        assert 'test' not in grizzly.scenario.variables
        with pytest.raises(AssertionError, match='variables have been found in templates, but have not been declared:\ntest'):
            resolve_variable(grizzly.scenario, '{{ test }}')

        grizzly.scenario.variables['test'] = 'some value'
        assert resolve_variable(grizzly.scenario, '{{ test }}') == 'some value'

        assert resolve_variable(grizzly.scenario, "now | format='%Y-%m-%d %H'") == "now | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly.scenario, "{{ test }} | format='%Y-%m-%d %H'") == "some value | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly.scenario, 'static value') == 'static value'
        assert resolve_variable(grizzly.scenario, '"static value"') == 'static value'
        assert resolve_variable(grizzly.scenario, "'static value'") == 'static value'
        assert resolve_variable(grizzly.scenario, "'static' value") == "'static' value"
        assert resolve_variable(grizzly.scenario, "static 'value'") == "static 'value'"

        with pytest.raises(ValueError, match='is incorrectly quoted'):
            resolve_variable(grizzly.scenario, '\'static value"')

        with pytest.raises(ValueError, match='is incorrectly quoted'):
            resolve_variable(grizzly.scenario, 'static \'value"')

        with pytest.raises(ValueError, match='is incorrectly quoted'):
            resolve_variable(grizzly.scenario, '\'static" value')

        grizzly.scenario.variables['number'] = 100
        assert resolve_variable(grizzly.scenario, '{{ (number * 0.25) | int }}') == 25

        assert resolve_variable(grizzly.scenario, '{{ (number * 0.25 * 0.2) | int }}') == 5

        with pytest.raises(ValueError, match='is not a correctly specified templating variable, variables must match'):
            resolve_variable(grizzly.scenario, '$env::HELLO_WORLD')

        with pytest.raises(AssertionError, match='environment variable "HELLO_WORLD" is not set'):
            resolve_variable(grizzly.scenario, '$env::HELLO_WORLD$')

        environ['HELLO_WORLD'] = 'first environment variable!'

        assert resolve_variable(grizzly.scenario, '$env::HELLO_WORLD$') == 'first environment variable!'

        environ['HELLO_WORLD'] = 'first "environment" variable!'
        assert resolve_variable(grizzly.scenario, '$env::HELLO_WORLD$') == 'first "environment" variable!'

        with pytest.raises(ValueError, match='is not a correctly specified templating variable, variables must match'):
            resolve_variable(grizzly.scenario, '$conf::sut.host')

        with pytest.raises(AssertionError, match='configuration variable "sut.host" is not set'):
            resolve_variable(grizzly.scenario, '$conf::sut.host$')

        grizzly.state.configuration['sut.host'] = 'http://host.docker.internal:8003'
        grizzly.state.configuration['sut.path'] = '/hello/world'

        assert resolve_variable(grizzly.scenario, '$conf::sut.host$') == 'http://host.docker.internal:8003'
        assert resolve_variable(grizzly.scenario, '$conf::sut.host$$conf::sut.path$') == 'http://host.docker.internal:8003/hello/world'

        grizzly.state.configuration['sut.greeting'] = 'hello "{{ test }}"!'
        assert resolve_variable(grizzly.scenario, '$conf::sut.greeting$') == 'hello "{{ test }}"!'

        assert resolve_variable(grizzly.scenario, '$test::hello$') == '$test::hello$'

        assert resolve_variable(grizzly.scenario, '') == ''

        assert resolve_variable(grizzly.scenario, '$conf::sut.host$ blah $env::HELLO_WORLD$ blah') == 'http://host.docker.internal:8003 blah first "environment" variable! blah'

        grizzly.scenario.variables['hello'] = 'world'
        assert resolve_variable(grizzly.scenario, '{{ hello }} $conf::sut.host$ right?') == 'world http://host.docker.internal:8003 right?'

        @templatingfilter
        def testuppercase(value: str) -> str:
            return value.upper()

        grizzly.scenario.variables['lowercase_value'] = 'foobar'

        assert resolve_variable(grizzly.scenario, 'hello {{ lowercase_value | testuppercase }}!') == 'hello FOOBAR!'

        # do not fail on undeclared variable if there is a check that the variable is defined or not in the template
        assert resolve_variable(grizzly.scenario, 'hello {{ world if world is defined else "world" }}') == 'hello world'
        assert resolve_variable(grizzly.scenario, 'hello {{ "world" if world is not defined else world }}') == 'hello world'
        grizzly.scenario.variables['world'] = 'foobar'
        assert resolve_variable(grizzly.scenario, 'hello {{ world if world is defined else "world" }}') == 'hello foobar'
        assert resolve_variable(grizzly.scenario, 'hello {{ "world" if world is not defined else world }}') == 'hello foobar'

        base_dir = environ.get('GRIZZLY_CONTEXT_ROOT', None)

        assert base_dir is not None

        test_file = Path(base_dir) / 'requests' / 'test' / 'foobar.txt'
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("""
hello {{ hello }}
write this "$conf::sut.greeting$"
""")

        assert (
            resolve_variable(grizzly.scenario, 'test/foobar.txt')
            == """
hello world
write this "hello "{{ test }}"!"
""".rstrip()
        )

        assert resolve_variable(grizzly.scenario, 'test/foobar.txt', try_file=False) == 'test/foobar.txt'

        assert (
            resolve_variable(grizzly.scenario, 'file://./test/foobar.txt', try_file=False)
            == """
hello {{ hello }}
write this "hello "{{ test }}"!"
"""
        )

        assert (
            resolve_variable(grizzly.scenario, 'file://test/foobar.txt', try_file=False)
            == """
hello {{ hello }}
write this "hello "{{ test }}"!"
"""
        )

        assert (
            resolve_variable(grizzly.scenario, 'file:///test/foobar.txt', try_file=False)
            == """
hello {{ hello }}
write this "hello "{{ test }}"!"
"""
        )

    finally:
        with suppress(KeyError):
            del environ['HELLO_WORLD']


def test__objectify() -> None:
    testdata: StrDict = {
        'AtomicIntegerIncrementer': {
            'test': 1337,
        },
        'test': 1338,
        'AtomicCsvReader': {
            'input': {
                'test1': 'hello',
                'test2': 'world!',
            },
        },
        'Test': {
            'test1': {
                'test2': {
                    'test3': 'value',
                },
            },
        },
        'tests': {
            'helpers': {
                'AtomicCustomVariable': {
                    'hello': 'world',
                    'foo': 'bar',
                },
            },
        },
    }

    obj = _objectify(testdata)

    assert obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils'
    assert obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
    assert obj['AtomicIntegerIncrementer'].test == 1337
    assert isinstance(obj['test'], int)
    assert obj['test'] == 1338
    assert obj['AtomicCsvReader'].__module__ == 'grizzly.testdata.utils'
    assert obj['AtomicCsvReader'].__class__.__name__ == 'Testdata'

    atomiccsvrow_input = getattr(obj['AtomicCsvReader'], 'input', None)
    assert atomiccsvrow_input is not None
    assert atomiccsvrow_input.__module__ == 'grizzly.testdata.utils'
    assert atomiccsvrow_input.__class__.__name__ == 'Testdata'
    assert getattr(atomiccsvrow_input, 'test1', None) == 'hello'
    assert getattr(atomiccsvrow_input, 'test2', None) == 'world!'

    assert obj['Test'].__module__ == 'grizzly.testdata.utils'
    assert obj['Test'].__class__.__name__ == 'Testdata'

    test = getattr(obj['Test'], 'test1', None)
    assert test is not None
    assert test.__module__ == 'grizzly.testdata.utils'
    assert test.__class__.__name__ == 'Testdata'
    test = getattr(test, 'test2', None)
    assert test is not None
    assert test.__module__ == 'grizzly.testdata.utils'
    assert test.__class__.__name__ == 'Testdata'
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

    actual = transform(grizzly.scenario, data, objectify=False)

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


def test_transform(grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture, caplog: LogCaptureFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    grizzly.scenarios.clear()
    grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test transform'))

    try:
        grizzly = cast('GrizzlyContext', behave.grizzly)
        data: StrDict = {
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

        obj = transform(grizzly.scenario, data)

        assert obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils'
        assert obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
        assert getattr(obj['AtomicIntegerIncrementer'], 'test', None) == 1337
        assert isinstance(obj['test'], int)
        assert obj['test'] == 1338
        assert obj['AtomicCsvReader'].__module__ == 'grizzly.testdata.utils'
        assert obj['AtomicCsvReader'].__class__.__name__ == 'Testdata'
        assert obj['AtomicCsvReader'].input.__module__ == 'grizzly.testdata.utils'
        assert obj['AtomicCsvReader'].input.__class__.__name__ == 'Testdata'
        assert getattr(obj['AtomicCsvReader'].input, 'test1', None) == 'hello'
        assert getattr(obj['AtomicCsvReader'].input, 'test2', None) == 'world!'
        assert obj['Test'].__module__ == 'grizzly.testdata.utils'
        assert obj['Test'].__class__.__name__ == 'Testdata'
        test = getattr(obj['Test'], 'test1', None)
        assert test is not None
        assert test.__module__ == 'grizzly.testdata.utils'
        assert test.__class__.__name__ == 'Testdata'
        test = getattr(test, 'test2', None)
        assert test is not None
        assert test.__module__ == 'grizzly.testdata.utils'
        assert test.__class__.__name__ == 'Testdata'
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


def test_resolve_template(grizzly_fixture: GrizzlyFixture) -> None:
    parent = grizzly_fixture()

    parent.user._scenario.variables.update({'hello': 'foo', 'foo': 100, 'baz': 0.25})

    assert 'world' not in parent.user._scenario.variables
    assert resolve_template(parent.user._scenario, '{{ hello if hello is defined else world }}') == 'foo'

    assert 'bar' not in parent.user._scenario.variables
    assert resolve_template(parent.user._scenario, '{{ (((foo | int) * (baz | float)) + 0.5) | int if foo is defined else bar }}') == '25'

    parent.user._scenario.variables.clear()
    parent.user._scenario.variables.update({'world': 'bar', 'bar': 100})
    assert 'hello' not in parent.user._scenario.variables
    assert resolve_template(parent.user._scenario, '{{ hello if hello is defined else world }}') == 'bar'

    assert 'foo' not in parent.user._scenario.variables
    assert resolve_template(parent.user._scenario, '{{ (((foo | int) * (baz | float)) + 0.5) | int if foo is defined else bar }}') == '100'

    with pytest.raises(AssertionError, match='variables have been found in templates, but have not been declared:\nfoobaz'):
        resolve_template(parent.user._scenario, '{{ foobaz }} yeah')
