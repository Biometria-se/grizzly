from typing import cast
from json import loads as jsonloads, dumps as jsondumps

from grizzly.testdata.ast import _parse_templates, get_template_variables
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario, GrizzlyContext
from grizzly.tasks import RequestTask, LogMessageTask

from tests.fixtures import RequestTaskFixture, BehaveFixture


def test__parse_template(request_task: RequestTaskFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result']['CsvRowValue1'] = '{{ AtomicCsvReader.test.header1 }}'
    source['result']['CsvRowValue2'] = '{{ AtomicCsvReader.test.header2 }}'
    source['result']['File'] = '{{ AtomicDirectoryContents.test }}'
    source['result']['TestSubString'] = '{{ a_sub_string[:3] }}'
    source['result']['TestString'] = '{{ a_string }}'
    source['result']['FooBar'] = '{{ (AtomicIntegerIncrementer.file_number | int) }}'
    source['result']['Expression'] = '{{ expression == "True" }}'

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1, behave=request_task.behave_fixture.create_scenario('TestScenario'))
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}
    variables = _parse_templates(templates)

    assert variables == {
        'TestScenario_001': {
            'messageID',
            'AtomicIntegerIncrementer.messageID',
            'AtomicDate.now',
            'AtomicCsvReader.test.header1',
            'AtomicCsvReader.test.header2',
            'AtomicDirectoryContents.test',
            'a_sub_string',
            'a_string',
            'AtomicIntegerIncrementer.file_number',
            'expression',
        },
    }


def test__parse_template_nested_pipe(request_task: RequestTaskFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result'] = {'FooBar': '{{ AtomicIntegerIncrementer.file_number | int }}'}

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1, behave=request_task.behave_fixture.create_scenario('TestScenario'))
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert variables == {
        'TestScenario_001': {
            'AtomicIntegerIncrementer.file_number',
        },
    }

    source['result'] = {'FooBar': '{{ (AtomicIntegerIncrementer.file_number | int) }}'}

    request.source = jsondumps(source)
    scenario.tasks.clear()
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert variables == {
        'TestScenario_001': {
            'AtomicIntegerIncrementer.file_number',
        },
    }

    request.source = "{{ '%08d' % key[:6] | int }}_{{ guid }}_{{ AtomicDate.date }}_{{ '%012d' % AtomicIntegerIncrementer.file_number }}"

    scenario.tasks.clear()
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert variables == {
        'TestScenario_001': {
            'key',
            'guid',
            'AtomicDate.date',
            'AtomicIntegerIncrementer.file_number',
        },
    }

    source = {
        'result': {
            'FooBar_1': '{{ (value1 * 0.25) | round | int }}',
            'FooBar_2': '{{ (((value2 * 0.25) | round) + 1) | int }}',
            'FooBar_3': '{{ ((value3 - 2) | int) + 1 }}',
            'FooBar_4': '{{ (4 * value40) + (5 - value41) }}',
            'FooBar_5': '{{ Atomic.test1 + Atomic.test2 + Atomic.test3 }}',
            'FooBar_6': '{{ value6 / 10.0 }}',
            'FooBar_7': '{{ value71 if value70 else value72 }}',
            'FooBar_8': "{{ value80 in [value81, value82, 'foobar'] }}",
            'FooBar_9': "{{ 'Hello ' ~ value9 ~ '!' }}",
            'FooBar_10': '{{ datetime.now() }}',
            'FooBar_11': '{{ value110 if value111 is divisibleby value112 }}',
            'FooBar_12': '{{ value120 if not value121 or (value122 and value123) else value124 }}',
        }
    }

    request.source = jsondumps(source)
    scenario.tasks.clear()
    scenario.tasks.add(request)
    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert variables == {
        'TestScenario_001': {
            'value1',
            'value2',
            'value3',
            'value40', 'value41',
            'Atomic.test1', 'Atomic.test2', 'Atomic.test3',
            'value6',
            'value70', 'value71', 'value72',
            'value80', 'value81', 'value82',
            'value9',
            'value110', 'value111', 'value112',
            'value120', 'value121', 'value122', 'value123', 'value124',
        },
    }


def test_get_template_variables(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    grizzly.scenario.tasks.clear()
    variables = get_template_variables(grizzly)
    assert variables == {}

    grizzly.scenario.context['host'] = 'http://test.nu'
    grizzly.scenario.user.class_name = 'TestUser'
    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.POST, name='Test POST request', endpoint='/api/test/post')
    )
    task = cast(RequestTask, grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicRandomString.test }}'

    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.GET, name='{{ env }} GET request', endpoint='/api/{{ env }}/get')
    )
    task = cast(RequestTask, grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicIntegerIncrementer.test }}'

    grizzly.scenario.tasks.add(
        LogMessageTask(message='{{ foo }}')
    )

    grizzly.scenario.orphan_templates.append('{{ foobar }}')

    variables = get_template_variables(grizzly)

    expected_scenario_name = '_'.join([grizzly.scenario.name, grizzly.scenario.identifier])

    assert grizzly.scenario.class_name == expected_scenario_name
    assert variables == {
        expected_scenario_name: {
            'AtomicRandomString.test',
            'AtomicIntegerIncrementer.test',
            'foo',
            'env',
            'foobar',
        },
    }
