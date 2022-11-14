from typing import cast
from json import loads as jsonloads, dumps as jsondumps

from grizzly.testdata.ast import _parse_templates, get_template_variables
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask, LogMessageTask

from ...fixtures import RequestTaskFixture


def test__parse_template(request_task: RequestTaskFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
    source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
    source['result']['File'] = '{{ AtomicDirectoryContents.test }}'
    source['result']['TestSubString'] = '{{ a_sub_string[:3] }}'
    source['result']['TestString'] = '{{ a_string }}'
    source['result']['FooBar'] = '{{ (AtomicIntegerIncrementer.file_number | int) }}'

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1)
    scenario.name = 'TestScenario'
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert 'TestScenario_001' in variables
    assert 'messageID' in variables['TestScenario_001']
    assert 'AtomicIntegerIncrementer.messageID' in variables['TestScenario_001']
    assert 'AtomicDate.now' in variables['TestScenario_001']
    assert 'AtomicCsvRow.test.header1' in variables['TestScenario_001']
    assert 'AtomicCsvRow.test.header2' in variables['TestScenario_001']
    assert 'AtomicDirectoryContents.test' in variables['TestScenario_001']
    assert 'a_sub_string' in variables['TestScenario_001']
    assert 'a_string' in variables['TestScenario_001']
    assert 'AtomicIntegerIncrementer.file_number' in variables['TestScenario_001']
    assert len(variables['TestScenario_001']) == 9


def test__parse_template_nested_pipe(request_task: RequestTaskFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result'] = {'FooBar': '{{ AtomicIntegerIncrementer.file_number | int }}'}

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1)
    scenario.name = 'TestScenario'
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert 'TestScenario_001' in variables
    assert 'AtomicIntegerIncrementer.file_number' in variables['TestScenario_001']
    assert len(variables['TestScenario_001']) == 1

    source['result'] = {'FooBar': '{{ (AtomicIntegerIncrementer.file_number | int) }}'}

    request.source = jsondumps(source)
    scenario.tasks.clear()
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert 'TestScenario_001' in variables
    assert 'AtomicIntegerIncrementer.file_number' in variables['TestScenario_001']
    assert len(variables['TestScenario_001']) == 1

    request.source = "{{ '%08d' % key[:6] | int }}_{{ guid }}_{{ AtomicDate.date }}_{{ '%012d' % AtomicIntegerIncrementer.file_number }}"

    scenario.tasks.clear()
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert 'TestScenario_001' in variables
    assert 'key' in variables['TestScenario_001']
    assert 'guid' in variables['TestScenario_001']
    assert 'AtomicDate.date' in variables['TestScenario_001']
    assert 'AtomicIntegerIncrementer.file_number' in variables['TestScenario_001']
    assert len(variables['TestScenario_001']) == 4

    source = {
        'result': {
            'FooBar_1': '{{ (value1 * 0.25) | round | int }}',
            'FooBar_2': '{{ (((value2 * 0.25) | round) + 1) | int }}',
            'FooBar_3': '{{ ((value3 - 2) | int) + 1 }}',
            'FooBar_4': '{{ (4 * value40) + (5 - value41) }}',
            'FooBar_5': '{{ Atomic.test1 + Atomic.test2 + Atomic.test3 }}',
        }
    }

    request.source = jsondumps(source)
    scenario.tasks.clear()
    scenario.tasks.add(request)
    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    print(variables)

    assert variables == {'TestScenario_001': {'value1', 'value2', 'value3', 'value40', 'value41', 'Atomic.test1', 'Atomic.test2', 'Atomic.test3'}}


def test_get_template_variables() -> None:
    variables = get_template_variables([])
    assert variables == {}

    scenario = GrizzlyContextScenario(1)
    scenario.name = 'TestScenario'
    scenario.context['host'] = 'http://test.nu'
    scenario.user.class_name = 'TestUser'
    scenario.tasks.add(
        RequestTask(RequestMethod.POST, name='Test POST request', endpoint='/api/test/post')
    )
    task = cast(RequestTask, scenario.tasks[-1])
    task.source = '{{ AtomicRandomString.test }}'

    scenario.tasks.add(
        RequestTask(RequestMethod.GET, name='{{ env }} GET request', endpoint='/api/{{ env }}/get')
    )
    task = cast(RequestTask, scenario.tasks[-1])
    task.source = '{{ AtomicIntegerIncrementer.test }}'

    scenario.tasks.add(
        LogMessageTask(message='{{ foo }}')
    )

    variables = get_template_variables(scenario.tasks)

    expected_scenario_name = '_'.join([scenario.name, scenario.identifier])

    assert scenario.class_name == expected_scenario_name
    assert expected_scenario_name in variables
    assert 'AtomicRandomString.test' in variables[expected_scenario_name]
    assert 'AtomicIntegerIncrementer.test' in variables[expected_scenario_name]
    assert 'foo' in variables[expected_scenario_name]
    assert 'env' in variables[expected_scenario_name]
