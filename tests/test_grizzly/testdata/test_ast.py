from typing import cast
from json import loads as jsonloads, dumps as jsondumps

from jinja2 import Template

from grizzly.testdata.ast import _parse_templates, get_template_variables
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask, PrintTask

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

    request.source = jsondumps(source)
    request.template = Template(request.source)
    scenario = GrizzlyContextScenario()
    scenario.name = 'TestScenario'
    scenario.add_task(request)

    templates = {scenario: set(request.get_templates())}

    variables = _parse_templates(templates)

    assert 'TestScenario_bdd2cac2' in variables
    assert len(variables['TestScenario_bdd2cac2']) == 8
    assert 'messageID' in variables['TestScenario_bdd2cac2']
    assert 'AtomicIntegerIncrementer.messageID' in variables['TestScenario_bdd2cac2']
    assert 'AtomicDate.now' in variables['TestScenario_bdd2cac2']
    assert 'AtomicCsvRow.test.header1' in variables['TestScenario_bdd2cac2']
    assert 'AtomicCsvRow.test.header2' in variables['TestScenario_bdd2cac2']
    assert 'AtomicDirectoryContents.test' in variables['TestScenario_bdd2cac2']
    assert 'a_sub_string' in variables['TestScenario_bdd2cac2']
    assert 'a_string' in variables['TestScenario_bdd2cac2']


def test_get_template_variables() -> None:
    variables = get_template_variables([])
    assert variables == {}

    scenario = GrizzlyContextScenario()
    scenario.name = 'TestScenario'
    scenario.context['host'] = 'http://test.nu'
    scenario.user.class_name = 'TestUser'
    scenario.add_task(
        RequestTask(RequestMethod.POST, name='Test POST request', endpoint='/api/test/post')
    )
    task = cast(RequestTask, scenario.tasks[-1])
    task.source = '{{ AtomicRandomString.test }}'
    task.template = Template(task.source)

    scenario.add_task(
        RequestTask(RequestMethod.GET, name='{{ env }} GET request', endpoint='/api/{{ env }}/get')
    )
    task = cast(RequestTask, scenario.tasks[-1])
    task.source = '{{ AtomicIntegerIncrementer.test }}'
    task.template = Template(task.source)

    scenario.add_task(
        PrintTask(message='{{ foo }}')
    )

    variables = get_template_variables(scenario.tasks)

    expected_scenario_name = '_'.join([scenario.name, scenario.identifier])

    assert scenario.get_name() == expected_scenario_name
    assert expected_scenario_name in variables
    assert 'AtomicRandomString.test' in variables[expected_scenario_name]
    assert 'AtomicIntegerIncrementer.test' in variables[expected_scenario_name]
    assert 'foo' in variables[expected_scenario_name]
    assert 'env' in variables[expected_scenario_name]
