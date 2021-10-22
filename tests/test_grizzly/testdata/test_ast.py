import os

from typing import List, Tuple, cast
from json import loads as jsonloads, dumps as jsondumps

import pytest

from jinja2 import Template
from jinja2.exceptions import TemplateError

from grizzly.testdata.ast import RequestSourceMapping, _parse_templates, get_template_variables
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.task import RequestTask
from ..fixtures import request_task, request_task_syntax_error  # pylint: disable=unused-import

# pylint: disable=redefined-outer-name,global-statement


@pytest.mark.usefixtures('request_task')
def test__parse_template(request_task: Tuple[str, str, RequestTask]) -> None:
    request = request_task[-1]

    assert request.source is not None

    source = jsonloads(request.source)
    source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
    source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
    source['result']['File'] = '{{ AtomicDirectoryContents.test }}'
    source['result']['TestSubString'] = '{{ a_sub_string[:3] }}'
    source['result']['TestString'] = '{{ a_string }}'

    request.source = jsondumps(source)
    request.template = Template(request.source)

    templates: RequestSourceMapping = {
        'TestScenario': set([('.', request)])
    }

    variables = _parse_templates(templates)

    assert 'TestScenario' in variables
    assert len(variables['TestScenario']) == 8
    assert 'messageID' in variables['TestScenario']
    assert 'AtomicIntegerIncrementer.messageID' in variables['TestScenario']
    assert 'AtomicDate.now' in variables['TestScenario']
    assert 'AtomicCsvRow.test.header1' in variables['TestScenario']
    assert 'AtomicCsvRow.test.header2' in variables['TestScenario']
    assert 'AtomicDirectoryContents.test' in variables['TestScenario']
    assert 'a_sub_string' in variables['TestScenario']
    assert 'a_string' in variables['TestScenario']


@pytest.mark.usefixtures('request_task_syntax_error')
def test__parse_template_syntax_error(request_task_syntax_error: Tuple[str, str]) -> None:
    templates: RequestSourceMapping = {
        'TestScenario': set([request_task_syntax_error])
    }

    with pytest.raises(TemplateError):
        _parse_templates(templates)

def test__parse_template_notfound() -> None:
    templates: RequestSourceMapping = {
        'TestScenario': set([(os.path.join('some', 'weird', 'path'), 'non-existing.j2.json')])
    }

    with pytest.raises(TemplateError):
        _parse_templates(templates)

def test_get_template_variables_none() -> None:
    variables = get_template_variables(None)
    assert variables == {}

    variables = get_template_variables([])
    assert variables == {}


def test_get_template_variables() -> None:
    tasks: List[RequestTask] = []

    scenario = GrizzlyContextScenario()
    scenario.name = 'TestScenario'
    scenario.context['host'] = 'http://test.nu'
    scenario.user_class_name = 'TestUser'
    scenario.add_task(
        RequestTask(RequestMethod.POST, name='Test POST request', endpoint='/api/test/post')
    )
    tasks.append(cast(RequestTask, scenario.tasks[-1]))
    tasks[-1].source = '{{ AtomicInteger.test }}'
    tasks[-1].template = Template(tasks[-1].source)

    scenario.add_task(
        RequestTask(RequestMethod.GET, name='Test GET request', endpoint='/api/test/get')
    )
    tasks.append(cast(RequestTask, scenario.tasks[-1]))
    tasks[-1].source = '{{ AtomicIntegerIncrementer.test }}'
    tasks[-1].template = Template(tasks[-1].source)

    variables = get_template_variables(tasks)

    expected_scenario_name = '_'.join([scenario.name, scenario.identifier])

    assert scenario.get_name() == expected_scenario_name
    assert expected_scenario_name in variables
    assert 'AtomicInteger.test' in variables[expected_scenario_name]
    assert 'AtomicIntegerIncrementer.test' in variables[expected_scenario_name]
