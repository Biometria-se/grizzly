"""Unit tests of grizzly.testdata.ast."""
from __future__ import annotations

from json import dumps as jsondumps
from json import loads as jsonloads
from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.tasks import DateTask, LogMessageTask, RequestTask
from grizzly.testdata.ast import _parse_templates, get_template_variables
from grizzly.types import RequestMethod

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture, RequestTaskFixture


def test__parse_template(request_task: RequestTaskFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result'].update({
        'CsvRowValue1': '{{ AtomicCsvReader.test.header1 }}',
        'CsvRowValue2': '{{ AtomicCsvReader.test.header2 }}',
        'File': '{{ AtomicDirectoryContents.test }}',
        'TestSubString': '{{ a_sub_string[:3] }}',
        'TestString': '{{ a_string if undeclared_variable is not defined else "foo" }}',
        'FooBar': '{{ (AtomicIntegerIncrementer.file_number | int) }}',
        'Expression': '{{ expression == "True" if undeclared_variable is defined else "True" }}',
        'Undefined': '{{ undeclared_variable if undeclared_variable is defined else "unknown" }}',
        'UndefinedAdvanced': '{{ AtomicIntegerIncrementer.undefined if AtomicIntegerIncrementer.undefined is defined else "hello" }}',
        'Content': '{{ some_weird_variable if some_weird_variable is defined else content }}',
    })

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1, behave=request_task.behave_fixture.create_scenario('TestScenario'))
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}
    variables, allowed_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

    assert variables == {
        'TestScenario_001': {
            'AtomicIntegerIncrementer.messageID',
            'AtomicIntegerIncrementer.file_number',
            'AtomicDirectoryContents.test',
            'messageID',
            'content',
            'AtomicDate.now',
            'AtomicCsvReader.test.header1',
            'AtomicCsvReader.test.header2',
            'a_sub_string',
            'a_string',
            'expression',
        },
    }

    assert allowed_unused == {'some_weird_variable', 'AtomicIntegerIncrementer.undefined', 'undeclared_variable'}


def test__parse_template_nested_pipe(request_task: RequestTaskFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result'] = {'FooBar': '{{ AtomicIntegerIncrementer.file_number | int }}'}

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1, behave=request_task.behave_fixture.create_scenario('TestScenario'))
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables, allow_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

    assert variables == {
        'TestScenario_001': {
            'AtomicIntegerIncrementer.file_number',
        },
    }

    assert allow_unused == set()

    source['result'] = {'FooBar': '{{ (AtomicIntegerIncrementer.file_number | int) }}'}

    request.source = jsondumps(source)
    scenario.tasks.clear()
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables, allow_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

    assert variables == {
        'TestScenario_001': {
            'AtomicIntegerIncrementer.file_number',
        },
    }
    assert allow_unused == set()

    request.source = "{{ '%08d' % key[:6] | int }}_{{ guid }}_{{ AtomicDate.date }}_{{ '%012d' % AtomicIntegerIncrementer.file_number }}"

    scenario.tasks.clear()
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    variables, allow_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

    assert variables == {
        'TestScenario_001': {
            'key',
            'guid',
            'AtomicDate.date',
            'AtomicIntegerIncrementer.file_number',
        },
    }

    assert allow_unused == set()

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
        },
    }

    request.source = jsondumps(source)
    scenario.tasks.clear()
    scenario.tasks.add(request)
    templates = {scenario: set(request.get_templates())}

    variables, allow_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

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
    assert allow_unused == set()

    request.source = '{%- set hello = world -%} {{ foobar }}'
    scenario.tasks.clear()
    scenario.tasks.add(request)
    templates = {scenario: set(request.get_templates())}

    variables, allow_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

    assert variables == {
        'TestScenario_001': {
            'foobar',
            'world',
        },
    }
    assert allow_unused == set()

    request.source = """
{%- set t1l = AtomicCsvReader.input.value1 -%}
{%- set first_parts = [] -%}
{%- for p in t1l.split(';') -%}
	{%- set parts = p.split('|') -%}
	{%- set _ = first_parts.append({'p': parts[0], 'c': parts[1]}) -%}
{%- endfor -%}
{%- set t2l = AtomicCsvReader.input.value2 -%}
{%- set second_parts = [] -%}
{%- for p in t2l.split(';') -%}
	{%- set parts = p.split('|') -%}
	{%- set _ = second_parts.append({'p': parts[0], 'c': parts[1]}) -%}
{%- endfor -%}
{
    "id": ["{{ id }}"],
    "list": [{
            "subId": "{{ first_subid }}",
            "isIncluded": true,
            "concurrencyTag": {{ first_concurrencytag | tojson }},
            "length": {{ AtomicCsvReader.input.first_length | int }},
            "width": {{ AtomicCsvReader.input.first_width | int }},
            "height": {{ AtomicCsvReader.input.first_height | int }},
            "volume": {{ ((AtomicCsvReader.input.first_width | int) / 100) * ((AtomicCsvReader.input.first_height | int) / 100) * ((AtomicCsvReader.input.first_length | int) / 100) }},
        }, {
            "subId": "{{ second_subid }}",
            "isIncluded": true,
            "concurrencyTag": {{ second_concurrencytag | tojson }},
            "length": {{ AtomicCsvReader.input.second_length | int }},
            "width": {{ AtomicCsvReader.input.second_width | int }},
            "height": {{ AtomicCsvReader.input.second_height | int }},
            "volume": {{ ((AtomicCsvReader.input.second_width | int) / 100) * ((AtomicCsvReader.input.second_height | int) / 100) * ((AtomicCsvReader.input.second_length | int) / 100) }},
        }
    ],
    "timestamp": "{{ datetime.now() }}"
}
"""  # noqa: E501
    scenario.tasks.clear()
    scenario.tasks.add(request)
    templates = {scenario: set(request.get_templates())}

    variables, allow_unused = _parse_templates(templates, env=request_task.behave_fixture.grizzly.state.jinja2)

    assert variables == {
        'TestScenario_001': {
            'AtomicCsvReader.input.value1',
            'AtomicCsvReader.input.value2',
            'id',
            'first_subid',
            'first_concurrencytag',
            'AtomicCsvReader.input.first_length',
            'AtomicCsvReader.input.first_width',
            'AtomicCsvReader.input.first_height',
            'second_subid',
            'second_concurrencytag',
            'AtomicCsvReader.input.second_length',
            'AtomicCsvReader.input.second_width',
            'AtomicCsvReader.input.second_height',
        },
    }
    assert allow_unused == set()


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
        RequestTask(RequestMethod.POST, name='Test POST request', endpoint='/api/test/post'),
    )
    task = cast(RequestTask, grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicRandomString.test[:2] }}'

    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.GET, name='{{ env }} GET request', endpoint='/api/{{ env }}/get'),
    )
    task = cast(RequestTask, grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicIntegerIncrementer.test }} {%- set hello = world -%}'

    grizzly.scenario.tasks.add(
        LogMessageTask(message='{{ foo }}'),
    )

    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.GET, name='Test GET request', endpoint='/api/test/post'),
    )
    task = cast(RequestTask, grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicRandomString.id.replace("-", "")[:2] }}'

    grizzly.scenario.tasks.add(
        DateTask('timestamp', '{{ datetime.now() }} | format="%Y%m%d"'),
    )

    grizzly.scenario.orphan_templates.append('{{ foobar }}')
    grizzly.state.variables.update({
        'AtomicRandomString.test': 'none',
        'AtomicRandomString.id': 'none',
        'AtomicIntegerIncrementer.test': 2,
        'foo': 'bar',
        'env': 'none',
        'foobar': 'barfoo',
        'world': 'hello',
    })

    variables = get_template_variables(grizzly)

    expected_scenario_name = f'{grizzly.scenario.name}_{grizzly.scenario.identifier}'

    assert grizzly.scenario.class_name == expected_scenario_name
    assert variables == {
        expected_scenario_name: {
            'AtomicRandomString.test',
            'AtomicRandomString.id',
            'AtomicIntegerIncrementer.test',
            'foo',
            'env',
            'foobar',
            'world',
        },
    }

    del grizzly.state.variables['foo']

    with pytest.raises(AssertionError, match='variables has been found in templates, but have not been declared: foo'):
        get_template_variables(grizzly)

    grizzly.state.variables.update({'foo': 'bar', 'bar': 'foo'})

    with pytest.raises(AssertionError, match='variables has been declared, but cannot be found in templates: bar'):
        get_template_variables(grizzly)
