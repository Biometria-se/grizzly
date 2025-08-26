"""Unit tests of grizzly.testdata.ast."""

from __future__ import annotations

import logging
from json import dumps as jsondumps
from json import loads as jsonloads
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.tasks import DateTask, LogMessageTask, RequestTask
from grizzly.testdata.ast import _parse_templates, get_template_variables
from grizzly.types import RequestMethod

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, RequestTaskFixture


def test__parse_template(request_task: RequestTaskFixture, caplog: LogCaptureFixture) -> None:
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result'].update(
        {
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
            'ModFilterStatic': '{{ "%08d" % (12312341234 | string)[:6] | int }}',
            'ModCallDynamic': '{{ "%08d" % str(file_id1)[:6] | int }}',
            'ModFilterDynamic': '{{ "%08d" % (file_id2 | string)[:6] | int }}',
            'ModCallStatic': '{{ "%08d" % str(12312341234234)[:6] | int }}',
            'ModFilterDynamicAtomic': '{{ "%08d" % (AtomicIntegerIncrementer.file_id1 | string)[:6] | int }}',
            'ModCallDynamicAtomic': '{{ "%08d" % str(AtomicIntegerIncrementer.file_id2)[:6] | int }}',
            'Call': '{{ AtomicRandomString.somevalue.replace("-", replacement_string) }}',
        },
    )

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1, behave=request_task.behave_fixture.create_scenario('TestScenario'), grizzly=request_task.behave_fixture.grizzly)
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    with caplog.at_level(logging.WARNING):
        actual_variables = _parse_templates(templates)

    expected_variables = {
        scenario: {
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
            'some_weird_variable',
            'AtomicIntegerIncrementer.undefined',
            'undeclared_variable',
            'file_id1',
            'file_id2',
            'AtomicIntegerIncrementer.file_id1',
            'AtomicIntegerIncrementer.file_id2',
            'AtomicRandomString.somevalue',
            'replacement_string',
        },
    }

    assert actual_variables == expected_variables

    assert actual_variables.__conditional__ == {'some_weird_variable', 'AtomicIntegerIncrementer.undefined', 'undeclared_variable', 'a_string', 'content', 'expression'}
    assert actual_variables.__local__ == set()
    assert actual_variables.__map__ == {
        'AtomicIntegerIncrementer.messageID': 'AtomicIntegerIncrementer.messageID',
        'AtomicIntegerIncrementer.file_number': 'AtomicIntegerIncrementer.file_number',
        'AtomicDirectoryContents.test': 'AtomicDirectoryContents.test',
        'messageID': 'messageID',
        'content': 'content',
        'AtomicDate.now': 'AtomicDate.now',
        'AtomicCsvReader.test.header1': 'AtomicCsvReader.test',
        'AtomicCsvReader.test.header2': 'AtomicCsvReader.test',
        'a_sub_string': 'a_sub_string',
        'a_string': 'a_string',
        'expression': 'expression',
        'some_weird_variable': 'some_weird_variable',
        'AtomicIntegerIncrementer.undefined': 'AtomicIntegerIncrementer.undefined',
        'undeclared_variable': 'undeclared_variable',
        'file_id1': 'file_id1',
        'file_id2': 'file_id2',
        'AtomicIntegerIncrementer.file_id1': 'AtomicIntegerIncrementer.file_id1',
        'AtomicIntegerIncrementer.file_id2': 'AtomicIntegerIncrementer.file_id2',
        'AtomicRandomString.somevalue': 'AtomicRandomString.somevalue',
        'replacement_string': 'replacement_string',
    }
    assert actual_variables.__init_map__ == {
        'AtomicIntegerIncrementer.messageID': {'AtomicIntegerIncrementer.messageID'},
        'AtomicIntegerIncrementer.file_number': {'AtomicIntegerIncrementer.file_number'},
        'AtomicDirectoryContents.test': {'AtomicDirectoryContents.test'},
        'messageID': {'messageID'},
        'content': {'content'},
        'AtomicDate.now': {'AtomicDate.now'},
        'AtomicCsvReader.test': {
            'AtomicCsvReader.test.header1',
            'AtomicCsvReader.test.header2',
        },
        'a_sub_string': {'a_sub_string'},
        'a_string': {'a_string'},
        'expression': {'expression'},
        'some_weird_variable': {'some_weird_variable'},
        'AtomicIntegerIncrementer.undefined': {'AtomicIntegerIncrementer.undefined'},
        'undeclared_variable': {'undeclared_variable'},
        'file_id1': {'file_id1'},
        'file_id2': {'file_id2'},
        'AtomicIntegerIncrementer.file_id1': {'AtomicIntegerIncrementer.file_id1'},
        'AtomicIntegerIncrementer.file_id2': {'AtomicIntegerIncrementer.file_id2'},
        'AtomicRandomString.somevalue': {'AtomicRandomString.somevalue'},
        'replacement_string': {'replacement_string'},
    }

    assert caplog.messages == []


def test__parse_template_nested_pipe(request_task: RequestTaskFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    request = request_task.request

    assert request.source is not None

    source = jsonloads(request.source)
    source['result'] = {'FooBar': '{{ AtomicIntegerIncrementer.file_number | int }}'}

    request.source = jsondumps(source)
    scenario = GrizzlyContextScenario(1, behave=request_task.behave_fixture.create_scenario('TestScenario'), grizzly=request_task.behave_fixture.grizzly)
    scenario.tasks.add(request)

    templates = {scenario: set(request.get_templates())}

    with caplog.at_level(logging.WARNING):
        actual_variables = _parse_templates(templates)

        assert actual_variables == {
            scenario: {
                'AtomicIntegerIncrementer.file_number',
            },
        }

        assert actual_variables.__conditional__ == set()
        assert actual_variables.__local__ == set()
        assert actual_variables.__map__ == {'AtomicIntegerIncrementer.file_number': 'AtomicIntegerIncrementer.file_number'}
        assert actual_variables.__init_map__ == {'AtomicIntegerIncrementer.file_number': {'AtomicIntegerIncrementer.file_number'}}

        source['result'] = {'FooBar': '{{ (AtomicIntegerIncrementer.file_number | int) }}'}

        request.source = jsondumps(source)
        scenario.tasks.clear()
        scenario.tasks.add(request)

        templates = {scenario: set(request.get_templates())}

        actual_variables = _parse_templates(templates)

        assert actual_variables == {
            scenario: {
                'AtomicIntegerIncrementer.file_number',
            },
        }
        assert actual_variables.__conditional__ == set()
        assert actual_variables.__local__ == set()
        assert actual_variables.__map__ == {'AtomicIntegerIncrementer.file_number': 'AtomicIntegerIncrementer.file_number'}
        assert actual_variables.__init_map__ == {'AtomicIntegerIncrementer.file_number': {'AtomicIntegerIncrementer.file_number'}}

        request.source = "{{ '%08d' % key[:6] | int }}_{{ guid }}_{{ AtomicDate.date }}_{{ '%012d' % AtomicIntegerIncrementer.file_number }}"

        scenario.tasks.clear()
        scenario.tasks.add(request)

        templates = {scenario: set(request.get_templates())}

        actual_variables = _parse_templates(templates)

        assert actual_variables == {
            scenario: {
                'key',
                'guid',
                'AtomicDate.date',
                'AtomicIntegerIncrementer.file_number',
            },
        }

        assert actual_variables.__conditional__ == set()
        assert actual_variables.__local__ == set()
        assert actual_variables.__map__ == {
            'key': 'key',
            'guid': 'guid',
            'AtomicDate.date': 'AtomicDate.date',
            'AtomicIntegerIncrementer.file_number': 'AtomicIntegerIncrementer.file_number',
        }
        assert actual_variables.__init_map__ == {
            'key': {'key'},
            'guid': {'guid'},
            'AtomicDate.date': {'AtomicDate.date'},
            'AtomicIntegerIncrementer.file_number': {'AtomicIntegerIncrementer.file_number'},
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
            },
        }

        request.source = jsondumps(source)
        scenario.tasks.clear()
        scenario.tasks.add(request)
        templates = {scenario: set(request.get_templates())}

        actual_variables = _parse_templates(templates)

        expected_variables = {
            scenario: {
                'value1',
                'value2',
                'value3',
                'value40',
                'value41',
                'Atomic.test1',
                'Atomic.test2',
                'Atomic.test3',
                'value6',
                'value70',
                'value71',
                'value72',
                'value80',
                'value81',
                'value82',
                'value9',
                'value110',
                'value111',
                'value112',
                'value120',
                'value121',
                'value122',
                'value123',
                'value124',
            },
        }

        assert actual_variables == expected_variables
        assert actual_variables.__conditional__ == set()
        assert actual_variables.__local__ == set()

        request.source = '{%- set hello = world -%} {{ foobar }}'
        scenario.tasks.clear()
        scenario.tasks.add(request)
        templates = {scenario: set(request.get_templates())}

        actual_variables = _parse_templates(templates)

        assert actual_variables == {
            scenario: {
                'foobar',
                'world',
            },
        }
        assert actual_variables.__conditional__ == set()
        assert actual_variables.__local__ == {'hello'}

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

        actual_variables = _parse_templates(templates)

        expected_variables = {
            scenario: {
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

        assert expected_variables == actual_variables
        assert actual_variables.__conditional__ == set()
        assert actual_variables.__local__ == {'t1l', 'first_parts', 't2l', 'second_parts'}

    assert caplog.messages == []


def test_get_template_variables(behave_fixture: BehaveFixture, caplog: LogCaptureFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    grizzly.scenario.tasks.clear()
    variables = get_template_variables(grizzly)
    assert variables == {}

    grizzly.scenario.context['host'] = 'http://test.nu'
    grizzly.scenario.user.class_name = 'TestUser'
    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.POST, name='Test POST request', endpoint='/api/test/post'),
    )
    task = cast('RequestTask', grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicRandomString.test[:2] }}{{ integer | string}}'

    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.GET, name='{{ env }} GET request', endpoint='/api/{{ env }}/get/{{ __internal__.id }}/{{ __external_id__ }}'),
    )
    task = cast('RequestTask', grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicIntegerIncrementer.test }} {%- set hello = world -%}'

    grizzly.scenario.tasks.add(
        LogMessageTask(message='{{ foo }}'),
    )

    grizzly.scenario.tasks.add(
        RequestTask(RequestMethod.GET, name='Test GET request', endpoint='/api/test/post'),
    )
    task = cast('RequestTask', grizzly.scenario.tasks()[-1])
    task.source = '{{ AtomicRandomString.id.replace("-", "")[:2] }}'

    grizzly.scenario.tasks.add(
        DateTask('timestamp', '{{ datetime.now() }} | format="%Y%m%d"'),
    )

    grizzly.scenario.orphan_templates.append('{{ foobar }}')
    grizzly.scenario.variables.update(
        {
            'AtomicRandomString.test': '%s | upper="True"',
            'AtomicRandomString.id': '%d%d%d',
            'AtomicIntegerIncrementer.test': 2,
            'foo': 'bar',
            'env': 'none',
            'foobar': 'barfoo',
            'world': 'hello',
            'integer': '0',
        },
    )

    with caplog.at_level(logging.WARNING):
        variables = get_template_variables(grizzly)

        assert variables == {
            grizzly.scenario: {
                'AtomicRandomString.test',
                'AtomicRandomString.id',
                'AtomicIntegerIncrementer.test',
                'foo',
                'env',
                'foobar',
                'world',
                'integer',
            },
        }

        del grizzly.scenario.variables['foo']
        del grizzly.scenario.variables['env']

        with pytest.raises(AssertionError, match='variables have been found in templates, but have not been declared:\nenv\nfoo'):
            get_template_variables(grizzly)

        grizzly.scenario.variables.update({'foo': 'bar', 'bar': 'foo', 'baz': 'zab'})

        with pytest.raises(AssertionError, match='variables have been declared, but cannot be found in templates:\nbar\nbaz'):
            get_template_variables(grizzly)

    assert caplog.messages == []


def test_get_template_variables_expressions(grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
    grizzly = grizzly_fixture.grizzly
    grizzly.scenario.tasks.clear()
    grizzly.scenario.orphan_templates.clear()
    grizzly.scenario.tasks().clear()
    grizzly.scenario.variables.clear()

    bob_csv = grizzly_fixture.test_context / 'requests' / 'bob.csv'
    bob_csv.parent.mkdir(exist_ok=True)
    bob_csv.touch()

    grizzly.scenario.variables.update(
        {
            'foo': 'bar',
            'bar': 'none',
            'quirk': 'none',
            'AtomicCsvReader.bob': 'bob.csv',
        },
    )
    grizzly.scenario.orphan_templates.append('{% set bar = foo %}')
    grizzly.scenario.orphan_templates.append('{% if bar == "bar" %}foo{% endif %}')
    grizzly.scenario.orphan_templates.append('{% set quirk = AtomicCsvReader.bob.quirk if AtomicCsvReader.bob is defined else AtomicCsvReader.alice.quirk %}')
    grizzly.scenario.orphan_templates.append('quirk: {{ quirk }}')
    grizzly.scenario.orphan_templates.append('{% if quirk == "always late" %}wake up early{% endif %}')

    with caplog.at_level(logging.WARNING):
        actual_variables = get_template_variables(grizzly)
        assert actual_variables == {grizzly.scenario: {'AtomicCsvReader.bob.quirk', 'bar', 'foo', 'quirk'}}

    assert caplog.messages == []


def test_get_template_variables___doc___example(grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
    grizzly = grizzly_fixture.grizzly
    grizzly.scenario.tasks.clear()
    grizzly.scenario.orphan_templates.clear()
    grizzly.scenario.tasks().clear()
    grizzly.scenario.variables.clear()

    input_csv = grizzly_fixture.test_context / 'requests' / 'input.csv'
    input_csv.parent.mkdir(exist_ok=True)
    input_csv.touch()

    grizzly.scenario.variables.update(
        {
            'AtomicCsvReader.input': 'input.csv',
            'AtomicIntegerIncrementer.id': '1',
            'foobar': 'True',
        },
    )

    grizzly.scenario.orphan_templates.append('{% set quirk = AtomicCsvReader.input.quirk if AtomicCsvReader.input is defined else "none" %}')
    grizzly.scenario.orphan_templates.append('{% set name = AtomicCsvReader.input.name if AtomicCsvReader.input is defined else "none" %}')
    grizzly.scenario.orphan_templates.append("""
    {
        "id": {{ AtomicIntegerIncrementer.id }},
        "name": "{{ name }}",
        "quirk": "{{ quirk }}",
        "foobar": {{ foobar }}
    }""")

    with caplog.at_level(logging.WARNING):
        actual_variables = get_template_variables(grizzly)
        assert actual_variables == {grizzly.scenario: {'AtomicCsvReader.input.quirk', 'AtomicCsvReader.input.name', 'AtomicIntegerIncrementer.id', 'foobar'}}

    assert caplog.messages == []
