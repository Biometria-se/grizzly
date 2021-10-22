import shutil

from os import path, environ
from typing import Dict, Any, Callable, List, Tuple, cast
from json import dumps as jsondumps, loads as jsonloads
from os import mkdir, path

import pytest

from _pytest.tmpdir import TempdirFactory

from jinja2 import Template
from behave.runner import Context

from grizzly.context import LocustContext
from grizzly.task import RequestTask
from grizzly.testdata.utils import (
    _get_variable_value,
    _objectify,
    initialize_testdata,
    transform,
)
from grizzly.testdata.variables import AtomicCsvRow, AtomicInteger, AtomicIntegerIncrementer

from ..fixtures import locust_context, request_task, behave_context  # pylint: disable=unused-import
from .fixtures import cleanup  # pylint: disable=unused-import

# pylint: disable=redefined-outer-name


@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_static(cleanup: Callable) -> None:
    try:
        context_locust = LocustContext()
        variable_name = 'test'

        context_locust.state.variables[variable_name] = 1337
        value = _get_variable_value(variable_name)
        assert value == 1337

        context_locust.state.variables[variable_name] = '1337'
        value = _get_variable_value(variable_name)
        assert value == 1337

        context_locust.state.variables[variable_name] = "'1337'"
        value = _get_variable_value(variable_name)
        assert value == '1337'
    finally:
        cleanup()


@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_AtomicInteger(cleanup: Callable) -> None:
    try:
        context_locust = LocustContext()
        variable_name = 'AtomicInteger.test'

        context_locust.state.variables[variable_name] = 1337
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        AtomicInteger.destroy()

        context_locust.state.variables[variable_name] = '1337'
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        AtomicInteger.destroy()
    finally:
        cleanup()

@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_AtomicIntegerIncrementer(cleanup: Callable) -> None:
    try:
        context_locust = LocustContext()

        variable_name = 'AtomicIntegerIncrementer.test'
        context_locust.state.variables[variable_name] = 1337
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        assert value['test'] == 1338
        AtomicIntegerIncrementer.destroy()

        context_locust.state.variables[variable_name] = '1337'
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        assert value['test'] == 1338
        AtomicIntegerIncrementer.destroy()
    finally:
        cleanup()


@pytest.mark.usefixtures('cleanup', 'tmpdir_factory')
def test__get_variable_value_AtomicCsvRow(cleanup: Callable, tmpdir_factory: TempdirFactory) -> None:
    test_context = str(tmpdir_factory.mktemp('test_context').mkdir('requests'))
    test_context_root = path.dirname(test_context)
    environ['LOCUST_CONTEXT_ROOT'] = test_context_root

    with open(path.join(test_context, 'test.csv'), 'w') as fd:
        fd.write('header1,header2\n')
        fd.write('value1,value2\n')
        fd.write('value3,value4\n')
        fd.flush()
    try:
        context_locust = LocustContext()
        variable_name = 'AtomicCsvRow.test'
        context_locust.state.variables['AtomicCsvRow.test'] = 'test.csv'
        value = _get_variable_value(variable_name)

        assert isinstance(value, AtomicCsvRow)
        assert 'test' in value._values
        assert 'test' in value._rows
        assert value['test'] == {'header1': 'value1', 'header2': 'value2'}
        assert value['test'] == {'header1': 'value3', 'header2': 'value4'}
        assert value['test'] is None
    finally:
        shutil.rmtree(test_context_root)
        cleanup()


def test__objectify() -> None:
    testdata: Dict[str, Any] = {
        'AtomicInteger': {
            'test': 1337,
        },
        'test': 1338,
        'AtomicCsvRow': {
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
        }
    }

    obj = _objectify(testdata)

    assert (
        obj['AtomicInteger'].__module__ == 'grizzly.testdata.utils' and
        obj['AtomicInteger'].__class__.__name__ == 'Testdata'
    )
    assert getattr(obj['AtomicInteger'], 'test') == 1337
    assert isinstance(obj['test'], int)
    assert obj['test'] == 1338
    assert (
        obj['AtomicCsvRow'].__module__ == 'grizzly.testdata.utils' and
        obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
    )
    atomiccsvrow_input = getattr(obj['AtomicCsvRow'], 'input', None)
    assert atomiccsvrow_input is not None
    assert (
        atomiccsvrow_input.__module__ == 'grizzly.testdata.utils' and
        atomiccsvrow_input.__class__.__name__ == 'Testdata'
    )
    assert getattr(atomiccsvrow_input, 'test1', None) == 'hello'
    assert getattr(atomiccsvrow_input, 'test2', None) == 'world!'

    assert (
        obj['Test'].__module__ == 'grizzly.testdata.utils' and
        obj['Test'].__class__.__name__ == 'Testdata'
    )
    test = getattr(obj['Test'], 'test1', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils' and
        test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test2', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils' and
        test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test3', None)
    assert test is not None
    assert isinstance(test, str)
    assert test == 'value'


def test_transform_raw() -> None:
    data = {
        'test.number.value': 1337,
        'test.number.description': 'simple description',
        'test.string.value': 'hello world!',
        'test.bool.value': True,
    }

    actual = transform(data, True)

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
            }
        }
    }


@pytest.mark.usefixtures('cleanup')
def test_transform(cleanup: Callable) -> None:
    try:
        data: Dict[str, Any] = {
            'AtomicInteger.test': 1337,
            'test': 1338,
            'AtomicCsvRow.input.test1': 'hello',
            'AtomicCsvRow.input.test2': 'world!',
            'Test.test1.test2.test3': 'value',
            'Test.test1.test2.test4': 'value',
            'Test.test2.test3': 'value',
        }

        obj = transform(data)

        assert (
            obj['AtomicInteger'].__module__ == 'grizzly.testdata.utils' and
            obj['AtomicInteger'].__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicInteger'], 'test', None) == 1337
        assert isinstance(obj['test'], int)
        assert obj['test'] == 1338
        assert (
            obj['AtomicCsvRow'].__module__ == 'grizzly.testdata.utils' and
            obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
        )
        assert (
            obj['AtomicCsvRow'].input.__module__ == 'grizzly.testdata.utils' and
            obj['AtomicCsvRow'].input.__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicCsvRow'].input, 'test1', None) == 'hello'
        assert getattr(obj['AtomicCsvRow'].input, 'test2', None) == 'world!'
        assert (
            obj['Test'].__module__ == 'grizzly.testdata.utils' and
            obj['Test'].__class__.__name__ == 'Testdata'
        )
        test = getattr(obj['Test'], 'test1', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils' and
            test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test2', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils' and
            test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test3', None)
        assert test is not None
        assert isinstance(test, str)
        assert test == 'value'
    finally:
        cleanup()


def test_initialize_testdata_no_tasks() -> None:
    testdata = initialize_testdata(None)
    assert testdata == {}

    testdata = initialize_testdata([])
    assert testdata == {}


@pytest.mark.usefixtures('request_task', 'cleanup')
def test_initialize_testdata_with_tasks(
    request_task: Tuple[str, str, RequestTask],
    cleanup: Callable,
) -> None:
    try:
        context_locust = LocustContext()
        context_locust.state.variables['AtomicIntegerIncrementer.messageID'] = 1337
        context_locust.state.variables['AtomicDate.now'] = 'now'
        _, _, request = request_task
        scenario = request.scenario
        scenario.add_task(request)
        testdata = initialize_testdata(cast(List[RequestTask], scenario.tasks))

        scenario_name = scenario.get_name()

        assert scenario_name in testdata
        assert len(testdata[scenario_name]) == 3
        assert 'messageID' in testdata[scenario_name]
        assert 'AtomicIntegerIncrementer.messageID' in testdata[scenario_name]
        assert 'AtomicDate.now' in testdata[scenario_name]
    finally:
        cleanup()


@pytest.mark.usefixtures('behave_context', 'locust_context', 'cleanup')
def test_initialize_testdata_with_payload_context(behave_context: Context, locust_context: Callable, cleanup: Callable) -> None:
    try:
        _, _, task, [context_root, _, request] = locust_context()
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

        source = jsonloads(request.source)
        source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
        source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
        source['result']['File'] = '{{ AtomicDirectoryContents.test }}'

        request.source = jsondumps(source)
        request.template = Template(request.source)

        context_locust = cast(LocustContext, behave_context.locust)
        context_locust.add_scenario(task.__class__.__name__)
        context_locust.state.variables['messageID'] = 123
        context_locust.state.variables['AtomicIntegerIncrementer.messageID'] = 456
        context_locust.state.variables['AtomicCsvRow.test'] = 'test.csv'
        context_locust.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
        context_locust.state.variables['AtomicDate.now'] = 'now'
        context_locust.scenario.user_class_name = 'TestUser'
        context_locust.scenario.context['host'] = 'http://test.nu'
        context_locust.scenario.iterations = 2
        context_locust.scenario.add_task(request)

        testdata = initialize_testdata(cast(List[RequestTask], context_locust.scenario.tasks))

        scenario_name = context_locust.scenario.get_name()

        assert scenario_name in testdata

        data = testdata[scenario_name]

        assert data['messageID'] == 123

        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 456
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 457
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 458
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 459

        assert data['AtomicCsvRow.test.header1']['test'] == {'header1': 'value1', 'header2': 'value2'}
        assert data['AtomicCsvRow.test.header2']['test']['header2'] == 'value4'
        with pytest.raises(TypeError):
            assert data['AtomicCsvRow.test.header1']['test']['header1'] is None
        assert data['AtomicCsvRow.test.header2']['test'] is None
        assert data['AtomicCsvRow.test.header1']['test'] is None

        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory/file1.txt'
        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory/file2.txt'
        assert data['AtomicDirectoryContents.test']['test'] is None
    finally:
        cleanup()
