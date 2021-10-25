import shutil

from os import path, environ
from typing import Callable, List, Tuple, cast
from json import dumps as jsondumps, loads as jsonloads
from os import mkdir, path

import pytest

from _pytest.tmpdir import TempdirFactory

from jinja2 import Template
from behave.runner import Context

from grizzly.context import GrizzlyContext
from grizzly.task import RequestTask
from grizzly.testdata.utils import (
    _get_variable_value,
    initialize_testdata,
)
from grizzly.testdata.variables import AtomicCsvRow, AtomicInteger, AtomicIntegerIncrementer

from ..fixtures import grizzly_context, request_task, behave_context  # pylint: disable=unused-import
from .fixtures import cleanup  # pylint: disable=unused-import

# pylint: disable=redefined-outer-name


@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_static(cleanup: Callable) -> None:
    try:
        grizzly = GrizzlyContext()
        variable_name = 'test'

        grizzly.state.variables[variable_name] = 1337
        value = _get_variable_value(variable_name)
        assert value == 1337

        grizzly.state.variables[variable_name] = '1337'
        value = _get_variable_value(variable_name)
        assert value == 1337

        grizzly.state.variables[variable_name] = "'1337'"
        value = _get_variable_value(variable_name)
        assert value == '1337'
    finally:
        cleanup()


@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_AtomicInteger(cleanup: Callable) -> None:
    try:
        grizzly = GrizzlyContext()
        variable_name = 'AtomicInteger.test'

        grizzly.state.variables[variable_name] = 1337
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        AtomicInteger.destroy()

        grizzly.state.variables[variable_name] = '1337'
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        AtomicInteger.destroy()
    finally:
        cleanup()

@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_AtomicIntegerIncrementer(cleanup: Callable) -> None:
    try:
        grizzly = GrizzlyContext()

        variable_name = 'AtomicIntegerIncrementer.test'
        grizzly.state.variables[variable_name] = 1337
        value = _get_variable_value(variable_name)
        assert value['test'] == 1337
        assert value['test'] == 1338
        AtomicIntegerIncrementer.destroy()

        grizzly.state.variables[variable_name] = '1337'
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
    environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

    with open(path.join(test_context, 'test.csv'), 'w') as fd:
        fd.write('header1,header2\n')
        fd.write('value1,value2\n')
        fd.write('value3,value4\n')
        fd.flush()
    try:
        grizzly = GrizzlyContext()
        variable_name = 'AtomicCsvRow.test'
        grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
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
        grizzly = GrizzlyContext()
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 1337
        grizzly.state.variables['AtomicDate.now'] = 'now'
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


@pytest.mark.usefixtures('behave_context', 'grizzly_context', 'cleanup')
def test_initialize_testdata_with_payload_context(behave_context: Context, grizzly_context: Callable, cleanup: Callable) -> None:
    try:
        _, _, task, [context_root, _, request] = grizzly_context()
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

        grizzly = cast(GrizzlyContext, behave_context.grizzly)
        grizzly.add_scenario(task.__class__.__name__)
        grizzly.state.variables['messageID'] = 123
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
        grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
        grizzly.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
        grizzly.state.variables['AtomicDate.now'] = 'now'
        grizzly.scenario.user_class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test.nu'
        grizzly.scenario.iterations = 2
        grizzly.scenario.add_task(request)

        testdata = initialize_testdata(cast(List[RequestTask], grizzly.scenario.tasks))

        scenario_name = grizzly.scenario.get_name()

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
