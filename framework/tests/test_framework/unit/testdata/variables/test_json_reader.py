"""Unit tests of grizzly.testdata.variables.csv_reader."""

from __future__ import annotations

import json
from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from grizzly.testdata.variables import AtomicJsonReader
from grizzly.testdata.variables.json_reader import atomicjsonreader__base_type__

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types import StrDict

    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


def test_atomicjsonreader__base_type__(grizzly_fixture: GrizzlyFixture) -> None:
    test_context = grizzly_fixture.test_context / 'requests'
    test_context.mkdir(exist_ok=True)

    with pytest.raises(ValueError, match=r'must be a JSON file with file extension \.json'):
        atomicjsonreader__base_type__('file1.txt')

    (test_context / 'a-directory.json').mkdir()

    with pytest.raises(ValueError, match='is not a file in'):
        atomicjsonreader__base_type__('a-directory.json')

    with pytest.raises(ValueError, match='is not a file in'):
        atomicjsonreader__base_type__('file1.json')

    test_file = test_context / 'file1.json'
    test_file.write_text('\n')

    with pytest.raises(ValueError, match='failed to load contents of'):
        atomicjsonreader__base_type__('file1.json')

    test_file.write_text('{"hello": "world"}')

    with pytest.raises(ValueError, match=r'contents of file1.json is not a list \(dict\)'):
        atomicjsonreader__base_type__('file1.json')

    test_file.write_text('[{"hello": "world"}]')

    assert atomicjsonreader__base_type__('file1.json') == 'file1.json'

    with pytest.raises(ValueError, match='is not allowed'):
        atomicjsonreader__base_type__('file1.json | arg1=test')

    assert atomicjsonreader__base_type__('file1.json|random=True') == 'file1.json | random=True'


class TestAtomicJsonReader:
    def test_variable(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:  # noqa: PLR0915
        test_context = grizzly_fixture.test_context / 'requests'
        test_context.mkdir(exist_ok=True)

        for count in range(1, 4):
            file = f'{count}.json'
            with (test_context / file).open('w') as fd:
                data: list[StrDict] = []
                for row in range(1, count + 1):
                    item: StrDict = {}
                    for column in range(1, count + 1):
                        item.update({f'header{column}{count}': f'value{column}{row}{count}'})
                    data.append(item)

                json.dump(data, fd)
                fd.flush()

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            instance1 = AtomicJsonReader(scenario=scenario1, variable='test1', value='1.json')
            instance2 = AtomicJsonReader(scenario=scenario2, variable='test1', value='1.json')

            for instance in [instance1, instance2]:
                assert len(instance._items['test1']) == 1

                actual_value = instance['test1']
                assert actual_value is not None
                assert isinstance(actual_value, dict)
                assert actual_value['header11'] == 'value111'
                assert len(instance._items['test1']) == 0

                actual_value = instance['test1']
                assert actual_value is None

            instance = AtomicJsonReader(scenario=scenario1, variable='test2', value='2.json')
            assert len(instance._items['test2']) == 2

            actual_value = instance['test2.header22']
            assert actual_value is not None
            assert len(instance._items['test2']) == 1
            assert actual_value == {'header22': 'value212'}

            actual_value = instance['test2']
            assert actual_value is not None
            assert len(instance._items['test2']) == 0
            assert actual_value == {'header12': 'value122', 'header22': 'value222'}

            actual_value = instance['test2']
            assert actual_value is None

            instance = AtomicJsonReader(scenario=scenario2, variable='test3', value='3.json')
            assert len(instance._items['test3']) == 3

            with pytest.raises(ValueError, match='AtomicJsonReader.test3: headerXX does not exists'):
                instance['test3.headerXX']

            assert instance['test3'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['test3'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['test3'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance.__getitem__('test3') is None

            with pytest.raises(NotImplementedError, match='AtomicJsonReader has not implemented "__setitem__"'):
                instance['test4'] = {'test': 'value'}
            assert 'test4' not in instance._items

            instance = AtomicJsonReader(scenario=scenario1, variable='test5', value='3.json')
            assert 'test5' in instance._items

            del instance['test5']
            assert 'test5' not in instance._items

            del instance['test5']
            del instance['test5.header13']

            instance = AtomicJsonReader(scenario=scenario2, variable='infinite', value='3.json | repeat=True')
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}

            instance = AtomicJsonReader(scenario=scenario1, variable='random', value='3.json | random=True')
            assert instance['random'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['random'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['random'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance.__getitem__('random') is None

            instance = AtomicJsonReader(scenario=scenario2, variable='randomrepeat', value='3.json | random=True, repeat=True')
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
        finally:
            cleanup()

    def test_clear_and_destroy(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        test_context = grizzly_fixture.test_context / 'requests'
        test_context.mkdir(exist_ok=True)

        with (test_context / 'test.json').open('w') as fd:
            json.dump([{'header1': 'value1'}], fd)
            fd.flush()

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            with suppress(Exception):
                AtomicJsonReader.destroy()

            with pytest.raises(ValueError, match='AtomicJsonReader is not instantiated'):
                AtomicJsonReader.destroy()

            with pytest.raises(ValueError, match='AtomicJsonReader is not instantiated'):
                AtomicJsonReader.clear()

            instance1 = AtomicJsonReader(scenario=scenario1, variable='test', value='test.json')
            instance2 = AtomicJsonReader(scenario=scenario2, variable='test', value='test.json')

            for instance in [instance1, instance2]:
                assert instance['test'] == {'header1': 'value1'}

                assert len(instance._values.keys()) == 1
                assert len(instance._items.keys()) == 1

            AtomicJsonReader.clear()

            for instance in [instance1, instance2]:
                assert len(instance._values.keys()) == 0
                assert len(instance._items.keys()) == 0

            AtomicJsonReader.destroy()
        finally:
            cleanup()

    def test___init___error(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly

            with pytest.raises(ValueError, match='is not a valid JSON source name, must be'):
                AtomicJsonReader(scenario=grizzly.scenario, variable='test.test', value='file1.json')
        finally:
            cleanup()
