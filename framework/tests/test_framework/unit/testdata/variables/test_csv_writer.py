"""Unit tests for grizzly.testdata.variable.csv_writer."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from grizzly.testdata.variables.csv_writer import AtomicCsvWriter, atomiccsvwriter__base_type__, atomiccsvwriter_message_handler, open_files
from grizzly.types.locust import Message

from test_framework.helpers import ANYUUID

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture, MockerFixture


def test_atomiccsvwriter__base_type__(grizzly_fixture: GrizzlyFixture) -> None:
    with pytest.raises(ValueError, match='AtomicCsvWriter: arguments are required'):
        atomiccsvwriter__base_type__('foobar')

    with pytest.raises(ValueError, match='AtomicCsvWriter: incorrect format in arguments: ""'):
        atomiccsvwriter__base_type__('foobar |')

    with pytest.raises(ValueError, match='AtomicCsvWriter: argument foo is not allowed'):
        atomiccsvwriter__base_type__('foobar | foo="bar"')

    with pytest.raises(ValueError, match='AtomicCsvWriter: argument headers is required'):
        atomiccsvwriter__base_type__('foobar | overwrite=True')

    with pytest.raises(ValueError, match='AtomicCsvWriter: foobar must be a CSV file with file extension .csv'):
        atomiccsvwriter__base_type__('foobar | headers="foo,bar"')

    (grizzly_fixture.test_context / 'requests' / 'foobar.csv').touch()

    with pytest.raises(ValueError, match='AtomicCsvWriter: foobar.csv already exists, remove existing file or add argument overwrite=True'):
        atomiccsvwriter__base_type__('foobar.csv | headers="foo,bar"')

    atomiccsvwriter__base_type__('foobar.csv | headers="foo,bar", overwrite=True')

    atomiccsvwriter__base_type__('foobaz.csv | headers="foo,baz"')


def test_atomiccsvwriter_message_handler(grizzly_fixture: GrizzlyFixture) -> None:
    try:
        parent = grizzly_fixture()

        destination_file = grizzly_fixture.test_context / 'requests' / 'foobar.csv'

        assert not destination_file.exists()

        message = Message(
            'atomiccsvwriter',
            data={
                'destination': 'foobar.csv',
                'row': {
                    'foo': 'hello',
                    'bar': 'world!',
                },
            },
            node_id=None,
        )

        atomiccsvwriter_message_handler(parent.user.environment, message)

        assert destination_file.exists()
        assert destination_file.read_text() == 'foo,bar\nhello,world!\n'

        message = Message(
            'atomiccsvwriter',
            data={
                'destination': 'foobar.csv',
                'row': {
                    'foo': 'bar',
                    'bar': 'foo',
                },
            },
            node_id=None,
        )

        atomiccsvwriter_message_handler(parent.user.environment, message)

        assert destination_file.read_text() == 'foo,bar\nhello,world!\nbar,foo\n'
    finally:
        for open_file in open_files.values():
            with suppress(Exception):
                open_file.close()


class TestAtomicCsvWriter:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))
        try:
            with pytest.raises(ValueError, match='AtomicCsvWriter.output.foo is not a valid CSV destination name, must be: AtomicCsvWriter.<name>'):
                AtomicCsvWriter(scenario=scenario1, variable='output.foo', value='foobar')

            t = AtomicCsvWriter(scenario=scenario1, variable='output', value='foobar.csv | headers="foo,bar"')

            assert t._settings == {'output': {'headers': ['foo', 'bar'], 'destination': 'foobar.csv', 'overwrite': False}}

            u = AtomicCsvWriter(scenario=scenario2, variable='foobar', value='output.csv | headers="bar,foo", overwrite=True')

            assert u is not t

            u = AtomicCsvWriter(scenario=scenario1, variable='foobar', value='output.csv | headers="bar,foo", overwrite=True')

            assert u is t

            assert t._settings == {
                'output': {
                    'headers': ['foo', 'bar'],
                    'destination': 'foobar.csv',
                    'overwrite': False,
                },
                'foobar': {
                    'headers': ['bar', 'foo'],
                    'destination': 'output.csv',
                    'overwrite': True,
                },
            }
        finally:
            cleanup()

    def test_clear(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            u = AtomicCsvWriter(scenario=scenario1, variable='foobar', value='output.csv | headers="bar,foo", overwrite=True')
            assert len(u._settings) == 1

            t = AtomicCsvWriter(scenario=scenario1, variable='output', value='foobar.csv | headers="foo,bar"')
            assert len(t._settings) == 2

            v = AtomicCsvWriter(scenario=scenario2, variable='output', value='output.csv | headers="foz,baz"')
            assert len(v._settings) == 1

            AtomicCsvWriter.clear()

            assert len(t._settings) == 0
            assert len(v._settings) == 0
        finally:
            cleanup()

    def test___getitem__(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        try:
            u = AtomicCsvWriter(scenario=grizzly.scenario, variable='foobar', value='output.csv | headers="bar,foo", overwrite=True')

            with pytest.raises(NotImplementedError, match='AtomicCsvWriter has not implemented "__getitem__"'):
                _ = u['foobar']
        finally:
            cleanup()

    def test___setitem__(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture, mocker: MockerFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            send_message_mock = mocker.patch.object(grizzly_fixture.grizzly.state.locust, 'send_message', return_value=None)

            t = AtomicCsvWriter(scenario=scenario1, variable='output', value='output.csv | headers="foo,bar"')

            assert not hasattr(t, '_buffer')

            with pytest.raises(ValueError, match='AtomicCsvWriter.world is not a valid reference'):
                t['world'] = 'hello'

            with pytest.raises(ValueError, match=r'AtomicCsvWriter.output: less values \(1\) than headers \(2\)'):
                t['output'] = 'hello'

            with pytest.raises(ValueError, match=r'AtomicCsvWriter.output: more values \(3\) than headers \(2\)'):
                t['output'] = 'hello,world,foo'

            t['output'] = 'hello, world'

            send_message_mock.assert_called_once_with(
                'atomiccsvwriter',
                {
                    'rid': ANYUUID(version=4),
                    'destination': 'output.csv',
                    'row': {'foo': 'hello', 'bar': 'world'},
                },
            )
            send_message_mock.reset_mock()

            with pytest.raises(ValueError, match='AtomicCsvWriter.output.foo is not a valid reference'):
                t['output.foo'] = 'world'

            send_message_mock.assert_not_called()

            t = AtomicCsvWriter(scenario=scenario2, variable='output', value='output.csv | headers="foo,bar"')
            t['output'] = 'world, hello'

            send_message_mock.assert_called_once_with(
                'atomiccsvwriter',
                {
                    'rid': ANYUUID(version=4),
                    'destination': 'output.csv',
                    'row': {'foo': 'world', 'bar': 'hello'},
                },
            )
            send_message_mock.reset_mock()
        finally:
            cleanup()
