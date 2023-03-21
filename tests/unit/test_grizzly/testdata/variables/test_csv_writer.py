import pytest

from grizzly.testdata.variables.csv_writer import atomiccsvwriter__base_type__, atomiccsvwriter_message_handler, AtomicCsvWriter
from grizzly.types.locust import Message

from tests.fixtures import GrizzlyFixture, MockerFixture, AtomicVariableCleanupFixture


def test_atomiccsvwriter__base_type__(grizzly_fixture: GrizzlyFixture) -> None:
    with pytest.raises(ValueError) as ve:
        atomiccsvwriter__base_type__('foobar')
    assert str(ve.value) == 'AtomicCsvWriter: arguments are required'

    with pytest.raises(ValueError) as ve:
        atomiccsvwriter__base_type__('foobar |')
    assert str(ve.value) == 'AtomicCsvWriter: incorrect format in arguments: ""'

    with pytest.raises(ValueError) as ve:
        atomiccsvwriter__base_type__('foobar | foo="bar"')
    assert str(ve.value) == 'AtomicCsvWriter: argument foo is not allowed'

    with pytest.raises(ValueError) as ve:
        atomiccsvwriter__base_type__('foobar | overwrite=True')
    assert str(ve.value) == 'AtomicCsvWriter: argument headers is required'

    with pytest.raises(ValueError) as ve:
        atomiccsvwriter__base_type__('foobar | headers="foo,bar"')
    assert str(ve.value) == 'AtomicCsvWriter: foobar must be a CSV file with file extension .csv'

    (grizzly_fixture.test_context / 'foobar.csv').touch()

    with pytest.raises(ValueError) as ve:
        atomiccsvwriter__base_type__('foobar.csv | headers="foo,bar"')
    assert str(ve.value) == 'AtomicCsvWriter: foobar.csv already exists, remove or add argument overwrite=True'

    atomiccsvwriter__base_type__('foobar.csv | headers="foo,bar", overwrite=True')

    atomiccsvwriter__base_type__('foobaz.csv | headers="foo,baz"')


def test_atomiccsvwriter_message_handler(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    env, _, _ = grizzly_fixture()

    destination_file = grizzly_fixture.test_context / 'foobar.csv'

    assert not destination_file.exists()

    message = Message('atomiccsvwriter', data={
        'destination': 'foobar.csv',
        'row': {
            'foo': 'hello',
            'bar': 'world!'
        }
    }, node_id=None)

    atomiccsvwriter_message_handler(env, message)

    assert destination_file.exists()
    assert destination_file.read_text() == 'foo,bar\nhello,world!\n'

    message = Message('atomiccsvwriter', data={
        'destination': 'foobar.csv',
        'row': {
            'foo': 'bar',
            'bar': 'foo'
        }
    }, node_id=None)

    atomiccsvwriter_message_handler(env, message)

    assert destination_file.read_text() == 'foo,bar\nhello,world!\nbar,foo\n'


class TestAtomicCsvWriter:
    def test___init__(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            with pytest.raises(ValueError) as ve:
                AtomicCsvWriter('output.foo', 'foobar')
            assert str(ve.value) == 'AtomicCsvWriter.output.foo is not a valid CSV destination name, must be: AtomicCsvWriter.<name>'

            t = AtomicCsvWriter('output', 'foobar.csv | headers="foo,bar"')

            assert t._settings == {'output': {'headers': ['foo', 'bar'], 'destination': 'foobar.csv', 'overwrite': False}}
            assert t._buffer == {'output': {}}

            u = AtomicCsvWriter('foobar', 'output.csv | headers="bar,foo", overwrite=True')

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
            assert t._buffer == {'output': {}, 'foobar': {}}
        finally:
            cleanup()

    def test_clear(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            u = AtomicCsvWriter('foobar', 'output.csv | headers="bar,foo", overwrite=True')
            assert len(u._settings) == 1
            assert len(u._buffer) == 1

            t = AtomicCsvWriter('output', 'foobar.csv | headers="foo,bar"')
            assert len(t._settings) == 2
            assert len(t._buffer) == 2

            AtomicCsvWriter.clear()

            assert len(t._settings) == 0
            assert len(t._buffer) == 0
        finally:
            cleanup()

    def test___getitem__(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            u = AtomicCsvWriter('foobar', 'output.csv | headers="bar,foo", overwrite=True')

            with pytest.raises(NotImplementedError) as nie:
                _ = u['foobar']
            assert str(nie.value) == 'AtomicCsvWriter has not implemented "__getitem__"'
        finally:
            cleanup()

    def test___setitem__(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture, mocker: MockerFixture) -> None:
        send_message_mock = mocker.patch.object(grizzly_fixture.grizzly.state.locust, 'send_message', return_value=None)

        t = AtomicCsvWriter('output', 'output.csv | headers="foo,bar"')

        assert t._buffer == {'output': {}}

        with pytest.raises(ValueError) as ve:
            t['output'] = 'hello'
        assert str(ve.value) == 'AtomicCsvWriter.output is not a valid reference'

        with pytest.raises(ValueError) as ve:
            t['output.baz'] = 'hello'
        assert str(ve.value) == 'AtomicCsvWriter.output has not specified header "baz"'

        t['output.foo'] = 'hello'

        send_message_mock.assert_not_called()
        assert t._buffer == {'output': {'foo': 'hello'}}

        with pytest.raises(ValueError) as ve:
            t['output.foo'] = 'world'
        assert str(ve.value) == 'AtomicCsvWriter.output has already set value for "foo" in current row'

        send_message_mock.assert_not_called()
        assert t._buffer == {'output': {'foo': 'hello'}}

        t['output.bar'] = 'world!'

        send_message_mock.assert_called_once_with('atomiccsvwriter', {
            'destination': 'output.csv',
            'row': {'foo': 'hello', 'bar': 'world!'}
        })
        assert t._buffer == {'output': {}}

        # pass through
        t['output'] = None
