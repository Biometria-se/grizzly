"""Unit tests of grizzly.testdata.variables.directory_contents."""

from __future__ import annotations

from contextlib import suppress
from os import sep
from typing import TYPE_CHECKING

import pytest
from grizzly.testdata.variables import AtomicDirectoryContents
from grizzly.testdata.variables.directory_contents import atomicdirectorycontents__base_type__

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


def test_atomicdirectorycontents__base_type__(grizzly_fixture: GrizzlyFixture) -> None:
    test_context = grizzly_fixture.test_context / 'requests'
    test_context.mkdir(exist_ok=True)

    test_file = test_context / 'test.txt'
    test_file.touch()
    test_file.write_text('\n')

    with pytest.raises(ValueError, match='is not a directory in'):
        atomicdirectorycontents__base_type__('test.txt')

    with pytest.raises(ValueError, match='is not a directory in'):
        atomicdirectorycontents__base_type__('non-existing-directory')

    (test_context / 'a-directory').mkdir()

    atomicdirectorycontents__base_type__('a-directory')

    with pytest.raises(ValueError, match='argument invalidarg is not allowed'):
        atomicdirectorycontents__base_type__('a-directory | invalidarg=True')

    assert atomicdirectorycontents__base_type__('a-directory|random=True') == 'a-directory | random=True'


class TestAtomicDirectoryContents:
    def test_variable(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:  # noqa: PLR0915
        test_context = grizzly_fixture.test_context / 'requests' / 'test_variable'
        test_context.mkdir(exist_ok=True)

        for directory in ['1-test', '2-test', '3-test']:
            (test_context / directory).mkdir()
            for file in ['1-test.json', '2-test.json', '3-test.json']:
                (test_context / directory / file).touch()

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            instances = [
                AtomicDirectoryContents(scenario=scenario1, variable='blobfiles', value='test_variable/1-test/'),
                AtomicDirectoryContents(scenario=scenario2, variable='blobfiles', value='test_variable/1-test/'),
            ]

            for instance in instances:
                with pytest.raises(NotImplementedError, match='AtomicDirectoryContents has not implemented "__setitem__"'):
                    instance['blobfiles'] = None

                assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}1-test.json'
                assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}2-test.json'
                assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}3-test.json'
                assert instance.__getitem__('blobfiles') is None

                del instance['blobfiles']
                del instance['blobfiles']

            instance.destroy()

            instance = AtomicDirectoryContents(scenario=scenario1, variable='blobfiles2', value='test_variable/2-test/')
            instance = AtomicDirectoryContents(scenario=scenario1, variable='blobfiles3', value='test_variable/3-test/')

            assert instance['blobfiles2'] == f'test_variable{sep}2-test{sep}1-test.json'
            assert instance['blobfiles3'] == f'test_variable{sep}3-test{sep}1-test.json'
            assert instance['blobfiles2'] == f'test_variable{sep}2-test{sep}2-test.json'
            assert instance['blobfiles3'] == f'test_variable{sep}3-test{sep}2-test.json'
            assert instance['blobfiles2'] == f'test_variable{sep}2-test{sep}3-test.json'
            assert instance['blobfiles3'] == f'test_variable{sep}3-test{sep}3-test.json'
            assert instance.__getitem__('blobfiles2') is None
            assert instance.__getitem__('blobfiles2') is None
            assert instance.__getitem__('blobfiles3') is None

            instance = AtomicDirectoryContents(scenario=scenario2, variable='blobfiles', value='test_variable/')

            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}3-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}2-test{sep}1-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}2-test{sep}2-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}2-test{sep}3-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}3-test{sep}1-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}3-test{sep}2-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}3-test{sep}3-test.json'
            assert instance.__getitem__('blobfiles') is None

            del instance['blobfiles']

            with pytest.raises(ValueError, match='asdf is not a valid boolean'):
                AtomicDirectoryContents(scenario=scenario1, variable='blobfiles', value='test_variable/1-test/ | repeat=asdf')

            with pytest.raises(ValueError, match='argument prefix is not allowed'):
                AtomicDirectoryContents(scenario=scenario1, variable='blobfiles', value='test_variable/1-test/ | repeat=True, prefix="test-"')

            instance = AtomicDirectoryContents(scenario=scenario2, variable='blobfiles', value='test_variable/1-test/ | repeat=True')

            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}3-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}3-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'test_variable{sep}1-test{sep}3-test.json'

            instance = AtomicDirectoryContents(scenario=scenario1, variable='random', value='test_variable/1-test/ | random=True')

            exepected = [
                f'test_variable{sep}1-test{sep}1-test.json',
                f'test_variable{sep}1-test{sep}2-test.json',
                f'test_variable{sep}1-test{sep}3-test.json',
            ]

            assert instance['random'] in exepected
            assert instance['random'] in exepected
            assert instance['random'] in exepected
            assert instance.__getitem__('random') is None

            instance = AtomicDirectoryContents(scenario=scenario2, variable='randomrepeat', value='test_variable/1-test/ | random=True, repeat=True')

            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
            assert instance['randomrepeat'] in exepected
        finally:
            cleanup()

    def test_clear_and_destroy(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        test_context = grizzly_fixture.test_context / 'requests' / 'test_clear_and_destory'
        test_context.mkdir(exist_ok=True)

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            with suppress(Exception):
                AtomicDirectoryContents.destroy()

            with pytest.raises(ValueError, match='is not instantiated'):
                AtomicDirectoryContents.destroy()

            with pytest.raises(ValueError, match='is not instantiated'):
                AtomicDirectoryContents.clear()

            instances = [
                AtomicDirectoryContents(scenario=scenario1, variable='test', value='test_clear_and_destory/'),
                AtomicDirectoryContents(scenario=scenario2, variable='test', value='test_clear_and_destory/'),
            ]

            for instance in instances:
                assert instance.__getitem__('test') is None

                assert len(instance._values.keys()) == 1
                assert len(instance._files.keys()) == 1

            AtomicDirectoryContents.clear()

            for instance in instances:
                assert len(instance._values.keys()) == 0
                assert len(instance._files.keys()) == 0

            AtomicDirectoryContents.destroy()
        finally:
            cleanup()
