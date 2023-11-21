"""Unit tests of grizzly.testdata.variables.directory_contents."""
from __future__ import annotations

from contextlib import suppress
from os import environ, sep
from shutil import rmtree
from typing import TYPE_CHECKING

import pytest

from grizzly.testdata.variables import AtomicDirectoryContents
from grizzly.testdata.variables.directory_contents import atomicdirectorycontents__base_type__

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory

    from tests.fixtures import AtomicVariableCleanupFixture


def test_atomicdirectorycontents__base_type__(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context.mkdir()
    test_context_root = test_context.parent.as_posix()

    try:
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

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
    finally:
        rmtree(test_context_root)

        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']


class TestAtomicDirectoryContents:
    def test_variable(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = test_context.parent.as_posix()

        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        for directory in ['1-test', '2-test', '3-test']:
            (test_context / directory).mkdir()
            for file in ['1-test.json', '2-test.json', '3-test.json']:
                (test_context / directory / file).touch()

        try:
            instance = AtomicDirectoryContents('blobfiles', '1-test/')

            with pytest.raises(NotImplementedError, match='AtomicDirectoryContents has not implemented "__setitem__"'):
                instance['blobfiles'] = None

            assert instance['blobfiles'] == f'1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{sep}3-test.json'
            assert instance.__getitem__('blobfiles') is None

            del instance['blobfiles']
            del instance['blobfiles']

            instance.destroy()

            instance = AtomicDirectoryContents('blobfiles2', '2-test/')
            instance = AtomicDirectoryContents('blobfiles3', '3-test/')

            assert instance['blobfiles2'] == f'2-test{sep}1-test.json'
            assert instance['blobfiles3'] == f'3-test{sep}1-test.json'
            assert instance['blobfiles2'] == f'2-test{sep}2-test.json'
            assert instance['blobfiles3'] == f'3-test{sep}2-test.json'
            assert instance['blobfiles2'] == f'2-test{sep}3-test.json'
            assert instance['blobfiles3'] == f'3-test{sep}3-test.json'
            assert instance.__getitem__('blobfiles2') is None
            assert instance.__getitem__('blobfiles2') is None
            assert instance.__getitem__('blobfiles3') is None

            instance = AtomicDirectoryContents('blobfiles', '.')

            assert instance['blobfiles'] == f'1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{sep}3-test.json'
            assert instance['blobfiles'] == f'2-test{sep}1-test.json'
            assert instance['blobfiles'] == f'2-test{sep}2-test.json'
            assert instance['blobfiles'] == f'2-test{sep}3-test.json'
            assert instance['blobfiles'] == f'3-test{sep}1-test.json'
            assert instance['blobfiles'] == f'3-test{sep}2-test.json'
            assert instance['blobfiles'] == f'3-test{sep}3-test.json'
            assert instance.__getitem__('blobfiles') is None

            del instance['blobfiles']

            with pytest.raises(ValueError, match='asdf is not a valid boolean'):
                AtomicDirectoryContents('blobfiles', '1-test/ | repeat=asdf')

            with pytest.raises(ValueError, match='argument prefix is not allowed'):
                AtomicDirectoryContents('blobfiles', '1-test/ | repeat=True, prefix="test-"')

            instance = AtomicDirectoryContents('blobfiles', '1-test/ | repeat=True')

            assert instance['blobfiles'] == f'1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{sep}3-test.json'
            assert instance['blobfiles'] == f'1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{sep}3-test.json'
            assert instance['blobfiles'] == f'1-test{sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{sep}3-test.json'

            instance = AtomicDirectoryContents('random', '1-test/ | random=True')

            assert instance['random'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['random'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['random'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance.__getitem__('random') is None

            instance = AtomicDirectoryContents('randomrepeat', '1-test/ | random=True, repeat=True')

            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{sep}1-test.json',
                f'1-test{sep}2-test.json',
                f'1-test{sep}3-test.json',
            ]
        finally:
            rmtree(test_context_root)

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

            cleanup()

    def test_clear_and_destroy(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = test_context.parent.as_posix()

        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            with suppress(Exception):
                AtomicDirectoryContents.destroy()

            with pytest.raises(ValueError, match='is not instantiated'):
                AtomicDirectoryContents.destroy()

            with pytest.raises(ValueError, match='is not instantiated'):
                AtomicDirectoryContents.clear()

            instance = AtomicDirectoryContents('test', '.')

            assert instance.__getitem__('test') is None

            assert len(instance._values.keys()) == 1
            assert len(instance._files.keys()) == 1

            AtomicDirectoryContents.clear()

            assert len(instance._values.keys()) == 0
            assert len(instance._files.keys()) == 0

            AtomicDirectoryContents.destroy()
        finally:
            rmtree(test_context_root)

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

            cleanup()
