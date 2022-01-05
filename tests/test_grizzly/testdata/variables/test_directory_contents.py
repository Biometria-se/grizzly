import os
import shutil

from typing import Callable

import pytest

from _pytest.tmpdir import TempdirFactory

from grizzly.testdata.variables import AtomicDirectoryContents
from grizzly.testdata.variables.directory_contents import atomicdirectorycontents__base_type__

from ..fixtures import cleanup  # pylint: disable=unused-import


def test_atomicdirectorycontents__base_type__(tmpdir_factory: TempdirFactory) -> None:
    test_context = tmpdir_factory.mktemp('test_context').mkdir('requests')
    test_context_root = os.path.dirname(str(test_context))

    try:
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        test_file = test_context.join('test.txt')
        test_file.write('\n')

        with pytest.raises(ValueError) as ve:
            atomicdirectorycontents__base_type__('test.txt')
        assert 'is not a directory in' in str(ve)

        with pytest.raises(ValueError) as ve:
            atomicdirectorycontents__base_type__('non-existing-directory')
        assert 'is not a directory in' in str(ve)

        os.mkdir(os.path.join(str(test_context), 'a-directory'))

        atomicdirectorycontents__base_type__('a-directory')

        with pytest.raises(ValueError) as ve:
            atomicdirectorycontents__base_type__('a-directory | invalidarg=True')
        assert 'argument invalidarg is not allowed' in str(ve)

        assert atomicdirectorycontents__base_type__('a-directory|random=True') == 'a-directory | random=True'
    finally:
        shutil.rmtree(test_context_root)

        try:
            del os.environ['GRIZZLY_CONTEXT_ROOT']
        except:
            pass

class TestAtomicDirectoryContents:
    @pytest.mark.usefixtures('cleanup')
    def test(self, cleanup: Callable, tmpdir_factory: TempdirFactory) -> None:
        test_context = str(tmpdir_factory.mktemp('test_context').mkdir('requests'))
        test_context_root = os.path.dirname(test_context)

        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        for directory in ['1-test', '2-test', '3-test']:
            os.mkdir(os.path.join(test_context, directory))
            for file in ['1-test.json', '2-test.json', '3-test.json']:
                with open(os.path.join(test_context, directory, file), 'w') as fd:
                    fd.write('')

        try:
            instance = AtomicDirectoryContents('blobfiles', '1-test/')

            instance['blobfiles'] = None

            assert instance['blobfiles'] == '1-test/1-test.json'
            assert instance['blobfiles'] == '1-test/2-test.json'
            assert instance['blobfiles'] == '1-test/3-test.json'
            assert instance['blobfiles'] == None

            del instance['blobfiles']
            del instance['blobfiles']

            instance.destroy()

            instance = AtomicDirectoryContents('blobfiles2', '2-test/')
            instance = AtomicDirectoryContents('blobfiles3', '3-test/')

            assert instance['blobfiles2'] == '2-test/1-test.json'
            assert instance['blobfiles3'] == '3-test/1-test.json'
            assert instance['blobfiles2'] == '2-test/2-test.json'
            assert instance['blobfiles3'] == '3-test/2-test.json'
            assert instance['blobfiles2'] == '2-test/3-test.json'
            assert instance['blobfiles3'] == '3-test/3-test.json'
            assert instance['blobfiles2'] == None
            assert instance['blobfiles2'] == None
            assert instance['blobfiles3'] == None

            instance = AtomicDirectoryContents('blobfiles', '.')

            assert instance['blobfiles'] == '1-test/1-test.json'
            assert instance['blobfiles'] == '1-test/2-test.json'
            assert instance['blobfiles'] == '1-test/3-test.json'
            assert instance['blobfiles'] == '2-test/1-test.json'
            assert instance['blobfiles'] == '2-test/2-test.json'
            assert instance['blobfiles'] == '2-test/3-test.json'
            assert instance['blobfiles'] == '3-test/1-test.json'
            assert instance['blobfiles'] == '3-test/2-test.json'
            assert instance['blobfiles'] == '3-test/3-test.json'
            assert instance['blobfiles'] == None

            del instance['blobfiles']

            with pytest.raises(ValueError):
                AtomicDirectoryContents('blobfiles', '1-test/ | repeat=asdf')

            with pytest.raises(ValueError):
                AtomicDirectoryContents('blobfiles', '1-test/ | repeat=True, prefix="test-"')

            instance = AtomicDirectoryContents('blobfiles', '1-test/ | repeat=True')

            assert instance['blobfiles'] == '1-test/1-test.json'
            assert instance['blobfiles'] == '1-test/2-test.json'
            assert instance['blobfiles'] == '1-test/3-test.json'
            assert instance['blobfiles'] == '1-test/1-test.json'
            assert instance['blobfiles'] == '1-test/2-test.json'
            assert instance['blobfiles'] == '1-test/3-test.json'
            assert instance['blobfiles'] == '1-test/1-test.json'
            assert instance['blobfiles'] == '1-test/2-test.json'
            assert instance['blobfiles'] == '1-test/3-test.json'

            instance = AtomicDirectoryContents('random', '1-test/ | random=True')

            assert instance['random'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['random'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['random'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['random'] == None

            instance = AtomicDirectoryContents('randomrepeat', '1-test/ | random=True, repeat=True')

            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
            assert instance['randomrepeat'] in [
                '1-test/1-test.json',
                '1-test/2-test.json',
                '1-test/3-test.json',
            ]
        finally:
            shutil.rmtree(test_context_root)

            try:
                del os.environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_clear_and_destroy(self, cleanup: Callable, tmpdir_factory: TempdirFactory) -> None:
        test_context = str(tmpdir_factory.mktemp('test_context').mkdir('requests'))
        test_context_root = os.path.dirname(test_context)

        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        try:
            try:
                AtomicDirectoryContents.destroy()
            except Exception:
                pass

            with pytest.raises(ValueError):
                AtomicDirectoryContents.destroy()

            with pytest.raises(ValueError):
                AtomicDirectoryContents.clear()

            instance = AtomicDirectoryContents('test', '.')

            assert instance['test'] == None

            assert len(instance._values.keys()) == 1
            assert len(instance._files.keys()) == 1

            AtomicDirectoryContents.clear()

            assert len(instance._values.keys()) == 0
            assert len(instance._files.keys()) == 0

            AtomicDirectoryContents.destroy()
        finally:
            shutil.rmtree(test_context_root)

            try:
                del os.environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass
            cleanup()
