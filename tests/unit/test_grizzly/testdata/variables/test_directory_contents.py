import os
import shutil

import pytest

from _pytest.tmpdir import TempPathFactory

from grizzly.testdata.variables import AtomicDirectoryContents
from grizzly.testdata.variables.directory_contents import atomicdirectorycontents__base_type__

from tests.fixtures import AtomicVariableCleanupFixture


def test_atomicdirectorycontents__base_type__(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context.mkdir()
    test_context_root = os.path.dirname(test_context)

    try:
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        test_file = test_context / 'test.txt'
        test_file.touch()
        test_file.write_text('\n')

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
    def test(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = os.path.dirname(test_context)

        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        for directory in ['1-test', '2-test', '3-test']:
            os.mkdir(os.path.join(test_context, directory))
            for file in ['1-test.json', '2-test.json', '3-test.json']:
                with open(os.path.join(test_context, directory, file), 'w') as fd:
                    fd.write('')

        try:
            instance = AtomicDirectoryContents('blobfiles', '1-test/')

            with pytest.raises(NotImplementedError) as nie:
                instance['blobfiles'] = None
            assert str(nie.value) == 'AtomicDirectoryContents has not implemented "__setitem__"'

            assert instance['blobfiles'] == f'1-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}3-test.json'
            assert instance.__getitem__('blobfiles') is None

            del instance['blobfiles']
            del instance['blobfiles']

            instance.destroy()

            instance = AtomicDirectoryContents('blobfiles2', '2-test/')
            instance = AtomicDirectoryContents('blobfiles3', '3-test/')

            assert instance['blobfiles2'] == f'2-test{os.sep}1-test.json'
            assert instance['blobfiles3'] == f'3-test{os.sep}1-test.json'
            assert instance['blobfiles2'] == f'2-test{os.sep}2-test.json'
            assert instance['blobfiles3'] == f'3-test{os.sep}2-test.json'
            assert instance['blobfiles2'] == f'2-test{os.sep}3-test.json'
            assert instance['blobfiles3'] == f'3-test{os.sep}3-test.json'
            assert instance.__getitem__('blobfiles2') is None
            assert instance.__getitem__('blobfiles2') is None
            assert instance.__getitem__('blobfiles3') is None

            instance = AtomicDirectoryContents('blobfiles', '.')

            assert instance['blobfiles'] == f'1-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}3-test.json'
            assert instance['blobfiles'] == f'2-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'2-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'2-test{os.sep}3-test.json'
            assert instance['blobfiles'] == f'3-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'3-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'3-test{os.sep}3-test.json'
            assert instance.__getitem__('blobfiles') is None

            del instance['blobfiles']

            with pytest.raises(ValueError):
                AtomicDirectoryContents('blobfiles', '1-test/ | repeat=asdf')

            with pytest.raises(ValueError):
                AtomicDirectoryContents('blobfiles', '1-test/ | repeat=True, prefix="test-"')

            instance = AtomicDirectoryContents('blobfiles', '1-test/ | repeat=True')

            assert instance['blobfiles'] == f'1-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}3-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}3-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}1-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}2-test.json'
            assert instance['blobfiles'] == f'1-test{os.sep}3-test.json'

            instance = AtomicDirectoryContents('random', '1-test/ | random=True')

            assert instance['random'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['random'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['random'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance.__getitem__('random') is None

            instance = AtomicDirectoryContents('randomrepeat', '1-test/ | random=True, repeat=True')

            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
            assert instance['randomrepeat'] in [
                f'1-test{os.sep}1-test.json',
                f'1-test{os.sep}2-test.json',
                f'1-test{os.sep}3-test.json',
            ]
        finally:
            shutil.rmtree(test_context_root)

            try:
                del os.environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            cleanup()

    def test_clear_and_destroy(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
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

            assert instance.__getitem__('test') is None

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
