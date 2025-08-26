"""Tests for grizzly_cli.argparse.bashcompletion.types."""

from __future__ import annotations

from argparse import ArgumentTypeError
from os import sep
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.argparse.bashcompletion.types import BashCompletionTypes

from test_cli.helpers import cwd, rm_rf

if TYPE_CHECKING:
    from _pytest.tmpdir import TempPathFactory


class TestBashCompletionTypes:
    class TestFile:
        def test___init__(self) -> None:
            assert BashCompletionTypes.File('*.txt').patterns == ['*.txt']
            assert BashCompletionTypes.File('*.txt', '*.json').patterns == ['*.txt', '*.json']
            assert BashCompletionTypes.File('*.txt', '*.json', '*.xml').patterns == ['*.txt', '*.json', '*.xml']

        def test___call__(self, tmp_path_factory: TempPathFactory) -> None:
            test_context = tmp_path_factory.mktemp('test_context')
            test_dir = test_context / 'test-dir'
            test_dir.mkdir()
            file = test_context / 'test.txt'
            file.touch()
            file.write_text('test.txt file')

            file = test_context / 'test.json'
            file.touch()
            file.write_text('{"value": "test.json file"}')

            file = test_context / 'test.xml'
            file.touch()
            file.write_text('<value>test.xml file</value>')

            try:
                with cwd(test_context):
                    impl = BashCompletionTypes.File('*.txt')

                    with pytest.raises(ArgumentTypeError) as ate:
                        impl('non-existing-directory/')
                    assert 'non-existing-directory/ does not exist' in str(ate)

                    with pytest.raises(ArgumentTypeError) as ate:
                        impl('test-dir/')
                    assert 'test-dir/ is not a file' in str(ate)

                    with pytest.raises(ArgumentTypeError) as ate:
                        impl('test.xml')
                    assert 'test.xml does not match *.txt' in str(ate)

                    assert impl('test.txt') == 'test.txt'

                    impl = BashCompletionTypes.File('*.txt', '*.json')

                    with pytest.raises(ArgumentTypeError) as ate:
                        impl('test.xml')
                    assert 'test.xml does not match *.txt' in str(ate)

                    assert impl('test.txt') == 'test.txt'
                    assert impl('test.json') == 'test.json'
            finally:
                rm_rf(test_context)

        def test_list_files(self, tmp_path_factory: TempPathFactory) -> None:
            test_context = tmp_path_factory.mktemp('test_context')
            (test_context / 'test.txt').write_text('test.txt file')
            (test_context / 'test.json').write_text('{"value": "test.json file"}')
            (test_context / 'test.xml').write_text('<value>test.xml file</value>')

            test_dir = test_context / 'test-dir'
            test_dir.mkdir()
            (test_dir / 'test.txt').write_text('sub test.txt file')

            hidden_dir = test_context / '.hidden'
            hidden_dir.mkdir()
            (hidden_dir / 'hidden.txt').write_text('hidden.txt file')
            try:
                with cwd(test_context):
                    impl = BashCompletionTypes.File('*.txt')
                    assert impl.list_files(None) == {
                        'test.txt': 'file',
                        'test-dir': 'dir',
                    }
                    assert impl.list_files('te') == {
                        'test.txt': 'file',
                        'test-dir': 'dir',
                    }

                    assert impl.list_files('test-') == {
                        'test-dir': 'dir',
                    }

                    assert impl.list_files(f'test-dir{sep}') == {
                        f'test-dir{sep}test.txt': 'file',
                    }

                    impl = BashCompletionTypes.File('*.txt', '*.json', '*.xml')
                    assert impl.list_files('te') == {
                        'test.txt': 'file',
                        'test.json': 'file',
                        'test.xml': 'file',
                        'test-dir': 'dir',
                    }
            finally:
                rm_rf(test_context)
