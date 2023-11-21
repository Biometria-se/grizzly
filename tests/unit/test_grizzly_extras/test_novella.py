"""Unit tests of grizzly_extras.novella."""
from __future__ import annotations

from shutil import rmtree
from typing import Dict, List, Union

import pytest

from grizzly_extras.novella import (
    NO_CHILD,
    GrizzlyMarkdown,
    MarkdownAstNode,
    MarkdownAstType,
    MarkdownHeading,
    _create_nav_node,
    _generate_dynamic_page,
    make_human_readable,
)


class TestMarkdownAstType:
    def test_from_value(self) -> None:
        assert MarkdownAstType.from_value('text') == MarkdownAstType.TEXT
        assert MarkdownAstType.from_value('emphasis') == MarkdownAstType.EMPHASIS
        assert MarkdownAstType.from_value(None) == MarkdownAstType.NONE

        with pytest.raises(ValueError, match='"foobar" is not a valid value of MarkdownAstType'):
            MarkdownAstType.from_value('foobar')


class TestMarkdownAstNode:
    def test_first_child(self) -> None:
        # <!-- no children
        node = MarkdownAstNode({}, 0)

        assert node.first_child.ast == NO_CHILD
        assert node.first_child.type == MarkdownAstType.NONE
        # -->

        # <!-- first child is BLANK_LINE
        node = MarkdownAstNode({'children': [{'type': MarkdownAstType.BLANK_LINE.value}, {'type': MarkdownAstType.TEXT.value, 'raw': 'foobar'}]}, 0)

        assert node.first_child.type == MarkdownAstType.TEXT
        assert node.first_child.raw == 'foobar'
        # -->

    def test_get_child(self) -> None:
        # <!-- no children
        node = MarkdownAstNode({}, 0)
        with pytest.raises(IndexError):
            node.get_child(10)
        # -->

        # <!-- 5th node text
        node = MarkdownAstNode({'children': ([{}] * 4) + [{'type': 'text', 'raw': 'foobar'}]}, 0)
        child_node = node.get_child(4)

        assert child_node.type == MarkdownAstType.TEXT
        assert child_node.raw == 'foobar'
        # -->

    @pytest.mark.parametrize('test_type', list(MarkdownAstType))
    def test_type(self, test_type: MarkdownAstType) -> None:
        node = MarkdownAstNode({'type': test_type.value}, 13)
        assert node.type == test_type

    @pytest.mark.parametrize('raw', ['foobar', None, 'hello world'])
    def test_raw(self, raw: str) -> None:
        node = MarkdownAstNode({'raw': raw}, 37)
        assert node.raw == raw


def test_markdown_heading() -> None:
    header = MarkdownHeading('foobar', 10)

    assert header.text == 'foobar'
    assert header.level == 10


def test_make_human_readable() -> None:
    assert make_human_readable('foo_bar_Http') == 'Foo Bar HTTP'
    assert make_human_readable('Iot_Api_hub_hello_world_foo_bar') == 'IoT API Hub Hello World Foo Bar'


def test__create_nav_node(tmp_path_factory: pytest.TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')
    try:
        target: List[Union[str, Dict[str, str]]] = []
        # <!-- not a file
        node = test_context
        _create_nav_node(target, 'foobar', node)
        assert target == []
        # -->

        # <!-- normal file
        node = test_context / 'foobar_sftp_api.py'
        node.touch()
        _create_nav_node(target, 'foobar', node)
        assert target == [{'Foobar SFTP API': 'foobar/foobar_sftp_api.md'}]
        # -->

        # <!-- __init__, no index
        node = test_context / '__init__.py'
        node.touch()
        _create_nav_node(target, 'foobar', node, with_index=False)
        assert target == [{'Foobar SFTP API': 'foobar/foobar_sftp_api.md'}]
        # -->

        # <!-- __init__, no index
        _create_nav_node(target, 'hello', node)
        assert target == ['hello/index.md', {'Foobar SFTP API': 'foobar/foobar_sftp_api.md'}]
        # -->
    finally:
        rmtree(test_context)


def test__generate_dynamic_page(tmp_path_factory: pytest.TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')
    test_input = test_context / 'input'
    test_input.mkdir()
    test_output = test_context / 'output'
    test_output.mkdir()

    try:
        # <!-- not a file
        _generate_dynamic_page(test_input, test_output, 'foobar', 'foo.bar')
        assert len(list(test_output.rglob('**/*'))) == 0
        # -->

        # <!-- normal file
        input_file = test_input / 'foobar_sftp_api.py'
        input_file.touch()
        _generate_dynamic_page(input_file, test_output / 'foobar', 'Foobar', 'foo.bar')
        assert len(list(test_output.rglob('**/*'))) != 0
        output_file = test_output / 'foobar' / 'foobar_sftp_api.md'
        assert output_file.exists()
        assert output_file.read_text() == """---
title: Foobar / Foobar SFTP API
---
@pydoc foo.bar.foobar_sftp_api
"""
        # -->

        # <!-- init file
        input_file = test_input / '__init__.py'
        input_file.touch()
        _generate_dynamic_page(input_file, test_output / 'foobar', 'Foobar', 'foo.bar')
        assert len(list(test_output.rglob('**/*'))) != 0
        output_file = test_output / 'foobar' / 'index.md'
        assert output_file.exists()
        assert output_file.read_text() == """---
title: Foobar
---
@pydoc foo.bar
"""
        # -->

        # <!-- normal file, do not overwrite
        input_file = test_input / 'foobar_sftp_api.py'
        input_file.touch()
        _generate_dynamic_page(input_file, test_output / 'foobar', 'Barfoo', 'bar.foo')
        assert len(list(test_output.rglob('**/*'))) != 0
        output_file = test_output / 'foobar' / 'foobar_sftp_api.md'
        assert output_file.exists()
        assert output_file.read_text() == """---
title: Foobar / Foobar SFTP API
---
@pydoc foo.bar.foobar_sftp_api
"""
        # -->
    finally:
        rmtree(test_context)


class TestGrizzlyMarkdown:
    def test__is_anchor(self) -> None:
        assert not GrizzlyMarkdown._is_anchor('<a')
        assert GrizzlyMarkdown._is_anchor('<a>')
        assert GrizzlyMarkdown._is_anchor('<a id="#hello.world">')
        assert not GrizzlyMarkdown._is_anchor('foobar')

    def test__get_header(self) -> None:
        node = MarkdownAstNode({}, 0)
        assert GrizzlyMarkdown._get_header(node) == MarkdownHeading('', 0)

        node = MarkdownAstNode({
            'attrs': {'level': 3},
            'children': [
                {'raw': 'he'},
                {'raw': 'llo'},
                {'raw': ' world'},
                {'raw': '!'},
            ],
        }, 0)
        assert GrizzlyMarkdown._get_header(node) == MarkdownHeading('hello world!', 3)

    def test__get_tokens(self) -> None:
        tokens = GrizzlyMarkdown._get_tokens('<a href="')
        assert [token.string for token in tokens] == ['utf-8', '<', 'a', 'href', '=', '"', '', '']

    def test_get_step_expression_from_code_block(self) -> None:
        code_block = """
@given(u'hello foobar world')
def step_hello_foobar_world(context: Context) -> None:
    pass
"""
        assert GrizzlyMarkdown.get_step_expression_from_code_block(code_block) == ('given', 'hello foobar world')

        code_block = """
# @TODO: some comment
@then(u'what in the world')
def step_what_in_the_world(context: Context) -> None:
    pass
"""
        assert GrizzlyMarkdown.get_step_expression_from_code_block(code_block) == ('then', 'what in the world')

        assert GrizzlyMarkdown.get_step_expression_from_code_block('foobar') is None

    def test_ast_reformat_block_code(self) -> None:
        node = MarkdownAstNode({'type': 'paragraph', 'raw': 'foobar'}, 0)
        assert GrizzlyMarkdown.ast_reformat_block_code(node).ast == node.ast

        node = MarkdownAstNode({'type': 'block_code', 'raw': '```python\nimport sys\nsys.exit(0)```', 'style': 'indent', 'marker': '~~~'}, 0)
        assert GrizzlyMarkdown.ast_reformat_block_code(node).ast == {
            'type': 'block_code',
            'raw': '    import sys\n    sys.exit(0)',
            'style': 'indent',
            'marker': '    ```',
            'attrs': {
                'info': 'python',
            },
        }

        node = MarkdownAstNode({'type': 'block_code', 'raw': '```python\nimport sys\nsys.exit(0)\n```', 'style': 'indent', 'marker': '~~~'}, 0)
        assert GrizzlyMarkdown.ast_reformat_block_code(node).ast == {
            'type': 'block_code',
            'raw': '    import sys\n    sys.exit(0)',
            'style': 'indent',
            'marker': '    ```',
            'attrs': {
                'info': 'python',
            },
        }

    def test_ast_reformat_admonitions(self) -> None:
        node = MarkdownAstNode({'type': 'block_code', 'raw': 'foobar'}, 0)
        assert GrizzlyMarkdown.ast_reformat_admonitions(node).ast == {'type': 'block_code', 'raw': 'foobar'}

        node = MarkdownAstNode({
            'type': 'paragraph',
            'children': [
                {'raw': '!!! foobar'},
                {'type': 'softbreak'},
                {'type': 'text', 'raw': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'},
                {'type': 'softbreak'},
                {'type': 'text', 'raw': 'Donec quis mollis sapien. Fusce ac purus sit amet tortor rutrum congue.'},
                {'type': 'softbreak'},
                {'type': 'text', 'raw': 'Suspendisse potenti. Morbi non lacus sed eros ullamcorper cursus.'},
            ],
        }, 1)

        assert GrizzlyMarkdown.ast_reformat_admonitions(node).ast == {
            'type': 'paragraph',
            'children': [
                {'raw': '!!! foobar'},
                {'type': 'softbreak'},
                {'type': 'text', 'raw': '    Lorem ipsum dolor sit amet, consectetur adipiscing elit.'},
                {'type': 'softbreak'},
                {'type': 'text', 'raw': '    Donec quis mollis sapien. Fusce ac purus sit amet tortor rutrum congue.'},
                {'type': 'softbreak'},
                {'type': 'text', 'raw': '    Suspendisse potenti. Morbi non lacus sed eros ullamcorper cursus.'},
            ],
        }

    def test_ast_reformat_recursive(self) -> None:
        node = MarkdownAstNode({
            'type': 'paragraph',
            'children': [
                {'raw': '!!! foobar'},
                {'type': 'softbreak'},
                {
                    'type': 'text',
                    'raw': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
                    'children': [
                        {
                            'type': 'paragraph',
                            'children': [
                                {'raw': '!!! bar'},
                                {'type': 'softbreak'},
                                {'type': 'text', 'raw': 'Suspendisse potenti. Morbi non lacus sed eros ullamcorper cursus.'},
                            ],
                        },
                    ],
                },
            ],
        }, 0)

        assert GrizzlyMarkdown.ast_reformat_recursive(node, GrizzlyMarkdown.ast_reformat_admonitions).ast == {
            'type': 'paragraph',
            'children': [
                {'raw': '!!! foobar'},
                {'type': 'softbreak'},
                {
                    'type': 'text',
                    'raw': '    Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
                    'children': [
                        {
                            'type': 'paragraph',
                            'children': [
                                {'raw': '!!! bar'},
                                {'type': 'softbreak'},
                                {'type': 'text', 'raw': '    Suspendisse potenti. Morbi non lacus sed eros ullamcorper cursus.'},
                            ],
                        },
                    ],
                },
            ],
        }
