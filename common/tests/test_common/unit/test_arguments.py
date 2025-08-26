"""Unit tests of grizzly_common.arguments."""

from __future__ import annotations

import pytest
from grizzly_common.arguments import get_unsupported_arguments, parse_arguments, split_value, unquote


@pytest.mark.parametrize('separator', ['|', ', '])
def test_split_value(separator: str) -> None:
    assert split_value(f'hello world  {separator} foo bar', separator) == ('hello world', 'foo bar')
    assert split_value(
        f'hello {separator} world {separator} foo {separator} bar',
        separator,
    ) == (
        'hello',
        f'world {separator} foo {separator} bar',
    )

    assert split_value('hello|=test | yo') == ('hello|=test', 'yo')
    assert split_value('hello | test|yo') == ('hello', 'test|yo')


def test_get_unsupported_arguments() -> None:
    assert get_unsupported_arguments(
        ['hello', 'world'],
        {
            'hello': True,
            'world': True,
            'foo': True,
            'bar': False,
        },
    ) == ['foo', 'bar']

    assert (
        get_unsupported_arguments(
            ['hello', 'world', 'foo', 'bar'],
            {
                'hello': True,
                'world': True,
                'foo': True,
                'bar': False,
            },
        )
        == []
    )


def test_unquote() -> None:
    assert unquote('"hello"') == 'hello'
    assert unquote("'hello world'") == 'hello world'
    assert unquote('foo bar') == 'foo bar'


@pytest.mark.parametrize('separator', ['=', ':', '%'])
def test_parse_arguments(separator: str) -> None:  # noqa: PLR0915
    with pytest.raises(ValueError, match='incorrect format in arguments:'):
        parse_arguments('argument', separator)

    with pytest.raises(ValueError, match='incorrect format in arguments:'):
        parse_arguments(f'arg1{separator}test arg2{separator}value', separator)

    with pytest.raises(ValueError, match='incorrect format for arguments:'):
        parse_arguments(f'args1{separator}test,', separator)

    with pytest.raises(ValueError, match='incorrect format for argument:'):
        parse_arguments(f'args1{separator}test,arg2', separator)

    with pytest.raises(ValueError, match='no quotes or spaces allowed in argument names'):
        parse_arguments(f'"test"{separator}value', separator)

    with pytest.raises(ValueError, match='no quotes or spaces allowed in argument names'):
        parse_arguments(f"'test'{separator}value", separator)

    with pytest.raises(ValueError, match='no quotes or spaces allowed in argument names'):
        parse_arguments(f'test variable{separator}value', separator)

    with pytest.raises(ValueError, match='value is incorrectly quoted'):
        parse_arguments(f'arg{separator}"value\'', separator)

    with pytest.raises(ValueError, match='value is incorrectly quoted'):
        parse_arguments(f'arg{separator}\'value"', separator)

    with pytest.raises(ValueError, match='value is incorrectly quoted'):
        parse_arguments(f'arg{separator}value"', separator)

    with pytest.raises(ValueError, match='value is incorrectly quoted'):
        parse_arguments(f"arg{separator}value'", separator)

    with pytest.raises(ValueError, match='value needs to be quoted'):
        parse_arguments(f'arg{separator}test value', separator)

    arguments = parse_arguments(f'arg1{separator}testvalue1, arg2{separator}"test value 2"', separator)

    assert arguments == {
        'arg1': 'testvalue1',
        'arg2': 'test value 2',
    }

    arguments = parse_arguments(f'arg1{separator}$.expression=="{{{{ value }}}}"', separator)

    assert arguments == {
        'arg1': '$.expression=="{{ value }}"',
    }

    arguments = parse_arguments(f"arg1{separator}$.expression|=\"['a', 'b', 'c']\"", separator)

    assert arguments == {
        'arg1': "$.expression|=\"['a', 'b', 'c']\"",
    }

    arguments = parse_arguments(f'arg1{separator}$.expression|=\'["a", "b", "c"]\'', separator)

    assert arguments == {
        'arg1': '$.expression|=\'["a", "b", "c"]\'',
    }

    arguments = parse_arguments(f'arg1{separator}$.expression|="[1, 2, 3]"', separator)

    assert arguments == {
        'arg1': '$.expression|="[1, 2, 3]"',
    }

    with pytest.raises(ValueError, match='incorrect format in arguments: '):
        parse_arguments(f'url{separator}http://www.example.com?query_string{separator}value', separator)

    with pytest.raises(ValueError, match='incorrect format in arguments: '):
        parse_arguments(f'url{separator}"http://www.example.com?query_string{separator}value', separator)

    with pytest.raises(ValueError, match='incorrect format in arguments: '):
        parse_arguments(f"url{separator}'http://www.example.com?query_string{separator}value", separator)

    arguments = parse_arguments(f"url{separator}'http://www.example.com?query_string{separator}value', argument{separator}False", separator)
    assert arguments == {
        'url': f'http://www.example.com?query_string{separator}value',
        'argument': 'False',
    }

    arguments = parse_arguments(f'value1{separator}"hello, world!, asdf", value2{separator}"foo, bar", value3{separator}"true, false, asdf"', separator)
    assert arguments == {
        'value1': 'hello, world!, asdf',
        'value2': 'foo, bar',
        'value3': 'true, false, asdf',
    }

    arguments = parse_arguments(
        (f'queue{separator}INCOMING.MESSAGES, expression{separator}\'//tag1/tag2/tag3[starts-with(text(), "Prefix{{{{ tag3_value }}}}") and //tag1/tag2/tag4[text() < 13]]\''),
        separator,
    )
    assert arguments == {
        'queue': 'INCOMING.MESSAGES',
        'expression': '//tag1/tag2/tag3[starts-with(text(), "Prefix{{ tag3_value }}") and //tag1/tag2/tag4[text() < 13]]',
    }

    arguments = parse_arguments(
        (
            f'queue{separator}INCOMING.MESSAGES, expression{separator}\'//tag1/tag2/tag3[starts-with(text(), "Prefix{{{{ tag3_value }}}}") '
            'and //tag1/tag2/tag4[starts-with(text(), "{{ tag4_value }}"]]\''
        ),
        separator,
    )
    assert arguments == {
        'queue': 'INCOMING.MESSAGES',
        'expression': '//tag1/tag2/tag3[starts-with(text(), "Prefix{{ tag3_value }}") and //tag1/tag2/tag4[starts-with(text(), "{{ tag4_value }}"]]',
    }

    arguments = parse_arguments(f'value1{separator}:, value2{separator}=, value3{separator}%', separator)
    assert arguments == {
        'value1': ':',
        'value2': '=',
        'value3': '%',
    }

    arguments = parse_arguments(f"expression{separator}'$.`this`[?hello='world' & world=2]', foo{separator}bar", separator, unquote=False)

    assert arguments == {
        'expression': "'$.`this`[?hello='world' & world=2]'",
        'foo': 'bar',
    }
