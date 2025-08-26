from __future__ import annotations

import string
import sys
from typing import cast

if sys.version_info >= (3, 11):
    from re._constants import ANY, BRANCH, IN, LITERAL, MAX_REPEAT, SUBPATTERN
    from re._constants import _NamedIntConstant as SreNamedIntConstant
else:
    from sre_constants import (
        ANY,
        BRANCH,
        IN,
        LITERAL,
        MAX_REPEAT,
        SUBPATTERN,
    )
    from sre_constants import (
        _NamedIntConstant as SreNamedIntConstant,
    )

import pytest
from grizzly_ls.text import (
    RegexPermutationResolver,
    SreParseValue,
    SreParseValueMaxRepeat,
    find_language,
    format_arg_line,
    get_current_line,
    get_step_parts,
)
from lsprotocol.types import Position
from pygls.workspace import TextDocument


class TestRegexPermutationResolver:
    def test__init__(self) -> None:
        resolver = RegexPermutationResolver('(hello|world)')

        assert sorted(resolver._handlers.keys()) == sorted(
            [
                ANY,
                BRANCH,
                LITERAL,
                MAX_REPEAT,
                SUBPATTERN,
            ]
        )
        assert resolver.pattern == '(hello|world)'

    def test_handle_any(self) -> None:
        resolver = RegexPermutationResolver('(hello|world)')

        assert ''.join(resolver.handle_any(1)) == string.printable
        assert ''.join(resolver.handle_any(1343)) == string.printable

    def test_handle_branch(self) -> None:
        resolver = RegexPermutationResolver('(hello|world)')

        assert sorted(
            resolver.handle_branch(
                cast(
                    'SreParseValue',
                    (
                        None,
                        [
                            [
                                (
                                    LITERAL,
                                    104,
                                ),
                                (
                                    LITERAL,
                                    101,
                                ),
                                (
                                    LITERAL,
                                    108,
                                ),
                                (
                                    LITERAL,
                                    108,
                                ),
                                (
                                    LITERAL,
                                    111,
                                ),
                            ],
                            [
                                (
                                    LITERAL,
                                    119,
                                ),
                                (
                                    LITERAL,
                                    111,
                                ),
                                (
                                    LITERAL,
                                    114,
                                ),
                                (
                                    LITERAL,
                                    108,
                                ),
                                (
                                    LITERAL,
                                    100,
                                ),
                            ],
                        ],
                    ),
                )
            )
        ) == sorted(['hello', 'world'])

    def test_handle_literal(self) -> None:
        resolver = RegexPermutationResolver('(hello|world)')

        assert resolver.handle_literal(ord('A')) == ['A']
        assert resolver.handle_literal(ord('Å')) == ['Å']

    def test_handle_max_repeat(self) -> None:
        resolver = RegexPermutationResolver('(world[s]?)')

        with pytest.raises(ValueError, match=r'too many repetitions requested \(5001>5000\)'):
            resolver.handle_max_repeat(
                cast(
                    'SreParseValueMaxRepeat',
                    (
                        0,
                        5001,
                        [
                            (
                                LITERAL,
                                104,
                            ),
                            (
                                LITERAL,
                                105,
                            ),
                        ],
                    ),
                )
            )

        assert resolver.handle_max_repeat(
            cast(
                'SreParseValueMaxRepeat',
                (
                    1,
                    2,
                    [
                        (
                            LITERAL,
                            104,
                        ),
                        (
                            LITERAL,
                            105,
                        ),
                    ],
                ),
            )
        ) == ['h', 'hh', 'i', 'ii']

    def test_handle_subpattern(self) -> None:
        resolver = RegexPermutationResolver('(world[s]?)')

        assert resolver.handle_subpattern(
            cast(
                'SreParseValue',
                [
                    [
                        (
                            LITERAL,
                            119,
                        ),
                        (
                            LITERAL,
                            111,
                        ),
                    ],
                    [
                        (
                            LITERAL,
                            104,
                        ),
                        (
                            LITERAL,
                            105,
                        ),
                    ],
                ],
            )
        ) == ['hi']

    def test_handle_token(self) -> None:
        resolver = RegexPermutationResolver('(world[s]?)')

        test = SreNamedIntConstant(name='test', value=1337)

        with pytest.raises(ValueError, match='unsupported regular expression construct test'):
            resolver.handle_token(test, 104)

        with pytest.raises(ValueError, match='unsupported regular expression construct IN'):
            resolver.handle_token(IN, 104)

    def test_cartesian_join(self) -> None:
        resolver = RegexPermutationResolver('(world[s]?)')

        result = resolver.cartesian_join([['hello', 'world'], ['foo']])
        assert list(result) == [['hello', 'foo'], ['world', 'foo']]

        result = resolver.cartesian_join([['hello', 'world'], ['foo', 'bar']])
        assert list(result) == [
            ['hello', 'foo'],
            ['hello', 'bar'],
            ['world', 'foo'],
            ['world', 'bar'],
        ]

    def test_resolve(self) -> None:
        assert RegexPermutationResolver.resolve('(world[s]?)') == ['world', 'worlds']
        assert sorted(RegexPermutationResolver.resolve('(foo|bar)?')) == sorted(['', 'foo', 'bar'])


def test_get_step_parts() -> None:
    assert get_step_parts('') == (
        None,
        None,
    )
    assert get_step_parts('   Giv') == (
        'Giv',
        None,
    )
    assert get_step_parts(' Given hello world') == (
        'Given',
        'hello world',
    )
    assert get_step_parts('  And are you "ok"?') == (
        'And',
        'are you "ok"?',
    )
    assert get_step_parts('     Then   make sure   that "value"  is "None"') == (
        'Then',
        'make sure that "value" is "None"',
    )


def test__format_arg_line() -> None:
    assert format_arg_line('hello_world (bool): foo bar description of argument') == '* hello_world `bool`: foo bar description of argument'
    assert format_arg_line('hello: strange stuff (bool)') == '* hello: strange stuff (bool)'


def test_get_current_line() -> None:
    text_document = TextDocument(
        'file://test.feature',
        """Feature:
    Scenario: test
        Then hello world!
        But foo bar
""",
    )

    assert get_current_line(text_document, Position(line=0, character=0)).strip() == 'Feature:'
    assert get_current_line(text_document, Position(line=1, character=543)).strip() == 'Scenario: test'
    assert get_current_line(text_document, Position(line=2, character=435)).strip() == 'Then hello world!'
    assert get_current_line(text_document, Position(line=3, character=534)).strip() == 'But foo bar'

    with pytest.raises(IndexError) as ie:
        assert get_current_line(text_document, Position(line=10, character=10)).strip() == 'Then hello world!'
    assert str(ie.value) == 'list index out of range'


def test_find_language() -> None:
    assert find_language('') == 'en'
    assert find_language('# language: ') == 'en'
    assert find_language('# language: asdf') == 'asdf'
    assert find_language('# language: s') == 'en'
    assert find_language('# language: en-US') == 'en-US'
