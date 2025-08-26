from __future__ import annotations

import inspect
import itertools
import re
import string
import sys
import tokenize
import unicodedata
from contextlib import suppress
from dataclasses import dataclass, field
from tokenize import TokenInfo
from typing import TYPE_CHECKING, Any, TypeAlias, Union, cast

from grizzly_ls.constants import MARKER_LANGUAGE

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Generator

    from lsprotocol.types import Position
    from pygls.workspace import TextDocument

    from grizzly_ls.server import GrizzlyLanguageServer


if sys.version_info >= (3, 11):
    from re._constants import ANY, BRANCH, LITERAL, MAX_REPEAT, SUBPATTERN
    from re._constants import _NamedIntConstant as SreNamedIntConstant
    from re._parser import SubPattern
    from re._parser import parse as sre_parse
else:  # pragma: no cover
    from abc import abstractmethod
    from sre_constants import (
        ANY,
        BRANCH,
        LITERAL,
        MAX_REPEAT,
        SUBPATTERN,
    )
    from sre_constants import (
        _NamedIntConstant as SreNamedIntConstant,
    )
    from sre_parse import parse as sre_parse
    from typing import Protocol

    if TYPE_CHECKING:  # pragma: no cover
        from collections.abc import Iterator

    class SubPattern(Protocol):
        """Tricking mypy into thinking SubPattern is iterable for py 3.10."""

        @abstractmethod
        def __iter__(self) -> Iterator[tuple[SreNamedIntConstant, int | tuple[int, int, list[tuple[SreNamedIntConstant, int]]]]]: ...

        @abstractmethod
        def __next__(self) -> tuple[SreNamedIntConstant, int | tuple[int, int, list[tuple[SreNamedIntConstant, int]]]]: ...


SreParseTokens: TypeAlias = Union[
    list[
        tuple[
            SreNamedIntConstant,
            int | tuple[int, int, list[tuple[SreNamedIntConstant, int]]],
        ]
    ]
    | SubPattern
]
SreParseValueBranch: TypeAlias = tuple[Any, list[SubPattern]]
SreParseValueMaxRepeat: TypeAlias = tuple[int, int, SubPattern]
SreParseValueSubpattern: TypeAlias = tuple[int, int, int, SreParseTokens]
SreParseValue: TypeAlias = Union[int, SreNamedIntConstant, SreParseValueMaxRepeat, SreParseValueBranch]


class regexp_handler:
    sre_type: SreNamedIntConstant

    def __init__(self, sre_type: SreNamedIntConstant) -> None:
        self.sre_type = sre_type

    def __call__(
        self,
        func: Callable[[RegexPermutationResolver, SreParseValue], list[str]],
    ) -> Callable[[RegexPermutationResolver, SreParseValue], list[str]]:
        func.__handler_type__ = self.sre_type  # type: ignore[attr-defined]

        return func

    @classmethod
    def make_registry(cls, instance: RegexPermutationResolver) -> dict[SreNamedIntConstant, Callable[[SreParseValue], list[str]]]:
        registry: dict[SreNamedIntConstant, Callable[[SreParseValue], list[str]]] = {}
        for name, func in inspect.getmembers(instance, predicate=inspect.ismethod):
            if name.startswith('_'):
                continue

            handler_type = getattr(func, '__handler_type__', None)

            if handler_type is None:
                continue

            registry.update({handler_type: func})

        return registry


class RegexPermutationResolver:
    """More or less a typed and stripped down version of:
    https://gist.github.com/Quacky2200/714acad06f3f80f6bdb92d7d49dea4bf.
    """

    _handlers: dict[SreNamedIntConstant, Callable[[SreParseValue], list[str]]]

    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self._handlers = regexp_handler.make_registry(self)

    @regexp_handler(ANY)
    def handle_any(self: RegexPermutationResolver, _: SreParseValue) -> list[str]:
        printables: list[str] = []
        printables[:0] = string.printable

        return printables

    @regexp_handler(BRANCH)
    def handle_branch(self: RegexPermutationResolver, token_value: SreParseValue) -> list[str]:
        token_value = cast('SreParseValueBranch', token_value)
        _, value = token_value
        options: set[str] = set()

        for tokens in value:
            option = self.permute_tokens(tokens)
            options.update(option)

        return list(options)

    @regexp_handler(LITERAL)
    def handle_literal(self: RegexPermutationResolver, value: SreParseValue) -> list[str]:
        value = cast('int', value)

        return [chr(value)]

    @regexp_handler(MAX_REPEAT)
    def handle_max_repeat(self: RegexPermutationResolver, value: SreParseValue) -> list[str]:
        minimum, maximum, subpattern = cast('SreParseValueMaxRepeat', value)

        if maximum > 5000:
            message = f'too many repetitions requested ({maximum}>5000)'
            raise ValueError(message)

        values: list[Generator[list[str], None, None]] = []

        for sub_token, sub_value in subpattern:  # type: ignore[attr-defined,unused-ignore]
            options = self.handle_token(sub_token, cast('SreParseValue', sub_value))

            for x in range(minimum, maximum + 1):
                joined = self.cartesian_join([options] * x)
                values.append(joined)

        return [''.join(it) for it in itertools.chain(*values)]

    @regexp_handler(SUBPATTERN)
    def handle_subpattern(self: RegexPermutationResolver, value: SreParseValue) -> list[str]:
        tokens = cast('SreParseValueSubpattern', value)[-1]
        return list(self.permute_tokens(tokens))

    def handle_token(self, token: SreNamedIntConstant, value: SreParseValue) -> list[str]:
        try:
            return self._handlers[token](value)
        except KeyError:
            message = f'unsupported regular expression construct {token}'
            raise ValueError(message) from None

    def permute_tokens(self, tokens: SreParseTokens) -> list[str]:
        lists: list[list[str]] = [self.handle_token(token, cast('SreParseValue', value)) for token, value in tokens]
        return [''.join(cartesian_list) for cartesian_list in self.cartesian_join(lists)]

    def cartesian_join(self, value: list[list[str]]) -> Generator[list[str], None, None]:
        def rloop(
            sequence: list[list[str]],
            combinations: list[str],
        ) -> Generator[list[str], None, None]:
            if len(sequence) > 0:
                for _combination in sequence[0]:
                    _combinations = [*combinations, _combination]
                    yield from rloop(sequence[1:], _combinations)
            else:
                yield combinations

        return rloop(value, [])

    def get_permutations(self) -> list[str]:
        tokens: SreParseTokens = [
            (
                token,
                value,
            )
            for token, value in sre_parse(self.pattern)  # type: ignore[attr-defined,unused-ignore]
        ]

        return self.permute_tokens(tokens)

    @staticmethod
    def resolve(pattern: str) -> list[str]:
        instance = RegexPermutationResolver(pattern)
        return instance.get_permutations()


@dataclass
class Coordinate:
    x: bool | None = field(default=False)
    y: bool | None = field(default=False)


@dataclass
class NormalizeHolder:
    permutations: Coordinate
    replacements: list[str]


class Normalizer:
    ls: GrizzlyLanguageServer
    custom_types: dict[str, NormalizeHolder]
    regex = re.compile(r'\{[^\}:]*\}')
    typed_regex = re.compile(r'\{[^:]*:([^\}]*)\}')

    def __init__(self, ls: GrizzlyLanguageServer, custom_types: dict[str, NormalizeHolder]) -> None:
        self.ls = ls
        self.custom_types = custom_types

    def _clean_pattern(self, pattern: str) -> tuple[str, bool]:
        matches = self.regex.finditer(pattern)
        for match in matches:
            pattern = pattern.replace(match.group(0), '')

        return pattern, matches is not None

    def _round1(self, variable_pattern: str, normalize: dict[str, NormalizeHolder]) -> list[str]:
        normalize_variations_y = {key: value for key, value in normalize.items() if value.permutations.y}
        variation_patterns: set[str] = set()
        patterns: list[str] = []

        if len(normalize_variations_y) > 0:
            variation_patterns = set()
            for variable, holder in normalize_variations_y.items():
                for replacement in holder.replacements:
                    variation_patterns.add(variable_pattern.replace(variable, replacement))

            patterns = list(variation_patterns)

        normalize_variations_x = {key: value for key, value in normalize.items() if value.permutations.x}
        if len(normalize_variations_x) > 0:
            matrix_components: list[list[str]] = [holder.replacements for holder in normalize_variations_x.values()]

            # create unique combinations of all replacements
            matrix = list(
                filter(
                    lambda p: p.count(p[0]) != len(p),
                    list(itertools.product(*matrix_components)),
                )
            )

            variation_patterns = set()
            for pattern in patterns:
                for row in matrix:
                    for variable in normalize_variations_x:
                        if variable not in pattern:
                            continue

                        for replacement in row:
                            # all variables in pattern has been normalized
                            if variable not in pattern:
                                break

                            # x replacements should only occur once in the pattern
                            if f' {replacement}' in pattern:
                                continue

                            pattern = pattern.replace(variable, replacement)  # noqa: PLW2901

                variation_patterns.add(pattern)

        return list(variation_patterns)

    def _round2(self, patterns: list[str], normalize: dict[str, NormalizeHolder]) -> list[str]:
        normalize_variations_y = {key: value for key, value in normalize.items() if value.permutations.y}
        if len(normalize_variations_y) > 0:
            repeat_round_2 = True

            # all remaining replacements needs to be resolved
            while repeat_round_2:
                repeat_round_2 = False
                variation_patterns = set()
                for pattern in patterns:
                    for variable, holder in normalize_variations_y.items():
                        if variable not in pattern:
                            continue

                        for replacement in holder.replacements:
                            normalized_pattern = pattern.replace(variable, replacement)
                            variation_patterns.add(normalized_pattern)
                            # are there any remaining replacements that should be resolved?
                            if '{' in normalized_pattern and '}' in normalized_pattern:
                                repeat_round_2 = True

                if len(variation_patterns) > 0:
                    patterns = list(variation_patterns)

        return patterns

    def __call__(self, pattern: str) -> list[str]:
        patterns: list[str] = []

        # replace all non typed variables first, will only result in 1 step
        pattern, has_matches = self._clean_pattern(pattern)

        # replace all typed variables, can result in more than 1 step
        normalize: dict[str, NormalizeHolder] = {}
        has_typed_matches = self.typed_regex.search(pattern)
        if has_typed_matches:
            typed_matches = self.typed_regex.finditer(pattern)
            for match in typed_matches:
                variable = match.group(0)
                variable_type = match.group(1)

                holder = self.custom_types.get(variable_type, None)
                if holder is not None:
                    normalize.update({variable: holder})
                elif len(variable_type) == 1:  # native types
                    normalize.update({variable: NormalizeHolder(permutations=Coordinate(), replacements=[''])})
                else:
                    with suppress(Exception):
                        start, end = match.span()

                        # if custom type is quoted (e.g. input variable), replace it with nothing
                        if pattern[start - 1] == pattern[end] == '"':
                            normalize.update({variable: NormalizeHolder(permutations=Coordinate(), replacements=[''])})

            # replace variables that does not create any variations
            normalize_no_variations = {key: value for key, value in normalize.items() if not value.permutations.x and not value.permutations.y}
            if len(normalize_no_variations) > 0:
                for variable, holder in normalize_no_variations.items():
                    for replacement in holder.replacements:
                        pattern = pattern.replace(variable, replacement)

            # round 1, to create possible prenumtations
            patterns = self._round1(pattern, normalize)

            # round 2, to normalize any additional unresolved prenumtations after normalizing x
            patterns = self._round2(patterns, normalize)

        # no variables in step, just add it
        if (not has_matches and not has_typed_matches) or len(patterns) < 1:
            patterns.append(pattern)

        return patterns


def get_step_parts(line: str) -> tuple[str | None, str | None]:
    if len(line) > 0:
        # remove multiple white spaces
        line = re.sub(r'^\s+', '', line)
        line = re.sub(r'\s{2,}', ' ', line)
        if sys.platform == 'win32':  # pragma: no cover
            line = line.replace('\r', '')

        try:
            keyword, step = line.split(' ', 1)
        except ValueError:
            keyword, step = line, None
        keyword = keyword.strip()
    else:
        keyword, step = None, None

    return keyword, step


def clean_help(text: str) -> str:
    # clean up markdown references
    matches = re.finditer(r'\[([^\]]+)\]\[([^\]]+)\]', text, re.MULTILINE)
    for match in matches:
        text = text.replace(match.group(), match.group(1))

    return '\n'.join([line.lstrip() for line in text.split('\n')])


def get_tokens(text: str) -> list[TokenInfo]:
    """Own implementation of `tokenize.tokenize`, since it behaves differently between platforms
    and/or python versions.

    Any word/section in a string that is only alphanumerical characters is classified as NAME,
    everything else is OP.
    """
    tokens: list[TokenInfo] = []

    sections = text.strip().split(' ')
    end: int = 0

    indentation_end = len(text) - len(text.strip())

    if indentation_end > 0:
        text_indentation = text[0:indentation_end]
        tokens.append(TokenInfo(tokenize.INDENT, string=text_indentation, start=(1, 0), end=(1, indentation_end), line=text))

    for section in sections:
        # find where we are in the text
        start = text.index(section, end)
        end = start + len(section)

        if section.isalpha():
            tokens.append(
                TokenInfo(
                    tokenize.NAME,
                    string=section,
                    start=(1, start),
                    end=(1, end),
                    line=text,
                )
            )
        else:
            end -= len(section)  # wind back, since we need to start in the begining of the current section

            for char in section:
                tokens.append(TokenInfo(tokenize.OP, string=char, start=(1, start), end=(1, end), line=text))

                # move forward in the section
                start = text.index(char, end)
                end = start + len(char)

    return tokens


def format_arg_line(line: str) -> str:
    try:
        argument, description = line.split(':', 1)
        arg_name, arg_type = argument.split(' ')
        arg_type = arg_type.replace('(', '').replace(')', '').strip()

        return f'* {arg_name} `{arg_type}`: {description.strip()}'
    except ValueError:
        return f'* {line}'


def find_language(source: str) -> str:
    language: str = 'en'

    for line in source.splitlines():
        stripped_line = line.strip()
        if stripped_line.startswith(MARKER_LANGUAGE):
            with suppress(ValueError):
                _, lang = stripped_line.split(': ', 1)
                lang = lang.strip()
                if len(lang) >= 2:
                    language = lang
            break

    return language


def get_current_line(text_document: TextDocument, position: Position) -> str:
    return text_document.source.split('\n')[position.line]


def normalize_text(text: str) -> str:
    text = unicodedata.normalize('NFKD', str(text)).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^\w\s-]', '', text)

    return re.sub(r'[-\s]+', '-', text).strip('-_')


def remove_if_statements(content: str) -> str:
    buffer: list[str] = []
    lines = content.splitlines()
    remove_endif = False

    for line in lines:
        stripped_line = line.strip()

        if stripped_line[:2] == '{%' and stripped_line[-2:] == '%}':
            if '{$' in stripped_line and '$}' in stripped_line and 'if' in stripped_line:
                remove_endif = True
                continue

            if remove_endif and 'endif' in stripped_line:
                remove_endif = False
                continue

        buffer.append(line)

    return '\n'.join(buffer)
